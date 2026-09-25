"""发件箱投递者：把已完成任务的终态结果可靠投递给 sink。

与 worker 相同的抢占/租约/退避/死信模型，可部署多个实例并发。
sink 抛普通异常 => 瞬时失败按退避重试；抛 PermanentDeliveryError
=> 不重试直接死信。无注册 sink 时使用仅记日志的本地空出口。
"""
from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

from .config import Settings
from .handlers import PermanentDeliveryError, SinkRegistry
from .models import DeadLetterReason, OutboxMessage, utcnow
from .store import TaskStore

logger = logging.getLogger("task.deliverer")


class Deliverer:
    def __init__(
        self,
        deliverer_id: str,
        store: TaskStore,
        sinks: SinkRegistry,
        settings: Settings,
    ) -> None:
        self.deliverer_id = deliverer_id
        self.store = store
        self.sinks = sinks
        self.settings = settings

    async def run(self, stop: asyncio.Event) -> None:
        logger.info("deliverer %s 启动", self.deliverer_id)
        try:
            while not stop.is_set():
                handled = await self._deliver_one()
                if not handled:
                    try:
                        await asyncio.wait_for(
                            stop.wait(), timeout=self.settings.poll_interval
                        )
                    except TimeoutError:
                        pass
        finally:
            logger.info("deliverer %s 停止", self.deliverer_id)

    async def _deliver_one(self) -> bool:
        msg = self.store.outbox_claim_next(
            self.deliverer_id,
            now=utcnow(),
            lease_seconds=self.settings.lease_seconds,
        )
        if msg is None:
            return False

        logger.info(
            "[%s] 投递消息 id=%s task=%s attempt=%d/%d",
            self.deliverer_id, msg.id, msg.task_id, msg.attempts, msg.max_attempts,
        )

        sink = self.sinks.get(msg.task_type)
        try:
            if sink is None:
                # 本地验证无外部系统：空出口视为成功，但必须留痕
                logger.info(
                    "[%s] 任务 %s 无注册 sink，按本地空出口确认投递 result=%r",
                    self.deliverer_id, msg.task_id, msg.result,
                )
            else:
                await sink(
                    msg.task_id, msg.task_type, msg.payload,
                    msg.result, msg.idempotency_key,
                )
        except PermanentDeliveryError as exc:
            logger.error(
                "[%s] 消息 %s 永久投递失败: %s", self.deliverer_id, msg.id, exc.message
            )
            self.store.outbox_mark_dead(
                msg.id, self.deliverer_id,
                now=utcnow(),
                detail=exc.message,
            )
            return True
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "[%s] 消息 %s 投递失败(attempt=%d): %s",
                self.deliverer_id, msg.id, msg.attempts, exc,
            )
            self._retry_or_dead(msg, f"{type(exc).__name__}: {exc}")
            return True

        self.store.outbox_mark_delivered(
            msg.id, self.deliverer_id,
            now=utcnow(),
        )
        logger.info("[%s] 消息 %s 投递成功", self.deliverer_id, msg.id)
        return True

    def _retry_or_dead(self, msg: OutboxMessage, error: str) -> None:
        if msg.attempts >= msg.max_attempts:
            self.store.outbox_mark_dead(
                msg.id, self.deliverer_id,
                now=utcnow(),
                detail=error,
            )
            logger.error(
                "[%s] 消息 %s 投递重试耗尽，任务 %s 进入死信(%s)",
                self.deliverer_id, msg.id, msg.task_id,
                DeadLetterReason.DELIVERY_RETRIES_EXHAUSTED.value,
            )
            return
        delay = self.settings.delivery_retry.delay_for(msg.attempts)
        backoff_until = utcnow() + timedelta(seconds=delay)
        self.store.outbox_mark_retry(
            msg.id, self.deliverer_id,
            now=utcnow(),
            backoff_until=backoff_until, error=error,
        )
        logger.info(
            "[%s] 消息 %s 退避 %.3fs 重试 (attempt=%d/%d)",
            self.deliverer_id, msg.id, delay, msg.attempts, msg.max_attempts,
        )
