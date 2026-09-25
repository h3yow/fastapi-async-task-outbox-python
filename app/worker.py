"""任务执行者：抢占租约 -> 限时执行 -> 按结果落库。

失败按有上限退避重试；处理器超时按重试处理，重试用尽进死信；
处理器抛 PermanentFailure 表示业务明确失败，立即进死信。
"""
from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

from .config import Settings
from .handlers import HandlerRegistry
from .models import (
    DeadLetterReason,
    ErrorCode,
    PermanentFailure,
    Task,
    TaskError,
    TaskStatus,
    utcnow,
)
from .store import TaskStore

logger = logging.getLogger("task.worker")


class _CancelledByRequest(Exception):
    """任务在运行中收到协作取消信号（区别于关闭流程的 CancelledError）。"""


class Worker:
    """单个执行者。多个 Worker 共享同一个 DB 文件即可安全并发。"""

    def __init__(
        self,
        worker_id: str,
        store: TaskStore,
        handlers: HandlerRegistry,
        settings: Settings,
    ) -> None:
        self.worker_id = worker_id
        self.store = store
        self.handlers = handlers
        self.settings = settings

    async def run(self, stop: asyncio.Event) -> None:
        """循环领取并执行任务，直到 stop 被置位。"""
        logger.info("worker %s 启动", self.worker_id)
        try:
            while not stop.is_set():
                handled = await self._execute_one()
                if not handled:
                    try:
                        await asyncio.wait_for(
                            stop.wait(), timeout=self.settings.poll_interval
                        )
                    except TimeoutError:
                        pass
        finally:
            logger.info("worker %s 停止", self.worker_id)

    async def _execute_one(self) -> bool:
        """尝试处理一个任务；返回是否实际领取到任务。"""
        now = utcnow()
        task = self.store.claim_next(
            self.worker_id, now=now, lease_seconds=self.settings.lease_seconds
        )
        if task is None:
            return False

        logger.info(
            "[%s] 领取任务 task=%s type=%s attempt=%d/%d status:%s->%s",
            self.worker_id, task.id, task.task_type, task.attempt,
            task.max_attempts, TaskStatus.PENDING.value, TaskStatus.RUNNING.value,
        )

        if task.cancel_requested:
            self.store.mark_cancelled(
                task.id, self.worker_id, now=utcnow(),
                detail={"reason": "执行前已收到取消请求"},
            )
            logger.info("[%s] 任务 %s 在执行前被取消", self.worker_id, task.id)
            return True

        handler = self.handlers.get(task.task_type)
        if handler is None:
            self._dead(
                task,
                DeadLetterReason.BUSINESS_REJECTED,
                {"error": f"未注册的 task_type: {task.task_type}"},
            )
            return True

        await self._run_handler(task, handler)
        return True

    async def _run_handler(self, task: Task, handler) -> None:
        """带超时、心跳与协作取消地执行处理器并按结果落库。"""
        handler_task = asyncio.create_task(handler(task.payload))
        hb_task = asyncio.create_task(self._heartbeat(task.id))

        result: object = None
        try:
            result = await asyncio.wait_for(
                self._watch_cancel(task.id, handler_task),
                timeout=self.settings.task_timeout,
            )
        except _CancelledByRequest:
            handler_task.cancel()
            hb_task.cancel()
            await self._settle(handler_task)
            return
        except TimeoutError:
            handler_task.cancel()
            hb_task.cancel()
            await self._settle(handler_task)
            err = f"执行超时（>{self.settings.task_timeout}s）"
            # 超时按瞬时失败重试；仅在最后一次尝试时以 TIMEOUT 原因进入死信
            last_attempt = task.attempt >= task.max_attempts
            self._on_failure(
                task, err, DeadLetterReason.TIMEOUT if last_attempt else None
            )
            return
        except PermanentFailure as exc:
            handler_task.cancel()
            hb_task.cancel()
            await self._settle(handler_task)
            logger.warning(
                "[%s] 任务 %s 业务明确失败: %s", self.worker_id, task.id, exc.message
            )
            self._dead(task, DeadLetterReason.BUSINESS_REJECTED, {"error": exc.message})
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - 任何失败都要分类落库
            hb_task.cancel()
            await self._settle(handler_task)
            logger.warning(
                "[%s] 任务 %s 执行失败(attempt=%d): %s",
                self.worker_id, task.id, task.attempt, exc,
            )
            self._on_failure(task, f"{type(exc).__name__}: {exc}", None)
            return
        finally:
            hb_task.cancel()

        await self._settle(hb_task)

        # 处理器返回后再次检查协作取消标志（取消优先于成功）
        fresh = self.store.get_task(task.id)
        if fresh is not None and fresh.cancel_requested and fresh.status is TaskStatus.RUNNING:
            self.store.mark_cancelled(
                task.id, self.worker_id, now=utcnow(),
                detail={"reason": "执行完成前收到取消请求，结果弃用"},
            )
            logger.info("[%s] 任务 %s 协作取消生效，结果不落库", self.worker_id, task.id)
            return

        self.store.mark_succeeded(
            task.id, self.worker_id, now=utcnow(), result=result,
            delivery_max_attempts=self.settings.delivery_retry.max_attempts,
        )
        logger.info(
            "[%s] 任务 %s 成功 attempt=%d result=%r",
            self.worker_id, task.id, task.attempt, result,
        )

    async def _watch_cancel(self, task_id: str, handler_task: asyncio.Task):
        """轮询 DB 中的 cancel_requested；一旦置位则取消处理器协程。"""
        while True:
            fresh = self.store.get_task(task_id)
            if fresh is not None and fresh.cancel_requested:
                handler_task.cancel()
                await self._settle(handler_task)
                self.store.mark_cancelled(
                    task_id, self.worker_id, now=utcnow(),
                    detail={"reason": "执行中收到协作取消信号"},
                )
                logger.info("[%s] 任务 %s 运行中被取消", self.worker_id, task_id)
                raise _CancelledByRequest
            if handler_task.done():
                return handler_task.result()
            await asyncio.sleep(min(0.02, self.settings.poll_interval))

    async def _heartbeat(self, task_id: str) -> None:
        interval = max(0.01, self.settings.lease_seconds / 3)
        try:
            while True:
                await asyncio.sleep(interval)
                expires = utcnow() + timedelta(seconds=self.settings.lease_seconds)
                self.store.heartbeat(task_id, self.worker_id, expires_at=expires)
        except asyncio.CancelledError:
            pass

    def _on_failure(
        self, task: Task, error: str, force_reason: DeadLetterReason | None
    ) -> None:
        """失败分流：用尽重试或强制原因 -> 死信；否则退避重试。"""
        fresh = self.store.get_task(task.id)
        if fresh is None or fresh.status is not TaskStatus.RUNNING:
            # 租约已被崩溃恢复接管，本执行者放弃写状态，避免覆盖
            logger.info(
                "[%s] 任务 %s 租约已易主，放弃落库", self.worker_id, task.id
            )
            return
        if task.attempt >= task.max_attempts or force_reason is not None:
            reason = force_reason or DeadLetterReason.RETRIES_EXHAUSTED
            self._dead(task, reason, {"error": error})
            return
        delay = self.settings.task_retry.delay_for(task.attempt)
        backoff_until = utcnow() + timedelta(seconds=delay)
        self.store.mark_retry(
            task.id, self.worker_id, now=utcnow(),
            backoff_until=backoff_until, error=error,
        )
        logger.info(
            "[%s] 任务 %s 退避 %.3fs 后重试 (attempt=%d/%d)",
            self.worker_id, task.id, delay, task.attempt, task.max_attempts,
        )

    def _dead(self, task: Task, reason: DeadLetterReason, detail: dict) -> None:
        try:
            self.store.mark_dead_letter(
                task.id, self.worker_id, now=utcnow(),
                reason=reason.value, detail=detail,
            )
            logger.error(
                "[%s] 任务 %s 进入死信 reason=%s detail=%s",
                self.worker_id, task.id, reason.value, detail,
            )
        except TaskError as exc:
            if exc.code is not ErrorCode.TASK_TERMINAL:
                raise

    @staticmethod
    async def _settle(_task: asyncio.Task) -> None:
        # 被取消/已失败的辅助协程（心跳、业务协程）结果不再关心，
        # 其失败分类已由调用方完成；这里仅回收协程避免未取异常告警。
        try:
            await _task
        except BaseException:  # noqa: BLE001,S110 - 见上
            pass
