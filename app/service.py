"""应用服务门面：组合 Store / Worker / Deliverer / 注册表，供 API 与测试使用。"""
from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

from .config import Settings, default_settings
from .deliverer import Deliverer
from .handlers import DeliverySink, HandlerRegistry, SinkRegistry, TaskHandler
from .models import ErrorCode, Task, TaskError, TaskEvent, TaskStatus
from .store import TaskStore, new_task_id
from .worker import Worker

logger = logging.getLogger("task.service")


class TaskService:
    """门面：持有主仓库、注册表与后台循环。

    每个后台执行者使用独立的 TaskStore（独立 SQLite 连接），
    以模拟/支持多进程多执行者通过同一数据库文件互斥协作；
    内存库（:memory:）场景下执行者共享主连接，仅供单进程测试。
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or default_settings()
        self.store = TaskStore(self.settings.db_path)
        self.handlers = HandlerRegistry()
        self.sinks = SinkRegistry()
        self._stop: asyncio.Event | None = None
        self._bg: list[asyncio.Task] = []

    # ---------- 生命周期 ----------
    def register_handler(self, task_type: str, handler: TaskHandler) -> None:
        self.handlers.register(task_type, handler)

    def register_sink(self, task_type: str, sink: DeliverySink) -> None:
        self.sinks.register(task_type, sink)

    async def start_background(self, worker_count: int | None = None) -> None:
        """启动 worker/deliverer 后台任务。"""
        if self._stop is not None:
            return
        count = self.settings.worker_count if worker_count is None else worker_count
        self._stop = asyncio.Event()
        for i in range(count):
            store = self._spawn_store()
            worker = Worker(f"worker-{i+1}", store, self.handlers, self.settings)
            self._bg.append(asyncio.create_task(worker.run(self._stop)))
        dstore = self._spawn_store()
        deliverer = Deliverer("deliverer-1", dstore, self.sinks, self.settings)
        self._bg.append(asyncio.create_task(deliverer.run(self._stop)))
        logger.info("后台执行者已启动: %d worker + 1 deliverer", count)

    async def stop_background(self) -> None:
        if self._stop is None:
            return
        self._stop.set()
        await asyncio.gather(*self._bg, return_exceptions=True)
        self._bg.clear()
        self._stop = None

    def _spawn_store(self) -> TaskStore:
        if self.settings.db_path == ":memory:":
            return self.store
        return TaskStore(self.settings.db_path)

    # ---------- 用例 ----------
    async def submit(
        self,
        task_type: str,
        payload: dict[str, Any],
        idempotency_key: str,
        *,
        max_attempts: int | None = None,
        actor: str = "api",
    ) -> tuple[Task, bool]:
        now = datetime.now(UTC)
        task = Task(
            id=new_task_id(),
            idempotency_key=idempotency_key,
            task_type=task_type,
            payload=payload,
            status=TaskStatus.PENDING,
            attempt=0,
            max_attempts=max_attempts or self.settings.task_retry.max_attempts,
            lease_owner=None,
            lease_expires_at=None,
            backoff_until=None,
            cancel_requested=False,
            replay_of=None,
            replay_seq=0,
            dead_letter_reason=None,
            dead_letter_detail=None,
            result=None,
            created_at=now,
            updated_at=now,
        )
        return self.store.create_task(task, actor=actor)

    async def cancel(self, task_id: str, *, actor: str = "api") -> Task:
        return self.store.request_cancel(
            task_id, now=datetime.now(UTC), actor=actor
        )

    async def replay(self, task_id: str, *, actor: str = "api") -> Task:
        return self.store.replay_dead_letter(
            task_id, now=datetime.now(UTC), actor=actor
        )

    async def get(self, task_id: str) -> Task:
        task = self.store.get_task(task_id)
        if task is None:
            raise TaskError(ErrorCode.TASK_NOT_FOUND, f"任务不存在: {task_id}")
        return task

    async def get_by_key(self, idempotency_key: str) -> Task:
        task = self.store.get_task_by_key(idempotency_key)
        if task is None:
            raise TaskError(ErrorCode.TASK_NOT_FOUND, f"幂等键不存在: {idempotency_key}")
        return task

    async def events(self, task_id: str) -> list[TaskEvent]:
        await self.get(task_id)
        return self.store.list_events(task_id)
