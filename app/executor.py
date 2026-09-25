"""执行器：多 worker 并发抢占、有上限退避重试、执行超时、失败隔离。"""
from __future__ import annotations

import concurrent.futures
import logging
import threading
import time
from typing import Callable

from .models import BusinessFailure, Task
from .repository import TaskRepository

logger = logging.getLogger("task.executor")

Handler = Callable[[dict], None]


class BackoffPolicy:
    """指数退避，带上限：base * 2^(attempt-1)，封顶 max_delay。"""

    def __init__(self, base_seconds: float = 0.05, max_seconds: float = 5.0):
        self.base = base_seconds
        self.max = max_seconds

    def delay(self, attempt: int) -> float:
        return min(self.base * (2 ** max(attempt - 1, 0)), self.max)


class Executor:
    """从仓储抢占任务并执行。进程崩溃后，租约过期的任务会被其他/重启后的执行者回收。"""

    def __init__(
        self,
        repo: TaskRepository,
        handlers: dict[str, Handler],
        *,
        worker_id: str,
        lease_seconds: float = 5.0,
        poll_interval: float = 0.05,
        execution_timeout: float = 10.0,
        backoff: BackoffPolicy | None = None,
    ):
        self._repo = repo
        self._handlers = handlers
        self.worker_id = worker_id
        self._lease = lease_seconds
        self._poll = poll_interval
        self._timeout = execution_timeout
        self._backoff = backoff or BackoffPolicy()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name=self.worker_id, daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout)

    def _loop(self) -> None:
        while not self._stop.is_set():
            did_work = self.run_once()
            if not did_work:
                self._stop.wait(self._poll)

    def run_once(self) -> bool:
        """抢占并执行一个任务；返回是否抢到。崩溃安全：先抢占（写库）再执行。"""
        tasks = self._repo.claim(self.worker_id, self._lease, limit=1)
        if not tasks:
            return False
        self._execute(tasks[0])
        return True

    def _execute(self, task: Task) -> None:
        import json
        handler = self._handlers.get(task.task_type)
        if handler is None:
            self._repo.fail(task.id, self.worker_id,
                            f"no handler registered for type {task.task_type}",
                            retryable=False)
            return
        payload = json.loads(task.payload)
        try:
            # 不用 with：超时场景下 shutdown(wait=True) 会被挂起的 handler 拖死
            pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
            try:
                future = pool.submit(handler, payload)
                future.result(timeout=self._timeout)
            finally:
                pool.shutdown(wait=False)
        except concurrent.futures.TimeoutError:
            # 超时：进死信，不静默吞掉
            self._repo.timeout_to_dead(task.id, self.worker_id, self._timeout)
            logger.warning("task %s timeout -> DEAD", task.id)
        except BusinessFailure as exc:
            # 业务明确失败：不重试，直接死信
            self._repo.fail(task.id, self.worker_id, str(exc), retryable=False)
            logger.warning("task %s business failure -> DEAD: %s", task.id, exc)
        except Exception as exc:  # noqa: BLE001 - 可重试的技术失败
            delay = self._backoff.delay(task.attempt)
            self._repo.fail(task.id, self.worker_id, repr(exc),
                            retryable=True, backoff_seconds=delay)
            logger.info("task %s retryable failure, backoff %.3fs: %r", task.id, delay, exc)
        else:
            self._repo.succeed(task.id, self.worker_id)
            logger.info("task %s succeeded (attempt %d)", task.id, task.attempt)
