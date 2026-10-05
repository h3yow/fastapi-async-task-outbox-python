"""执行器：多 worker 并发抢占、有上限退避重试、执行超时、失败隔离。

长任务支持：
- 执行期间按租约周期自动续租，长任务不会因耗时超过默认租约被回收；
- handler 可通过 TaskContext 上报进度、感知取消请求。
"""
from __future__ import annotations

import concurrent.futures
import inspect
import logging
import threading
import time
from typing import Callable

from .models import BusinessFailure, Task, TaskCancelled
from .repository import TaskRepository

logger = logging.getLogger("task.executor")

Handler = Callable[[dict], None]


class TaskContext:
    """传给长任务 handler 的运行上下文。

    - report_progress(percent, step)：上报进度，可随时被查询；
    - is_cancel_requested() / throw_if_cancelled()：感知取消请求。
      取消标记由执行器心跳线程从库中轮询后写入本地事件，handler 侧零 IO。
    """

    def __init__(self, repo: TaskRepository, task_id: str, worker_id: str,
                 cancel_event: threading.Event):
        self._repo = repo
        self._task_id = task_id
        self._worker_id = worker_id
        self._cancel_event = cancel_event

    def report_progress(self, percent: float | None = None,
                        step: str | None = None) -> None:
        if percent is not None:
            percent = max(0.0, min(100.0, float(percent)))
        self._repo.report_progress(self._task_id, self._worker_id, percent, step)

    def is_cancel_requested(self) -> bool:
        return self._cancel_event.is_set()

    def throw_if_cancelled(self) -> None:
        if self._cancel_event.is_set():
            raise TaskCancelled(f"task {self._task_id} cancel requested")


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
        # 兼容旧的一参 handler(payload)；两参 handler(payload, ctx) 获得长任务能力
        self._wants_ctx = {t: _handler_wants_ctx(h) for t, h in handlers.items()}

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

        cancel_event = threading.Event()
        ctx = TaskContext(self._repo, task.id, self.worker_id, cancel_event)
        stop_hb = threading.Event()
        lease_lost = threading.Event()
        hb = threading.Thread(
            target=self._heartbeat,
            args=(task.id, cancel_event, stop_hb, lease_lost),
            name=f"{self.worker_id}-hb", daemon=True)
        hb.start()
        try:
            # 不用 with：超时场景下 shutdown(wait=True) 会被挂起的 handler 拖死
            pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
            try:
                if self._wants_ctx.get(task.task_type, False):
                    future = pool.submit(handler, payload, ctx)
                else:
                    future = pool.submit(handler, payload)
                future.result(timeout=self._timeout)
            finally:
                pool.shutdown(wait=False)
        except concurrent.futures.TimeoutError:
            # 超时：进死信，不静默吞掉
            self._safe_finish(task, self._repo.timeout_to_dead,
                              task.id, self.worker_id, self._timeout)
            logger.warning("task %s timeout -> DEAD", task.id)
        except TaskCancelled as exc:
            # 任务感知到取消请求并停下：收敛到 CANCELLED
            self._safe_finish(task, self._repo.complete_cancel, task.id, self.worker_id)
            logger.info("task %s cancelled while running: %s", task.id, exc)
        except BusinessFailure as exc:
            # 业务明确失败：不重试，直接死信
            self._safe_finish(task, self._repo.fail, task.id, self.worker_id,
                              str(exc), retryable=False)
            logger.warning("task %s business failure -> DEAD: %s", task.id, exc)
        except Exception as exc:  # noqa: BLE001 - 可重试的技术失败
            delay = self._backoff.delay(task.attempt)
            self._safe_finish(task, self._repo.fail, task.id, self.worker_id,
                              repr(exc), retryable=True, backoff_seconds=delay)
            logger.info("task %s retryable failure, backoff %.3fs: %r", task.id, delay, exc)
        else:
            if self._repo.is_cancel_requested(task.id):
                # handler 感知取消后提前正常返回：同样收敛到 CANCELLED，
                # 保证“请求取消 -> 明确的取消结果”不依赖 handler 必须抛 TaskCancelled
                self._safe_finish(task, self._repo.complete_cancel,
                                  task.id, self.worker_id)
                logger.info("task %s cancelled while running (handler returned early)",
                            task.id)
            else:
                self._safe_finish(task, self._repo.succeed, task.id, self.worker_id)
                logger.info("task %s succeeded (attempt %d)", task.id, task.attempt)
        finally:
            stop_hb.set()
            hb.join(timeout=1.0)

    def _heartbeat(self, task_id: str, cancel_event: threading.Event,
                   stop: threading.Event, lease_lost: threading.Event) -> None:
        """执行期间按租约周期续租，并轮询取消标记。

        只要 handler 还在正常执行，租约就会被不断延长，
        长任务不会因耗时超过默认租约而被回收或交给别人。
        """
        interval = max(self._lease / 3.0, 0.02)
        while not stop.wait(interval):
            if not self._repo.renew_lease(task_id, self.worker_id, self._lease):
                lease_lost.set()
                logger.warning("task %s lease lost during execution (worker %s)",
                               task_id, self.worker_id)
                return
            if self._repo.is_cancel_requested(task_id):
                cancel_event.set()

    def _safe_finish(self, task: Task, fn, *args, **kwargs) -> None:
        """写执行结果。若租约已丢失（任务被回收并由他人完成），
        仓储会抛 InvalidTransitionError —— 丢弃本地结果，不覆盖既成事实。"""
        from .models import InvalidTransitionError
        try:
            fn(*args, **kwargs)
        except InvalidTransitionError:
            logger.warning("task %s result discarded: lease lost or already finished",
                           task.id)


def _handler_wants_ctx(handler: Handler) -> bool:
    """判断 handler 是否声明了第二个位置参数（TaskContext）。"""
    try:
        sig = inspect.signature(handler)
    except (TypeError, ValueError):
        return False
    params = list(sig.parameters.values())
    if any(p.kind is inspect.Parameter.VAR_POSITIONAL for p in params):
        return True
    positional = [p for p in params
                  if p.kind in (inspect.Parameter.POSITIONAL_ONLY,
                                inspect.Parameter.POSITIONAL_OR_KEYWORD)]
    return len(positional) >= 2
