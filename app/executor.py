"""执行器：多 worker 并发抢占、有上限退避重试、执行超时、失败隔离。

长任务支持：
- 心跳续租：handler 执行期间按 heartbeat_interval 续租，任务再久也不会
  因超过默认租约时长被回收或交给第二个执行者；
- 进度上报与取消感知：handler 若声明第二个参数，会收到 TaskContext，
  可上报进度、查询/响应取消请求。
"""
from __future__ import annotations

import concurrent.futures
import inspect
import json
import logging
import threading
import time
from typing import Callable

from .models import (
    BusinessFailure,
    DomainError,
    InvalidTransitionError,
    Task,
    TaskCancelled,
    TaskStatus,
)
from .repository import TaskRepository

logger = logging.getLogger("task.executor")

Handler = Callable[..., None]


class TaskContext:
    """交给长任务 handler 的执行上下文：进度上报 + 取消感知。

    所有操作都携带当前执行纪元，迟到的旧纪元写入会被仓储拒绝。
    """

    def __init__(self, repo: TaskRepository, task_id: str, worker_id: str,
                 epoch: int):
        self._repo = repo
        self.task_id = task_id
        self._worker_id = worker_id
        self._epoch = epoch

    def report_progress(self, percent: float | None = None,
                        step: str | None = None,
                        message: str | None = None) -> None:
        try:
            self._repo.report_progress(self.task_id, self._worker_id, self._epoch,
                                       percent=percent, step=step, message=message)
        except InvalidTransitionError as exc:
            # 任务已终态/已易主（如取消已收敛）：转成停止信号让 handler 尽快退出
            raise TaskCancelled(
                f"task {self.task_id} no longer owned by this worker") from exc

    def is_cancel_requested(self) -> bool:
        try:
            return self._repo.get(self.task_id).cancel_requested
        except DomainError:
            return True  # 任务已不可查/已易主：按应当停止处理

    def throw_if_cancelled(self) -> None:
        if self.is_cancel_requested():
            raise TaskCancelled(f"task {self.task_id} cancel requested")


def _accepts_context(handler: Handler) -> bool:
    """兼容短任务：handler 声明两个参数才注入 TaskContext。"""
    try:
        sig = inspect.signature(handler)
    except (TypeError, ValueError):
        return False
    params = [p for p in sig.parameters.values()
              if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)]
    return len(params) >= 2 or any(p.kind is p.VAR_POSITIONAL
                                   for p in sig.parameters.values())


class _LeaseLost(Exception):
    """续租时发现租约已丢失（被回收/已终态）：执行者放弃本次执行。"""


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
        heartbeat_interval: float | None = None,
        backoff: BackoffPolicy | None = None,
    ):
        self._repo = repo
        self._handlers = handlers
        self.worker_id = worker_id
        self._lease = lease_seconds
        self._poll = poll_interval
        self._timeout = execution_timeout
        # 心跳间隔默认租约的 1/3，保证租约到期前至少续期两次
        self._heartbeat = (heartbeat_interval if heartbeat_interval is not None
                           else lease_seconds / 3)
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
        handler = self._handlers.get(task.task_type)
        if handler is None:
            self._repo.fail(task.id, self.worker_id,
                            f"no handler registered for type {task.task_type}",
                            retryable=False, epoch=task.epoch)
            return
        payload = json.loads(task.payload)
        # 兼容短任务：只有声明第二个参数的 handler 才注入 TaskContext
        if _accepts_context(handler):
            args = (payload, TaskContext(self._repo, task.id,
                                         self.worker_id, task.epoch))
        else:
            args = (payload,)
        try:
            # 不用 with：超时场景下 shutdown(wait=True) 会被挂起的 handler 拖死
            pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
            try:
                future = pool.submit(handler, *args)
                self._await_with_heartbeat(future, task)
            finally:
                pool.shutdown(wait=False)
        except concurrent.futures.TimeoutError:
            outcome = ("timeout", f"execution timeout after {self._timeout}s")
        except TaskCancelled as exc:
            outcome = ("cancelled", str(exc))
        except BusinessFailure as exc:
            outcome = ("business", str(exc))
        except _LeaseLost as exc:
            logger.warning("task %s lease lost, abandon execution: %s", task.id, exc)
            return
        except Exception as exc:  # noqa: BLE001 - 可重试的技术失败
            outcome = ("retryable", repr(exc))
        else:
            outcome = ("ok", None)
        self._finalize(task, outcome)

    def _await_with_heartbeat(self, future: "concurrent.futures.Future",
                              task: Task) -> None:
        """等待 handler 完成。等待期间按心跳间隔续租：只要任务还在正常执行，
        租约就不会到期，系统不会将其回收或交给第二个执行者。"""
        deadline = time.monotonic() + self._timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise concurrent.futures.TimeoutError()
            try:
                future.result(timeout=min(self._heartbeat, remaining))
                return
            except concurrent.futures.TimeoutError:
                try:
                    self._repo.renew_lease(task.id, self.worker_id,
                                           task.epoch, self._lease)
                except DomainError as exc:
                    raise _LeaseLost(str(exc)) from exc

    def _finalize(self, task: Task, outcome: tuple[str, str | None]) -> None:
        """收敛任务终态。取消请求优先：只要请求方已置取消标记且本执行者仍
        持有租约，无论 handler 以何种方式退出，都收敛到 CANCELLED。"""
        kind, detail = outcome
        try:
            if kind != "timeout" and self._cancel_wins(task):
                self._repo.confirm_cancelled(task.id, self.worker_id, task.epoch)
                logger.info("task %s cancel requested during run -> CANCELLED "
                            "(handler exit: %s)", task.id, kind)
                return
            if kind == "ok":
                self._repo.succeed(task.id, self.worker_id, epoch=task.epoch)
                logger.info("task %s succeeded (attempt %d)", task.id, task.attempt)
            elif kind == "timeout":
                # 超时：进死信，不静默吞掉；若已被请求取消则收敛到 CANCELLED
                self._repo.timeout_to_dead(task.id, self.worker_id,
                                           self._timeout, epoch=task.epoch)
                logger.warning("task %s timeout -> DEAD", task.id)
            elif kind == "cancelled":
                self._repo.confirm_cancelled(task.id, self.worker_id, task.epoch)
                logger.info("task %s acknowledged cancel -> CANCELLED", task.id)
            elif kind == "business":
                # 业务明确失败：不重试，直接死信
                self._repo.fail(task.id, self.worker_id, detail,
                                retryable=False, epoch=task.epoch)
                logger.warning("task %s business failure -> DEAD: %s", task.id, detail)
            else:
                delay = self._backoff.delay(task.attempt)
                self._repo.fail(task.id, self.worker_id, detail,
                                retryable=True, backoff_seconds=delay,
                                epoch=task.epoch)
                logger.info("task %s retryable failure, backoff %.3fs: %s",
                            task.id, delay, detail)
        except DomainError as exc:
            # 与取消/回收并发时状态可能已变：放弃收尾，以库内状态为准
            logger.warning("task %s finalize skipped (%s): %s", task.id, kind, exc)

    def _cancel_wins(self, task: Task) -> bool:
        """handler 已退出且取消标记已置位：取消请求优先于 handler 的结局。"""
        try:
            current = self._repo.get(task.id)
        except DomainError:
            return False
        return (current.cancel_requested
                and current.status is TaskStatus.RUNNING
                and current.lease_owner == self.worker_id)
