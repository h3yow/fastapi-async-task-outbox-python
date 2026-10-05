"""任务领域模型：状态机、数据结构与领域异常。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime


class TaskStatus(str, enum.Enum):
    """任务状态机。

    合法跃迁：
      PENDING   -> RUNNING            (被某个执行者抢占)
      PENDING   -> CANCELLED          (提交方取消)
      RUNNING   -> SUCCEEDED          (执行成功)
      RUNNING   -> PENDING            (失败重试，带退避)
      RUNNING   -> DEAD               (超过重试上限 / 超时 / 业务明确失败)
      RUNNING   -> PENDING            (租约过期被回收，进程重启恢复)
      RUNNING   -> CANCELLED          (运行中被请求取消，任务感知后收敛)
      DEAD      -> PENDING            (人工重放，保留历史)
    终态：SUCCEEDED / CANCELLED。DEAD 可经人工重放回到 PENDING。
    运行中取消是两阶段：先置 cancel_requested 标记（状态仍为 RUNNING），
    任务感知并停下后再跃迁到 CANCELLED。
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    CANCELLED = "CANCELLED"
    DEAD = "DEAD"


TERMINAL_STATUSES = frozenset({TaskStatus.SUCCEEDED, TaskStatus.CANCELLED})

ALLOWED_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.PENDING: frozenset({TaskStatus.RUNNING, TaskStatus.CANCELLED}),
    TaskStatus.RUNNING: frozenset({TaskStatus.SUCCEEDED, TaskStatus.PENDING,
                                   TaskStatus.DEAD, TaskStatus.CANCELLED}),
    TaskStatus.DEAD: frozenset({TaskStatus.PENDING}),
    TaskStatus.SUCCEEDED: frozenset(),
    TaskStatus.CANCELLED: frozenset(),
}


class DomainError(Exception):
    """领域错误基类，携带可区分的 reason code。"""

    code = "domain_error"


class InvalidTransitionError(DomainError):
    code = "invalid_transition"

    def __init__(self, task_id: str, current: TaskStatus, target: TaskStatus):
        super().__init__(f"task {task_id}: illegal transition {current.value} -> {target.value}")
        self.task_id = task_id
        self.current = current
        self.target = target


class TaskNotFoundError(DomainError):
    code = "task_not_found"


class TaskAlreadyTerminalError(DomainError):
    code = "task_already_terminal"

    def __init__(self, task_id: str, status: TaskStatus):
        super().__init__(f"task {task_id} already in terminal status {status.value}")
        self.task_id = task_id
        self.status = status


class CancelAlreadyRequestedError(DomainError):
    """运行中的任务已被请求取消过，重复请求被可区分地拒绝。"""

    code = "cancel_already_requested"

    def __init__(self, task_id: str):
        super().__init__(f"task {task_id} cancellation already requested")
        self.task_id = task_id


class BusinessFailure(Exception):
    """业务处理器显式抛出的明确失败：不重试，直接进入死信。"""


class TaskCancelled(Exception):
    """任务在执行中感知到取消请求后停止。由 TaskContext.throw_if_cancelled 抛出，
    也可由业务 handler 自行抛出。执行器捕获后将任务收敛到 CANCELLED。"""


@dataclass(frozen=True)
class Task:
    id: str
    idempotency_key: str
    task_type: str
    payload: str
    status: TaskStatus
    attempt: int
    max_attempts: int
    run_at: datetime
    lease_owner: str | None
    lease_expires_at: datetime | None
    created_at: datetime
    updated_at: datetime
    last_error: str | None = None
    cancel_requested: bool = False


@dataclass(frozen=True)
class TaskProgress:
    """任务运行进度快照。任务从未上报过时，查询返回 None（对外表现为“未知”）。

    进度随“一次运行”生命周期：每次被抢占开始新一轮执行时清零，
    人工重放（replay）也会清零，不会把上一轮的进度带到下一轮。
    """

    task_id: str
    percent: float | None
    step: str | None
    updated_at: datetime


@dataclass(frozen=True)
class TaskEvent:
    """任务状态流水（审计/判定依据），追加写，永不修改。"""

    id: int
    task_id: str
    from_status: TaskStatus | None
    to_status: TaskStatus
    reason: str
    actor: str
    created_at: datetime
