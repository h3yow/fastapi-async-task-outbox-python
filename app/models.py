"""领域模型：状态机、数据结构与错误定义。

本文件只包含类型定义，不依赖任何 I/O。
"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


def utcnow() -> datetime:
    """统一的 UTC 时间来源，测试中可替换。"""
    return datetime.now(UTC)


class TaskStatus(str, enum.Enum):
    """任务状态机。

    合法跃迁：
        PENDING    -> RUNNING / CANCELLED / DEAD_LETTER
        RUNNING    -> PENDING(退避重试) / SUCCEEDED / CANCELLED / DEAD_LETTER
        DEAD_LETTER-> PENDING（人工重放，保留历史并自增 replay 序号）
    SUCCEEDED / CANCELLED 为不可重放终态；DEAD_LETTER 是可重放终态，
    执行阶段与投递阶段失败均落入 DEAD_LETTER，以 dead_letter_reason 区分。
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    CANCELLED = "CANCELLED"
    DEAD_LETTER = "DEAD_LETTER"


TERMINAL_STATUSES = frozenset(
    {TaskStatus.SUCCEEDED, TaskStatus.CANCELLED, TaskStatus.DEAD_LETTER}
)
REPLAYABLE_STATUSES = frozenset({TaskStatus.DEAD_LETTER})


class DeadLetterReason(str, enum.Enum):
    """进入死信的可区分原因。"""

    RETRIES_EXHAUSTED = "RETRIES_EXHAUSTED"
    BUSINESS_REJECTED = "BUSINESS_REJECTED"
    TIMEOUT = "TIMEOUT"
    DELIVERY_RETRIES_EXHAUSTED = "DELIVERY_RETRIES_EXHAUSTED"


class ErrorCode(str, enum.Enum):
    """非法操作的可区分错误码（HTTP 层据此返回 4xx）。"""

    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"
    TASK_NOT_FOUND = "TASK_NOT_FOUND"
    ALREADY_CANCELLED = "ALREADY_CANCELLED"
    TASK_TERMINAL = "TASK_TERMINAL"
    TASK_RUNNING = "TASK_RUNNING"
    NOT_REPLAYABLE = "NOT_REPLAYABLE"


class TaskError(Exception):
    """带错误码的领域异常，便于调用方区分拒绝原因。"""

    def __init__(self, code: ErrorCode, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class PermanentFailure(Exception):
    """任务处理器主动抛出：业务明确失败，不重试，直接进死信。"""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


@dataclass(slots=True)
class Task:
    """任务聚合。"""

    id: str
    idempotency_key: str
    task_type: str
    payload: dict[str, Any]
    status: TaskStatus
    attempt: int
    max_attempts: int
    lease_owner: str | None
    lease_expires_at: datetime | None
    backoff_until: datetime | None
    cancel_requested: bool
    replay_of: str | None
    replay_seq: int
    dead_letter_reason: str | None
    dead_letter_detail: str | None
    result: Any
    created_at: datetime
    updated_at: datetime


@dataclass(slots=True)
class TaskEvent:
    """append-only 状态流水，任何状态变化/失败都必须留痕，禁止静默。"""

    id: int | None
    task_id: str
    seq: int
    at: datetime
    actor: str
    from_status: TaskStatus | None
    to_status: TaskStatus | None
    event_type: str
    attempt: int
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class OutboxMessage:
    """事务发件箱：任务终态产物的可靠投递记录。"""

    id: int | None
    task_id: str
    task_type: str
    payload: dict[str, Any]
    result: Any
    idempotency_key: str
    status: str  # PENDING / DELIVERED / DEAD
    attempts: int
    max_attempts: int
    backoff_until: datetime | None
    last_error: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(slots=True)
class RetryPolicy:
    """有上限的指数退避：delay = base * factor**(n-1)，封顶 cap。"""

    max_attempts: int = 5
    base_delay: float = 0.05
    factor: float = 2.0
    cap_delay: float = 1.0

    def delay_for(self, attempt: int) -> float:
        """第 attempt 次执行失败后，下一次执行前的等待秒数。

        确定性退避（不引入随机抖动，保证测试可重复）：
            delay = min(cap, base * factor**(attempt-1))
        """
        if attempt < 1:
            return 0.0
        delay = self.base_delay * (self.factor ** (attempt - 1))
        return min(self.cap_delay, delay)
