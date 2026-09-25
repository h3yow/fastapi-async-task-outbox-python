"""任务处理器注册表与投递出口（sink）。

处理器是纯 Python 协程，通过键名注册；出口 sink 负责把终态结果
投递给外部系统。两者均以 idempotency_key 支持幂等判定。
"""
from __future__ import annotations

from collections.abc import Awaitable, Callable

TaskHandler = Callable[[dict[str, object]], Awaitable[object]]
# (payload) -> result；抛异常=执行失败；抛 PermanentFailure=业务明确失败。

DeliverySink = Callable[[str, str, dict[str, object], object, str], Awaitable[None]]
# (task_id, task_type, payload, result, idempotency_key) -> None
# 抛异常=投递瞬时失败（重试）；抛 PermanentDeliveryError=永久失败（直接死信）。


class PermanentDeliveryError(Exception):
    """投递出口主动抛出：不可重试，发件箱消息直接进死信。"""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class HandlerRegistry:
    """task_type -> handler 的注册表。"""

    def __init__(self) -> None:
        self._handlers: dict[str, TaskHandler] = {}

    def register(self, task_type: str, handler: TaskHandler) -> None:
        if not task_type:
            raise ValueError("task_type 不能为空")
        self._handlers[task_type] = handler

    def get(self, task_type: str) -> TaskHandler | None:
        return self._handlers.get(task_type)


class SinkRegistry:
    """投递出口注册表；缺省的空出口仅表示“本地验证无外部系统”。"""

    def __init__(self) -> None:
        self._sinks: dict[str, DeliverySink] = {}

    def register(self, task_type: str, sink: DeliverySink) -> None:
        if not task_type:
            raise ValueError("task_type 不能为空")
        self._sinks[task_type] = sink

    def get(self, task_type: str) -> DeliverySink | None:
        return self._sinks.get(task_type)
