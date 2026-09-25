"""运行期可调参数（租约、退避、并发轮询间隔等）。"""
from __future__ import annotations

from dataclasses import dataclass, field

from .models import RetryPolicy


@dataclass(slots=True)
class Settings:
    db_path: str = ":memory:"
    task_retry: RetryPolicy = field(default_factory=lambda: RetryPolicy())
    delivery_retry: RetryPolicy = field(
        default_factory=lambda: RetryPolicy(max_attempts=5, base_delay=0.05)
    )
    lease_seconds: float = 5.0
    poll_interval: float = 0.02
    task_timeout: float = 5.0
    worker_count: int = 1


def default_settings(db_path: str = ":memory:") -> Settings:
    """默认设置；测试可直接构造 Settings 覆盖。"""
    return Settings(db_path=db_path)
