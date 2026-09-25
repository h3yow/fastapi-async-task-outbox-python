"""pytest 公共夹具与判定辅助。

每个测试使用独立的文件型 SQLite（WAL），并通过 ``dump_flow`` 把
任务状态流水与判定依据打印到测试日志，满足可解释、可审计要求。
"""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

import pytest

from app.config import Settings, default_settings
from app.models import RetryPolicy, TaskStatus
from app.service import TaskService

TERMINAL = {TaskStatus.SUCCEEDED, TaskStatus.CANCELLED, TaskStatus.DEAD_LETTER}


def fast_settings(db_path: str, *, max_attempts: int = 3,
                  lease_seconds: float = 0.3, task_timeout: float = 2.0) -> Settings:
    """测试用加速参数：退避毫秒级、租约短、轮询快。"""
    s = default_settings(db_path)
    s.task_retry = RetryPolicy(
        max_attempts=max_attempts, base_delay=0.02, factor=2.0, cap_delay=0.05
    )
    s.delivery_retry = RetryPolicy(
        max_attempts=max_attempts, base_delay=0.02, factor=2.0, cap_delay=0.05
    )
    s.lease_seconds = lease_seconds
    s.poll_interval = 0.005
    s.task_timeout = task_timeout
    return s


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    return str(tmp_path / "tasks.db")


@pytest.fixture
def settings(db_path: str) -> Settings:
    return fast_settings(db_path)


@pytest.fixture
def service(settings: Settings) -> TaskService:
    svc = TaskService(settings)
    yield svc
    svc.store.close()


async def wait_status(
    service: TaskService, task_id: str, expected, timeout: float = 5.0
) -> Any:
    """轮询等待任务进入目标状态（必须精确匹配），返回最终任务。"""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    task = await service.get(task_id)
    while loop.time() < deadline:
        task = await service.get(task_id)
        if task.status == expected:
            return task
        await asyncio.sleep(0.01)
    return task


def dump_flow(service: TaskService, task_id: str, title: str) -> None:
    """打印任务状态流水与判定依据（同时写日志与 stdout，-s 或失败时可见）。"""
    log = logging.getLogger("test.flow")
    task = service.store.get_task(task_id)
    events = service.store.list_events(task_id)
    line = "=" * 78
    rows = [
        line,
        f"判定场景: {title}",
        (
            f"任务 {task_id} type={task.task_type} 最终状态={task.status.value} "
            f"attempt={task.attempt}/{task.max_attempts} "
            f"死信原因={task.dead_letter_reason} 重放序号={task.replay_seq}"
        ),
    ]
    for e in events:
        frm = e.from_status.value if e.from_status else "-"
        to = e.to_status.value if e.to_status else "-"
        rows.append(
            f"  seq={e.seq:<2} {e.event_type:<18} {frm} -> {to:<11} "
            f"actor={e.actor:<12} attempt={e.attempt} detail={e.detail}"
        )
    outbox = service.store.conn.execute(
        "SELECT id,status,attempts,last_error FROM task_outbox WHERE task_id=?",
        (task_id,),
    ).fetchall()
    for o in outbox:
        rows.append(
            f"  outbox id={o['id']} status={o['status']} "
            f"attempts={o['attempts']} last_error={o['last_error']}"
        )
    rows.append(f"判定依据: {_reason(task)}")
    rows.append(line)
    text = "\n".join(rows)
    print("\n" + text)  # -s 时直接可见；失败时出现在 captured stdout
    log.info("\n%s", text)


def _reason(task) -> str:
    return {
        TaskStatus.SUCCEEDED: "执行成功且发件箱已/待确认，链路闭环",
        TaskStatus.CANCELLED: "收到取消信号且在生效点被执行者采纳",
        TaskStatus.DEAD_LETTER: f"终态失败原因={task.dead_letter_reason}，可人工重放",
        TaskStatus.RUNNING: "已被唯一执行者持有租约",
        TaskStatus.PENDING: "等待领取或退避窗口中",
    }[task.status]
