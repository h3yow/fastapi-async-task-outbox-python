"""幂等提交与取消语义。

覆盖：
- 同一幂等键重复提交不产生新执行记录（deduplicated=True）；
- 同键不同载荷以 IDEMPOTENCY_CONFLICT 拒绝；
- 待执行任务取消、重复取消（ALREADY_CANCELLED）、终态取消（TASK_TERMINAL）。
"""
from __future__ import annotations

import pytest

from app.models import ErrorCode, TaskError, TaskStatus
from tests.conftest import dump_flow, wait_status


async def test_idempotent_submit_does_not_duplicate(service):
    async def echo(payload):
        return {"echo": payload}

    service.register_handler("echo", echo)
    await service.start_background()

    t1, created1 = await service.submit("echo", {"a": 1}, "key-1")
    t2, created2 = await service.submit("echo", {"a": 1}, "key-1")
    t3, created3 = await service.submit("echo", {"a": 1}, "key-1")

    assert created1 is True
    assert created2 is False and created3 is False
    assert t1.id == t2.id == t3.id

    final = await wait_status(service, t1.id, TaskStatus.SUCCEEDED)
    assert final.status is TaskStatus.SUCCEEDED
    events = service.store.list_events(t1.id)
    assert sum(1 for e in events if e.event_type == "SUBMITTED") == 1
    dump_flow(service, t1.id, "同幂等键三次提交只产生一条执行记录")
    await service.stop_background()


async def test_same_key_different_payload_conflict(service):
    await service.submit("echo", {"a": 1}, "key-2")
    with pytest.raises(TaskError) as ei:
        await service.submit("echo", {"a": 2}, "key-2")
    assert ei.value.code is ErrorCode.IDEMPOTENCY_CONFLICT

    with pytest.raises(TaskError) as ei2:
        await service.submit("other", {"a": 1}, "key-2")
    assert ei2.value.code is ErrorCode.IDEMPOTENCY_CONFLICT


async def test_cancel_pending_and_double_cancel_rejected(service):
    t, _ = await service.submit("echo", {}, "key-cancel-1")
    # 不启动 worker，任务保持 PENDING
    cancelled = await service.cancel(t.id)
    assert cancelled.status is TaskStatus.CANCELLED

    with pytest.raises(TaskError) as ei:
        await service.cancel(t.id)
    assert ei.value.code is ErrorCode.ALREADY_CANCELLED
    dump_flow(service, t.id, "PENDING 取消后重复取消必须被拒绝")


async def test_cancel_terminal_succeeded_rejected(service):
    async def echo(payload):
        return "done"

    service.register_handler("echo", echo)
    await service.start_background()
    t, _ = await service.submit("echo", {}, "key-cancel-2")
    final = await wait_status(service, t.id, TaskStatus.SUCCEEDED)
    assert final.status is TaskStatus.SUCCEEDED

    with pytest.raises(TaskError) as ei:
        await service.cancel(t.id)
    assert ei.value.code is ErrorCode.TASK_TERMINAL
    dump_flow(service, t.id, "已成功任务取消被拒绝（TASK_TERMINAL）")
    await service.stop_background()


async def test_cancel_dead_letter_rejected_but_replay_allowed(service):
    from app.models import PermanentFailure

    async def boom(payload):
        raise PermanentFailure("明确失败")

    service.register_handler("echo", boom)
    await service.start_background()
    t, _ = await service.submit("echo", {}, "key-cancel-3")
    final = await wait_status(service, t.id, TaskStatus.DEAD_LETTER)
    assert final.status is TaskStatus.DEAD_LETTER

    with pytest.raises(TaskError) as ei:
        await service.cancel(t.id)
    assert ei.value.code is ErrorCode.TASK_TERMINAL

    replayed = await service.replay(t.id)
    assert replayed.status is TaskStatus.PENDING
    assert replayed.replay_seq == 1
    dump_flow(service, t.id, "死信不可取消但可人工重放")
    await service.stop_background()


async def test_cancel_during_backoff_window(service):
    """任务失败后退避中（PENDING+backoff_until）仍可被取消。"""
    async def fail_once(payload):
        raise RuntimeError("临时故障")

    # 只尝试 1 次：失败即死信，无法观测退避；改为 2 次并在退避窗口取消
    service.settings.task_retry.max_attempts = 5
    service.register_handler("flaky", fail_once)
    await service.start_background()
    t, _ = await service.submit("flaky", {}, "key-cancel-backoff")

    # 等待出现第一次 RETRY_BACKOFF（任务回到 PENDING 且 backoff_until 在未来）
    import asyncio as _aio
    for _ in range(200):
        await _aio.sleep(0.01)
        task = await service.get(t.id)
        if any(e.event_type == "RETRY_BACKOFF"
               for e in service.store.list_events(t.id)):
            break
    task = await service.get(t.id)
    assert task.status is TaskStatus.PENDING and task.backoff_until is not None

    cancelled = await service.cancel(t.id)
    assert cancelled.status is TaskStatus.CANCELLED
    # 退避到期后执行者不得再把它领走
    await _aio.sleep(0.3)
    final = await service.get(t.id)
    assert final.status is TaskStatus.CANCELLED
    dump_flow(service, t.id, "退避窗口中取消，到期后不被重新领取")
    await service.stop_background()
