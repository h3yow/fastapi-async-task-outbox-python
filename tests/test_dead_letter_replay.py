"""失败隔离、死信与人工重放。

覆盖三种死信原因：
- RETRIES_EXHAUSTED：连续瞬时失败达到上限；
- BUSINESS_REJECTED：处理器抛 PermanentFailure，立即死信不重试；
- TIMEOUT：处理器执行超时，重试达到上限后死信；
- DELIVERY_RETRIES_EXHAUSTED：sink 永久失败 / 重试用尽。
重放保留全部历史，replay_seq 自增；投递死信重放只重投递不再执行处理器。
"""
from __future__ import annotations

import asyncio

import pytest

from app.handlers import PermanentDeliveryError
from app.models import (
    DeadLetterReason,
    ErrorCode,
    PermanentFailure,
    TaskError,
    TaskStatus,
)
from tests.conftest import dump_flow, wait_status


async def test_retries_exhausted_enters_dead_letter(service):
    calls = {"n": 0}

    async def always_bad(payload):
        calls["n"] += 1
        raise RuntimeError("持续失败")

    service.register_handler("bad", always_bad)
    await service.start_background()
    t, _ = await service.submit("bad", {}, "dl-1")
    final = await wait_status(service, t.id, TaskStatus.DEAD_LETTER, timeout=8)
    assert final.status is TaskStatus.DEAD_LETTER
    assert final.dead_letter_reason == DeadLetterReason.RETRIES_EXHAUSTED.value
    assert calls["n"] == final.max_attempts
    assert final.attempt == final.max_attempts
    dump_flow(service, t.id, "连续失败达到上限 -> RETRIES_EXHAUSTED")
    await service.stop_background()


async def test_business_failure_is_immediate_dead_letter(service):
    calls = {"n": 0}

    async def reject(payload):
        calls["n"] += 1
        raise PermanentFailure("余额不足，业务明确拒绝")

    service.register_handler("pay", reject)
    await service.start_background()
    t, _ = await service.submit("pay", {}, "dl-2")
    final = await wait_status(service, t.id, TaskStatus.DEAD_LETTER)
    assert final.dead_letter_reason == DeadLetterReason.BUSINESS_REJECTED.value
    assert calls["n"] == 1  # 明确失败不重试
    assert "余额不足" in final.dead_letter_detail
    dump_flow(service, t.id, "业务明确失败 -> 立即 BUSINESS_REJECTED，不重试")
    await service.stop_background()


async def test_timeout_retries_then_dead_letter(settings):
    from app.models import RetryPolicy
    from app.service import TaskService

    settings.task_timeout = 0.05
    settings.task_retry = RetryPolicy(
        max_attempts=2, base_delay=0.01, factor=2.0, cap_delay=0.02
    )
    svc = TaskService(settings)
    calls = {"n": 0}

    async def slow(payload):
        calls["n"] += 1
        await asyncio.sleep(1.0)  # 必超时

    svc.register_handler("slow", slow)
    await svc.start_background()
    t, _ = await svc.submit("slow", {}, "dl-3")
    final = await wait_status(svc, t.id, TaskStatus.DEAD_LETTER, timeout=8)
    assert final.status is TaskStatus.DEAD_LETTER
    assert final.dead_letter_reason == DeadLetterReason.TIMEOUT.value
    assert calls["n"] == 2
    dump_flow(svc, t.id, "执行超时按重试处理，用尽后 TIMEOUT 死信")
    await svc.stop_background()
    svc.store.close()


async def test_replay_preserves_history_and_succeeds(service):
    calls = {"n": 0}

    async def flaky(payload):
        calls["n"] += 1
        if not fixed.is_set():
            raise RuntimeError("依赖尚未恢复")
        return {"healed": True}

    fixed = asyncio.Event()
    service.register_handler("flaky", flaky)
    await service.start_background()
    t, _ = await service.submit("flaky", {}, "dl-4")
    dead = await wait_status(service, t.id, TaskStatus.DEAD_LETTER, timeout=8)
    assert dead.status is TaskStatus.DEAD_LETTER
    history_before = service.store.list_events(t.id)
    assert len(history_before) >= 4  # SUBMITTED + CLAIMED/RETRY + DEAD

    # 人工修复后重放（Event 置位后，重放的执行才会成功）
    fixed.set()
    replayed = await service.replay(t.id, actor="ops@example")
    assert replayed.status is TaskStatus.PENDING
    assert replayed.replay_seq == 1
    assert replayed.replay_of == t.id

    final = await wait_status(service, t.id, TaskStatus.SUCCEEDED, timeout=8)
    assert final.status is TaskStatus.SUCCEEDED
    assert final.result == {"healed": True}

    all_events = service.store.list_events(t.id)
    # 历史完整保留：重放前的流水原样作为前缀，之后新增 REPLAYED 与成功流水
    assert all_events[: len(history_before)] == history_before
    after = all_events[len(history_before):]
    assert after[0].event_type == "REPLAYED"
    assert after[0].actor == "ops@example"
    assert any(e.event_type == "SUCCEEDED" for e in after)
    # 重放事件里可追溯重放序号
    assert after[0].detail["replay_seq"] == 1
    dump_flow(service, t.id, "人工重放保留全部历史并最终成功")
    await service.stop_background()


async def test_replay_non_dead_letter_rejected(service):
    async def echo(payload):
        return 1

    service.register_handler("echo", echo)
    await service.start_background()
    t, _ = await service.submit("echo", {}, "dl-5")
    await wait_status(service, t.id, TaskStatus.SUCCEEDED)
    with pytest.raises(TaskError) as ei:
        await service.replay(t.id)
    assert ei.value.code is ErrorCode.NOT_REPLAYABLE
    await service.stop_background()


async def test_delivery_dead_letter_and_replay_redelivers(service):
    delivered = []

    async def echo(payload):
        return {"r": 1}

    async def permanent_bad_sink(task_id, task_type, payload, result, idem_key):
        raise PermanentDeliveryError("下游系统永久关闭")

    service.register_handler("echo", echo)
    service.register_sink("echo", permanent_bad_sink)
    await service.start_background()
    t, _ = await service.submit("echo", {}, "dl-6")
    # 任务先成功，投递永久失败后任务被反压为投递死信
    final = await wait_status(service, t.id, TaskStatus.DEAD_LETTER, timeout=8)
    assert final.dead_letter_reason == \
        DeadLetterReason.DELIVERY_RETRIES_EXHAUSTED.value

    # 历史中应同时保留 SUCCEEDED 与 OUTBOX_DEAD 事件
    kinds = [e.event_type for e in service.store.list_events(t.id)]
    assert "SUCCEEDED" in kinds and "OUTBOX_DEAD" in kinds

    # 修复下游后重放：只重投递，处理器不再执行
    async def good_sink(task_id, task_type, payload, result, idem_key):
        delivered.append((idem_key, result))

    service.register_sink("echo", good_sink)
    await service.replay(t.id, actor="ops")
    for _ in range(300):
        await asyncio.sleep(0.02)
        row = service.store.conn.execute(
            "SELECT status FROM task_outbox WHERE task_id=?", (t.id,)
        ).fetchone()
        if row[0] == "DELIVERED":
            break
    assert row[0] == "DELIVERED"
    assert delivered == [("dl-6", {"r": 1})]
    dump_flow(service, t.id, "投递死信重放：保留成功历史，仅重新投递")
    await service.stop_background()
