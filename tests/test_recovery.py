"""进程崩溃/重启恢复。

用“停止后台循环 + 等待租约过期 + 用新服务实例重新打开同一 DB 文件”
模拟进程退出后重启（状态完全来自持久化数据）。
"""
from __future__ import annotations

import asyncio

from app.config import Settings
from app.models import TaskStatus
from app.service import TaskService
from tests.conftest import dump_flow, fast_settings, wait_status


async def test_worker_crash_mid_execution_recovers_via_lease(settings: Settings):
    invocations = {"n": 0}

    async def job(payload):
        invocations["n"] += 1
        # 长于租约(0.3s)：执行者在“业务执行中”被杀，心跳随进程消失
        await asyncio.sleep(0.6)
        return "ok"

    svc1 = TaskService(settings)
    svc1.register_handler("job", job)
    await svc1.start_background()
    t, _ = await svc1.submit("job", {}, "restart-1")
    await asyncio.sleep(0.05)  # 确保已被领取并进入 RUNNING
    running = await svc1.get(t.id)
    assert running.status is TaskStatus.RUNNING

    # 模拟进程中途退出：不优雅停止（不取消处理器协程），直接关闭所有 DB 连接，
    # 使 RUNNING 租约真实地“悬挂”在数据库里。
    svc1.store.close()
    for bg in svc1._bg:
        bg.cancel()
    await asyncio.gather(*svc1._bg, return_exceptions=True)
    svc1._bg.clear()
    svc1._stop = None

    # 等待租约过期，随后“重启”：新进程打开同一文件
    await asyncio.sleep(settings.lease_seconds + 0.1)
    settings2 = fast_settings(settings.db_path)
    svc2 = TaskService(settings2)
    svc2.register_handler("job", job)
    await svc2.start_background()

    final = await wait_status(svc2, t.id, TaskStatus.SUCCEEDED, timeout=6)
    assert final.status is TaskStatus.SUCCEEDED
    # 处理器确实被调用过两次（崩溃前一次 + 恢复后一次）=> at-least-once 执行
    assert invocations["n"] == 2, invocations
    events = svc2.store.list_events(t.id)
    assert any(e.event_type == "LEASE_RECLAIMED" for e in events), \
        [e.event_type for e in events]
    dump_flow(svc2, t.id, "执行中途崩溃 -> 租约过期 -> 重启恢复成功")
    await svc2.stop_background()
    svc2.store.close()


async def test_deliverer_crash_redelivers_without_duplicate_sink(settings: Settings):
    """投递中途退出：重启后重复投递，但 sink 以幂等键去重，外部只生效一次。"""
    delivered: list[str] = []
    sink_calls = {"n": 0}

    async def job(payload):
        return {"v": 42}

    async def flaky_sink(task_id, task_type, payload, result, idem_key):
        sink_calls["n"] += 1
        if sink_calls["n"] == 1:
            # 已产生外部副作用，但确认前“崩溃”
            delivered.append(idem_key)
            await asyncio.sleep(0.6)  # 长于租约，投递者被杀
            return
        delivered.append(idem_key)

    svc1 = TaskService(settings)
    svc1.register_handler("job", job)
    svc1.register_sink("job", flaky_sink)
    await svc1.start_background()
    t, _ = await svc1.submit("job", {}, "restart-2")
    final = await wait_status(svc1, t.id, TaskStatus.SUCCEEDED, timeout=5)
    assert final.status is TaskStatus.SUCCEEDED
    # 等到发件箱被领取（attempts>=1）再杀投递者
    await asyncio.sleep(0.05)
    await svc1.stop_background()
    svc1.store.close()

    # 幂等 sink 模拟：重启后收到重复投递，按幂等键去重
    seen: set[str] = set(delivered)
    await asyncio.sleep(settings.lease_seconds + 0.1)

    async def idempotent_sink(task_id, task_type, payload, result, idem_key):
        if idem_key in seen:
            return  # 重复投递，外部副作用不再生效
        seen.add(idem_key)

    settings2 = fast_settings(settings.db_path)
    svc2 = TaskService(settings2)
    svc2.register_sink("job", idempotent_sink)
    await svc2.start_background()

    for _ in range(300):
        await asyncio.sleep(0.02)
        row = svc2.store.conn.execute(
            "SELECT status FROM task_outbox WHERE task_id=?", (t.id,)
        ).fetchone()
        if row and row[0] == "DELIVERED":
            break
    assert row[0] == "DELIVERED"
    # 外部副作用恰好一次
    assert list(seen) == ["restart-2"]
    dump_flow(svc2, t.id, "投递中途崩溃 -> 重启重投 -> 幂等 sink 去重生效一次")
    await svc2.stop_background()
    svc2.store.close()
