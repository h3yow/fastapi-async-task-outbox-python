"""多执行者并发抢占。

覆盖：N 个 worker 并发处理同一批 M 个任务时：
- 每个任务恰好被执行一次（无重复执行）；
- 同一时刻同一任务只被一个 owner 持有（租约排他）；
- 全部任务最终成功，发件箱每条恰好确认一次。
"""
from __future__ import annotations

import asyncio

from app.models import TaskStatus
from tests.conftest import dump_flow, wait_status


async def test_concurrent_workers_claim_exclusively(service):
    active: dict[int, int] = {}
    invocations: dict[int, int] = {}
    executed: list[int] = []
    lock = asyncio.Lock()

    async def exclusive(payload):
        seq = payload["seq"]
        async with lock:
            active[seq] = active.get(seq, 0) + 1
            invocations[seq] = invocations.get(seq, 0) + 1
            executed.append(seq)
        await asyncio.sleep(0.02)  # 制造重叠窗口，引诱重复抢占
        async with lock:
            active[seq] -= 1
        return seq

    service.register_handler("job", exclusive)

    n_tasks = 20
    task_ids = []
    for i in range(n_tasks):
        t, created = await service.submit("job", {"seq": i}, f"ck-{i}")
        assert created
        task_ids.append(t.id)

    await service.start_background(worker_count=4)

    finals = []
    for tid in task_ids:
        finals.append(await wait_status(service, tid, TaskStatus.SUCCEEDED, timeout=8))

    assert all(t.status is TaskStatus.SUCCEEDED for t in finals)
    # 同一任务处理器只被调用一次
    assert sorted(executed) == list(range(n_tasks))
    assert len(executed) == n_tasks
    assert all(v == 1 for v in invocations.values())
    # 执行期间同任务并发度绝不超过 1
    assert all(v == 0 for v in active.values())

    # 事件校验：每个任务恰好一次 CLAIMED
    for tid in task_ids[:5]:
        events = service.store.list_events(tid)
        claims = [e for e in events if e.event_type == "CLAIMED"]
        assert len(claims) == 1
    dump_flow(service, task_ids[0], "4 worker 并发处理 20 任务，每任务只执行一次")

    # 发件箱：每条恰好 DELIVERED 一次（投递异步于任务成功，需显式等待）
    for _ in range(500):
        await asyncio.sleep(0.01)
        row = service.store.conn.execute(
            "SELECT COUNT(*) FROM task_outbox WHERE status='DELIVERED'"
        ).fetchone()
        if row[0] == n_tasks:
            break
    assert row[0] == n_tasks
    rows = service.store.conn.execute(
        "SELECT status, COUNT(*) FROM task_outbox GROUP BY status"
    ).fetchall()
    counts = {r[0]: r[1] for r in rows}
    assert counts.get("DELIVERED") == n_tasks, counts
    # 投递也只确认一次：attempts 分布全部为 1
    assert service.store.conn.execute(
        "SELECT COUNT(*) FROM task_outbox WHERE attempts <> 1"
    ).fetchone()[0] == 0
    await service.stop_background()
