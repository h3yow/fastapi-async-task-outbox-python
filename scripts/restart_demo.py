"""本地可重复的“真实进程重启”验证脚本（无外部中间件、无真实凭据）。

用法：
    uv run python scripts/restart_demo.py

流程（每个阶段都是独立子进程，状态全部来自同一个 SQLite 文件）：
  1. seed    : 提交 3 个任务后进程退出（任务未被执行）；
  2. crash   : 启动执行者，让长任务领取租约后在执行中途强制退出进程；
  3. restart : 再次启动执行者，回收过期租约并把任务执行完成、投递确认；
  4. inspect : 打印每个任务的状态流水与最终判定。
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import tempfile
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import Settings
from app.models import RetryPolicy
from app.service import TaskService


def settings_for(db: str) -> Settings:
    s = Settings(db_path=db)
    s.task_retry = RetryPolicy(max_attempts=5, base_delay=0.05, cap_delay=0.2)
    s.delivery_retry = RetryPolicy(max_attempts=5, base_delay=0.05, cap_delay=0.2)
    s.lease_seconds = 0.5
    s.poll_interval = 0.02
    s.task_timeout = 5.0
    return s


async def seed(db: str) -> None:
    svc = TaskService(settings_for(db))
    for i in range(3):
        await svc.submit("demo-job", {"n": i}, f"demo-key-{i}")
    print("[seed] 已提交 3 个任务后进程退出（不启动执行者）")
    svc.store.close()


async def crash(db: str) -> None:
    """领取长任务 -> 在业务执行中途 os._exit 强制退出（模拟 kill -9）。"""
    svc = TaskService(settings_for(db))

    async def long_job(payload):  # pragma: no cover - 子进程内运行
        print(f"[crash] 处理器开始执行 payload={payload}，随后进程将被强杀", flush=True)
        await asyncio.sleep(30)
        return "never"

    svc.register_handler("demo-job", long_job)
    await svc.start_background()
    await asyncio.sleep(1.0)  # 确保 3 个任务中至少一个进入 RUNNING
    rows = svc.store.conn.execute(
        "SELECT id,status,lease_owner,lease_expires_at FROM tasks "
        "WHERE status='RUNNING'"
    ).fetchall()
    print(f"[crash] RUNNING 租约悬挂: "
          f"{[{k: r[k] for k in r.keys()} for r in rows]}")  # noqa: SIM118
    print("[crash] 进程以 os._exit(1) 强制退出，模拟 kill -9", flush=True)
    os._exit(1)


async def restart(db: str) -> None:
    svc = TaskService(settings_for(db))
    invoked = []

    async def demo_job(payload):
        invoked.append(payload["n"])
        return {"processed": payload["n"]}

    delivered = []

    async def demo_sink(task_id, task_type, payload, result, idem_key):
        delivered.append((idem_key, result))

    svc.register_handler("demo-job", demo_job)
    svc.register_sink("demo-job", demo_sink)
    print("[restart] 等待超过租约期后启动执行者，回收过期租约 ...")
    await svc.start_background()
    for _ in range(500):
        await asyncio.sleep(0.02)
        ok = svc.store.conn.execute(
            "SELECT COUNT(*) FROM task_outbox WHERE status='DELIVERED'"
        ).fetchone()[0]
        if ok == 3:
            break
    print(f"[restart] 处理器调用序号: {sorted(invoked)}（每个任务恰好一次）")
    print(f"[restart] 投递确认: {sorted(delivered)}")
    assert sorted(invoked) == [0, 1, 2]
    assert len(delivered) == 3
    await svc.stop_background()
    svc.store.close()
    print("[restart] 全部任务执行并投递成功，进程正常退出")


async def inspect(db: str) -> None:
    svc = TaskService(settings_for(db))
    rows = svc.store.conn.execute(
        "SELECT id,idempotency_key,status,attempt,dead_letter_reason FROM tasks "
        "ORDER BY idempotency_key"
    ).fetchall()
    for r in rows:
        print("-" * 70)
        print(f"任务 {r['idempotency_key']} -> {r['status']} attempt={r['attempt']}")
        for e in svc.store.list_events(r["id"]):
            frm = e.from_status.value if e.from_status else "-"
            to = e.to_status.value if e.to_status else "-"
            print(f"  {e.seq:>2}. {e.event_type:<16} {frm} -> {to:<11} "
                  f"actor={e.actor} {e.detail if e.detail else ''}")
    svc.store.close()
    print("-" * 70)
    print("[inspect] 判定: 崩溃前悬挂的 RUNNING 租约被重启进程回收，"
          "任务无丢失、最终恰好执行/投递一次")


PHASES = {"seed": seed, "crash": crash, "restart": restart, "inspect": inspect}


async def amain() -> None:
    logging.basicConfig(level=logging.WARNING)
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=list(PHASES))
    parser.add_argument("--db", default=None)
    args = parser.parse_args()
    db = args.db or os.path.join(tempfile.gettempdir(), "restart-demo.db")
    if args.phase == "seed":
        for suffix in ("", "-wal", "-shm"):
            p = Path(db + suffix)
            if p.exists():
                p.unlink()
    await PHASES[args.phase](db)
    if args.phase == "inspect":
        print(f"DB 文件: {db}")


if __name__ == "__main__":
    try:
        asyncio.run(amain())
    except SystemExit:
        raise
    except Exception:  # noqa: BLE001
        traceback.print_exc()
        sys.exit(2)
