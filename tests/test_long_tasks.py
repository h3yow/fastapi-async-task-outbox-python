"""长任务场景：进度上报与查询、执行期独占（心跳续租）、运行中取消、可区分拒绝。"""
import threading
import time

import pytest

from app.executor import BackoffPolicy, Executor
from app.models import (
    CancelAlreadyRequestedError,
    InvalidTransitionError,
    TaskAlreadyTerminalError,
    TaskCancelled,
    TaskStatus,
)

from .conftest import dump_flow, wait_until


def make_executor(repo, handlers, worker_id, **kw):
    kw.setdefault("poll_interval", 0.01)
    kw.setdefault("lease_seconds", 0.3)
    kw.setdefault("backoff", BackoffPolicy(base_seconds=0.01, max_seconds=0.1))
    return Executor(repo, handlers, worker_id=worker_id, **kw)


# ---------- 1. 进度上报与查询 ----------

def test_progress_report_and_query(repo):
    t, _ = repo.submit("long", {}, "k-prog")

    # 未上报过：查询返回 None（未知），不报错
    assert repo.get_progress(t.id) is None

    claimed = repo.claim("w-1", 10)
    repo.report_progress(t.id, "w-1", 40.0, "step-2/5: transforming")
    p = repo.get_progress(t.id)
    assert p is not None and p.percent == 40.0 and "step-2/5" in p.step

    # 覆盖式更新：只保留最新快照
    repo.report_progress(t.id, "w-1", 80.0, "step-4/5: loading")
    p = repo.get_progress(t.id)
    assert p.percent == 80.0 and "step-4/5" in p.step

    # 非租约持有者上报：拒绝，防止回收后旧执行者写脏数据
    with pytest.raises(InvalidTransitionError):
        repo.report_progress(t.id, "w-other", 1.0, "stale")
    dump_flow(repo, t.id, "进度 40->80 覆盖更新；非持有者上报被拒绝 invalid_transition")


def test_progress_reset_after_replay(repo):
    """人工重放后进度从头开始算，不带上一轮。"""
    attempts = {"n": 0}

    def flaky(payload, ctx):
        attempts["n"] += 1
        ctx.report_progress(66.0, f"run-{attempts['n']} midway")
        if attempts["n"] == 1:
            raise RuntimeError("transient")

    repo.submit("flaky", {}, "k-replay-prog", max_attempts=1)
    ex = make_executor(repo, {"flaky": flaky}, "w-1")
    ex.start()
    assert wait_until(lambda: repo.list_tasks(TaskStatus.DEAD) != [], timeout=5)
    ex.stop()
    task = repo.list_tasks(TaskStatus.DEAD)[0]
    assert repo.get_progress(task.id).percent == 66.0  # 死信时仍可查到最后快照

    repo.replay(task.id)
    assert repo.get_progress(task.id) is None, "重放后进度必须清零（未知）"

    ex2 = make_executor(repo, {"flaky": flaky}, "w-2")
    ex2.start()
    assert wait_until(lambda: repo.get(task.id).status is TaskStatus.SUCCEEDED, timeout=5)
    ex2.stop()
    p = repo.get_progress(task.id)
    assert p is not None and "run-2" in p.step, "新一轮运行的进度从头开始"
    dump_flow(repo, task.id, "重放后进度清零；第二轮运行从 66% 的快照重新开始上报")


def test_progress_unknown_via_api(repo):
    """接口层：未上报进度的任务返回 reported=false，不报错。"""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from app.api import create_router
    app = FastAPI()
    app.include_router(create_router(repo))
    client = TestClient(app)

    r = client.post("/tasks", json={"task_type": "echo", "payload": {},
                                    "idempotency_key": "k-api-prog"})
    tid = r.json()["task"]["id"]
    r = client.get(f"/tasks/{tid}/progress")
    assert r.status_code == 200
    assert r.json() == {"task_id": tid, "reported": False,
                        "percent": None, "step": None, "updated_at": None}


# ---------- 2. 执行期独占：耗时远超默认租约也只被执行一次 ----------

def test_long_task_exceeds_lease_still_exactly_once(repo):
    """任务耗时 ~0.9s，租约 0.2s：心跳续租保证不被回收、不被第二个执行者重复跑。"""
    runs = []
    runs_lock = threading.Lock()

    def long_handler(payload, ctx):
        with runs_lock:
            runs.append(payload["n"])
        for i in range(9):
            time.sleep(0.1)  # 总计 ~0.9s，远超 0.2s 租约
            ctx.report_progress((i + 1) * 100.0 / 9, f"chunk {i + 1}/9")

    repo.submit("long", {"n": 1}, "k-long-lease")
    executors = [make_executor(repo, {"long": long_handler}, f"w-{i}",
                               lease_seconds=0.2) for i in range(3)]
    for ex in executors:
        ex.start()
    assert wait_until(
        lambda: repo.list_tasks(TaskStatus.SUCCEEDED) != [], timeout=15)
    # 成功后再观察一段时间：租约若被回收，会在此期间被重复执行
    time.sleep(0.6)
    for ex in executors:
        ex.stop()
    with runs_lock:
        assert runs == [1], "长任务必须恰好执行一次，租约未被中途回收"
    task = repo.list_tasks()[0]
    events = repo.list_events(task.id)
    assert not any("reclaimed" in e.reason for e in events), "执行期间不得发生租约回收"
    p = repo.get_progress(task.id)
    assert p.percent == 100.0
    dump_flow(repo, task.id, "0.9s 长任务 / 0.2s 租约：无 reclaimed，恰好执行一次")


# ---------- 3. 运行中取消：任务感知并收敛到 CANCELLED ----------

def test_cancel_running_task_converges(repo):
    started = threading.Event()

    def long_handler(payload, ctx):
        started.set()
        while True:
            ctx.throw_if_cancelled()  # 协作式取消点
            time.sleep(0.02)

    t, _ = repo.submit("long", {}, "k-cancel-run")
    ex = make_executor(repo, {"long": long_handler}, "w-1", lease_seconds=0.2)
    ex.start()
    assert started.wait(5), "任务应已开始执行"

    requested = repo.cancel(t.id, actor="tester")
    assert requested.status is TaskStatus.RUNNING and requested.cancel_requested

    assert wait_until(lambda: repo.get(t.id).status is TaskStatus.CANCELLED, timeout=5)
    ex.stop()
    task = repo.get(t.id)
    assert task.status is TaskStatus.CANCELLED
    events = repo.list_events(t.id)
    reasons = [e.reason for e in events]
    assert any("cancel requested" in r for r in reasons), "取消请求必须留痕"
    assert any("acknowledged" in r for r in reasons), "任务感知取消并收敛必须留痕"
    dump_flow(repo, t.id, "RUNNING --cancel requested--> RUNNING --acknowledged--> CANCELLED")


def test_handler_may_raise_task_cancelled_itself(repo):
    """handler 也可自行抛 TaskCancelled（如轮询到 is_cancel_requested 后）。"""
    started = threading.Event()

    def long_handler(payload, ctx):
        started.set()
        while not ctx.is_cancel_requested():
            time.sleep(0.02)
        raise TaskCancelled("stopped by requester")

    t, _ = repo.submit("long", {}, "k-cancel-self")
    ex = make_executor(repo, {"long": long_handler}, "w-1", lease_seconds=0.2)
    ex.start()
    assert started.wait(5)
    repo.cancel(t.id)
    assert wait_until(lambda: repo.get(t.id).status is TaskStatus.CANCELLED, timeout=5)
    ex.stop()
    dump_flow(repo, t.id, "handler 自抛 TaskCancelled，同样收敛到 CANCELLED")


# ---------- 4. 可区分的拒绝：重复取消 / 终态取消 / 死信取消 ----------

def test_cancel_rejections_are_distinguishable(repo):
    started = threading.Event()

    def long_handler(payload, ctx):
        started.set()
        while not ctx.is_cancel_requested():
            time.sleep(0.02)

    t, _ = repo.submit("long", {}, "k-cancel-dup")
    ex = make_executor(repo, {"long": long_handler}, "w-1", lease_seconds=0.2)
    ex.start()
    assert started.wait(5)

    repo.cancel(t.id)
    # 重复取消（已置标记）：cancel_already_requested
    with pytest.raises(CancelAlreadyRequestedError) as ei:
        repo.cancel(t.id)
    assert ei.value.code == "cancel_already_requested"

    assert wait_until(lambda: repo.get(t.id).status is TaskStatus.CANCELLED, timeout=5)
    ex.stop()
    # 终态后再取消：task_already_terminal
    with pytest.raises(TaskAlreadyTerminalError) as ei2:
        repo.cancel(t.id)
    assert ei2.value.code == "task_already_terminal"

    # 死信不可取消（只能重放）：invalid_transition
    def bad(payload):
        raise RuntimeError("boom")

    repo.submit("bad", {}, "k-cancel-dead", max_attempts=1)
    ex2 = make_executor(repo, {"bad": bad}, "w-2")
    ex2.start()
    assert wait_until(lambda: repo.list_tasks(TaskStatus.DEAD) != [], timeout=5)
    ex2.stop()
    dead = repo.list_tasks(TaskStatus.DEAD)[0]
    with pytest.raises(InvalidTransitionError) as ei3:
        repo.cancel(dead.id)
    assert ei3.value.code == "invalid_transition"
    dump_flow(repo, t.id, "重复取消/终态取消/死信取消分别得到三种可区分 code")


# ---------- 5. 取消与进度上报并发：状态不被带乱 ----------

def test_cancel_and_progress_concurrent(repo):
    started = threading.Event()

    def long_handler(payload, ctx):
        started.set()
        for i in range(200):
            ctx.throw_if_cancelled()
            ctx.report_progress(i / 2.0, f"iter {i}")
            time.sleep(0.005)

    t, _ = repo.submit("long", {}, "k-cancel-race")
    ex = make_executor(repo, {"long": long_handler}, "w-1", lease_seconds=0.2)
    ex.start()
    assert started.wait(5)

    # 取消请求与进度上报并发发生
    time.sleep(0.1)
    repo.cancel(t.id)
    assert wait_until(lambda: repo.get(t.id).status is TaskStatus.CANCELLED, timeout=5)
    ex.stop()

    task = repo.get(t.id)
    assert task.status is TaskStatus.CANCELLED
    p = repo.get_progress(t.id)
    assert p is not None and 0.0 <= p.percent <= 100.0, "进度快照必须合法"
    # 状态流水完整且单调：PENDING -> RUNNING -> (cancel requested) -> CANCELLED
    flow = [(e.from_status, e.to_status) for e in repo.list_events(t.id)]
    statuses = [flow[0][1]] + [to for _, to in flow]
    assert statuses[0] is TaskStatus.PENDING and statuses[-1] is TaskStatus.CANCELLED
    dump_flow(repo, t.id, "取消与进度并发：状态收敛 CANCELLED，流水完整可追溯")
