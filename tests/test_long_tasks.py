"""长任务场景：进度上报与查询、超租约时长独占、运行中取消、并发安全。"""
import threading
import time

import pytest

from app.executor import BackoffPolicy, Executor
from app.models import (
    InvalidTransitionError,
    TaskAlreadyTerminalError,
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
    t, _ = repo.submit("staged", {}, "k-prog")

    # 未上报过进度的任务：查询返回“未知”，不报错
    assert repo.get_progress(t.id) is None

    started = threading.Event()

    def handler(payload, ctx):
        started.set()
        for i in range(1, 4):
            ctx.report_progress(percent=i * 25.0, step=f"stage-{i}",
                                message=f"processing stage {i}")
            time.sleep(0.05)

    ex = make_executor(repo, {"staged": handler}, "w-1")
    ex.start()
    assert started.wait(5)
    # 执行过程中可随时查到最新进度
    assert wait_until(lambda: (repo.get_progress(t.id) is not None
                               and repo.get_progress(t.id).percent >= 50.0), timeout=5)
    p = repo.get_progress(t.id)
    assert p.step in ("stage-2", "stage-3") and p.message
    assert wait_until(lambda: repo.get(t.id).status is TaskStatus.SUCCEEDED, timeout=5)
    ex.stop()
    # 终态后进度仍可查（最后一轮纪元的快照）
    final = repo.get_progress(t.id)
    assert final is not None and final.percent == 75.0
    dump_flow(repo, t.id, "执行中进度可实时查询；未上报时按未知处理不报错")


def test_progress_reset_after_replay(repo):
    attempts = {"n": 0}

    def handler(payload, ctx):
        attempts["n"] += 1
        ctx.report_progress(percent=80.0, step="almost-done")
        if attempts["n"] == 1:
            raise RuntimeError("boom")  # 第一轮失败进 DEAD（max_attempts=1）

    t, _ = repo.submit("staged", {}, "k-prog-replay", max_attempts=1)
    ex = make_executor(repo, {"staged": handler}, "w-1")
    ex.start()
    assert wait_until(lambda: repo.get(t.id).status is TaskStatus.DEAD, timeout=5)
    ex.stop()
    assert repo.get_progress(t.id).percent == 80.0  # 死信时仍可见最后一轮进度

    repo.replay(t.id)
    assert repo.get_progress(t.id) is None, "人工重放后进度必须从头开始（未知）"

    ex2 = make_executor(repo, {"staged": handler}, "w-2")
    ex2.start()
    assert wait_until(lambda: repo.get(t.id).status is TaskStatus.SUCCEEDED, timeout=5)
    ex2.stop()
    p = repo.get_progress(t.id)
    assert p is not None and p.percent == 80.0, "新一轮执行的进度是新纪元的快照"
    dump_flow(repo, t.id, "重放后旧进度清零，新一轮执行重新上报")


# ---------- 2. 超过默认租约时长依然独占、只执行一次 ----------

def test_long_task_outlives_lease_executed_exactly_once(repo):
    runs, lock = [], threading.Lock()

    def handler(payload, ctx):
        with lock:
            runs.append(threading.current_thread().name)
        # 运行 1.2s，远超 0.3s 的默认租约；期间执行器心跳续租保持独占
        for i in range(12):
            ctx.report_progress(percent=(i + 1) / 12 * 100, step=f"chunk-{i + 1}")
            time.sleep(0.1)

    repo.submit("long", {}, "k-long")
    executors = [make_executor(repo, {"long": handler}, f"w-{i}") for i in range(3)]
    for ex in executors:
        ex.start()
    assert wait_until(
        lambda: repo.list_tasks(TaskStatus.SUCCEEDED) != [], timeout=15)
    for ex in executors:
        ex.stop()
    assert len(runs) == 1, "长任务超过租约时长也必须只被执行一次"
    task = repo.list_tasks()[0]
    events = repo.list_events(task.id)
    claims = [e for e in events if e.to_status is TaskStatus.RUNNING
              and "claim" in e.reason or "reclaimed" in e.reason]
    assert len(claims) == 1, "执行期间不得被第二个执行者回收/抢占"
    dump_flow(repo, task.id,
              "运行 1.2s > 租约 0.3s：心跳续租保持独占，无 reclaimed，恰好执行一次")


# ---------- 3. 运行中取消：感知、收敛、可追溯 ----------

def test_cancel_running_task_converges(repo):
    observed_cancel = threading.Event()
    finished_normally = threading.Event()

    def handler(payload, ctx):
        for i in range(200):
            ctx.throw_if_cancelled()
            ctx.report_progress(percent=min(i, 99), step=f"step-{i}")
            time.sleep(0.01)
        finished_normally.set()

    t, _ = repo.submit("long", {}, "k-cancel-run")
    ex = make_executor(repo, {"long": handler}, "w-1")
    ex.start()
    # 等任务真正跑起来并上报过进度
    assert wait_until(lambda: repo.get_progress(t.id) is not None, timeout=5)

    # 请求方发起取消：RUNNING 不直接变终态，而是置取消标记
    resp = repo.request_cancel(t.id, actor="api-tester")
    assert resp.status is TaskStatus.RUNNING and resp.cancel_requested
    assert resp.cancel_requested_by == "api-tester"

    # 执行者尽快感知并收敛到 CANCELLED 终态
    assert wait_until(lambda: repo.get(t.id).status is TaskStatus.CANCELLED, timeout=5)
    ex.stop()
    assert not finished_normally.is_set(), "被取消的任务不得跑到正常结束"

    events = repo.list_events(t.id)
    req = [e for e in events if "cancel requested by api-tester" in e.reason]
    ack = [e for e in events if e.to_status is TaskStatus.CANCELLED
           and e.from_status is TaskStatus.RUNNING]
    assert req and ack, "取消请求与执行者确认都必须可追溯"
    dump_flow(repo, t.id, "RUNNING 置取消标记 -> 执行者感知 -> 收敛 CANCELLED")


# ---------- 4. 重复取消与不允许的取消：可区分拒绝 ----------

def test_cancel_rejections_are_distinguishable(repo):
    # 终态重复取消 -> task_already_terminal
    t1, _ = repo.submit("echo", {}, "k-c1")
    repo.request_cancel(t1.id)
    assert repo.get(t1.id).status is TaskStatus.CANCELLED
    with pytest.raises(TaskAlreadyTerminalError) as e1:
        repo.request_cancel(t1.id)
    assert e1.value.code == "task_already_terminal"

    # 已成功任务再取消 -> task_already_terminal
    repo.submit("echo", {}, "k-c2")
    ex = make_executor(repo, {"echo": lambda p: None}, "w-1")
    ex.start()
    t2 = repo.get_by_idempotency_key("k-c2")
    assert wait_until(lambda: repo.get(t2.id).status is TaskStatus.SUCCEEDED, timeout=5)
    ex.stop()
    with pytest.raises(TaskAlreadyTerminalError) as e2:
        repo.request_cancel(t2.id)
    assert e2.value.code == "task_already_terminal"

    # 死信不可取消（只能重放）-> invalid_transition
    def boom(payload):
        raise RuntimeError("x")

    repo.submit("boom", {}, "k-c3", max_attempts=1)
    ex2 = make_executor(repo, {"boom": boom}, "w-2")
    ex2.start()
    t3 = repo.get_by_idempotency_key("k-c3")
    assert wait_until(lambda: repo.get(t3.id).status is TaskStatus.DEAD, timeout=5)
    ex2.stop()
    with pytest.raises(InvalidTransitionError) as e3:
        repo.request_cancel(t3.id)
    assert e3.value.code == "invalid_transition"
    dump_flow(repo, t3.id, "终态取消=task_already_terminal；DEAD 取消=invalid_transition")


def test_duplicate_cancel_request_is_idempotent(repo):
    def handler(payload, ctx):
        for _ in range(200):
            ctx.throw_if_cancelled()
            time.sleep(0.01)

    t, _ = repo.submit("long", {}, "k-dup-cancel")
    ex = make_executor(repo, {"long": handler}, "w-1")
    ex.start()
    assert wait_until(lambda: repo.get(t.id).status is TaskStatus.RUNNING, timeout=5)

    r1 = repo.request_cancel(t.id, actor="api")
    r2 = repo.request_cancel(t.id, actor="api")  # 重复请求：幂等，不报错
    assert r1.cancel_requested and r2.cancel_requested
    assert wait_until(lambda: repo.get(t.id).status is TaskStatus.CANCELLED, timeout=5)
    ex.stop()
    req_events = [e for e in repo.list_events(t.id)
                  if "waiting for worker" in e.reason]
    assert len(req_events) == 1, "重复取消请求只记录一次，状态不被带乱"
    dump_flow(repo, t.id, "运行中重复取消请求幂等，仅一条 cancel requested 事件")


# ---------- 5. 取消与进度上报并发：状态不被带乱 ----------

def test_cancel_races_with_progress_reporting(repo):
    def handler(payload, ctx):
        for i in range(1000):
            ctx.report_progress(percent=i % 100, step=f"s-{i}")
            ctx.throw_if_cancelled()
            time.sleep(0.002)

    t, _ = repo.submit("long", {}, "k-race")
    ex = make_executor(repo, {"long": handler}, "w-1")
    ex.start()
    assert wait_until(lambda: repo.get_progress(t.id) is not None, timeout=5)

    # 取消与高频进度上报并发发生
    repo.request_cancel(t.id, actor="api-racer")
    assert wait_until(lambda: repo.get(t.id).status is TaskStatus.CANCELLED, timeout=5)
    ex.stop()

    task = repo.get(t.id)
    assert task.status is TaskStatus.CANCELLED and task.lease_owner is None
    terminal = [e for e in repo.list_events(t.id)
                if e.to_status in (TaskStatus.SUCCEEDED, TaskStatus.CANCELLED,
                                   TaskStatus.DEAD)]
    assert len(terminal) == 1 and terminal[0].to_status is TaskStatus.CANCELLED, \
        "并发下只能收敛到一个终态，且是 CANCELLED"
    dump_flow(repo, t.id, "取消与进度并发：唯一终态 CANCELLED，租约正常释放")
