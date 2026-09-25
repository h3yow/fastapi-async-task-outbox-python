"""场景测试：幂等提交、并发抢占、重启恢复、死信重放、取消与非法跃迁。"""
import threading
import time

import pytest

from app.executor import BackoffPolicy, Executor
from app.models import (
    BusinessFailure,
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


# ---------- 1. 幂等提交 ----------

def test_idempotent_submit(repo):
    t1, c1 = repo.submit("echo", {"n": 1}, "key-1")
    t2, c2 = repo.submit("echo", {"n": 1}, "key-1")
    assert c1 is True and c2 is False
    assert t1.id == t2.id
    assert len(repo.list_tasks()) == 1
    dump_flow(repo, t1.id, "重复提交返回原任务，事件流水中只有一次 submitted")


# ---------- 2. 并发抢占：同一任务只被执行一次 ----------

def test_concurrent_claim_exactly_once(repo):
    done, lock = [], threading.Lock()

    def handler(payload):
        with lock:
            done.append(payload["n"])
        time.sleep(0.005)

    for i in range(30):
        repo.submit("count", {"n": i}, f"k-{i}")
    executors = [make_executor(repo, {"count": handler}, f"w-{i}") for i in range(5)]
    for ex in executors:
        ex.start()
    assert wait_until(lambda: len(repo.list_tasks(TaskStatus.SUCCEEDED)) == 30, timeout=15)
    for ex in executors:
        ex.stop()
    assert sorted(done) == list(range(30)), "每个任务必须恰好被执行一次"
    t = repo.list_tasks()[0]
    dump_flow(repo, t.id, "30 个任务各有一次 claimed 与 succeeded，无重复执行")


# ---------- 3. 重启恢复：执行中途崩溃，租约过期后被回收，不丢不重 ----------

def test_restart_recovery_after_crash(repo):
    effects = []

    def handler(payload):
        effects.append(payload["n"])  # 业务副作用只应生效一次

    # 第一次“进程”：抢占后模拟崩溃（不归还租约、不写结果）
    repo.submit("count", {"n": 7}, "k-7")
    ex1 = make_executor(repo, {"count": handler}, "worker-old", lease_seconds=0.2)
    claimed = ex1._repo.claim("worker-old", 0.2)
    assert len(claimed) == 1  # 抢到了，但进程随即崩溃，handler 未执行

    # 租约未过期时，新进程抢不到
    ex2 = make_executor(repo, {"count": handler}, "worker-new", lease_seconds=0.2)
    assert ex2.run_once() is False
    # 租约过期后，新进程回收并执行
    time.sleep(0.25)
    assert ex2.run_once() is True
    task = repo.get(claimed[0].id)
    assert task.status is TaskStatus.SUCCEEDED
    assert effects == [7], "崩溃恢复后副作用恰好生效一次"
    dump_flow(repo, task.id, "lease expired, reclaimed -> 任务未丢失且未重复生效")


# ---------- 4. 死信：重试上限 / 业务明确失败 / 超时，随后人工重放 ----------

def test_retry_exhaustion_then_replay(repo):
    attempts = {"n": 0}

    def flaky(payload):
        attempts["n"] += 1
        if attempts["n"] <= 3:
            raise RuntimeError("transient")
        # 第 4 次（重放后）成功

    repo.submit("flaky", {}, "k-flaky", max_attempts=3)
    ex = make_executor(repo, {"flaky": flaky}, "w-1")
    ex.start()
    assert wait_until(lambda: repo.list_tasks(TaskStatus.DEAD) != [], timeout=10)
    ex.stop()
    task = repo.list_tasks(TaskStatus.DEAD)[0]
    assert task.attempt == 3 and "transient" in task.last_error

    # 人工重放：回到 PENDING，历史保留
    replayed = repo.replay(task.id, actor="operator-tester")
    assert replayed.status is TaskStatus.PENDING and replayed.attempt == 0
    events_before = len(repo.list_events(task.id))

    ex2 = make_executor(repo, {"flaky": flaky}, "w-2")
    ex2.start()
    assert wait_until(lambda: repo.get(task.id).status is TaskStatus.SUCCEEDED, timeout=10)
    ex2.stop()
    events = repo.list_events(task.id)
    assert len(events) > events_before, "重放后的执行历史必须追加保留"
    assert any(e.reason.startswith("manual replay") for e in events)
    dump_flow(repo, task.id, "3 次失败进 DEAD；重放后成功；全部历史可追溯")


def test_business_failure_goes_dead_without_retry(repo):
    def bad(payload):
        raise BusinessFailure("invalid order")

    repo.submit("bad", {}, "k-bad", max_attempts=5)
    ex = make_executor(repo, {"bad": bad}, "w-1")
    ex.start()
    assert wait_until(lambda: repo.list_tasks(TaskStatus.DEAD) != [], timeout=5)
    ex.stop()
    task = repo.list_tasks(TaskStatus.DEAD)[0]
    assert task.attempt == 1, "业务明确失败不得重试"
    dump_flow(repo, task.id, "BusinessFailure 一次即进 DEAD，不消耗重试预算")


def test_execution_timeout_goes_dead(repo):
    def slow(payload):
        time.sleep(5)

    repo.submit("slow", {}, "k-slow")
    ex = make_executor(repo, {"slow": slow}, "w-1", execution_timeout=0.1)
    ex.start()
    assert wait_until(lambda: repo.list_tasks(TaskStatus.DEAD) != [], timeout=5)
    ex.stop()
    task = repo.list_tasks(TaskStatus.DEAD)[0]
    assert "timeout" in (task.last_error or "")
    dump_flow(repo, task.id, "执行超时进入 DEAD，失败未被静默吞掉")


# ---------- 5. 取消与非法跃迁 ----------

def test_cancel_rules(repo):
    t, _ = repo.submit("echo", {}, "k-cancel")
    cancelled = repo.cancel(t.id)
    assert cancelled.status is TaskStatus.CANCELLED
    # 已结束任务重复取消：拒绝，原因可区分
    with pytest.raises(TaskAlreadyTerminalError) as ei:
        repo.cancel(t.id)
    assert ei.value.code == "task_already_terminal"

    # RUNNING 状态不可取消：非法跃迁
    t2, _ = repo.submit("echo", {}, "k-cancel-2")
    repo.claim("w-x", 10)
    with pytest.raises(InvalidTransitionError) as ei2:
        repo.cancel(t2.id)
    assert ei2.value.code == "invalid_transition"

    # 死信重放的非法跃迁：非 DEAD 不可 replay
    with pytest.raises(InvalidTransitionError):
        repo.replay(t2.id)
    dump_flow(repo, t.id, "重复取消被拒绝 task_already_terminal；RUNNING 取消被拒绝 invalid_transition")
