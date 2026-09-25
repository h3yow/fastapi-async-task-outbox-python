import logging
import threading
import time

import pytest

from app.db import Database
from app.executor import BackoffPolicy, Executor
from app.models import TaskStatus
from app.repository import TaskRepository

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")


@pytest.fixture()
def repo():
    db = Database(":memory:")
    yield TaskRepository(db)
    db.close()


def wait_until(predicate, timeout=5.0, interval=0.01):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def dump_flow(repo, task_id, verdict=""):
    """打印任务状态流水与判定依据。"""
    print(f"\n===== task {task_id} 状态流水 =====")
    for e in repo.list_events(task_id):
        frm = e.from_status.value if e.from_status else "-"
        print(f"  [{e.created_at}] {frm} -> {e.to_status.value} "
              f"| actor={e.actor} | reason={e.reason}")
    if verdict:
        print(f"  判定依据: {verdict}")
