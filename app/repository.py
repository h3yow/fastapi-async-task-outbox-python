"""任务仓储：所有状态跃迁在此收敛，保证原子性与可审计。"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone

from .db import Database
from .models import (
    ALLOWED_TRANSITIONS,
    TERMINAL_STATUSES,
    InvalidTransitionError,
    Task,
    TaskAlreadyTerminalError,
    TaskEvent,
    TaskNotFoundError,
    TaskStatus,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None


def _parse(s: str | None) -> datetime | None:
    return datetime.fromisoformat(s) if s else None


def _row_to_task(row) -> Task:
    return Task(
        id=row["id"],
        idempotency_key=row["idempotency_key"],
        task_type=row["task_type"],
        payload=row["payload"],
        status=TaskStatus(row["status"]),
        attempt=row["attempt"],
        max_attempts=row["max_attempts"],
        run_at=_parse(row["run_at"]),
        lease_owner=row["lease_owner"],
        lease_expires_at=_parse(row["lease_expires_at"]),
        created_at=_parse(row["created_at"]),
        updated_at=_parse(row["updated_at"]),
        last_error=row["last_error"],
    )


class TaskRepository:
    def __init__(self, db: Database):
        self._db = db

    # ---------- 查询 ----------

    def get(self, task_id: str) -> Task:
        with self._db.lock:
            row = self._db.conn.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
        if row is None:
            raise TaskNotFoundError(f"task {task_id} not found")
        return _row_to_task(row)

    def get_by_idempotency_key(self, key: str) -> Task | None:
        with self._db.lock:
            row = self._db.conn.execute(
                "SELECT * FROM tasks WHERE idempotency_key = ?", (key,)
            ).fetchone()
        return _row_to_task(row) if row else None

    def list_events(self, task_id: str) -> list[TaskEvent]:
        with self._db.lock:
            rows = self._db.conn.execute(
                "SELECT * FROM task_events WHERE task_id = ? ORDER BY id", (task_id,)
            ).fetchall()
        return [
            TaskEvent(
                id=r["id"],
                task_id=r["task_id"],
                from_status=TaskStatus(r["from_status"]) if r["from_status"] else None,
                to_status=TaskStatus(r["to_status"]),
                reason=r["reason"],
                actor=r["actor"],
                created_at=_parse(r["created_at"]),
            )
            for r in rows
        ]

    def list_tasks(self, status: TaskStatus | None = None) -> list[Task]:
        sql = "SELECT * FROM tasks"
        args: tuple = ()
        if status is not None:
            sql += " WHERE status = ?"
            args = (status.value,)
        sql += " ORDER BY created_at"
        with self._db.lock:
            rows = self._db.conn.execute(sql, args).fetchall()
        return [_row_to_task(r) for r in rows]

    # ---------- 写入（全部在锁内完成，事件与状态同事务） ----------

    def submit(
        self,
        task_type: str,
        payload: dict,
        idempotency_key: str,
        max_attempts: int = 3,
        actor: str = "api",
    ) -> tuple[Task, bool]:
        """幂等提交。返回 (task, created)。键已存在时返回原任务，不新增记录。"""
        now = _now()
        with self._db.lock, self._db.conn:
            existing = self._db.conn.execute(
                "SELECT * FROM tasks WHERE idempotency_key = ?", (idempotency_key,)
            ).fetchone()
            if existing is not None:
                return _row_to_task(existing), False
            task_id = uuid.uuid4().hex
            self._db.conn.execute(
                """INSERT INTO tasks
                   (id, idempotency_key, task_type, payload, status, attempt,
                    max_attempts, run_at, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (task_id, idempotency_key, task_type, json.dumps(payload),
                 TaskStatus.PENDING.value, 0, max_attempts,
                 _iso(now), _iso(now), _iso(now)),
            )
            self._append_event(task_id, None, TaskStatus.PENDING,
                               f"submitted type={task_type}", actor)
            return self.get(task_id), True

    def claim(self, worker_id: str, lease_seconds: float, limit: int = 1) -> list[Task]:
        """原子抢占：PENDING 且到点，或 RUNNING 但租约过期（进程崩溃恢复）。"""
        now = _now()
        lease_exp = now + timedelta(seconds=lease_seconds)
        claimed: list[Task] = []
        with self._db.lock, self._db.conn:
            rows = self._db.conn.execute(
                """SELECT id, status FROM tasks
                   WHERE (status = ? AND run_at <= ?)
                      OR (status = ? AND lease_expires_at <= ?)
                   ORDER BY run_at LIMIT ?""",
                (TaskStatus.PENDING.value, _iso(now),
                 TaskStatus.RUNNING.value, _iso(now), limit),
            ).fetchall()
            for row in rows:
                # 条件更新保证同一任务只被一个执行者抢占
                cur = self._db.conn.execute(
                    """UPDATE tasks SET status=?, lease_owner=?, lease_expires_at=?,
                          attempt=attempt+1, updated_at=?
                       WHERE id=? AND status=? AND
                             (lease_expires_at IS NULL OR lease_expires_at <= ? OR lease_owner=?)""",
                    (TaskStatus.RUNNING.value, worker_id, _iso(lease_exp), _iso(now),
                     row["id"], row["status"], _iso(now), worker_id),
                )
                if cur.rowcount == 1:
                    frm = TaskStatus(row["status"])
                    reason = ("claimed" if frm == TaskStatus.PENDING
                              else "lease expired, reclaimed after crash")
                    self._append_event(row["id"], frm, TaskStatus.RUNNING, reason, worker_id)
                    claimed.append(self.get(row["id"]))
        return claimed

    def succeed(self, task_id: str, worker_id: str) -> Task:
        return self._finish(task_id, worker_id, TaskStatus.SUCCEEDED,
                            "handler succeeded", clear_error=True)

    def fail(
        self,
        task_id: str,
        worker_id: str,
        error: str,
        *,
        retryable: bool,
        backoff_seconds: float = 0.0,
    ) -> Task:
        """失败处理：可重试且未超上限 -> 回 PENDING（带退避）；否则进 DEAD。"""
        with self._db.lock, self._db.conn:
            task = self.get(task_id)
            self._assert_owner(task, worker_id)
            if retryable and task.attempt < task.max_attempts:
                run_at = _now() + timedelta(seconds=backoff_seconds)
                self._db.conn.execute(
                    """UPDATE tasks SET status=?, run_at=?, lease_owner=NULL,
                          lease_expires_at=NULL, last_error=?, updated_at=?
                       WHERE id=?""",
                    (TaskStatus.PENDING.value, _iso(run_at), error, _iso(_now()), task_id),
                )
                self._append_event(task_id, TaskStatus.RUNNING, TaskStatus.PENDING,
                                   f"retryable failure (attempt {task.attempt}/"
                                   f"{task.max_attempts}), backoff {backoff_seconds:.3f}s: {error}",
                                   worker_id)
            else:
                reason = ("non-retryable business failure" if not retryable
                          else f"retry budget exhausted ({task.attempt}/{task.max_attempts})")
                self._db.conn.execute(
                    """UPDATE tasks SET status=?, lease_owner=NULL, lease_expires_at=NULL,
                          last_error=?, updated_at=? WHERE id=?""",
                    (TaskStatus.DEAD.value, error, _iso(_now()), task_id),
                )
                self._append_event(task_id, TaskStatus.RUNNING, TaskStatus.DEAD,
                                   f"{reason}: {error}", worker_id)
            return self.get(task_id)

    def timeout_to_dead(self, task_id: str, worker_id: str, timeout_seconds: float) -> Task:
        with self._db.lock, self._db.conn:
            task = self.get(task_id)
            self._assert_owner(task, worker_id)
            self._db.conn.execute(
                """UPDATE tasks SET status=?, lease_owner=NULL, lease_expires_at=NULL,
                      last_error=?, updated_at=? WHERE id=?""",
                (TaskStatus.DEAD.value, f"execution timeout after {timeout_seconds}s",
                 _iso(_now()), task_id),
            )
            self._append_event(task_id, TaskStatus.RUNNING, TaskStatus.DEAD,
                               f"execution timeout after {timeout_seconds}s", worker_id)
            return self.get(task_id)

    def cancel(self, task_id: str, actor: str = "api") -> Task:
        """取消。仅 PENDING 可取消；终态重复取消会被拒绝并给出可区分原因。"""
        with self._db.lock, self._db.conn:
            task = self.get(task_id)
            if task.status in TERMINAL_STATUSES:
                raise TaskAlreadyTerminalError(task_id, task.status)
            if task.status is TaskStatus.RUNNING:
                raise InvalidTransitionError(task_id, task.status, TaskStatus.CANCELLED)
            self._db.conn.execute(
                "UPDATE tasks SET status=?, updated_at=? WHERE id=?",
                (TaskStatus.CANCELLED.value, _iso(_now()), task_id),
            )
            self._append_event(task_id, TaskStatus.PENDING, TaskStatus.CANCELLED,
                               "cancelled by requester", actor)
            return self.get(task_id)

    def replay(self, task_id: str, actor: str = "operator") -> Task:
        """人工重放死信：DEAD -> PENDING，attempt 归零，历史事件完整保留。"""
        with self._db.lock, self._db.conn:
            task = self.get(task_id)
            if task.status is not TaskStatus.DEAD:
                raise InvalidTransitionError(task_id, task.status, TaskStatus.PENDING)
            now = _now()
            self._db.conn.execute(
                """UPDATE tasks SET status=?, attempt=0, run_at=?, lease_owner=NULL,
                      lease_expires_at=NULL, last_error=NULL, updated_at=? WHERE id=?""",
                (TaskStatus.PENDING.value, _iso(now), _iso(now), task_id),
            )
            self._append_event(task_id, TaskStatus.DEAD, TaskStatus.PENDING,
                               "manual replay, history preserved", actor)
            return self.get(task_id)

    # ---------- 内部 ----------

    def _finish(self, task_id: str, worker_id: str, target: TaskStatus,
                reason: str, clear_error: bool = False) -> Task:
        with self._db.lock, self._db.conn:
            task = self.get(task_id)
            self._assert_owner(task, worker_id)
            self._db.conn.execute(
                """UPDATE tasks SET status=?, lease_owner=NULL, lease_expires_at=NULL,
                      last_error=?, updated_at=? WHERE id=?""",
                (target.value, None if clear_error else task.last_error,
                 _iso(_now()), task_id),
            )
            self._append_event(task_id, TaskStatus.RUNNING, target, reason, worker_id)
            return self.get(task_id)

    def _assert_owner(self, task: Task, worker_id: str) -> None:
        if task.status is not TaskStatus.RUNNING or task.lease_owner != worker_id:
            raise InvalidTransitionError(task.id, task.status, TaskStatus.RUNNING)

    def _append_event(self, task_id: str, from_status: TaskStatus | None,
                      to_status: TaskStatus, reason: str, actor: str) -> None:
        self._db.conn.execute(
            """INSERT INTO task_events (task_id, from_status, to_status, reason, actor, created_at)
               VALUES (?,?,?,?,?,?)""",
            (task_id, from_status.value if from_status else None,
             to_status.value, reason, actor, _iso(_now())),
        )
