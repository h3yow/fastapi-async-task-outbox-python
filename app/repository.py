"""任务仓储：所有状态跃迁在此收敛，保证原子性与可审计。"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone

from .db import Database
from .models import (
    ALLOWED_TRANSITIONS,
    TERMINAL_STATUSES,
    CancelAlreadyRequestedError,
    InvalidTransitionError,
    Task,
    TaskAlreadyTerminalError,
    TaskEvent,
    TaskNotFoundError,
    TaskProgress,
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
        cancel_requested=bool(row["cancel_requested"]),
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
                          attempt=attempt+1, cancel_requested=0, updated_at=?
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
                    # 新一轮运行开始：上一轮残留的进度作废，从“未知”重新计
                    self._db.conn.execute(
                        "DELETE FROM task_progress WHERE task_id=?", (row["id"],))
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
        """请求取消。

        - PENDING：直接落 CANCELLED（终态）；
        - RUNNING：两阶段——先置 cancel_requested 标记（状态仍 RUNNING），
          执行中的任务感知并停下后由持有者收敛到 CANCELLED；
        - 重复请求（RUNNING 且已置标记）：CancelAlreadyRequestedError；
        - 终态（SUCCEEDED/CANCELLED）：TaskAlreadyTerminalError；
        - DEAD：InvalidTransitionError（死信只能重放，不能取消）。
        三类拒绝原因可区分。
        """
        with self._db.lock, self._db.conn:
            task = self.get(task_id)
            if task.status in TERMINAL_STATUSES:
                raise TaskAlreadyTerminalError(task_id, task.status)
            if task.status is TaskStatus.RUNNING:
                if task.cancel_requested:
                    raise CancelAlreadyRequestedError(task_id)
                self._db.conn.execute(
                    "UPDATE tasks SET cancel_requested=1, updated_at=? WHERE id=?",
                    (_iso(_now()), task_id),
                )
                self._append_event(task_id, TaskStatus.RUNNING, TaskStatus.RUNNING,
                                   "cancel requested, waiting for task to stop", actor)
                return self.get(task_id)
            if task.status is TaskStatus.DEAD:
                raise InvalidTransitionError(task_id, task.status, TaskStatus.CANCELLED)
            self._db.conn.execute(
                "UPDATE tasks SET status=?, updated_at=? WHERE id=?",
                (TaskStatus.CANCELLED.value, _iso(_now()), task_id),
            )
            self._append_event(task_id, TaskStatus.PENDING, TaskStatus.CANCELLED,
                               "cancelled by requester", actor)
            return self.get(task_id)

    # ---------- 长任务：进度 / 租约续期 / 运行中取消 ----------

    def report_progress(self, task_id: str, worker_id: str,
                        percent: float | None, step: str | None) -> None:
        """执行中上报进度（upsert）。仅当前租约持有者可上报，避免回收后旧执行者写脏数据。"""
        now = _now()
        with self._db.lock, self._db.conn:
            task = self.get(task_id)
            self._assert_owner(task, worker_id)
            self._db.conn.execute(
                """INSERT INTO task_progress (task_id, percent, step, updated_at)
                   VALUES (?,?,?,?)
                   ON CONFLICT(task_id) DO UPDATE SET
                       percent=excluded.percent, step=excluded.step,
                       updated_at=excluded.updated_at""",
                (task_id, percent, step, _iso(now)),
            )

    def get_progress(self, task_id: str) -> TaskProgress | None:
        """查询进度快照；从未上报过返回 None（对外为“未知”），不报错。"""
        with self._db.lock:
            row = self._db.conn.execute(
                "SELECT * FROM task_progress WHERE task_id = ?", (task_id,)
            ).fetchone()
        if row is None:
            return None
        return TaskProgress(task_id=row["task_id"], percent=row["percent"],
                            step=row["step"], updated_at=_parse(row["updated_at"]))

    def renew_lease(self, task_id: str, worker_id: str, lease_seconds: float) -> bool:
        """长任务执行期间续租。返回是否仍持有租约（False 表示已被回收/状态已变）。"""
        now = _now()
        lease_exp = now + timedelta(seconds=lease_seconds)
        with self._db.lock, self._db.conn:
            cur = self._db.conn.execute(
                """UPDATE tasks SET lease_expires_at=?, updated_at=?
                   WHERE id=? AND status=? AND lease_owner=?""",
                (_iso(lease_exp), _iso(now), task_id,
                 TaskStatus.RUNNING.value, worker_id),
            )
            return cur.rowcount == 1

    def is_cancel_requested(self, task_id: str) -> bool:
        """执行器心跳轮询用：该任务是否被请求取消。"""
        with self._db.lock:
            row = self._db.conn.execute(
                "SELECT cancel_requested FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
        return bool(row and row["cancel_requested"])

    def complete_cancel(self, task_id: str, worker_id: str) -> Task:
        """运行中的任务感知取消并停下后，由持有者收敛到 CANCELLED。

        幂等防护：若任务已被并发地以其他方式终结（如刚好成功），
        当前状态不是 RUNNING 时抛 InvalidTransitionError，由执行器按既成事实记录日志。
        """
        with self._db.lock, self._db.conn:
            task = self.get(task_id)
            self._assert_owner(task, worker_id)
            if not task.cancel_requested:
                raise InvalidTransitionError(task_id, task.status, TaskStatus.CANCELLED)
            self._db.conn.execute(
                """UPDATE tasks SET status=?, lease_owner=NULL, lease_expires_at=NULL,
                      cancel_requested=0, updated_at=? WHERE id=?""",
                (TaskStatus.CANCELLED.value, _iso(_now()), task_id),
            )
            self._append_event(task_id, TaskStatus.RUNNING, TaskStatus.CANCELLED,
                               "cancel requested acknowledged, task stopped", worker_id)
            return self.get(task_id)

    def replay(self, task_id: str, actor: str = "operator") -> Task:
        """人工重放死信：DEAD -> PENDING，attempt 归零，历史事件完整保留。
        同时清空取消标记与进度快照：重放后的新运行进度从头开始算。"""
        with self._db.lock, self._db.conn:
            task = self.get(task_id)
            if task.status is not TaskStatus.DEAD:
                raise InvalidTransitionError(task_id, task.status, TaskStatus.PENDING)
            now = _now()
            self._db.conn.execute(
                """UPDATE tasks SET status=?, attempt=0, run_at=?, lease_owner=NULL,
                      lease_expires_at=NULL, last_error=NULL, cancel_requested=0,
                      updated_at=? WHERE id=?""",
                (TaskStatus.PENDING.value, _iso(now), _iso(now), task_id),
            )
            self._db.conn.execute(
                "DELETE FROM task_progress WHERE task_id=?", (task_id,))
            self._append_event(task_id, TaskStatus.DEAD, TaskStatus.PENDING,
                               "manual replay, history preserved, progress reset", actor)
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
