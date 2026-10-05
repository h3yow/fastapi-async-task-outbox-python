"""任务仓储：所有状态跃迁在此收敛，保证原子性与可审计。"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone

from .db import Database
from .models import (
    ALLOWED_TRANSITIONS,
    TERMINAL_STATUSES,
    DomainError,
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
        epoch=row["epoch"],
        cancel_requested=bool(row["cancel_requested"]),
        cancel_requested_by=row["cancel_requested_by"],
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
                # 条件更新保证同一任务只被一个执行者抢占；
                # epoch+1 开启新执行纪元，上一纪元的进度/续期写入随之失效；
                # 同时清掉上一轮的取消标记，新一轮执行不继承旧取消请求。
                cur = self._db.conn.execute(
                    """UPDATE tasks SET status=?, lease_owner=?, lease_expires_at=?,
                          attempt=attempt+1, epoch=epoch+1,
                          cancel_requested=0, cancel_requested_by=NULL, updated_at=?
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

    def succeed(self, task_id: str, worker_id: str, epoch: int | None = None) -> Task:
        return self._finish(task_id, worker_id, TaskStatus.SUCCEEDED,
                            "handler succeeded", clear_error=True, epoch=epoch)

    def fail(
        self,
        task_id: str,
        worker_id: str,
        error: str,
        *,
        retryable: bool,
        backoff_seconds: float = 0.0,
        epoch: int | None = None,
    ) -> Task:
        """失败处理：可重试且未超上限 -> 回 PENDING（带退避）；否则进 DEAD。"""
        with self._db.lock, self._db.conn:
            task = self.get(task_id)
            self._assert_owner(task, worker_id, epoch)
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

    def timeout_to_dead(self, task_id: str, worker_id: str, timeout_seconds: float,
                        epoch: int | None = None) -> Task:
        with self._db.lock, self._db.conn:
            task = self.get(task_id)
            self._assert_owner(task, worker_id, epoch)
            # 执行超时前已被请求取消：收敛到 CANCELLED 而非 DEAD，取消可追溯
            if task.cancel_requested:
                return self._confirm_cancelled_locked(task, worker_id,
                                                      f"cancel requested by "
                                                      f"{task.cancel_requested_by or 'unknown'}; "
                                                      f"execution timeout forced stop")
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

    # ---------- 长任务：进度 / 租约续期 / 运行中取消 ----------

    def report_progress(self, task_id: str, worker_id: str, epoch: int,
                        percent: float | None = None, step: str | None = None,
                        message: str | None = None) -> None:
        """执行者上报进度。仅当前租约持有者 + 当前纪元可写。

        与取消并发安全：取消只动 tasks 行的标记位，进度写 task_progress 行，
        两者在同一锁内各自校验纪元，不会互相覆盖状态。
        """
        if percent is not None and not 0.0 <= percent <= 100.0:
            raise DomainError(f"percent out of range: {percent}")
        with self._db.lock, self._db.conn:
            task = self.get(task_id)
            self._assert_owner(task, worker_id, epoch)
            self._db.conn.execute(
                """INSERT INTO task_progress (task_id, epoch, percent, step, message, updated_at)
                   VALUES (?,?,?,?,?,?)
                   ON CONFLICT(task_id) DO UPDATE SET
                       epoch=excluded.epoch, percent=excluded.percent,
                       step=excluded.step, message=excluded.message,
                       updated_at=excluded.updated_at""",
                (task_id, epoch, percent, step, message, _iso(_now())),
            )

    def get_progress(self, task_id: str) -> TaskProgress | None:
        """查询当前纪元的最新进度；未上报过或纪元已过期返回 None（未知）。

        人工重放后任务进入新纪元，旧纪元的进度随之失效，查询回到“未知”。
        """
        with self._db.lock:
            task = self.get(task_id)  # 任务不存在时照常 404
            row = self._db.conn.execute(
                "SELECT * FROM task_progress WHERE task_id = ?", (task_id,)
            ).fetchone()
        if row is None or row["epoch"] != task.epoch:
            return None
        return TaskProgress(
            task_id=row["task_id"], epoch=row["epoch"], percent=row["percent"],
            step=row["step"], message=row["message"],
            updated_at=_parse(row["updated_at"]),
        )

    def renew_lease(self, task_id: str, worker_id: str, epoch: int,
                    lease_seconds: float) -> Task:
        """心跳续租：仅当前持有者 + 当前纪元可续。

        长任务在执行期间持续续租，租约就不会到期，系统不会将其回收或交给
        第二个执行者；持有者丢失（租约已被回收/任务已终态）时抛错，执行者
        据此尽快放弃执行。
        """
        now = _now()
        lease_exp = now + timedelta(seconds=lease_seconds)
        with self._db.lock, self._db.conn:
            task = self.get(task_id)
            self._assert_owner(task, worker_id, epoch)
            self._db.conn.execute(
                "UPDATE tasks SET lease_expires_at=?, updated_at=? WHERE id=?",
                (_iso(lease_exp), _iso(now), task_id),
            )
            return self.get(task_id)

    def request_cancel(self, task_id: str, actor: str = "api") -> Task:
        """请求取消。

        - PENDING：直接取消，落 CANCELLED 终态；
        - RUNNING：置取消标记（幂等），执行者感知后收敛到 CANCELLED；
        - 终态：拒绝，task_already_terminal（含重复取消）；
        - DEAD：拒绝，invalid_transition（死信只能重放，不能取消）。
        """
        with self._db.lock, self._db.conn:
            task = self.get(task_id)
            if task.status in TERMINAL_STATUSES:
                raise TaskAlreadyTerminalError(task_id, task.status)
            if task.status is TaskStatus.DEAD:
                raise InvalidTransitionError(task_id, task.status, TaskStatus.CANCELLED)
            now = _now()
            if task.status is TaskStatus.PENDING:
                self._db.conn.execute(
                    "UPDATE tasks SET status=?, updated_at=? WHERE id=?",
                    (TaskStatus.CANCELLED.value, _iso(now), task_id),
                )
                self._append_event(task_id, TaskStatus.PENDING, TaskStatus.CANCELLED,
                                   "cancelled by requester before execution", actor)
            else:  # RUNNING：只置标记，不抢执行者的状态机
                if not task.cancel_requested:
                    self._db.conn.execute(
                        """UPDATE tasks SET cancel_requested=1, cancel_requested_by=?,
                              updated_at=? WHERE id=?""",
                        (actor, _iso(now), task_id),
                    )
                    self._append_event(task_id, TaskStatus.RUNNING, TaskStatus.RUNNING,
                                       f"cancel requested by {actor}, "
                                       f"waiting for worker {task.lease_owner} to stop",
                                       actor)
            return self.get(task_id)

    def confirm_cancelled(self, task_id: str, worker_id: str, epoch: int) -> Task:
        """执行者确认取消：RUNNING + 持有租约 + 当前纪元 + 已置取消标记
        -> CANCELLED。未置标记时拒绝（防止执行者单方面取消正常任务）。"""
        with self._db.lock, self._db.conn:
            task = self.get(task_id)
            self._assert_owner(task, worker_id, epoch)
            if not task.cancel_requested:
                raise InvalidTransitionError(task_id, task.status, TaskStatus.CANCELLED)
            return self._confirm_cancelled_locked(
                task, worker_id,
                f"cancel requested by {task.cancel_requested_by or 'unknown'} "
                f"acknowledged, worker stopped execution")

    def _confirm_cancelled_locked(self, task: Task, worker_id: str,
                                  reason: str) -> Task:
        """调用方必须已持有锁并完成归属校验。"""
        self._db.conn.execute(
            """UPDATE tasks SET status=?, lease_owner=NULL, lease_expires_at=NULL,
                  updated_at=? WHERE id=?""",
            (TaskStatus.CANCELLED.value, _iso(_now()), task.id),
        )
        self._append_event(task.id, TaskStatus.RUNNING, TaskStatus.CANCELLED,
                           reason, worker_id)
        return self.get(task.id)

    def replay(self, task_id: str, actor: str = "operator") -> Task:
        """人工重放死信：DEAD -> PENDING，attempt 归零，历史事件完整保留。

        取消标记一并清零；旧纪元的进度因 epoch 失配自然失效，新一轮执行
        的进度从零开始。
        """
        with self._db.lock, self._db.conn:
            task = self.get(task_id)
            if task.status is not TaskStatus.DEAD:
                raise InvalidTransitionError(task_id, task.status, TaskStatus.PENDING)
            now = _now()
            self._db.conn.execute(
                """UPDATE tasks SET status=?, attempt=0, run_at=?, lease_owner=NULL,
                      lease_expires_at=NULL, last_error=NULL,
                      cancel_requested=0, cancel_requested_by=NULL, updated_at=?
                   WHERE id=?""",
                (TaskStatus.PENDING.value, _iso(now), _iso(now), task_id),
            )
            # 进度从头开始：清掉上一执行纪元的进度快照
            self._db.conn.execute(
                "DELETE FROM task_progress WHERE task_id=?", (task_id,)
            )
            self._append_event(task_id, TaskStatus.DEAD, TaskStatus.PENDING,
                               "manual replay, history preserved", actor)
            return self.get(task_id)

    # ---------- 内部 ----------

    def _finish(self, task_id: str, worker_id: str, target: TaskStatus,
                reason: str, clear_error: bool = False,
                epoch: int | None = None) -> Task:
        with self._db.lock, self._db.conn:
            task = self.get(task_id)
            self._assert_owner(task, worker_id, epoch)
            self._db.conn.execute(
                """UPDATE tasks SET status=?, lease_owner=NULL, lease_expires_at=NULL,
                      last_error=?, updated_at=? WHERE id=?""",
                (target.value, None if clear_error else task.last_error,
                 _iso(_now()), task_id),
            )
            self._append_event(task_id, TaskStatus.RUNNING, target, reason, worker_id)
            return self.get(task_id)

    def _assert_owner(self, task: Task, worker_id: str,
                      epoch: int | None = None) -> None:
        if task.status is not TaskStatus.RUNNING or task.lease_owner != worker_id:
            raise InvalidTransitionError(task.id, task.status, TaskStatus.RUNNING)
        if epoch is not None and task.epoch != epoch:
            # 纪元不匹配：来自上一执行纪元的迟到写入，拒绝以防污染新一轮执行
            raise InvalidTransitionError(task.id, task.status, TaskStatus.RUNNING)

    def _append_event(self, task_id: str, from_status: TaskStatus | None,
                      to_status: TaskStatus, reason: str, actor: str) -> None:
        self._db.conn.execute(
            """INSERT INTO task_events (task_id, from_status, to_status, reason, actor, created_at)
               VALUES (?,?,?,?,?,?)""",
            (task_id, from_status.value if from_status else None,
             to_status.value, reason, actor, _iso(_now())),
        )
