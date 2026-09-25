"""SQLite 持久化层：任务、事件流水、事务发件箱。

所有写操作在单个 ``BEGIN IMMEDIATE`` 事务内完成，
配合 WAL + busy_timeout 实现多执行者的互斥抢占。
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any

from .models import (
    DeadLetterReason,
    ErrorCode,
    OutboxMessage,
    Task,
    TaskError,
    TaskEvent,
    TaskStatus,
)

_TASK_COLUMNS = [
    "id",
    "idempotency_key",
    "task_type",
    "payload_json",
    "status",
    "attempt",
    "max_attempts",
    "lease_owner",
    "lease_expires_at",
    "backoff_until",
    "cancel_requested",
    "replay_of",
    "replay_seq",
    "dead_letter_reason",
    "dead_letter_detail",
    "result_json",
    "created_at",
    "updated_at",
]


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt is not None else None


def _parse(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value is not None else None


class TaskStore:
    """任务仓库。每个实例使用独立连接（每个执行者一个实例）。"""

    def __init__(self, path: str = ":memory:") -> None:
        self.path = path
        self.conn = self._connect(path)
        self._init_schema()

    # ---------- 基础 ----------
    def close(self) -> None:
        self.conn.close()

    @staticmethod
    def _connect(path: str) -> sqlite3.Connection:
        conn = sqlite3.connect(
            path,
            timeout=5.0,
            isolation_level=None,  # 手动事务
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                idempotency_key TEXT NOT NULL UNIQUE,
                task_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                status TEXT NOT NULL,
                attempt INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL,
                lease_owner TEXT,
                lease_expires_at TEXT,
                backoff_until TEXT,
                cancel_requested INTEGER NOT NULL DEFAULT 0,
                replay_of TEXT,
                replay_seq INTEGER NOT NULL DEFAULT 0,
                dead_letter_reason TEXT,
                dead_letter_detail TEXT,
                result_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_tasks_claim
                ON tasks(status, backoff_until);

            CREATE TABLE IF NOT EXISTS task_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT NOT NULL,
                seq INTEGER NOT NULL,
                at TEXT NOT NULL,
                actor TEXT NOT NULL,
                from_status TEXT,
                to_status TEXT,
                event_type TEXT NOT NULL,
                attempt INTEGER NOT NULL DEFAULT 0,
                detail_json TEXT NOT NULL DEFAULT '{}',
                UNIQUE(task_id, seq)
            );
            CREATE INDEX IF NOT EXISTS idx_events_task ON task_events(task_id, seq);

            CREATE TABLE IF NOT EXISTS task_outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT NOT NULL,
                task_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                result_json TEXT,
                idempotency_key TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL,
                backoff_until TEXT,
                last_error TEXT,
                lease_owner TEXT,
                lease_expires_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(task_id)
            );
            CREATE INDEX IF NOT EXISTS idx_outbox_claim
                ON task_outbox(status, backoff_until);
            """
        )

    # ---------- 行映射 ----------
    @staticmethod
    def _row_to_task(row: sqlite3.Row) -> Task:
        return Task(
            id=row["id"],
            idempotency_key=row["idempotency_key"],
            task_type=row["task_type"],
            payload=json.loads(row["payload_json"]),
            status=TaskStatus(row["status"]),
            attempt=row["attempt"],
            max_attempts=row["max_attempts"],
            lease_owner=row["lease_owner"],
            lease_expires_at=_parse(row["lease_expires_at"]),
            backoff_until=_parse(row["backoff_until"]),
            cancel_requested=bool(row["cancel_requested"]),
            replay_of=row["replay_of"],
            replay_seq=row["replay_seq"],
            dead_letter_reason=row["dead_letter_reason"],
            dead_letter_detail=row["dead_letter_detail"],
            result=(
                json.loads(row["result_json"])
                if row["result_json"] is not None
                else None
            ),
            created_at=_parse(row["created_at"]),  # type: ignore[arg-type]
            updated_at=_parse(row["updated_at"]),  # type: ignore[arg-type]
        )

    @staticmethod
    def _row_to_event(row: sqlite3.Row) -> TaskEvent:
        return TaskEvent(
            id=row["id"],
            task_id=row["task_id"],
            seq=row["seq"],
            at=_parse(row["at"]),  # type: ignore[arg-type]
            actor=row["actor"],
            from_status=(
                TaskStatus(row["from_status"])
                if row["from_status"] is not None
                else None
            ),
            to_status=(
                TaskStatus(row["to_status"]) if row["to_status"] is not None else None
            ),
            event_type=row["event_type"],
            attempt=row["attempt"],
            detail=json.loads(row["detail_json"]),
        )

    @staticmethod
    def _row_to_outbox(row: sqlite3.Row) -> OutboxMessage:
        return OutboxMessage(
            id=row["id"],
            task_id=row["task_id"],
            task_type=row["task_type"],
            payload=json.loads(row["payload_json"]),
            result=(
                json.loads(row["result_json"])
                if row["result_json"] is not None
                else None
            ),
            idempotency_key=row["idempotency_key"],
            status=row["status"],
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            backoff_until=_parse(row["backoff_until"]),
            last_error=row["last_error"],
            created_at=_parse(row["created_at"]),  # type: ignore[arg-type]
            updated_at=_parse(row["updated_at"]),  # type: ignore[arg-type]
        )

    def _add_event(
        self,
        conn: sqlite3.Connection,
        task_id: str,
        actor: str,
        event_type: str,
        *,
        from_status: TaskStatus | None,
        to_status: TaskStatus | None,
        attempt: int,
        detail: dict[str, Any] | None = None,
        at: datetime | None = None,
    ) -> None:
        ts = _iso(at or datetime.now(UTC))
        next_seq = conn.execute(
            "SELECT COALESCE(MAX(seq), 0) + 1 FROM task_events WHERE task_id = ?",
            (task_id,),
        ).fetchone()[0]
        conn.execute(
            """
            INSERT INTO task_events
                (task_id, seq, at, actor, from_status, to_status,
                 event_type, attempt, detail_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                task_id,
                next_seq,
                ts,
                actor,
                from_status.value if from_status else None,
                to_status.value if to_status else None,
                event_type,
                attempt,
                json.dumps(detail or {}, ensure_ascii=False),
            ),
        )

    # ---------- 任务 ----------
    def create_task(
        self,
        task: Task,
        *,
        actor: str,
    ) -> tuple[Task, bool]:
        """幂等创建。返回 (任务, 是否新建)；同键不同载荷返回冲突。"""
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT * FROM tasks WHERE idempotency_key = ?",
                (task.idempotency_key,),
            ).fetchone()
            if row is not None:
                existing = self._row_to_task(row)
                if (
                    existing.task_type != task.task_type
                    or existing.payload != task.payload
                ):
                    raise TaskError(
                        ErrorCode.IDEMPOTENCY_CONFLICT,
                        "相同幂等键已用于不同的 task_type/payload",
                    )
                conn.execute("COMMIT")
                return existing, False

            conn.execute(
                """
                INSERT INTO tasks (
                    id, idempotency_key, task_type, payload_json, status,
                    attempt, max_attempts, lease_owner, lease_expires_at,
                    backoff_until, cancel_requested, replay_of, replay_seq,
                    dead_letter_reason, dead_letter_detail, result_json,
                    created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, 0, NULL, 0,
                        NULL, NULL, NULL, ?, ?)
                """,
                (
                    task.id,
                    task.idempotency_key,
                    task.task_type,
                    json.dumps(task.payload, ensure_ascii=False),
                    TaskStatus.PENDING.value,
                    0,
                    task.max_attempts,
                    _iso(task.created_at),
                    _iso(task.updated_at),
                ),
            )
            self._add_event(
                conn,
                task.id,
                actor,
                "SUBMITTED",
                from_status=None,
                to_status=TaskStatus.PENDING,
                attempt=0,
                detail={"idempotency_key": task.idempotency_key},
                at=task.created_at,
            )
            conn.execute("COMMIT")
            return task, True
        except Exception:
            conn.execute("ROLLBACK")
            raise

    def get_task(self, task_id: str) -> Task | None:
        row = self.conn.execute(
            "SELECT * FROM tasks WHERE id = ?", (task_id,)
        ).fetchone()
        return self._row_to_task(row) if row else None

    def get_task_by_key(self, idempotency_key: str) -> Task | None:
        row = self.conn.execute(
            "SELECT * FROM tasks WHERE idempotency_key = ?", (idempotency_key,)
        ).fetchone()
        return self._row_to_task(row) if row else None

    def list_events(self, task_id: str) -> list[TaskEvent]:
        rows = self.conn.execute(
            "SELECT * FROM task_events WHERE task_id = ? ORDER BY seq", (task_id,)
        ).fetchall()
        return [self._row_to_event(r) for r in rows]

    def claim_next(
        self,
        owner: str,
        *,
        now: datetime,
        lease_seconds: float,
    ) -> Task | None:
        """原子抢占一个可执行任务（含过期租约回收），无任务返回 None。

        条件：PENDING 且退避已过；或 RUNNING 且租约过期（崩溃恢复）。
        """
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            now_iso = _iso(now)
            expires_iso = _iso(
                datetime.fromtimestamp(now.timestamp() + lease_seconds, UTC)
            )
            row = conn.execute(
                """
                SELECT * FROM tasks
                WHERE
                    (status = 'PENDING'
                        AND (backoff_until IS NULL OR backoff_until <= ?))
                    OR (status = 'RUNNING' AND lease_expires_at <= ?)
                ORDER BY created_at
                LIMIT 1
                """,
                (now_iso, now_iso),
            ).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return None
            task_id = row["id"]
            prev_status = TaskStatus(row["status"])
            recovered = prev_status == TaskStatus.RUNNING
            conn.execute(
                """
                UPDATE tasks
                SET status = 'RUNNING',
                    attempt = attempt + 1,
                    lease_owner = ?,
                    lease_expires_at = ?,
                    backoff_until = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (owner, expires_iso, now_iso, task_id),
            )
            new_attempt = row["attempt"] + 1
            self._add_event(
                conn,
                task_id,
                owner,
                "LEASE_RECLAIMED" if recovered else "CLAIMED",
                from_status=prev_status,
                to_status=TaskStatus.RUNNING,
                attempt=new_attempt,
                detail={"lease_seconds": lease_seconds} if recovered else {},
                at=now,
            )
            conn.execute("COMMIT")
            return self.get_task(task_id)
        except Exception:
            conn.execute("ROLLBACK")
            raise

    def heartbeat(self, task_id: str, owner: str, *, expires_at: datetime) -> bool:
        """续租；仅持有者成功。"""
        cur = self.conn.execute(
            """
            UPDATE tasks SET lease_expires_at = ?, updated_at = ?
            WHERE id = ? AND lease_owner = ? AND status = 'RUNNING'
            """,
            (_iso(expires_at), _iso(expires_at), task_id, owner),
        )
        return cur.rowcount == 1

    def mark_succeeded(
        self, task_id: str, owner: str, *, now: datetime, result: Any,
        delivery_max_attempts: int | None = None,
    ) -> None:
        """任务成功：写事件 + 写发件箱（同一事务，事务发件箱模式）。"""
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if row is None:
                raise TaskError(ErrorCode.TASK_NOT_FOUND, f"任务不存在: {task_id}")
            task = self._row_to_task(row)
            if task.lease_owner != owner or task.status != TaskStatus.RUNNING:
                raise TaskError(
                    ErrorCode.TASK_RUNNING, "租约已失效，无法标记成功（可能已被回收）"
                )
            now_iso = _iso(now)
            result_json = json.dumps(result, ensure_ascii=False)
            conn.execute(
                """
                UPDATE tasks
                SET status = 'SUCCEEDED', lease_owner = NULL,
                    lease_expires_at = NULL, result_json = ?, updated_at = ?
                WHERE id = ?
                """,
                (result_json, now_iso, task_id),
            )
            conn.execute(
                """
                INSERT INTO task_outbox
                    (task_id, task_type, payload_json, result_json,
                     idempotency_key, status, attempts, max_attempts,
                     backoff_until, last_error, lease_owner, lease_expires_at,
                     created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 'PENDING', 0, ?, NULL, NULL, NULL, NULL, ?, ?)
                """,
                (
                    task_id,
                    task.task_type,
                    row["payload_json"],
                    result_json,
                    task.idempotency_key,
                    delivery_max_attempts
                    if delivery_max_attempts is not None
                    else task.max_attempts,
                    now_iso,
                    now_iso,
                ),
            )
            self._add_event(
                conn,
                task_id,
                owner,
                "SUCCEEDED",
                from_status=TaskStatus.RUNNING,
                to_status=TaskStatus.SUCCEEDED,
                attempt=task.attempt,
                detail={"result": result},
                at=now,
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    def mark_retry(
        self,
        task_id: str,
        owner: str,
        *,
        now: datetime,
        backoff_until: datetime,
        error: str,
    ) -> None:
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if row is None:
                raise TaskError(ErrorCode.TASK_NOT_FOUND, f"任务不存在: {task_id}")
            task = self._row_to_task(row)
            if task.lease_owner != owner or task.status != TaskStatus.RUNNING:
                raise TaskError(ErrorCode.TASK_RUNNING, "租约已失效，状态已被回收方改写")
            now_iso = _iso(now)
            conn.execute(
                """
                UPDATE tasks
                SET status = 'PENDING', lease_owner = NULL,
                    lease_expires_at = NULL, backoff_until = ?, updated_at = ?
                WHERE id = ?
                """,
                (_iso(backoff_until), now_iso, task_id),
            )
            self._add_event(
                conn,
                task_id,
                owner,
                "RETRY_BACKOFF",
                from_status=TaskStatus.RUNNING,
                to_status=TaskStatus.PENDING,
                attempt=task.attempt,
                detail={"error": error, "backoff_until": backoff_until.isoformat()},
                at=now,
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    def mark_dead_letter(
        self,
        task_id: str,
        owner: str,
        *,
        now: datetime,
        reason: str,
        detail: dict[str, Any],
    ) -> None:
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if row is None:
                raise TaskError(ErrorCode.TASK_NOT_FOUND, f"任务不存在: {task_id}")
            task = self._row_to_task(row)
            if task.status not in (TaskStatus.RUNNING, TaskStatus.PENDING):
                raise TaskError(
                    ErrorCode.TASK_TERMINAL,
                    f"任务当前状态 {task.status.value} 不可进入死信",
                )
            now_iso = _iso(now)
            prev = task.status
            conn.execute(
                """
                UPDATE tasks
                SET status = 'DEAD_LETTER', lease_owner = NULL,
                    lease_expires_at = NULL, backoff_until = NULL,
                    dead_letter_reason = ?, dead_letter_detail = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    reason,
                    json.dumps(detail, ensure_ascii=False),
                    now_iso,
                    task_id,
                ),
            )
            self._add_event(
                conn,
                task_id,
                owner,
                "DEAD_LETTERED",
                from_status=prev,
                to_status=TaskStatus.DEAD_LETTER,
                attempt=task.attempt,
                detail={"reason": reason, **detail},
                at=now,
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    def mark_cancelled(
        self, task_id: str, owner: str, *, now: datetime, detail: dict[str, Any]
    ) -> None:
        """执行者在观测到 cancel_requested 后将任务收敛到 CANCELLED。"""
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if row is None:
                raise TaskError(ErrorCode.TASK_NOT_FOUND, f"任务不存在: {task_id}")
            task = self._row_to_task(row)
            if task.status not in (TaskStatus.RUNNING, TaskStatus.PENDING):
                raise TaskError(
                    ErrorCode.TASK_TERMINAL,
                    f"任务当前状态 {task.status.value} 不可取消",
                )
            prev = task.status
            now_iso = _iso(now)
            conn.execute(
                """
                UPDATE tasks
                SET status = 'CANCELLED', lease_owner = NULL,
                    lease_expires_at = NULL, backoff_until = NULL, updated_at = ?
                WHERE id = ?
                """,
                (now_iso, task_id),
            )
            self._add_event(
                conn,
                task_id,
                owner,
                "CANCELLED",
                from_status=prev,
                to_status=TaskStatus.CANCELLED,
                attempt=task.attempt,
                detail=detail,
                at=now,
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    def request_cancel(self, task_id: str, *, now: datetime, actor: str) -> Task:
        """校验并执行取消，非法跃迁抛 TaskError。

        - PENDING：直接收敛到 CANCELLED；
        - RUNNING：设置 cancel_requested（协作式取消），事件记录原因；
        - CANCELLED / 其它终态：拒绝，错误码可区分。
        """
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if row is None:
                raise TaskError(ErrorCode.TASK_NOT_FOUND, f"任务不存在: {task_id}")
            task = self._row_to_task(row)
            now_iso = _iso(now)
            if task.status == TaskStatus.CANCELLED:
                raise TaskError(
                    ErrorCode.ALREADY_CANCELLED, "任务已取消，不能重复取消"
                )
            if task.status in (TaskStatus.SUCCEEDED, TaskStatus.DEAD_LETTER):
                raise TaskError(
                    ErrorCode.TASK_TERMINAL,
                    f"任务已结束（{task.status.value}），不能取消",
                )
            if task.status == TaskStatus.PENDING:
                conn.execute(
                    """
                    UPDATE tasks
                    SET status = 'CANCELLED', cancel_requested = 1,
                        backoff_until = NULL, updated_at = ?
                    WHERE id = ?
                    """,
                    (now_iso, task_id),
                )
                self._add_event(
                    conn,
                    task_id,
                    actor,
                    "CANCEL_REQUESTED",
                    from_status=TaskStatus.PENDING,
                    to_status=TaskStatus.CANCELLED,
                    attempt=task.attempt,
                    detail={"immediate": True},
                    at=now,
                )
            elif task.status == TaskStatus.RUNNING:
                conn.execute(
                    "UPDATE tasks SET cancel_requested = 1, updated_at = ? WHERE id = ?",
                    (now_iso, task_id),
                )
                self._add_event(
                    conn,
                    task_id,
                    actor,
                    "CANCEL_REQUESTED",
                    from_status=TaskStatus.RUNNING,
                    to_status=TaskStatus.RUNNING,
                    attempt=task.attempt,
                    detail={"immediate": False},
                    at=now,
                )
            conn.execute("COMMIT")
            return self.get_task(task_id)  # type: ignore[return-value]
        except Exception:
            conn.execute("ROLLBACK")
            raise

    def replay_dead_letter(
        self, task_id: str, *, now: datetime, actor: str
    ) -> Task:
        """死信人工重放：保留全部历史，任务回到 PENDING 并自增 replay 序号。"""
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if row is None:
                raise TaskError(ErrorCode.TASK_NOT_FOUND, f"任务不存在: {task_id}")
            task = self._row_to_task(row)
            if task.status != TaskStatus.DEAD_LETTER:
                raise TaskError(
                    ErrorCode.NOT_REPLAYABLE,
                    f"仅 DEAD_LETTER 任务可重放，当前状态 {task.status.value}",
                )
            now_iso = _iso(now)
            # 若是投递死信重放，发件箱行也回到 PENDING；执行死信重放则重置执行计数
            conn.execute(
                """
                UPDATE tasks
                SET status = 'PENDING', attempt = 0, lease_owner = NULL,
                    lease_expires_at = NULL, backoff_until = NULL,
                    cancel_requested = 0,
                    replay_of = COALESCE(replay_of, id),
                    replay_seq = replay_seq + 1,
                    dead_letter_reason = NULL, dead_letter_detail = NULL,
                    result_json = NULL, updated_at = ?
                WHERE id = ?
                """,
                (now_iso, task_id),
            )
            outbox = conn.execute(
                "SELECT id FROM task_outbox WHERE task_id = ?", (task_id,)
            ).fetchone()
            if outbox is not None:
                # 任务曾成功过（投递死信）：重放时仅重投递，不再执行处理器
                conn.execute(
                    """
                    UPDATE task_outbox
                    SET status = 'PENDING', attempts = 0, backoff_until = NULL,
                        last_error = NULL, lease_owner = NULL,
                        lease_expires_at = NULL, updated_at = ?
                    WHERE task_id = ?
                    """,
                    (now_iso, task_id),
                )
            self._add_event(
                conn,
                task_id,
                actor,
                "REPLAYED",
                from_status=TaskStatus.DEAD_LETTER,
                to_status=TaskStatus.PENDING,
                attempt=0,
                detail={"replay_seq": task.replay_seq + 1,
                        "delivery_only": outbox is not None},
                at=now,
            )
            conn.execute("COMMIT")
            return self.get_task(task_id)  # type: ignore[return-value]
        except Exception:
            conn.execute("ROLLBACK")
            raise

    def reclaim_expired(self, *, now: datetime) -> int:
        """显式回收（测试/启动期使用）；正常由 claim_next 顺带完成。"""
        cur = self.conn.execute(
            """
            UPDATE tasks
            SET status = 'PENDING', lease_owner = NULL, lease_expires_at = NULL
            WHERE status = 'RUNNING' AND lease_expires_at <= ?
            """,
            (_iso(now),),
        )
        return cur.rowcount

    # ---------- 发件箱 ----------
    def outbox_claim_next(
        self, owner: str, *, now: datetime, lease_seconds: float
    ) -> OutboxMessage | None:
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            now_iso = _iso(now)
            expires_iso = _iso(
                datetime.fromtimestamp(now.timestamp() + lease_seconds, UTC)
            )
            row = conn.execute(
                """
                SELECT * FROM task_outbox
                WHERE
                    (status = 'PENDING'
                        AND (backoff_until IS NULL OR backoff_until <= ?))
                    OR (status = 'DELIVERING' AND lease_expires_at <= ?)
                ORDER BY id
                LIMIT 1
                """,
                (now_iso, now_iso),
            ).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return None
            message_id = row["id"]
            recovered = row["status"] == "DELIVERING"
            conn.execute(
                """
                UPDATE task_outbox
                SET status = 'DELIVERING', attempts = attempts + 1,
                    lease_owner = ?, lease_expires_at = ?, backoff_until = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (owner, expires_iso, now_iso, message_id),
            )
            self._add_event(
                conn,
                row["task_id"],
                owner,
                "OUTBOX_CLAIMED" if not recovered else "OUTBOX_RECLAIMED",
                from_status=None,
                to_status=None,
                attempt=row["attempts"] + 1,
                detail={"outbox_id": message_id},
                at=now,
            )
            conn.execute("COMMIT")
            out = self.conn.execute(
                "SELECT * FROM task_outbox WHERE id = ?", (message_id,)
            ).fetchone()
            return self._row_to_outbox(out)
        except Exception:
            conn.execute("ROLLBACK")
            raise

    def outbox_mark_delivered(
        self, message_id: int, owner: str, *, now: datetime
    ) -> None:
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT * FROM task_outbox WHERE id = ?", (message_id,)
            ).fetchone()
            if row is None or row["lease_owner"] != owner:
                raise TaskError(ErrorCode.TASK_RUNNING, "发件箱租约失效")
            now_iso = _iso(now)
            conn.execute(
                """
                UPDATE task_outbox
                SET status = 'DELIVERED', lease_owner = NULL,
                    lease_expires_at = NULL, updated_at = ?
                WHERE id = ?
                """,
                (now_iso, message_id),
            )
            self._add_event(
                conn,
                row["task_id"],
                owner,
                "OUTBOX_DELIVERED",
                from_status=None,
                to_status=None,
                attempt=row["attempts"],
                detail={"outbox_id": message_id},
                at=now,
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    def outbox_mark_retry(
        self,
        message_id: int,
        owner: str,
        *,
        now: datetime,
        backoff_until: datetime,
        error: str,
    ) -> None:
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT * FROM task_outbox WHERE id = ?", (message_id,)
            ).fetchone()
            if row is None or row["lease_owner"] != owner:
                raise TaskError(ErrorCode.TASK_RUNNING, "发件箱租约失效")
            now_iso = _iso(now)
            conn.execute(
                """
                UPDATE task_outbox
                SET status = 'PENDING', lease_owner = NULL,
                    lease_expires_at = NULL, backoff_until = ?,
                    last_error = ?, updated_at = ?
                WHERE id = ?
                """,
                (_iso(backoff_until), now_iso, error, now_iso, message_id),
            )
            self._add_event(
                conn,
                row["task_id"],
                owner,
                "OUTBOX_RETRY",
                from_status=None,
                to_status=None,
                attempt=row["attempts"],
                detail={"error": error, "backoff_until": backoff_until.isoformat()},
                at=now,
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    def outbox_mark_dead(
        self,
        message_id: int,
        owner: str,
        *,
        now: datetime,
        detail: str,
    ) -> None:
        """发件箱用尽重试 -> 消息 DEAD 且任务进入 DEAD_LETTER（可重放）。"""
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT * FROM task_outbox WHERE id = ?", (message_id,)
            ).fetchone()
            if row is None or row["lease_owner"] != owner:
                raise TaskError(ErrorCode.TASK_RUNNING, "发件箱租约失效")
            now_iso = _iso(now)
            conn.execute(
                """
                UPDATE task_outbox
                SET status = 'DEAD', lease_owner = NULL, lease_expires_at = NULL,
                    last_error = ?, updated_at = ?
                WHERE id = ?
                """,
                (detail, now_iso, message_id),
            )
            conn.execute(
                """
                UPDATE tasks
                SET status = 'DEAD_LETTER', dead_letter_reason = ?,
                    dead_letter_detail = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    DeadLetterReason.DELIVERY_RETRIES_EXHAUSTED.value,
                    json.dumps({"outbox_id": message_id, "error": detail},
                               ensure_ascii=False),
                    now_iso,
                    row["task_id"],
                ),
            )
            self._add_event(
                conn,
                row["task_id"],
                owner,
                "OUTBOX_DEAD",
                from_status=TaskStatus.SUCCEEDED,
                to_status=TaskStatus.DEAD_LETTER,
                attempt=row["attempts"],
                detail={"reason": DeadLetterReason.DELIVERY_RETRIES_EXHAUSTED.value,
                        "error": detail, "outbox_id": message_id},
                at=now,
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise


def new_task_id() -> str:
    return uuid.uuid4().hex
