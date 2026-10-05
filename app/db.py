"""SQLite 持久化层：连接管理与建表。本地可验证，无外部中间件。"""
from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id              TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    task_type       TEXT NOT NULL,
    payload         TEXT NOT NULL,
    status          TEXT NOT NULL,
    attempt         INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 3,
    run_at          TEXT NOT NULL,
    lease_owner     TEXT,
    lease_expires_at TEXT,
    last_error      TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tasks_poll
    ON tasks (status, run_at);

CREATE TABLE IF NOT EXISTS task_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id     TEXT NOT NULL,
    from_status TEXT,
    to_status   TEXT NOT NULL,
    reason      TEXT NOT NULL,
    actor       TEXT NOT NULL,
    created_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_task ON task_events (task_id, id);

CREATE TABLE IF NOT EXISTS task_progress (
    task_id     TEXT PRIMARY KEY,
    epoch       INTEGER NOT NULL,
    percent     REAL,
    step        TEXT,
    message     TEXT,
    updated_at  TEXT NOT NULL
);
"""

# 旧库就地升级：新增列若不存在则 ALTER TABLE 补齐（SQLite 不支持 IF NOT EXISTS 加列）
_MIGRATIONS = (
    ("tasks", "epoch", "ALTER TABLE tasks ADD COLUMN epoch INTEGER NOT NULL DEFAULT 0"),
    ("tasks", "cancel_requested",
     "ALTER TABLE tasks ADD COLUMN cancel_requested INTEGER NOT NULL DEFAULT 0"),
    ("tasks", "cancel_requested_by", "ALTER TABLE tasks ADD COLUMN cancel_requested_by TEXT"),
)


class Database:
    """线程安全的 SQLite 封装。每个线程一个连接，写操作串行化。"""

    def __init__(self, path: str | Path = ":memory:"):
        self._path = str(path)
        self._lock = threading.RLock()
        # 单连接 + RLock：内存库与文件库行为一致，写事务天然串行
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("PRAGMA foreign_keys=ON")
        with self._lock, self._conn:
            self._conn.executescript(SCHEMA)
            self._migrate()

    def _migrate(self) -> None:
        for table, column, ddl in _MIGRATIONS:
            cols = {r["name"] for r in self._conn.execute(f"PRAGMA table_info({table})")}
            if column not in cols:
                self._conn.execute(ddl)

    @property
    def conn(self) -> sqlite3.Connection:
        return self._conn

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    def close(self) -> None:
        with self._lock:
            self._conn.close()
