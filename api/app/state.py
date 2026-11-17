"""SQLite 持久化：基准备份 / 核对点 / 恢复任务的状态全部落盘。

API 重启后据此续跑或按原点重试，不会丢状态、不会把半成品当成功。
"""
import sqlite3
import threading
from datetime import datetime, timezone

from . import config

_LOCK = threading.RLock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS backups (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    path        TEXT NOT NULL,
    status      TEXT NOT NULL,          -- running | done | failed
    start_lsn   TEXT,                   -- 备份起始 WAL 位点（恢复可用性的下界）
    timeline    INTEGER,
    size_bytes  INTEGER,
    error       TEXT,
    created_at  TEXT NOT NULL,
    finished_at TEXT
);
CREATE TABLE IF NOT EXISTS checkpoints (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE,
    restore_lsn TEXT NOT NULL,          -- pg_create_restore_point 返回的 LSN
    wal_file    TEXT NOT NULL,          -- 该 LSN 所在的 WAL 段文件名
    status      TEXT NOT NULL,          -- waiting_wal | ready | expired
    created_at  TEXT NOT NULL,
    ready_at    TEXT
);
CREATE TABLE IF NOT EXISTS jobs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    checkpoint_name  TEXT NOT NULL,
    status           TEXT NOT NULL,     -- queued | waiting_wal | preparing | recovering
                                        -- | verifying | ready | failed
                                        -- | superseded | stopped | cancelled
    detail           TEXT,
    error            TEXT,
    port             INTEGER,
    data_dir         TEXT,
    log_tail         TEXT,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL,
    ready_at         TEXT
);
"""

# 进行中（未终结）的任务状态：一次只允许一个
ACTIVE_JOB_STATES = ("queued", "waiting_wal", "preparing", "recovering", "verifying")
# 允许发起重试的终结状态
RETRYABLE_JOB_STATES = ("failed", "stopped", "cancelled", "superseded")


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(config.STATE_DB, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=30000")
    return con


def init_db() -> None:
    with _LOCK:
        con = _connect()
        try:
            con.executescript(SCHEMA)
            con.commit()
        finally:
            con.close()


def execute(sql: str, params: tuple = ()) -> int:
    with _LOCK:
        con = _connect()
        try:
            cur = con.execute(sql, params)
            con.commit()
            return cur.lastrowid
        finally:
            con.close()


def all_rows(sql: str, params: tuple = ()) -> list[dict]:
    with _LOCK:
        con = _connect()
        try:
            return [dict(r) for r in con.execute(sql, params).fetchall()]
        finally:
            con.close()


def one_row(sql: str, params: tuple = ()) -> dict | None:
    rows = all_rows(sql, params)
    return rows[0] if rows else None


def update_job(job_id: int, **fields) -> None:
    if not fields:
        return
    fields["updated_at"] = now()
    cols = ", ".join(f"{k}=?" for k in fields)
    execute(f"UPDATE jobs SET {cols} WHERE id=?", (*fields.values(), job_id))
