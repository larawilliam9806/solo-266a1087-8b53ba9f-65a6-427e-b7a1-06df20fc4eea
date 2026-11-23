"""SQLite 数据访问层。

批次目录与 Parquet 文件都落盘保存，元数据（批次、版号、列对照、暂存记录）
存于 SQLite。WAL 模式保证流式下载期间仍可写入。
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from flask import current_app, g

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_no      TEXT NOT NULL UNIQUE,
    time_point    TEXT NOT NULL,
    note          TEXT NOT NULL DEFAULT '',
    created_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS versions (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id           INTEGER NOT NULL REFERENCES batches(id),
    version_no         INTEGER NOT NULL,
    is_official        INTEGER NOT NULL DEFAULT 0,
    parquet_path       TEXT NOT NULL,
    tree_id_column     TEXT NOT NULL,
    created_at         TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (batch_id, version_no)
);

CREATE TABLE IF NOT EXISTS version_metrics (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id    INTEGER NOT NULL REFERENCES versions(id) ON DELETE CASCADE,
    metric_key    TEXT NOT NULL,
    source_column TEXT NOT NULL,
    unit          TEXT NOT NULL CHECK (unit IN ('m', 'cm')),
    UNIQUE (version_id, metric_key)
);

CREATE TABLE IF NOT EXISTS staged_files (
    token        TEXT PRIMARY KEY,
    parquet_path TEXT NOT NULL,
    columns      TEXT NOT NULL,
    created_at   TEXT NOT NULL DEFAULT (datetime('now'))
);
"""


def _connect(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def init_db(app) -> None:
    path = Path(app.config["DATABASE_PATH"])
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = _connect(path)
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()


def get_db() -> sqlite3.Connection:
    """请求级连接（g 管理，请求结束自动关闭）。"""
    if "db" not in g:
        g.db = _connect(current_app.config["DATABASE_PATH"])
    return g.db


def standalone_connection() -> sqlite3.Connection:
    """给流式生成器使用的独立连接（生命周期不受请求上下文约束）。"""
    return _connect(current_app.config["DATABASE_PATH"])


def close_db(_exc=None) -> None:
    db = g.pop("db", None)
    if db is not None:
        db.close()
