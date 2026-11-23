"""业务服务层：暂存上传、列对照校验、换交易务、批次/版号查询。"""
from __future__ import annotations

import json
import os
import sqlite3
import uuid
from dataclasses import dataclass

from .constants import METRIC_KEYS, METRIC_LABELS, UNITS
from .db import get_db
from .parquet_tools import is_numeric_column, parquet_columns


class ValidationError(Exception):
    """表单或列对照校验失败，消息直接展示给用户。"""


# ---------- 暂存 ----------

def stage_upload(file_storage, staging_dir: str) -> str:
    """保存上传的 Parquet 到暂存区，返回 token。"""
    if not file_storage or not file_storage.filename:
        raise ValidationError("请选择要上传的 Parquet 文件。")
    if not file_storage.filename.lower().endswith(".parquet"):
        raise ValidationError("仅支持 .parquet 文件。")

    token = uuid.uuid4().hex
    path = os.path.join(staging_dir, f"{token}.parquet")
    file_storage.save(path)
    try:
        columns = parquet_columns(path)
    except Exception as exc:  # 损坏/非 Parquet
        os.remove(path)
        raise ValidationError(f"文件无法按 Parquet 解析：{exc}") from exc

    db = get_db()
    db.execute(
        "INSERT INTO staged_files (token, parquet_path, columns) VALUES (?, ?, ?)",
        (token, path, json.dumps(columns, ensure_ascii=False)),
    )
    db.commit()
    return token


def get_stage(token: str) -> sqlite3.Row | None:
    if not token:
        return None
    return get_db().execute(
        "SELECT * FROM staged_files WHERE token = ?", (token,)
    ).fetchone()


def stage_columns(stage: sqlite3.Row) -> list[str]:
    return json.loads(stage["columns"])


# ---------- 列对照 ----------

def parse_mappings(form) -> dict[str, dict[str, str]]:
    """从表单解析高度指标选列：{metric_key: {"column": ..., "unit": ...}}。"""
    mappings: dict[str, dict[str, str]] = {}
    for key in METRIC_KEYS:
        column = (form.get(f"metric_{key}_col") or "").strip()
        unit = (form.get(f"metric_{key}_unit") or "").strip()
        if not column:
            continue  # 文件里没有该指标或用户不选，输出时该列留空
        mappings[key] = {"column": column, "unit": unit}
    return mappings


def validate_mappings(
    parquet_path: str,
    columns: list[str],
    tree_id_column: str,
    mappings: dict[str, dict[str, str]],
) -> dict[str, tuple[str, str]]:
    """校验样木号列与高度列对照，返回 {metric_key: (源列, 单位)}。"""
    if not tree_id_column:
        raise ValidationError("请指定样木号所在列。")
    if tree_id_column not in columns:
        raise ValidationError(f"样木号列「{tree_id_column}」不在文件列中。")
    if not mappings:
        raise ValidationError("请至少为一个高度指标选择来源列。")

    seen_columns: set[str] = set()
    normalized: dict[str, tuple[str, str]] = {}
    for key, cfg in mappings.items():
        if key not in METRIC_KEYS:
            raise ValidationError(f"未知高度指标：{key}")
        column, unit = cfg["column"], cfg["unit"]
        if column not in columns:
            raise ValidationError(
                f"{METRIC_LABELS[key]}的来源列「{column}」不在文件列中。"
            )
        if column == tree_id_column:
            raise ValidationError(
                f"{METRIC_LABELS[key]}的来源列不能与样木号列相同。"
            )
        if column in seen_columns:
            raise ValidationError(f"源列「{column}」被多个高度指标重复选用。")
        if unit not in UNITS:
            raise ValidationError(
                f"{METRIC_LABELS[key]}的单位必须是米(m)或厘米(cm)。"
            )
        if not is_numeric_column(parquet_path, column):
            raise ValidationError(
                f"{METRIC_LABELS[key]}的来源列「{column}」不是数值列，无法换算高度。"
            )
        seen_columns.add(column)
        normalized[key] = (column, unit)
    return normalized


# ---------- 发布（新批次或换交） ----------

@dataclass
class PublishResult:
    batch_id: int
    batch_no: str
    version_id: int
    version_no: int


def publish_version(
    stage: sqlite3.Row,
    tree_id_column: str,
    mappings: dict[str, tuple[str, str]],
    batch_no: str,
    time_point: str,
    note: str,
    published_dir: str,
    batch_id: int | None = None,
) -> PublishResult:
    """暂存文件 + 列对照在一个事务里一并生效。

    换交（batch_id 已存在）时先落文件再写库；任一步失败则回滚并清理，
    原正式版行与原文件都不受影响——换交失败沿用原版。
    """
    batch_no = (batch_no or "").strip()
    time_point = (time_point or "").strip()
    if not batch_no:
        raise ValidationError("批次号不能为空。")
    if not time_point:
        raise ValidationError("时点不能为空。")

    db = get_db()
    staged_path = stage["parquet_path"]
    target_path: str | None = None
    try:
        db.execute("BEGIN IMMEDIATE")

        if batch_id is None:
            existing = db.execute(
                "SELECT id FROM batches WHERE batch_no = ?", (batch_no,)
            ).fetchone()
            if existing is not None:
                raise ValidationError(f"批次号「{batch_no}」已存在，请改用换交。")
            cur = db.execute(
                "INSERT INTO batches (batch_no, time_point, note) VALUES (?, ?, ?)",
                (batch_no, time_point, note.strip()),
            )
            batch_id = cur.lastrowid
        else:
            row = db.execute(
                "SELECT id, time_point FROM batches WHERE id = ?", (batch_id,)
            ).fetchone()
            if row is None:
                raise ValidationError("目标批次不存在。")
            if row["time_point"] != time_point:
                raise ValidationError(
                    "时点与该批次既有记录不一致；同一批次的时点不可更改。"
                )

        next_no = db.execute(
            "SELECT COALESCE(MAX(version_no), 0) + 1 FROM versions WHERE batch_id = ?",
            (batch_id,),
        ).fetchone()[0]

        batch_dir = os.path.join(published_dir, str(batch_id))
        os.makedirs(batch_dir, exist_ok=True)
        target_path = os.path.join(batch_dir, f"v{next_no}.parquet")
        os.replace(staged_path, target_path)  # 同卷原子改名

        db.execute(
            "UPDATE versions SET is_official = 0 WHERE batch_id = ?", (batch_id,)
        )
        cur = db.execute(
            """INSERT INTO versions
                   (batch_id, version_no, is_official, parquet_path, tree_id_column)
               VALUES (?, ?, 1, ?, ?)""",
            (batch_id, next_no, target_path, tree_id_column),
        )
        version_id = cur.lastrowid
        for key, (column, unit) in mappings.items():
            db.execute(
                """INSERT INTO version_metrics (version_id, metric_key, source_column, unit)
                   VALUES (?, ?, ?, ?)""",
                (version_id, key, column, unit),
            )

        db.execute("DELETE FROM staged_files WHERE token = ?", (stage["token"],))
        db.commit()
    except Exception:
        db.rollback()
        if target_path and os.path.exists(target_path):
            try:
                os.remove(target_path)
            except OSError:
                pass
        raise

    return PublishResult(batch_id, batch_no, version_id, next_no)


# ---------- 查询 ----------

def list_batches() -> list[sqlite3.Row]:
    return get_db().execute(
        """
        SELECT b.*,
               COUNT(v.id)                                  AS version_count,
               SUM(v.is_official)                           AS official_count,
               (SELECT version_no FROM versions
                 WHERE batch_id = b.id AND is_official = 1) AS official_version_no
          FROM batches b
          LEFT JOIN versions v ON v.batch_id = b.id
         GROUP BY b.id
         ORDER BY b.time_point, b.batch_no
        """
    ).fetchall()


def get_batch(batch_id: int) -> sqlite3.Row | None:
    return get_db().execute(
        "SELECT * FROM batches WHERE id = ?", (batch_id,)
    ).fetchone()


def list_versions(batch_id: int) -> list[sqlite3.Row]:
    return get_db().execute(
        "SELECT * FROM versions WHERE batch_id = ? ORDER BY version_no DESC",
        (batch_id,),
    ).fetchall()


def get_version(version_id: int) -> sqlite3.Row | None:
    return get_db().execute(
        """
        SELECT v.*, b.batch_no, b.time_point
          FROM versions v JOIN batches b ON b.id = v.batch_id
         WHERE v.id = ?
        """,
        (version_id,),
    ).fetchone()


def get_published_version(version_id: int) -> sqlite3.Row | None:
    """取已发布（正式版）版本；历史版与不存在均返回 None。"""
    return get_db().execute(
        """
        SELECT v.*, b.batch_no, b.time_point, b.id AS batch_id
          FROM versions v JOIN batches b ON b.id = v.batch_id
         WHERE v.id = ? AND v.is_official = 1
        """,
        (version_id,),
    ).fetchone()


def list_published_versions() -> list[sqlite3.Row]:
    """全部已发布版本，按时间倒序（晚的在前）供复测核对选择。"""
    return get_db().execute(
        """
        SELECT v.id, v.version_no, v.is_official, v.parquet_path,
               v.tree_id_column, v.batch_id, b.batch_no, b.time_point
          FROM versions v JOIN batches b ON b.id = v.batch_id
         WHERE v.is_official = 1
         ORDER BY b.time_point DESC, b.batch_no, v.version_no DESC
        """
    ).fetchall()


def metric_map(version_id: int) -> dict[str, tuple[str, str]]:
    rows = get_db().execute(
        "SELECT metric_key, source_column, unit FROM version_metrics WHERE version_id = ?",
        (version_id,),
    ).fetchall()
    return {r["metric_key"]: (r["source_column"], r["unit"]) for r in rows}


def official_versions_for(batch_ids: list[int]) -> list[sqlite3.Row]:
    """取这些批次当前正式版，并强制同一时点约束。"""
    if not batch_ids:
        raise ValidationError("请先勾选批次。")
    placeholders = ",".join("?" for _ in batch_ids)
    rows = get_db().execute(
        f"""
        SELECT v.*, b.batch_no, b.time_point, b.id AS batch_id
          FROM versions v JOIN batches b ON b.id = v.batch_id
         WHERE v.is_official = 1 AND b.id IN ({placeholders})
         ORDER BY b.batch_no
        """,
        batch_ids,
    ).fetchall()
    if len(rows) != len(set(batch_ids)):
        raise ValidationError("所选批次中存在没有正式版的批次，无法并表。")
    time_points = {r["time_point"] for r in rows}
    if len(time_points) > 1:
        raise ValidationError(
            "勾选批次的时点不一致（%s），并表仅限同一时点。"
            % "、".join(sorted(time_points))
        )
    return rows
