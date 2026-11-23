"""复测核对：同一样地前后两期已发布版本的换牌核对。

核对流程全部在一个 SQLite 连接的**临时表**内完成，且该连接设置
``PRAGMA temp_store=FILE``，临时表/临时索引由磁盘支撑：

1. 两个版本的 Parquet 按 RecordBatch 分批扫描（批大小 1 万行），
   逐批 executemany 灌入临时表，内存不随文件总行数增长；
2. 换牌 CSV 逐行流式解析后同样入临时表；
3. 空号、重号、换牌合规性全部用 SQL 校验，一条出错整份核对表都不发出；
4. 先按换牌表配对，剩余同号记录自动配对，再补两期单边记录；
5. 结果用服务端游标 ``fetchmany`` 流式产出 CSV。

连接关闭后所有临时表自动销毁，不留副本。
"""
from __future__ import annotations

import csv
import os
import sqlite3
import uuid
from dataclasses import dataclass, field

from .constants import (
    PRESENCE_BOTH,
    PRESENCE_CURR_ONLY,
    PRESENCE_PREV_ONLY,
    READ_BATCH_SIZE,
    SWAP_CURR_HEADER,
    SWAP_HEADERS,
    SWAP_PREV_HEADER,
)
from .parquet_tools import iter_tree_id_height


class RecheckFormError(Exception):
    """换牌文件本身无法解析（编码/表头/列数），不进入逐行校验。"""


@dataclass
class RecheckIssue:
    """一条核对错误：来源 + 记录序号 + 原因。"""

    source: str
    seq: int
    reason: str

    @property
    def label(self) -> str:
        if self.seq:
            return f"{self.source} · 第 {self.seq} 条记录：{self.reason}"
        return f"{self.source}：{self.reason}"


@dataclass
class RecheckResult:
    issues: list[RecheckIssue] = field(default_factory=list)
    total_rows: int = 0
    both_count: int = 0
    prev_only_count: int = 0
    curr_only_count: int = 0

    @property
    def ok(self) -> bool:
        return not self.issues


# ---------- 临时表结构 ----------

_TEMP_SCHEMA = """
CREATE TEMP TABLE temp_prev_census (
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,
    tree_id    TEXT NOT NULL,
    height_m   TEXT NOT NULL DEFAULT ''
);
CREATE TEMP TABLE temp_curr_census (
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,
    tree_id    TEXT NOT NULL,
    height_m   TEXT NOT NULL DEFAULT ''
);
CREATE TEMP TABLE temp_swap (
    seq       INTEGER PRIMARY KEY AUTOINCREMENT,
    prev_id   TEXT,
    curr_id   TEXT
);
CREATE TEMP TABLE temp_match (
    row_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    category      INTEGER NOT NULL,
    source_seq    INTEGER NOT NULL,
    prev_id       TEXT,
    curr_id       TEXT,
    prev_height   TEXT NOT NULL DEFAULT '',
    curr_height   TEXT NOT NULL DEFAULT '',
    presence      TEXT NOT NULL
);
"""


def _create_temp_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(_TEMP_SCHEMA)


def _load_census(
    conn: sqlite3.Connection,
    table: str,
    version: sqlite3.Row,
    height_source: tuple[str, str] | None,
) -> int:
    """分批扫描一个版本的 Parquet，逐批写入临时表，返回总行数。"""
    if table not in ("temp_prev_census", "temp_curr_census"):
        raise ValueError(table)
    total = 0
    batch: list[tuple[str, str]] = []
    for tree_id, height_text in iter_tree_id_height(
        version["parquet_path"], version["tree_id_column"], height_source
    ):
        batch.append((tree_id, height_text))
        if len(batch) >= READ_BATCH_SIZE:
            conn.executemany(
                f"INSERT INTO {table} (tree_id, height_m) VALUES (?, ?)", batch
            )
            total += len(batch)
            batch.clear()
    if batch:
        conn.executemany(
            f"INSERT INTO {table} (tree_id, height_m) VALUES (?, ?)", batch
        )
        total += len(batch)
    return total


def load_swap_file(conn: sqlite3.Connection, path: str) -> int:
    """流式解析两列换牌 CSV 并分批写入 temp_swap，返回换牌记录数。

    逐行读取、攒满一个批大小就 executemany 落盘，换牌记录不整份驻留内存，
    内存开销只与批大小有关，不随换牌表总行数增长。

    空单元格存 NULL；仅结构性错误（缺文件、编码、表头、列数、无记录）在此抛出，
    业务合规性（空号、重号、不存在）交给 SQL 校验并带记录序号。
    """
    if not path or not os.path.exists(path):
        raise RecheckFormError("换牌 CSV 文件不存在或已失效，请重新上传。")

    count = 0
    batch: list[tuple[str | None, str | None]] = []

    def flush() -> None:
        nonlocal count
        if batch:
            conn.executemany(
                "INSERT INTO temp_swap (prev_id, curr_id) VALUES (?, ?)", batch
            )
            count += len(batch)
            batch.clear()

    try:
        # 直接对文件对象迭代，避免 read() 整份载入
        with open(path, "r", encoding="utf-8-sig", newline="") as fh:
            reader = csv.reader(fh)
            try:
                header = next(reader)
            except StopIteration as exc:
                raise RecheckFormError(
                    "换牌 CSV 内容为空，需要表头与至少一行记录。"
                ) from exc
            except UnicodeDecodeError as exc:
                raise RecheckFormError(
                    "换牌 CSV 不是有效的 UTF-8 文本文件。"
                ) from exc

            if [c.strip() for c in header] != list(SWAP_HEADERS):
                raise RecheckFormError(
                    f"换牌 CSV 表头必须是两列「{SWAP_PREV_HEADER}」"
                    f"「{SWAP_CURR_HEADER}」（分别填前期、本期样木号）。"
                )

            for line_no, raw in enumerate(reader, start=2):
                if not raw:
                    continue  # 真正的空行（零个字段）跳过，不占记录序号
                if len(raw) != 2:
                    raise RecheckFormError(
                        f"换牌 CSV 第 {line_no} 行不是两列，"
                        "请按模板每行只填两个样木号。"
                    )
                # 两个空字段的行予以保留，交由校验按“一行至少填一侧”报错
                prev = raw[0].strip() or None
                curr = raw[1].strip() or None
                batch.append((prev, curr))
                if len(batch) >= READ_BATCH_SIZE:
                    flush()
            flush()
    except UnicodeDecodeError as exc:
        raise RecheckFormError("换牌 CSV 不是有效的 UTF-8 文本文件。") from exc
    except OSError as exc:
        raise RecheckFormError(f"换牌 CSV 无法读取：{exc}") from exc

    if count == 0:
        raise RecheckFormError("换牌 CSV 只有表头，没有任何换牌记录。")
    return count


# ---------- 校验 ----------

def _validate(conn: sqlite3.Connection) -> list[RecheckIssue]:
    issues: list[RecheckIssue] = []

    def collect(sql: str, source: str) -> None:
        for seq, reason in conn.execute(sql):
            issues.append(RecheckIssue(source, int(seq), reason))

    # 任一期普查有空号
    collect(
        """
        SELECT seq, '样木号为空（空号），普查记录不允许缺号。'
          FROM temp_prev_census
         WHERE tree_id IS NULL OR trim(tree_id) = ''
        """,
        "前期普查",
    )
    collect(
        """
        SELECT seq, '样木号为空（空号），普查记录不允许缺号。'
          FROM temp_curr_census
         WHERE tree_id IS NULL OR trim(tree_id) = ''
        """,
        "本期普查",
    )

    # 任一期普查有重号
    collect(
        """
        SELECT MIN(seq), '样木号「' || tree_id || '」在本期普查中重复登记。'
          FROM temp_prev_census
         WHERE trim(tree_id) <> ''
         GROUP BY tree_id
        HAVING COUNT(*) > 1
        """,
        "前期普查",
    )
    collect(
        """
        SELECT MIN(seq), '样木号「' || tree_id || '」在本期普查中重复登记。'
          FROM temp_curr_census
         WHERE trim(tree_id) <> ''
         GROUP BY tree_id
        HAVING COUNT(*) > 1
        """,
        "本期普查",
    )

    # 换牌表一行至少填一侧（按换牌记录序号去重，普查重号不会放大行数）
    collect(
        """
        SELECT seq, '前期号与本期号都未填写，每行至少填一侧。'
          FROM temp_swap
         WHERE prev_id IS NULL AND curr_id IS NULL
         GROUP BY seq
        """,
        "换牌表",
    )

    # 换牌表同一列只能用一次
    collect(
        """
        SELECT MIN(seq), '前期样木号「' || prev_id || '」在换牌表前期列重复出现。'
          FROM temp_swap
         WHERE prev_id IS NOT NULL
         GROUP BY prev_id
        HAVING COUNT(*) > 1
        """,
        "换牌表",
    )
    collect(
        """
        SELECT MIN(seq), '本期样木号「' || curr_id || '」在换牌表本期列重复出现。'
          FROM temp_swap
         WHERE curr_id IS NOT NULL
         GROUP BY curr_id
        HAVING COUNT(*) > 1
        """,
        "换牌表",
    )

    # 每个填入的号在对应版本中须存在（按换牌记录序号去重，普查重号不放大）
    collect(
        """
        SELECT s.seq, '前期样木号「' || s.prev_id || '」在前期选定版本中不存在。'
          FROM temp_swap s
         WHERE s.prev_id IS NOT NULL
           AND NOT EXISTS (
                 SELECT 1 FROM temp_prev_census p WHERE p.tree_id = s.prev_id
               )
         GROUP BY s.seq
        """,
        "换牌表",
    )
    collect(
        """
        SELECT s.seq, '本期样木号「' || s.curr_id || '」在本期选定版本中不存在。'
          FROM temp_swap s
         WHERE s.curr_id IS NOT NULL
           AND NOT EXISTS (
                 SELECT 1 FROM temp_curr_census c WHERE c.tree_id = s.curr_id
               )
         GROUP BY s.seq
        """,
        "换牌表",
    )

    issues.sort(key=lambda item: (item.source, item.seq, item.reason))
    return issues


# ---------- 配对 ----------

def _match(conn: sqlite3.Connection) -> None:
    """先换牌配对，再对两侧都未被换牌占用的同号记录自动配对，最后补单边。"""
    conn.execute("DELETE FROM temp_match")

    # ① 换牌表配对（保留换牌表原始行序）
    conn.execute(
        f"""
        INSERT INTO temp_match
          (category, source_seq, prev_id, curr_id, prev_height, curr_height, presence)
        SELECT 0, s.seq,
               s.prev_id, s.curr_id,
               COALESCE(p.height_m, ''),
               COALESCE(c.height_m, ''),
               CASE
                 WHEN s.prev_id IS NOT NULL AND s.curr_id IS NOT NULL
                   THEN '{PRESENCE_BOTH}'
                 WHEN s.prev_id IS NOT NULL THEN '{PRESENCE_PREV_ONLY}'
                 ELSE '{PRESENCE_CURR_ONLY}'
               END
          FROM temp_swap s
          LEFT JOIN temp_prev_census p ON p.tree_id = s.prev_id
          LEFT JOIN temp_curr_census c ON c.tree_id = s.curr_id
        """
    )

    # ② 两侧都未被换牌表占用的同号记录自动配对
    conn.execute(
        f"""
        INSERT INTO temp_match
          (category, source_seq, prev_id, curr_id, prev_height, curr_height, presence)
        SELECT 1, p.seq, p.tree_id, c.tree_id, p.height_m, c.height_m,
               '{PRESENCE_BOTH}'
          FROM temp_prev_census p
          JOIN temp_curr_census c ON c.tree_id = p.tree_id
         WHERE NOT EXISTS (SELECT 1 FROM temp_swap s WHERE s.prev_id = p.tree_id)
           AND NOT EXISTS (SELECT 1 FROM temp_swap s WHERE s.curr_id = c.tree_id)
        """
    )

    # ③ 仅前期登记：前期记录未被换牌占用，且没被同号自动配对。
    #    注意不能要求“本期不存在同号”——若该同号在本期被换牌单边行占用，
    #    自动配对不会发生，这株前期记录仍应落到仅前期登记。
    conn.execute(
        f"""
        INSERT INTO temp_match
          (category, source_seq, prev_id, curr_id, prev_height, curr_height, presence)
        SELECT 2, p.seq, p.tree_id, NULL, p.height_m, '',
               '{PRESENCE_PREV_ONLY}'
          FROM temp_prev_census p
         WHERE NOT EXISTS (SELECT 1 FROM temp_swap s WHERE s.prev_id = p.tree_id)
           AND NOT EXISTS (SELECT 1 FROM temp_match m WHERE m.category = 1
                            AND m.prev_id = p.tree_id)
        """
    )

    # ④ 仅本期登记：本期记录未被换牌占用，且没被同号自动配对（同理，不要求同号不存在）
    conn.execute(
        f"""
        INSERT INTO temp_match
          (category, source_seq, prev_id, curr_id, prev_height, curr_height, presence)
        SELECT 3, c.seq, NULL, c.tree_id, '', c.height_m,
               '{PRESENCE_CURR_ONLY}'
          FROM temp_curr_census c
         WHERE NOT EXISTS (SELECT 1 FROM temp_swap s WHERE s.curr_id = c.tree_id)
           AND NOT EXISTS (SELECT 1 FROM temp_match m WHERE m.category = 1
                            AND m.curr_id = c.tree_id)
        """
    )


# ---------- 结果产出 ----------

_RESULT_SQL = """
SELECT prev_id, curr_id, prev_height, curr_height, presence
  FROM temp_match
 ORDER BY category, source_seq
"""


def _summarize(conn: sqlite3.Connection) -> tuple[int, int, int, int]:
    row = conn.execute(
        """
        SELECT COUNT(*),
               SUM(CASE WHEN presence = ? THEN 1 ELSE 0 END),
               SUM(CASE WHEN presence = ? THEN 1 ELSE 0 END),
               SUM(CASE WHEN presence = ? THEN 1 ELSE 0 END)
          FROM temp_match
        """,
        (PRESENCE_BOTH, PRESENCE_PREV_ONLY, PRESENCE_CURR_ONLY),
    ).fetchone()
    total = int(row[0] or 0)
    return total, int(row[1] or 0), int(row[2] or 0), int(row[3] or 0)


def iter_recheck_rows(
    conn: sqlite3.Connection,
    prev_version: sqlite3.Row,
    curr_version: sqlite3.Row,
):
    """服务端游标逐批产出核对结果行（dict），内存不随结果行数增长。"""
    cursor = conn.execute(_RESULT_SQL)
    prev_batch = prev_version["batch_no"]
    curr_batch = curr_version["batch_no"]
    while True:
        part = cursor.fetchmany(READ_BATCH_SIZE)
        if not part:
            break
        for prev_id, curr_id, prev_height, curr_height, presence in part:
            yield {
                "prev_batch_no": prev_batch,
                "prev_version_no": str(prev_version["version_no"]),
                "prev_tree_id": prev_id or "",
                "curr_batch_no": curr_batch,
                "curr_version_no": str(curr_version["version_no"]),
                "curr_tree_id": curr_id or "",
                "tree_height_m_prev": prev_height or "",
                "tree_height_m_curr": curr_height or "",
                "presence": presence,
            }


def preview_recheck_rows(
    conn: sqlite3.Connection,
    prev_version: sqlite3.Row,
    curr_version: sqlite3.Row,
    limit: int,
) -> list[dict]:
    rows: list[dict] = []
    for row in iter_recheck_rows(conn, prev_version, curr_version):
        rows.append(row)
        if len(rows) >= limit:
            break
    return rows


# ---------- 主编排 ----------

def height_source_for(
    conn: sqlite3.Connection, version_id: int
) -> tuple[str, str] | None:
    """取该版本树高指标的 (源列, 单位)；未配置树高则返回 None。"""
    row = conn.execute(
        "SELECT source_column, unit FROM version_metrics "
        "WHERE version_id = ? AND metric_key = 'tree_height'",
        (version_id,),
    ).fetchone()
    if row is None:
        return None
    return row["source_column"], row["unit"]


def run_recheck(
    conn: sqlite3.Connection,
    prev_version: sqlite3.Row,
    curr_version: sqlite3.Row,
    swap_path: str,
) -> RecheckResult:
    """在给定连接上跑完整核对；任一步出错都不生成配对结果。"""
    _create_temp_tables(conn)
    _load_census(
        conn,
        "temp_prev_census",
        prev_version,
        height_source_for(conn, prev_version["id"]),
    )
    _load_census(
        conn,
        "temp_curr_census",
        curr_version,
        height_source_for(conn, curr_version["id"]),
    )
    load_swap_file(conn, swap_path)

    # 临时索引同样落在磁盘（temp_store=FILE）：让存在性校验与配对的
    # NOT EXISTS / JOIN 走索引而非全表扫描，耗时随行数近线性，而不是平方级。
    conn.execute("CREATE INDEX idx_prev_id ON temp_prev_census(tree_id)")
    conn.execute("CREATE INDEX idx_curr_id ON temp_curr_census(tree_id)")
    conn.execute("CREATE INDEX idx_swap_prev ON temp_swap(prev_id)")
    conn.execute("CREATE INDEX idx_swap_curr ON temp_swap(curr_id)")

    issues = _validate(conn)
    result = RecheckResult(issues=issues)
    if issues:
        return result

    _match(conn)
    total, both, prev_only, curr_only = _summarize(conn)
    result.total_rows = total
    result.both_count = both
    result.prev_only_count = prev_only
    result.curr_only_count = curr_only
    return result


def new_swap_token() -> str:
    return uuid.uuid4().hex
