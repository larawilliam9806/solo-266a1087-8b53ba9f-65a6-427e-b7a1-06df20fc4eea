"""Parquet 读取与单位换算。

全部基于 pyarrow 分批扫描，不使用 pandas，内存占用与批大小相关，
不随文件总行数增长。
"""
from __future__ import annotations

import csv
import io
from typing import Iterable, Iterator

import pyarrow as pa
import pyarrow.parquet as pq

from .constants import OUTPUT_COLUMNS, PREVIEW_ROWS, READ_BATCH_SIZE


def parquet_columns(path: str) -> list[str]:
    return pq.ParquetFile(path).schema_arrow.names


def is_numeric_column(path: str, column: str) -> bool:
    schema = pq.ParquetFile(path).schema_arrow
    return pa.types.is_integer(schema.field(column).type) or pa.types.is_floating(
        schema.field(column).type
    )


def _to_float(value) -> float | None:
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    # NaN 等一律视为缺项
    if f != f:
        return None
    return f


def convert_height(value, unit: str) -> float | None:
    """高度统一成米；厘米除以 100，缺项返回 None（输出留空）。"""
    f = _to_float(value)
    if f is None:
        return None
    return f / 100.0 if unit == "cm" else f


def format_height(value: float | None) -> str:
    if value is None:
        return ""
    return format(value, ".6f").rstrip("0").rstrip(".")


def iter_output_rows(
    path: str,
    tree_id_column: str,
    metric_sources: dict[str, tuple[str, str]],
    batch_no: str,
    version_no: int,
) -> Iterator[dict[str, str]]:
    """按批扫描 Parquet，产出统一口径的输出行（生成器，不整表载入）。

    metric_sources: {metric_key: (源列名, 单位)}，没有配置的指标输出空串。
    保留 Parquet 中全部样木行，不做任何过滤。
    """
    wanted = [tree_id_column]
    for source_col, _unit in metric_sources.values():
        if source_col not in wanted:
            wanted.append(source_col)

    pf = pq.ParquetFile(path)
    for batch in pf.iter_batches(batch_size=READ_BATCH_SIZE, columns=wanted):
        columns = {name: batch.column(name).to_pylist() for name in batch.schema.names}
        id_values = columns[tree_id_column]
        for i in range(batch.num_rows):
            row = {
                "batch_no": batch_no,
                "version_no": str(version_no),
                "tree_id": "" if id_values[i] is None else str(id_values[i]),
            }
            for metric_key, out_col in (
                ("tree_height", "tree_height_m"),
                ("under_branch_height", "under_branch_height_m"),
            ):
                if metric_key in metric_sources:
                    source_col, unit = metric_sources[metric_key]
                    row[out_col] = format_height(
                        convert_height(columns[source_col][i], unit)
                    )
                else:
                    row[out_col] = ""
            yield row


def preview_rows(
    path: str,
    tree_id_column: str,
    metric_sources: dict[str, tuple[str, str]],
    batch_no: str,
    version_no: int,
    limit: int = PREVIEW_ROWS,
) -> list[dict[str, str]]:
    """只取前 limit 行：iter_batches 读到足够行即停止，不读完整批。"""
    rows: list[dict[str, str]] = []
    for row in iter_output_rows(
        path, tree_id_column, metric_sources, batch_no, version_no
    ):
        rows.append(row)
        if len(rows) >= limit:
            break
    return rows


def csv_chunks(rows: Iterable[dict[str, str]], include_bom: bool = True) -> Iterator[bytes]:
    """把输出行流式编码为 CSV 块。带 UTF-8 BOM，Excel 直接打开不乱码。"""
    if include_bom:
        yield "\ufeff".encode("utf-8")
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=OUTPUT_COLUMNS)
    writer.writeheader()
    yield buffer.getvalue().encode("utf-8")
    buffer.seek(0)
    buffer.truncate(0)
    for row in rows:
        writer.writerow(row)
        if buffer.tell() >= 64 * 1024:
            yield buffer.getvalue().encode("utf-8")
            buffer.seek(0)
            buffer.truncate(0)
    if buffer.tell():
        yield buffer.getvalue().encode("utf-8")
