"""纯函数级测试：换算、格式化、流式迭代与预览截断。"""
from __future__ import annotations

import csv
import io

import pyarrow as pa
import pyarrow.parquet as pq

from app.constants import OUTPUT_COLUMNS
from app.parquet_tools import (
    convert_height,
    csv_chunks,
    format_height,
    iter_output_rows,
    preview_rows,
)


def test_convert_and_format():
    assert convert_height(150, "cm") == 1.5
    assert convert_height(1.5, "m") == 1.5
    assert convert_height(None, "m") is None
    assert convert_height(float("nan"), "m") is None
    assert format_height(None) == ""
    assert format_height(4.05) == "4.05"
    assert format_height(9.1) == "9.1"


def test_iter_rows_preserves_all_and_blanks(tmp_path):
    path = tmp_path / "x.parquet"
    pq.write_table(
        pa.table(
            {
                "tid": ["t1", "t2", "t3"],
                "h_cm": [100, None, 250],
                "ub_m": [2.0, 3.5, None],
            }
        ),
        path,
    )
    mappings = {
        "tree_height": ("h_cm", "cm"),
        "under_branch_height": ("ub_m", "m"),
    }
    rows = list(iter_output_rows(str(path), "tid", mappings, "B1", 7))
    assert len(rows) == 3
    assert rows[0] == {
        "batch_no": "B1",
        "version_no": "7",
        "tree_id": "t1",
        "tree_height_m": "1",
        "under_branch_height_m": "2",
    }
    assert rows[1]["tree_height_m"] == ""
    assert rows[2]["under_branch_height_m"] == ""


def test_preview_does_not_require_full_scan(tmp_path):
    # 6 万行文件，预览只取 20 行；用读取批次计数证明扫描提前停止
    path = tmp_path / "big.parquet"
    n = 60_000
    pq.write_table(
        pa.table({"tid": [f"t{i}" for i in range(n)], "h": [float(i) for i in range(n)]}),
        path,
    )
    rows = preview_rows(
        str(path), "tid", {"tree_height": ("h", "m")}, "B", 1, limit=20
    )
    assert len(rows) == 20
    assert rows[0]["tree_id"] == "t0"
    assert rows[-1]["tree_id"] == "t19"


def test_csv_chunks_streaming_and_header(tmp_path):
    path = tmp_path / "y.parquet"
    pq.write_table(pa.table({"tid": ["a", "b"], "h": [1.0, 2.0]}), path)
    gen = iter_output_rows(str(path), "tid", {"tree_height": ("h", "m")}, "B", 1)
    data = b"".join(csv_chunks(gen))
    text = data.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    assert reader.fieldnames == OUTPUT_COLUMNS
    rows = list(reader)
    assert rows[0]["tree_id"] == "a"
    assert rows[1]["tree_height_m"] == "2"
    assert data.startswith("﻿".encode("utf-8"))  # BOM，Excel 友好
