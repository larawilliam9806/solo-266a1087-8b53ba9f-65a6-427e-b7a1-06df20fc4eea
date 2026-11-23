"""生成演示用 Parquet 样例（独立于应用运行，仅供手工体验）。

用法：
    python scripts/make_sample_parquet.py /tmp/sample_data
会在输出目录下生成三个文件，模拟“同名列语义不同、单位不同”的历史表：
  plot_a_2024.parquet   height 列是树高，单位米
  plot_b_2024.parquet   height 列是枝下高，单位厘米；另有 tree_h 树高（厘米）
  plot_a_2024_v2.parquet 换交用：列名/单位与首版不同
"""
from __future__ import annotations

import os
import sys

# 兼容 `python3 -I` 隔离运行：用户级 site-packages 不会自动加载
for _candidate in (
    os.path.expanduser("~/.local/lib/python3.11/site-packages"),
    "/usr/local/lib/python3.11/dist-packages",
):
    if os.path.isdir(_candidate) and _candidate not in sys.path:
        sys.path.append(_candidate)

import pyarrow as pa
import pyarrow.parquet as pq


def write(path: str, table: pa.Table) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pq.write_table(table, path, compression="snappy")
    print(f"wrote {path} ({table.num_rows} rows)")


def main(out_dir: str) -> None:
    n = 30

    # A 班：height = 树高（米），无枝下高列
    a = pa.table(
        {
            "sample_tree_no": [f"A-{i:03d}" for i in range(1, n + 1)],
            "height": [round(8 + (i % 7) * 0.35, 2) for i in range(n)],
            "dbh_cm": [12 + (i % 9) for i in range(n)],
        }
    )
    write(os.path.join(out_dir, "plot_a_2024.parquet"), a)

    # B 班：同名列 height 在这里是枝下高（厘米）；tree_h 才是树高（厘米）
    b = pa.table(
        {
            "sample_tree_no": [f"B-{i:03d}" for i in range(1, n + 1)],
            "height": [320 + (i % 11) * 12 for i in range(n)],
            "tree_h": [950 + (i % 13) * 25 for i in range(n)],
            "dbh_cm": [10 + (i % 8) for i in range(n)],
            "note": [None if i % 4 else "偏冠" for i in range(n)],
        }
    )
    write(os.path.join(out_dir, "plot_b_2024.parquet"), b)

    # A 班换交版：列名改成 tree_id / 树高（厘米，带缺项）/ 枝下高（米）
    a_v2 = pa.table(
        {
            "tree_id": [f"A-{i:03d}" for i in range(1, n + 6)],
            "shu_gao_cm": [None if i % 10 == 0 else 850 + (i % 7) * 30 for i in range(n + 5)],
            "zhi_xia_gao_m": [round(3.1 + (i % 6) * 0.2, 2) for i in range(n + 5)],        }
    )
    write(os.path.join(out_dir, "plot_a_2024_v2.parquet"), a_v2)


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "/tmp/forestry_sample"
    main(target)
