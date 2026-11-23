"""复测核对（换牌）端到端测试。

覆盖：换牌配对优先、同号自动配对、单边登记、前导零保留、树高换算、
空号/重号/换牌违规的“来源+序号+原因”整表拒绝、下载重算与换交隔离、
流式下载与模板。
"""
from __future__ import annotations

import csv
import io

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from conftest import publish_form, stage_token_from_redirect, upload


def _parse_csv(data: bytes) -> list[dict]:
    return list(csv.DictReader(io.StringIO(data.decode("utf-8-sig"))))


def _swap_csv(rows, *, header=("prev_tree_id", "curr_tree_id"), bom=False):
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(header)
    writer.writerows(rows)
    text = buf.getvalue()
    if bom:
        text = "﻿" + text
    return io.BytesIO(text.encode("utf-8"))


def _post_swap(client, prev_id, curr_id, rows, *, filename="swap.csv", header=None):
    payload = {
        "prev_version_id": str(prev_id),
        "curr_version_id": str(curr_id),
    }
    if header is None:
        f = _swap_csv(rows)
    else:
        f = _swap_csv(rows, header=header)
    payload["swap_file"] = (f, filename)
    return client.post(
        "/recheck",
        data=payload,
        content_type="multipart/form-data",
        follow_redirects=True,
    )


def _publish(client, parquet_factory, name, tree_ids, heights, *, batch_no, time_point,
             tree_id_col="tid", height_col="h", unit="m"):
    path = parquet_factory(
        name, {tree_id_col: tree_ids, height_col: heights}
    )
    token = stage_token_from_redirect(upload(client, path))
    client.post(
        "/batches/new",
        data=publish_form(
            token,
            tree_id_column=tree_id_col,
            metrics={"tree_height": (height_col, unit)},
            batch_no=batch_no,
            time_point=time_point,
        ),
        follow_redirects=True,
    )


@pytest.fixture()
def two_periods(client, parquet_factory):
    # 同一样地前后两期 = 两个批次（批次号唯一、时点各自固定），各有一个正式版
    _publish(
        client, parquet_factory, "p1.parquet",
        ["001", "002", "003"], [10.0, 11.0, 12.0],
        batch_no="PLOT-1-2024", time_point="2024-09",
    )
    _publish(
        client, parquet_factory, "p2.parquet",
        ["001", "002", "004"], [1000.0, 1100.0, 1400.0],
        batch_no="PLOT-1-2025", time_point="2025-09", unit="m",
    )
    return 1, 2


# ---------- 页面与模板 ----------

def test_recheck_page_lists_official_versions_and_template(client, two_periods):
    page = client.get("/recheck")
    assert page.status_code == 200
    assert "PLOT-1-2024" in page.text
    assert "2024-09" in page.text and "2025-09" in page.text
    assert 'name="prev_version_id"' in page.text
    assert 'name="curr_version_id"' in page.text

    resp = client.get("/recheck/template")
    assert resp.status_code == 200
    rows = list(csv.reader(io.StringIO(resp.data.decode("utf-8-sig"))))
    assert rows[0] == ["prev_tree_id", "curr_tree_id"]
    assert resp.data.startswith("﻿".encode("utf-8"))


def test_index_links_to_recheck(client, two_periods):
    assert '/recheck' in client.get("/").text


def test_same_version_rejected(client, two_periods):
    resp = _post_swap(client, 1, 1, [("001", "001")])
    assert "两个不同的版本" in resp.text


def test_missing_file_rejected(client, two_periods):
    resp = client.post(
        "/recheck",
        data={"prev_version_id": "1", "curr_version_id": "2"},
        content_type="multipart/form-data",
        follow_redirects=True,
    )
    assert "换牌 CSV" in resp.text


def test_non_csv_rejected(client, two_periods):
    resp = client.post(
        "/recheck",
        data={
            "prev_version_id": "1",
            "curr_version_id": "2",
            "swap_file": (io.BytesIO(b"x"), "swap.txt"),
        },
        content_type="multipart/form-data",
        follow_redirects=True,
    )
    assert ".csv" in resp.text


def test_bad_header_rejected(client, two_periods):
    resp = _post_swap(
        client, 1, 2, [("001", "001")],
        header=("old_id", "new_id"),
    )
    assert "表头必须是两列" in resp.text


def test_swap_file_with_only_header_rejected(client, two_periods):
    resp = _post_swap(client, 1, 2, [])
    assert "只有表头" in resp.text


# ---------- 核对规则 ----------

def test_auto_pair_same_id_and_single_side_rows(client, two_periods):
    # 没有任何换牌记录以外的占用：同号 001/002 自动配对，003 仅前期，004 仅本期
    resp = _post_swap(client, 1, 2, [("001", "001")])
    assert "核对结果（共" in resp.text
    assert "共 4 株" in resp.text
    assert "两期均登记 2 株" in resp.text
    assert "仅前期登记 1 株" in resp.text and "仅本期登记 1 株" in resp.text

    # 预览里保留前导零
    assert ">001<" in resp.text


def test_swap_pairing_takes_priority_and_download_matches(client, parquet_factory):
    # 两期编号完全不相交：3 株 ↔ 3 株，全部只能靠换牌表配对
    _publish(
        client, parquet_factory, "old.parquet",
        ["001", "002", "003"], [10.0, 11.0, 12.0],
        batch_no="PLOT-OLD", time_point="2024-09",
    )
    _publish(
        client, parquet_factory, "new.parquet",
        ["101", "102", "103"], [100.0, 110.0, 1400.0],
        batch_no="PLOT-NEW", time_point="2025-09", unit="m",
    )
    # 换牌表只覆盖两株；剩下的两株自动落到“仅前期/仅本期登记”
    resp = _post_swap(client, 1, 2, [("003", "103"), ("001", "101")])
    assert "核对结果（共" in resp.text
    import re
    match = re.search(r"/recheck/(\d+)/download", resp.text)
    assert match
    rid = match.group(1)

    data = client.get(f"/recheck/{rid}/download").data
    rows = _parse_csv(data)
    assert len(rows) == 4

    by_prev = {r["prev_tree_id"]: r for r in rows if r["prev_tree_id"]}
    # 换牌配对：003 ↔ 103，先按换牌表，不允许再把它们当单边
    swapped = by_prev["003"]
    assert swapped["curr_tree_id"] == "103"
    assert swapped["presence"] == "两期均登记"
    assert swapped["prev_batch_no"] == "PLOT-OLD"
    assert swapped["prev_version_no"] == "1"
    assert swapped["curr_version_no"] == "1"
    # 树高：前期 12 米；本期 103 为 1400（米口径，沿用文件数字）
    assert swapped["tree_height_m_prev"] == "12"
    assert swapped["tree_height_m_curr"] == "1400"

    # 未被换牌表占用的两株分别标单边
    prev_only = by_prev["002"]
    assert prev_only["curr_tree_id"] == ""
    assert prev_only["presence"] == "仅前期登记"
    curr_only = next(r for r in rows if r["curr_tree_id"] == "102")
    assert curr_only["prev_tree_id"] == ""
    assert curr_only["presence"] == "仅本期登记"

    # 换牌记录保留换牌表原始行序：003 在前、001 在后
    swap_rows = [r for r in rows if r["presence"] == "两期均登记"]
    assert [r["prev_tree_id"] for r in swap_rows] == ["003", "001"]


def test_auto_pair_uses_same_id_when_not_occupied_by_swap(client, two_periods):
    # 换牌占 003↔004；同号 001、002 两侧都未被占用 → 自动配对
    resp = _post_swap(client, 1, 2, [("003", "004")])
    assert "核对结果（共" in resp.text
    import re
    rid = re.search(r"/recheck/(\d+)/download", resp.text).group(1)
    rows = _parse_csv(client.get(f"/recheck/{rid}/download").data)
    assert len(rows) == 3  # 003/004 换牌 + 001 + 002
    by_prev = {r["prev_tree_id"]: r for r in rows if r["prev_tree_id"]}
    assert by_prev["001"]["curr_tree_id"] == "001"
    assert by_prev["002"]["curr_tree_id"] == "002"
    assert by_prev["003"]["curr_tree_id"] == "004"
    assert {r["presence"] for r in rows} == {"两期均登记"}


def test_one_sided_swap_rows_are_marked(client, two_periods):
    # 只填一侧：前期 003 在本期已消失；本期 004 为新登记
    resp = _post_swap(client, 1, 2, [("003", None), (None, "004")])
    assert "核对结果（共" in resp.text
    import re
    rid = re.search(r"/recheck/(\d+)/download", resp.text).group(1)
    rows = _parse_csv(client.get(f"/recheck/{rid}/download").data)
    prev_only = next(r for r in rows if r["prev_tree_id"] == "003")
    curr_only = next(r for r in rows if r["curr_tree_id"] == "004")
    assert prev_only["curr_tree_id"] == ""
    assert prev_only["presence"] == "仅前期登记"
    assert curr_only["prev_tree_id"] == ""
    assert curr_only["presence"] == "仅本期登记"


def test_one_sided_swap_occupies_one_side_other_side_not_lost(client, two_periods):
    # 换牌单边行 (空, 001) 占用本期 001；前期同号 001 不能自动配对，
    # 必须作为“仅前期登记”保留下来，而不是被静默丢弃。
    resp = _post_swap(client, 1, 2, [("003", "004"), (None, "001")])
    assert "核对结果（共" in resp.text
    import re
    rid = re.search(r"/recheck/(\d+)/download", resp.text).group(1)
    rows = _parse_csv(client.get(f"/recheck/{rid}/download").data)

    # 4 个配对结果：003↔004 两期、001 仅本期（换牌）、001 仅前期（落单）、002↔002 两期
    assert len(rows) == 4
    by_presence = {}
    for r in rows:
        by_presence.setdefault(r["presence"], []).append(r)
    prev_only = {(r["prev_tree_id"], r["curr_tree_id"])
                 for r in by_presence.get("仅前期登记", [])}
    curr_only = {(r["prev_tree_id"], r["curr_tree_id"])
                 for r in by_presence.get("仅本期登记", [])}
    assert ("001", "") in prev_only
    assert ("", "001") in curr_only
    # 003/004 换牌两期均登记，002 同号自动配对
    both = {(r["prev_tree_id"], r["curr_tree_id"])
            for r in by_presence["两期均登记"]}
    assert ("003", "004") in both and ("002", "002") in both


def test_height_converted_to_meters_and_blank_kept(client, parquet_factory, two_periods):
    # 第三版本树高列单位厘米；第四版本未配置树高
    _publish(
        client, parquet_factory, "cm.parquet",
        ["T1", "T2"], [150, 250],
        batch_no="CM", time_point="2025-01", height_col="h_cm", unit="cm",
    )
    # 第四版本只配置枝下高、不配置树高 → 核对输出的树高应为空
    path = parquet_factory(
        "noheight.parquet", {"tid": ["T1", "T3"], "ub_m": [2.0, 3.0]}
    )
    token = stage_token_from_redirect(upload(client, path))
    client.post(
        "/batches/new",
        data=publish_form(
            token, tree_id_column="tid",
            metrics={"under_branch_height": ("ub_m", "m")},
            batch_no="NOH", time_point="2026-01",
        ),
        follow_redirects=True,
    )
    # 前期 = 厘米树高版（id=3），本期 = 无树高版（id=4）
    resp = _post_swap(client, 3, 4, [("T1", "T1")])
    assert "核对结果（共" in resp.text
    import re
    rid = re.search(r"/recheck/(\d+)/download", resp.text).group(1)
    rows = _parse_csv(client.get(f"/recheck/{rid}/download").data)
    t1 = next(r for r in rows if r["prev_tree_id"] == "T1")
    assert t1["tree_height_m_prev"] == "1.5"  # 150 cm → 1.5 m
    assert t1["tree_height_m_curr"] == ""      # 本期未配置树高
    # T2 仅前期；T3 仅本期，两期树高都缺项时均为空
    t3 = next(r for r in rows if r["curr_tree_id"] == "T3")
    assert t3["tree_height_m_prev"] == "" and t3["tree_height_m_curr"] == ""


def test_empty_swap_side_row_rejected(client, two_periods):
    resp = _post_swap(client, 1, 2, [(None, None), ("001", "001")])
    assert "至少填一侧" in resp.text
    assert "换牌表" in resp.text
    assert "<td>1</td>" in resp.text
    # 整份不发出：没有下载入口
    import re
    assert not re.search(r"/recheck/\d+/download", resp.text)


def test_swap_id_not_in_version_rejected(client, two_periods):
    resp = _post_swap(client, 1, 2, [("999", "001")])
    assert "前期选定版本中不存在" in resp.text and "999" in resp.text
    assert "<td>1</td>" in resp.text

    resp = _post_swap(client, 1, 2, [("001", "888")])
    assert "本期选定版本中不存在" in resp.text and "888" in resp.text


def test_swap_column_duplicate_rejected(client, two_periods):
    resp = _post_swap(client, 1, 2, [("001", "001"), ("001", "002")])
    assert "前期列重复" in resp.text
    assert "<td>1</td>" in resp.text

    resp = _post_swap(client, 1, 2, [("001", "002"), ("002", "002")])
    assert "本期列重复" in resp.text


def test_census_empty_id_rejected_with_source_and_seq(client, parquet_factory, two_periods):
    _publish(
        client, parquet_factory, "empty.parquet",
        ["A1", None, "A3"], [1.0, 2.0, 3.0],
        batch_no="E", time_point="2024-05",
    )
    resp = _post_swap(client, 1, 3, [("001", "A1")])
    assert "空号" in resp.text
    assert "本期普查" in resp.text
    assert "<td>2</td>" in resp.text
    import re
    assert not re.search(r"/recheck/\d+/download", resp.text)


def test_census_duplicate_id_reported_with_first_seq(client, parquet_factory, two_periods):
    _publish(
        client, parquet_factory, "dup.parquet",
        ["D1", "D1", "D2"], [1.0, 2.0, 3.0],
        batch_no="D", time_point="2024-05",
    )
    resp = _post_swap(client, 1, 3, [("001", "D2")])
    assert "重复登记" in resp.text and "D1" in resp.text
    assert "本期普查" in resp.text
    assert "<td>1</td>" in resp.text


def test_multiple_issues_all_listed(client, parquet_factory, two_periods):
    # 本期普查自身有重号；换牌表同时有重列与不存在号
    _publish(
        client, parquet_factory, "multi.parquet",
        ["X1", "X1"], [1.0, 2.0],
        batch_no="M", time_point="2025-05",
    )
    resp = _post_swap(
        client, 1, 3,
        [("001", "X1"), ("002", "X1"), ("999", None)],
    )
    assert "本期普查" in resp.text       # 重号
    assert "换牌表" in resp.text         # 本期列重复
    assert "前期选定版本中不存在" in resp.text  # 999 不在前期
    import re
    assert not re.search(r"/recheck/\d+/download", resp.text)


def test_bom_and_blank_lines_in_swap_csv_accepted(client, two_periods):
    # UTF-8 BOM + 真正的空行（零字段）跳过，不占换牌记录序号
    text = "﻿prev_tree_id,curr_tree_id\n\n001,001\n\n"
    resp = client.post(
        "/recheck",
        data={
            "prev_version_id": "1",
            "curr_version_id": "2",
            "swap_file": (io.BytesIO(text.encode("utf-8")), "swap.csv"),
        },
        content_type="multipart/form-data",
        follow_redirects=True,
    )
    assert "核对结果（共" in resp.text


def test_two_empty_cells_row_is_not_blank_and_rejected(client, two_periods):
    # 只有逗号的行是两个空字段（不是空行），必须按“至少填一侧”拦截
    text = "prev_tree_id,curr_tree_id\n,\n001,001\n"
    resp = client.post(
        "/recheck",
        data={
            "prev_version_id": "1",
            "curr_version_id": "2",
            "swap_file": (io.BytesIO(text.encode("utf-8")), "swap.csv"),
        },
        content_type="multipart/form-data",
        follow_redirects=True,
    )
    assert "至少填一侧" in resp.text
    assert "<td>1</td>" in resp.text


def test_wrong_column_count_row_rejected(client, two_periods):
    text = "prev_tree_id,curr_tree_id\n001,001,extra\n"
    resp = client.post(
        "/recheck",
        data={
            "prev_version_id": "1",
            "curr_version_id": "2",
            "swap_file": (io.BytesIO(text.encode()), "swap.csv"),
        },
        content_type="multipart/form-data",
        follow_redirects=True,
    )
    assert "不是两列" in resp.text and "第 2 行" in resp.text


# ---------- 版本固定：期间批次换交不影响已发核对表 ----------

def test_download_uses_selected_versions_after_replace(client, parquet_factory, two_periods):
    resp = _post_swap(client, 1, 2, [("001", "001")])
    import re
    rid = re.search(r"/recheck/(\d+)/download", resp.text).group(1)
    before = _parse_csv(client.get(f"/recheck/{rid}/download").data)
    assert len(before) == 4

    # 批次 2（本期）换交：新正式版编号完全不同
    new_path = parquet_factory(
        "new.parquet", {"tid": ["N1", "N2"], "h": [9.0, 8.0]}
    )
    token = stage_token_from_redirect(upload(client, new_path, target="replace:2"))
    client.post(
        "/batches/2/replace",
        data=publish_form(
            token, tree_id_column="tid",
            metrics={"tree_height": ("h", "m")},
        ),
        follow_redirects=True,
    )

    # 核对下载仍按原选定的 v1（版本 id=2）重算，结果不变
    after = _parse_csv(client.get(f"/recheck/{rid}/download").data)
    assert after == before

    # 而核对页下拉里批次 PLOT-1 的本期正式版已换成 v2
    page = client.get("/recheck").text
    assert "v2（正式版）" in page


def test_historical_version_not_selectable(client, parquet_factory, two_periods):
    # 让版本 2 变成历史版
    new_path = parquet_factory(
        "new2.parquet", {"tid": ["N1"], "h": [9.0]}
    )
    token = stage_token_from_redirect(upload(client, new_path, target="replace:2"))
    client.post(
        "/batches/2/replace",
        data=publish_form(
            token, tree_id_column="tid",
            metrics={"tree_height": ("h", "m")},
        ),
        follow_redirects=True,
    )
    resp = _post_swap(client, 1, 2, [("001", "001")])
    assert "不是正式版" in resp.text


def test_download_unknown_id_404(client, two_periods):
    assert client.get("/recheck/99999/download").status_code == 404


# ---------- 流式 / 内存有界 ----------

def test_large_recheck_spans_many_batches_and_stays_correct(client, parquet_factory, tmp_path):
    # 1.2 万行 + 换牌 1.2 万行，超过 1 万行的装载批大小，验证分块落盘正确、
    # 且临时索引让存在性校验/配对保持近线性
    n = 12_000
    old_ids = [f"{i:07d}" for i in range(n)]          # 前导零文本
    new_ids = [f"{i + 500_000:07d}" for i in range(n)]
    _publish(
        client, parquet_factory, "big_old.parquet",
        old_ids, [float(i) for i in range(n)],
        batch_no="BIG-OLD", time_point="2024-01",
    )
    _publish(
        client, parquet_factory, "big_new.parquet",
        new_ids, [float(i) + 0.5 for i in range(n)],
        batch_no="BIG-NEW", time_point="2025-01",
    )

    # 换牌表用流式生成的临时文件，行数本身也跨多个批
    import csv as _csv
    swap_path = str(tmp_path / "big_swap.csv")
    with open(swap_path, "w", encoding="utf-8", newline="") as fh:
        w = _csv.writer(fh)
        w.writerow(["prev_tree_id", "curr_tree_id"])
        for i in range(n):
            w.writerow([old_ids[i], new_ids[i]])
    with open(swap_path, "rb") as fh:
        resp = client.post(
            "/recheck",
            data={
                "prev_version_id": "1",
                "curr_version_id": "2",
                "swap_file": (fh, "big_swap.csv"),
            },
            content_type="multipart/form-data",
            follow_redirects=True,
        )
    assert "核对结果（共" in resp.text
    import re
    rid = re.search(r"/recheck/(\d+)/download", resp.text).group(1)

    # 流式下载完整行数，且前导零保留、树高正确
    data = client.get(f"/recheck/{rid}/download").data
    rows = _parse_csv(data)
    assert len(rows) == n
    first, last = rows[0], rows[-1]
    assert first["prev_tree_id"] == "0000000"
    assert first["curr_tree_id"] == "0500000"
    assert first["tree_height_m_prev"] == "0"
    assert first["tree_height_m_curr"] == "0.5"
    assert last["prev_tree_id"] == f"{n - 1:07d}"
    assert {r["presence"] for r in rows} == {"两期均登记"}


def test_load_swap_file_is_bounded_and_indexes_from_one(app, tmp_path):
    # 纯函数级：换牌 CSV 逐行落临时表，记录序号从 1 开始
    from app.db import recheck_connection
    from app.recheck import load_swap_file, RecheckFormError

    swap = tmp_path / "swap.csv"
    swap.write_text(
        "prev_tree_id,curr_tree_id\n"
        "A1,B1\n"
        "\n"            # 真空行跳过
        ",\n"           # 两空字段保留
        "A2,\n",
        encoding="utf-8",
    )
    conn = recheck_connection(app.config["DATABASE_PATH"])
    try:
        # 核对连接的临时表/临时索引必须由磁盘支撑，内存不随总行数增长
        assert conn.execute("PRAGMA temp_store").fetchone()[0] == 1  # 1 = FILE
        conn.executescript(
            "CREATE TEMP TABLE temp_swap ("
            "seq INTEGER PRIMARY KEY AUTOINCREMENT, prev_id TEXT, curr_id TEXT);"
        )
        count = load_swap_file(conn, str(swap))
        assert count == 3
        got = [tuple(r) for r in conn.execute(
            "SELECT seq, prev_id, curr_id FROM temp_swap ORDER BY seq"
        )]
        assert got == [(1, "A1", "B1"), (2, None, None), (3, "A2", None)]
    finally:
        conn.close()

    # 只有表头 → 结构性报错
    empty = tmp_path / "empty.csv"
    empty.write_text("prev_tree_id,curr_tree_id\n", encoding="utf-8")
    conn = recheck_connection(app.config["DATABASE_PATH"])
    try:
        conn.executescript(
            "CREATE TEMP TABLE temp_swap ("
            "seq INTEGER PRIMARY KEY AUTOINCREMENT, prev_id TEXT, curr_id TEXT);"
        )
        with pytest.raises(RecheckFormError):
            load_swap_file(conn, str(empty))
    finally:
        conn.close()


