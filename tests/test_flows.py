"""端到端流程：上传→列对照→发布→预览→换交→并表下载。"""
from __future__ import annotations

import csv
import io

from conftest import publish_form, stage_token_from_redirect, upload


def _parse_csv(data: bytes) -> list[dict]:
    return list(csv.DictReader(io.StringIO(data.decode("utf-8-sig"))))


def test_full_publish_replace_and_merge(client, parquet_factory):
    # 批次 A：height 列是树高，单位米，25 行
    path_a = parquet_factory(
        "a.parquet",
        {
            "sample_tree_no": [f"A-{i:03d}" for i in range(1, 26)],
            "height": [round(10 + i * 0.1, 2) for i in range(25)],
        },
    )
    # 批次 B：同名列 height 在这里是枝下高，单位厘米；tree_h 是树高（厘米）
    path_b = parquet_factory(
        "b.parquet",
        {
            "sample_tree_no": [f"B-{i:03d}" for i in range(1, 26)],
            "height": [400 + i * 5 for i in range(25)],
            "tree_h": [900 + i * 10 for i in range(25)],
        },
    )

    # ---- 发布批次 A ----
    token_a = stage_token_from_redirect(upload(client, path_a))
    resp = client.post(
        "/batches/new",
        data=publish_form(
            token_a,
            tree_id_column="sample_tree_no",
            metrics={"tree_height": ("height", "m")},
            batch_no="PLOT-A",
            time_point="2024-09",
        ),
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert "已发布为正式版" in resp.text

    # ---- 发布批次 B：同名列映射成枝下高，厘米自动换米 ----
    token_b = stage_token_from_redirect(upload(client, path_b))
    resp = client.post(
        "/batches/new",
        data=publish_form(
            token_b,
            tree_id_column="sample_tree_no",
            metrics={
                "under_branch_height": ("height", "cm"),
                "tree_height": ("tree_h", "cm"),
            },
            batch_no="PLOT-B",
            time_point="2024-09",
        ),
        follow_redirects=True,
    )
    assert resp.status_code == 200

    index = client.get("/").text
    assert "PLOT-A" in index and "PLOT-B" in index
    assert 'value="1"' in index and 'value="2"' in index

    # ---- 预览：每批只取前 20 行，且带批次号/版号 ----
    resp = client.post("/merge/preview", data={"batch_ids": ["1", "2"]})
    assert resp.status_code == 200
    assert resp.text.count("<tr>") >= 2 * (20 + 1)  # 表头 + 每批 20 行
    assert "A-001" in resp.text and "B-020" in resp.text
    assert "A-021" not in resp.text and "B-021" not in resp.text

    # ---- 全量流式下载：所有行保留，单位统一为米 ----
    resp = client.get("/merge/stream?batch_ids=1&batch_ids=2")
    assert resp.status_code == 200
    rows = _parse_csv(resp.data)
    assert len(rows) == 50  # 25 + 25，所有样木行保留
    assert [r for r in rows if r["batch_no"] == "PLOT-A"][0] == {
        "batch_no": "PLOT-A",
        "version_no": "1",
        "tree_id": "A-001",
        "tree_height_m": "10",
        "under_branch_height_m": "",  # 未配置指标留空
    }
    b_first = next(r for r in rows if r["tree_id"] == "B-001")
    assert b_first["tree_height_m"] == "9"          # 900 cm -> 9 m
    assert b_first["under_branch_height_m"] == "4"  # 400 cm -> 4 m
    assert b_first["version_no"] == "1"

    # ---- 换交批次 A：新文件列名/单位全变，含缺项 ----
    path_a2 = parquet_factory(
        "a_v2.parquet",
        {
            "tree_id": [f"A-{i:03d}" for i in range(1, 31)],  # 30 行
            "shu_gao_cm": [None if i % 10 == 0 else 1200 + i for i in range(30)],
            "zhi_xia_m": [300 + i for i in range(30)],  # 故意给厘米值但标米会在数值校验通过
        },
    )
    resp = upload(client, path_a2, target="replace:1")
    token_a2 = stage_token_from_redirect(resp)
    resp = client.post(
        "/batches/1/replace",
        data=publish_form(
            token_a2,
            tree_id_column="tree_id",
            metrics={
                "tree_height": ("shu_gao_cm", "cm"),
                "under_branch_height": ("zhi_xia_m", "cm"),
            },
        ),
        follow_redirects=True,
    )
    assert "换交成功" in resp.text

    # 旧版（v1）与新版（v2）都在；v2 是正式版
    detail = client.get("/batches/1").text
    assert "v1" in detail and "v2" in detail
    assert detail.index("v2") < detail.index("v1")  # 新版在前

    # 再并表：A 用 v2 的 30 行，B 仍 25 行
    resp = client.get("/merge/stream?batch_ids=1&batch_ids=2")
    rows = _parse_csv(resp.data)
    assert len(rows) == 55
    a_rows = [r for r in rows if r["batch_no"] == "PLOT-A"]
    assert {r["version_no"] for r in a_rows} == {"2"}
    missing = next(r for r in a_rows if r["tree_id"] == "A-011")
    assert missing["tree_height_m"] == ""  # 缺项留空
    a1 = next(r for r in a_rows if r["tree_id"] == "A-002")
    assert a1["tree_height_m"] == "12.01"     # 1201 cm
    assert a1["under_branch_height_m"] == "3.01"  # 301 cm

    # ---- 旧预览仍可打开、旧版全量明细仍可下载（换交前的口径） ----
    old_page = client.get("/versions/1")
    assert old_page.status_code == 200
    assert "sample_tree_no" in old_page.text
    old_csv = client.get("/versions/1/download")
    old_rows = _parse_csv(old_csv.data)
    assert len(old_rows) == 25
    assert old_rows[0]["tree_id"] == "A-001"
    assert old_rows[0]["tree_height_m"] == "10"  # 当时按米映射


def test_replace_failure_keeps_original(client, parquet_factory):
    path = parquet_factory(
        "ok.parquet",
        {"tid": [f"T-{i}" for i in range(5)], "h_m": [1.1 * (i + 1) for i in range(5)]},
    )
    token = stage_token_from_redirect(upload(client, path))
    client.post(
        "/batches/new",
        data=publish_form(
            token,
            tree_id_column="tid",
            metrics={"tree_height": ("h_m", "m")},
            batch_no="P1",
            time_point="2024-03",
        ),
    )

    # 换交文件里根本没有选的样木号列 → 校验失败
    bad = parquet_factory(
        "bad.parquet",
        {"other_id": ["x1"], "other_h": [2.0]}
    )
    token_bad = stage_token_from_redirect(upload(client, bad, target="replace:1"))
    resp = client.post(
        "/batches/1/replace",
        data=publish_form(
            token_bad,
            tree_id_column="tid",  # 新文件无此列
            metrics={"tree_height": ("other_h", "m")},
        ),
        follow_redirects=True,
    )
    assert "不在文件列中" in resp.text

    # 正式版仍是 v1，数据原样可用
    detail = client.get("/batches/1").text
    assert "v2" not in detail
    csv_rows = _parse_csv(client.get("/versions/1/download").data)
    assert len(csv_rows) == 5
    assert csv_rows[0]["tree_id"] == "T-0"

    # 重复源列 / 非数值列 / 无指标 也都应被拒绝
    dup = parquet_factory("dup.parquet", {"tid": ["a"], "h": [1.0]})
    token_dup = stage_token_from_redirect(upload(client, dup, target="replace:1"))
    resp = client.post(
        "/batches/1/replace",
        data=publish_form(
            token_dup,
            tree_id_column="tid",
            metrics={
                "tree_height": ("h", "m"),
                "under_branch_height": ("h", "m"),
            },
        ),
        follow_redirects=True,
    )
    assert "重复选用" in resp.text

    text_col = parquet_factory("txt.parquet", {"tid": ["a"], "h": ["高"]})
    token_txt = stage_token_from_redirect(upload(client, text_col, target="replace:1"))
    resp = client.post(
        "/batches/1/replace",
        data=publish_form(
            token_txt,
            tree_id_column="tid",
            metrics={"tree_height": ("h", "m")},
        ),
        follow_redirects=True,
    )
    assert "不是数值列" in resp.text


def test_replace_page_upload_form_posts_to_uploads(client, parquet_factory):
    path = parquet_factory(
        "ok.parquet",
        {"tid": ["T-1"], "h_m": [1.2]},
    )
    token = stage_token_from_redirect(upload(client, path))
    client.post(
        "/batches/new",
        data=publish_form(
            token,
            tree_id_column="tid",
            metrics={"tree_height": ("h_m", "m")},
            batch_no="P1",
            time_point="2024-03",
        ),
    )
    page = client.get("/batches/1/replace").text
    # 换交页的第一步上传必须打到通用上传路由，而不是换交提交路由
    assert 'action="/uploads"' in page
    assert 'name="target" value="replace:1"' in page


def test_time_point_mismatch_rejected(client, parquet_factory):
    p1 = parquet_factory("p1.parquet", {"tid": ["a"], "h": [1.0]})
    t1 = stage_token_from_redirect(upload(client, p1))
    client.post(
        "/batches/new",
        data=publish_form(
            t1, tree_id_column="tid",
            metrics={"tree_height": ("h", "m")},
            batch_no="P1", time_point="2024-09",
        ),
    )
    p2 = parquet_factory("p2.parquet", {"tid": ["b"], "h": [2.0]})
    t2 = stage_token_from_redirect(upload(client, p2))
    client.post(
        "/batches/new",
        data=publish_form(
            t2, tree_id_column="tid",
            metrics={"tree_height": ("h", "m")},
            batch_no="P2", time_point="2025-03",
        ),
    )

    resp = client.post("/merge/preview", data={"batch_ids": ["1", "2"]},
                       follow_redirects=True)
    assert "时点不一致" in resp.text

    resp = client.get("/merge/stream?batch_ids=1&batch_ids=2")
    assert resp.status_code == 400

    # 只选一个批次仍正常
    resp = client.get("/merge/stream?batch_ids=1")
    assert resp.status_code == 200
    assert _parse_csv(resp.data)[0]["tree_id"] == "a"


def test_invalid_uploads_rejected(client, parquet_factory):
    # 非 parquet 内容
    import io as _io

    resp = client.post(
        "/uploads",
        data={"target": "new", "file": (_io.BytesIO(b"not parquet"), "x.parquet")},
        content_type="multipart/form-data",
        follow_redirects=True,
    )
    assert "无法按 Parquet 解析" in resp.text

    # 重复批次号
    path = parquet_factory("q.parquet", {"tid": ["a"], "h": [1.0]})
    token = stage_token_from_redirect(upload(client, path))
    client.post(
        "/batches/new",
        data=publish_form(
            token, tree_id_column="tid",
            metrics={"tree_height": ("h", "m")},
            batch_no="DUP", time_point="2024-01",
        ),
    )
    token2 = stage_token_from_redirect(upload(client, path))
    resp = client.post(
        "/batches/new",
        data=publish_form(
            token2, tree_id_column="tid",
            metrics={"tree_height": ("h", "m")},
            batch_no="DUP", time_point="2024-01",
        ),
        follow_redirects=True,
    )
    assert "已存在" in resp.text
