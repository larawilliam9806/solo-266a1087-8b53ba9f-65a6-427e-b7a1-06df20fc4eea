"""Flask 应用工厂与全部 HTTP 路由。"""
from __future__ import annotations

import os
from datetime import datetime
from urllib.parse import quote

from flask import (
    Flask,
    abort,
    flash,
    redirect,
    render_template,
    request,
    Response,
    url_for,
)

from .constants import HEIGHT_METRICS, OUTPUT_COLUMNS, PREVIEW_ROWS
from .db import close_db, init_db, standalone_connection
from .parquet_tools import csv_chunks, iter_output_rows, preview_rows
from .services import (
    ValidationError,
    get_batch,
    get_stage,
    get_version,
    list_batches,
    list_versions,
    metric_map,
    official_versions_for,
    parse_mappings,
    publish_version,
    stage_columns,
    stage_upload,
    validate_mappings,
)


def create_app(config: dict | None = None) -> Flask:
    app = Flask(__name__)
    app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-secret-change-me")
    data_dir = os.environ.get("DATA_DIR", os.path.join(app.instance_path, "data"))
    staging_dir = os.path.join(data_dir, "staging")
    published_dir = os.path.join(data_dir, "published")
    app.config.update(
        DATABASE_PATH=os.path.join(data_dir, "forestry.db"),
        STAGING_DIR=staging_dir,
        PUBLISHED_DIR=published_dir,
    )
    if config:
        app.config.update(config)

    # 测试可能覆盖目录；按最终配置统一确保目录与库父目录存在
    os.makedirs(app.config["STAGING_DIR"], exist_ok=True)
    os.makedirs(app.config["PUBLISHED_DIR"], exist_ok=True)
    os.makedirs(os.path.dirname(app.config["DATABASE_PATH"]), exist_ok=True)

    init_db(app)
    app.teardown_appcontext(close_db)

    # ---------- 首页：批次目录 + 并表勾选 ----------
    @app.route("/")
    def index():
        return render_template("index.html", batches=list_batches())

    # ---------- 新批次：第一步上传，第二步选列 ----------
    @app.route("/batches/new", methods=["GET"])
    def new_batch_form():
        stage = get_stage(request.args.get("token", ""))
        return render_template(
            "batch_form.html",
            mode="new",
            stage=stage,
            columns=stage_columns(stage) if stage else [],
            metrics=HEIGHT_METRICS,
        )

    @app.route("/uploads", methods=["POST"])
    def upload():
        target = request.form.get("target", "new")
        if target.startswith("replace:"):
            try:
                target_batch_id = int(target.split(":", 1)[1])
            except ValueError:
                abort(404)
            if get_batch(target_batch_id) is None:
                abort(404)
        try:
            token = stage_upload(
                request.files.get("file"), app.config["STAGING_DIR"]
            )
        except ValidationError as exc:
            flash(str(exc), "error")
            if target.startswith("replace:"):
                return redirect(url_for("replace_form", batch_id=target_batch_id))
            return redirect(url_for("new_batch_form"))

        if target.startswith("replace:"):
            return redirect(
                url_for(
                    "replace_form",
                    batch_id=target_batch_id,
                    token=token,
                )
            )
        return redirect(url_for("new_batch_form", token=token))

    @app.route("/batches/new", methods=["POST"])
    def create_batch():
        stage = get_stage(request.form.get("token", ""))
        if stage is None:
            flash("暂存文件已失效，请重新上传 Parquet。", "error")
            return redirect(url_for("new_batch_form"))
        try:
            columns = stage_columns(stage)
            tree_id_column = request.form.get("tree_id_column", "").strip()
            raw_mappings = parse_mappings(request.form)
            mappings = validate_mappings(
                stage["parquet_path"], columns, tree_id_column, raw_mappings
            )
            result = publish_version(
                stage,
                tree_id_column,
                mappings,
                batch_no=request.form.get("batch_no", ""),
                time_point=request.form.get("time_point", ""),
                note=request.form.get("note", ""),
                published_dir=app.config["PUBLISHED_DIR"],
            )
        except ValidationError as exc:
            flash(str(exc), "error")
            return redirect(url_for("new_batch_form", token=stage["token"]))
        flash(f"批次「{result.batch_no}」v{result.version_no} 已发布为正式版。")
        return redirect(url_for("batch_detail", batch_id=result.batch_id))

    # ---------- 换交：新文件对照新列，成功才替代正式版 ----------
    @app.route("/batches/<int:batch_id>/replace", methods=["GET"])
    def replace_form(batch_id: int):
        batch = get_batch(batch_id)
        if batch is None:
            abort(404)
        stage = get_stage(request.args.get("token", ""))
        return render_template(
            "batch_form.html",
            mode="replace",
            batch=batch,
            stage=stage,
            columns=stage_columns(stage) if stage else [],
            metrics=HEIGHT_METRICS,
        )

    @app.route("/batches/<int:batch_id>/replace", methods=["POST"])
    def replace_batch(batch_id: int):
        batch = get_batch(batch_id)
        if batch is None:
            abort(404)
        stage = get_stage(request.form.get("token", ""))
        if stage is None:
            flash("暂存文件已失效，请重新上传 Parquet。", "error")
            return redirect(url_for("replace_form", batch_id=batch_id))
        try:
            columns = stage_columns(stage)
            tree_id_column = request.form.get("tree_id_column", "").strip()
            raw_mappings = parse_mappings(request.form)
            mappings = validate_mappings(
                stage["parquet_path"], columns, tree_id_column, raw_mappings
            )
            # 时点沿用批次既有记录，保证同一批次时点不变
            result = publish_version(
                stage,
                tree_id_column,
                mappings,
                batch_no=batch["batch_no"],
                time_point=batch["time_point"],
                note=batch["note"],
                published_dir=app.config["PUBLISHED_DIR"],
                batch_id=batch_id,
            )
        except ValidationError as exc:
            flash(str(exc), "error")
            return redirect(
                url_for("replace_form", batch_id=batch_id, token=stage["token"])
            )
        flash(f"换交成功：v{result.version_no} 已成为正式版，旧版保留可下载。")
        return redirect(url_for("batch_detail", batch_id=batch_id))

    # ---------- 批次详情 / 版号详情 ----------
    @app.route("/batches/<int:batch_id>")
    def batch_detail(batch_id: int):
        batch = get_batch(batch_id)
        if batch is None:
            abort(404)
        return render_template(
            "batch_detail.html",
            batch=batch,
            versions=list_versions(batch_id),
        )

    @app.route("/versions/<int:version_id>")
    def version_detail(version_id: int):
        version = get_version(version_id)
        if version is None:
            abort(404)
        mappings = metric_map(version_id)
        rows = preview_rows(
            version["parquet_path"],
            version["tree_id_column"],
            mappings,
            version["batch_no"],
            version["version_no"],
        )
        return render_template(
            "version_detail.html",
            version=version,
            mappings=mappings,
            metrics=HEIGHT_METRICS,
            rows=rows,
            headers=OUTPUT_COLUMNS,
            preview_n=PREVIEW_ROWS,
        )

    @app.route("/versions/<int:version_id>/download")
    def version_download(version_id: int):
        conn = standalone_connection()
        try:
            version = conn.execute(
                """
                SELECT v.*, b.batch_no FROM versions v
                  JOIN batches b ON b.id = v.batch_id
                 WHERE v.id = ?
                """,
                (version_id,),
            ).fetchone()
            if version is None:
                conn.close()
                abort(404)
            metric_rows = conn.execute(
                "SELECT metric_key, source_column, unit FROM version_metrics WHERE version_id = ?",
                (version_id,),
            ).fetchall()
            mappings = {r["metric_key"]: (r["source_column"], r["unit"]) for r in metric_rows}
        except Exception:
            conn.close()
            raise

        def generate():
            try:
                yield from csv_chunks(
                    iter_output_rows(
                        version["parquet_path"],
                        version["tree_id_column"],
                        mappings,
                        version["batch_no"],
                        version["version_no"],
                    )
                )
            finally:
                conn.close()

        filename = f"{version['batch_no']}_v{version['version_no']}_full.csv"
        return Response(
            generate(),
            mimetype="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": (
                    f"attachment; filename=\"{filename}\"; "
                    f"filename*=UTF-8''{quote(filename)}"
                ),
                "X-Content-Type-Options": "nosniff",
            },
        )

    # ---------- 勾选批次：同时点预览 + 全量流式下载 ----------
    def _selected_batch_ids() -> list[int]:
        raw = request.values.getlist("batch_ids")
        try:
            return [int(x) for x in raw]
        except ValueError:
            raise ValidationError("批次参数不合法。")

    @app.route("/merge/preview", methods=["POST"])
    def merge_preview():
        try:
            batch_ids = _selected_batch_ids()
            versions = official_versions_for(batch_ids)
        except ValidationError as exc:
            flash(str(exc), "error")
            return redirect(url_for("index"))

        previews = []
        for version in versions:
            mappings = metric_map(version["id"])
            previews.append(
                {
                    "version": version,
                    "rows": preview_rows(
                        version["parquet_path"],
                        version["tree_id_column"],
                        mappings,
                        version["batch_no"],
                        version["version_no"],
                    ),
                }
            )
        return render_template(
            "merge_preview.html",
            previews=previews,
            headers=OUTPUT_COLUMNS,
            preview_n=PREVIEW_ROWS,
            batch_ids=batch_ids,
            time_point=versions[0]["time_point"],
        )

    @app.route("/merge/stream")
    def merge_stream():
        try:
            batch_ids = _selected_batch_ids()
        except ValidationError as exc:
            abort(400, description=str(exc))

        conn = standalone_connection()
        try:
            placeholders = ",".join("?" for _ in batch_ids)
            versions = conn.execute(
                f"""
                SELECT v.*, b.batch_no, b.time_point, b.id AS batch_id
                  FROM versions v JOIN batches b ON b.id = v.batch_id
                 WHERE v.is_official = 1 AND b.id IN ({placeholders})
                 ORDER BY b.batch_no
                """,
                batch_ids,
            ).fetchall()
        except Exception:
            conn.close()
            raise
        if len(versions) != len(set(batch_ids)):
            conn.close()
            abort(400, description="所选批次中存在没有正式版的批次。")
        time_points = {r["time_point"] for r in versions}
        if len(time_points) > 1:
            conn.close()
            abort(400, description="勾选批次的时点不一致，并表仅限同一时点。")

        def merged_rows():
            try:
                for version in versions:
                    metric_rows = conn.execute(
                        "SELECT metric_key, source_column, unit FROM version_metrics WHERE version_id = ?",
                        (version["id"],),
                    ).fetchall()
                    mappings = {
                        r["metric_key"]: (r["source_column"], r["unit"])
                        for r in metric_rows
                    }
                    # 保留该批次全部样木行，逐批流式产出，内存不随总行数增长
                    yield from iter_output_rows(
                        version["parquet_path"],
                        version["tree_id_column"],
                        mappings,
                        version["batch_no"],
                        version["version_no"],
                    )
            finally:
                conn.close()

        stamp = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = f"merge_{versions[0]['time_point']}_{stamp}.csv"
        return Response(
            csv_chunks(merged_rows()),
            mimetype="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": (
                    f"attachment; filename=\"{filename}\"; "
                    f"filename*=UTF-8''{quote(filename)}"
                ),
                "X-Content-Type-Options": "nosniff",
            },
        )

    return app
