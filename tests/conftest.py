import io
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app  # noqa: E402


@pytest.fixture()
def app(tmp_path):
    data_dir = tmp_path / "data"
    application = create_app(
        {
            "TESTING": True,
            "DATABASE_PATH": str(data_dir / "test.db"),
            "STAGING_DIR": str(data_dir / "staging"),
            "PUBLISHED_DIR": str(data_dir / "published"),
        }
    )
    yield application


@pytest.fixture()
def client(app):
    return app.test_client()


def write_parquet(path, data: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pq.write_table(pa.table(data), path)


def upload(client, path, target="new"):
    with open(path, "rb") as fh:
        return client.post(
            "/uploads",
            data={"target": target, "file": (io.BytesIO(fh.read()), os.path.basename(path))},
            content_type="multipart/form-data",
            follow_redirects=False,
        )


def stage_token_from_redirect(resp):
    return resp.headers["Location"].split("token=")[-1]


def publish_form(token, *, tree_id_column, metrics, batch_no="", time_point="", note=""):
    """metrics: {metric_key: (column, unit)}"""
    form = {
        "token": token,
        "tree_id_column": tree_id_column,
        "batch_no": batch_no,
        "time_point": time_point,
        "note": note,
    }
    for key in ("tree_height", "under_branch_height"):
        if key in metrics:
            col, unit = metrics[key]
            form[f"metric_{key}_col"] = col
            form[f"metric_{key}_unit"] = unit
        else:
            form[f"metric_{key}_col"] = ""
            form[f"metric_{key}_unit"] = "m"
    return form


@pytest.fixture()
def parquet_factory(tmp_path):
    def _factory(name, data):
        path = tmp_path / name
        write_parquet(str(path), data)
        return str(path)

    return _factory
