"""应用包：create_app 工厂与 gunicorn 入口。"""
from .routes import create_app

__all__ = ["create_app"]
