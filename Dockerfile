FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DATA_DIR=/data

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /data && \
    adduser --disabled-password --gecos "" appuser && \
    chown -R appuser:appuser /app /data
USER appuser

EXPOSE 5000

# gunicorn 多 worker 下通过 SQLite WAL + 文件系统协调；线程模式足够本场景
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "2", "--threads", "4", \
     "--timeout", "120", "wsgi:app"]
