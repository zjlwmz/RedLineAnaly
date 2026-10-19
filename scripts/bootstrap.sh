#!/usr/bin/env bash
# 环境初始化：检查依赖 → 生成演示数据 → 确认 RabbitMQ
set -euo pipefail

cd "$(dirname "$0")/.."
PY=${PY:-/usr/bin/python3}

echo "==> 检查 Python 依赖"
$PY - <<'EOF'
import importlib, sys
missing = []
for m in ("osgeo", "shapely", "pyproj", "pika", "fastapi", "uvicorn"):
    try:
        importlib.import_module(m)
    except Exception:
        missing.append(m)
if missing:
    print("缺少依赖: " + ", ".join(missing))
    sys.exit(1)
print("依赖 OK")
EOF

echo "==> 检查 RabbitMQ (${AMQP_HOST:-127.0.0.1}:${AMQP_PORT:-5672})"
$PY - <<'EOF'
import os, sys, time
import pika
for _ in range(15):
    try:
        c = pika.BlockingConnection(pika.ConnectionParameters(
            os.environ.get("AMQP_HOST", "127.0.0.1"), int(os.environ.get("AMQP_PORT", "5672"))))
        c.close(); print("RabbitMQ OK"); sys.exit(0)
    except Exception:
        time.sleep(1)
print("RabbitMQ 不可达（计算将降级为本地串行模式）")
EOF

echo "==> 生成演示数据"
$PY scripts/gen_data.py

echo "==> 完成"
