#!/usr/bin/env bash
# 一键拉起：RabbitMQ 容器 + 主节点 + N 个计算节点
#   WORKERS=3 PORT=8001 bash scripts/run_all.sh
set -euo pipefail

cd "$(dirname "$0")/.."
PY=${PY:-/usr/bin/python3}
PORT=${PORT:-8001}
WORKERS=${WORKERS:-2}
RMQ_NAME=${RMQ_NAME:-redline-rmq}
LOG_DIR=${LOG_DIR:-/tmp/redline-logs}
mkdir -p "$LOG_DIR"

cleanup() {
  echo "==> 停止进程"
  jobs -p | xargs -r kill 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# 1) RabbitMQ
if ! docker ps --format '{{.Names}}' | grep -qx "$RMQ_NAME"; then
  if docker ps -a --format '{{.Names}}' | grep -qx "$RMQ_NAME"; then
    docker start "$RMQ_NAME" >/dev/null
  else
    docker run -d --name "$RMQ_NAME" -p 5672:5672 rabbitmq:3-alpine >/dev/null
  fi
fi
echo "==> 等待 RabbitMQ 就绪"
for _ in $(seq 1 40); do
  if $PY -c "
import pika,sys
try:
    pika.BlockingConnection(pika.ConnectionParameters('127.0.0.1',5672)).close()
except Exception:
    sys.exit(1)" 2>/dev/null; then break; fi
  sleep 1
done

# 2) 主节点
echo "==> 启动主节点 :$PORT"
$PY -m uvicorn app.main:app --host 0.0.0.0 --port "$PORT" --log-level info > "$LOG_DIR/main.log" 2>&1 &
MAIN_PID=$!

# 3) 计算节点
for i in $(seq 1 "$WORKERS"); do
  echo "==> 启动 worker-$i"
  $PY -m app.worker --id "worker-$i" > "$LOG_DIR/worker-$i.log" 2>&1 &
done

# 4) 等主节点就绪
for _ in $(seq 1 60); do
  if curl -sf "http://127.0.0.1:$PORT/healthz" >/dev/null 2>&1; then break; fi
  sleep 1
done
echo "==> 就绪：http://127.0.0.1:$PORT/   （日志：$LOG_DIR）"

wait $MAIN_PID
