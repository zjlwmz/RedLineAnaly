"""计算节点。

启动：
    python -m app.worker --id worker-1
    python -m app.worker --id worker-2

每个节点绑定自己的独占队列 redline.task.<id>，prefetch=1，处理完 ack，
失败 nack 并转发到死信队列（不 requeue，避免毒消息死循环）。
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import sys
import time
import traceback

import pika
from osgeo import ogr

from . import geo, topology
from .config import settings
from .overlay import overlay_province
from .redline_index import RedlineStore

ogr.UseExceptions()

_stop = False


def _handle_sigterm(signum, frame):  # noqa: ARG001
    global _stop
    _stop = True


def run(worker_id: str) -> None:
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    # 等主节点把拓扑建好（首次启动时两者可能同时起来）
    conn = None
    for _ in range(60):
        try:
            conn = topology.connect()
            break
        except Exception as exc:  # pragma: no cover
            print(f"[{worker_id}] 连接 RabbitMQ 失败，重试: {exc}", flush=True)
            time.sleep(1)
    if conn is None:
        raise SystemExit("RabbitMQ 不可达")

    ch = conn.channel()
    topology.declare_topology(ch)
    queue = topology.register_worker(ch, worker_id)
    ch.basic_qos(prefetch_count=1)
    store = RedlineStore()

    print(f"[{worker_id}] 就绪，监听 {queue}  (host={socket.gethostname()} pid={os.getpid()})", flush=True)

    def on_message(chan, method, properties, body):  # noqa: ARG001
        t0 = time.perf_counter()
        try:
            task = json.loads(body)
        except Exception:
            chan.basic_nack(method.delivery_tag, requeue=False)
            print(f"[{worker_id}] 消息体无法解析，已丢弃", flush=True)
            return

        province_code = task.get("province_code")
        print(f"[{worker_id}] 收到任务 {task.get('task_id')} 省={province_code}", flush=True)
        try:
            input_geom = geo.parse_geojson_geometry(task["input_wkt"], int(task.get("input_srid", 4490)))
            input_area = float(task.get("input_area_m2") or geo.area_m2(input_geom))
            result = overlay_province(
                input_geom=input_geom,
                province_code=province_code,
                province_name=task.get("province_name", province_code),
                buffer_m=float(task.get("buffer_m", settings.buffer_m)),
                input_area_m2=input_area,
                store=store,
            )
            result.update({
                "batch_id": task["batch_id"],
                "task_id": task["task_id"],
                "worker_id": worker_id,
                "status": result.get("status", "success"),
                "total_elapsed_ms": round((time.perf_counter() - t0) * 1000, 2),
            })
            chan.basic_publish(
                exchange=settings.exchange,
                routing_key="result",
                body=json.dumps(result, ensure_ascii=False).encode(),
                properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"),
            )
            chan.basic_ack(method.delivery_tag)
            print(
                f"[{worker_id}] 完成 {task.get('task_id')} 命中 {result.get('patch_count', 0)} 图斑 "
                f"{result.get('total_elapsed_ms')}ms",
                flush=True,
            )
        except Exception as exc:
            tb = traceback.format_exc(limit=3)
            print(f"[{worker_id}] 失败 {task.get('task_id')}: {exc}\n{tb}", flush=True)
            err = {
                "batch_id": task.get("batch_id"),
                "task_id": task.get("task_id"),
                "province_code": province_code,
                "province_name": task.get("province_name"),
                "worker_id": worker_id,
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
                "matches": [],
            }
            chan.basic_publish(
                exchange=settings.exchange,
                routing_key="result",
                body=json.dumps(err, ensure_ascii=False).encode(),
                properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"),
            )
            chan.basic_ack(method.delivery_tag)  # 已把错误回传，消息不再重试

    ch.basic_consume(queue=queue, on_message_callback=on_message)
    try:
        while not _stop:
            conn.process_data_events(time_limit=1)
    finally:
        try:
            ch.basic_cancel("")
            conn.close()
        except Exception:
            pass
        print(f"[{worker_id}] 已停止", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", default=f"worker-{socket.gethostname()}")
    args = parser.parse_args()
    run(args.id)


if __name__ == "__main__":
    sys.exit(main())
