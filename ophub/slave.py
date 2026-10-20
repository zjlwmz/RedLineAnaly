"""Slave 节点：RabbitMQ 消费者，消费共享任务队列，执行单省红线压盖计算并回传结果。

任务消息体：
  {
    "task_id": "<batch_id>:<province_code>",
    "batch_id": "...",
    "feature_id": "...",
    "province_code": "...",
    "province_name": "...",
    "input_wkt": "POLYGON((...))",       // CGCS2000 经纬度
    "input_srid": 4490,
    "input_area_m2": 1234567.89,
    "buffer_m": 50.0,
    "severe_ratio": 0.5
  }

结果消息体（success / no_data）：
  {
    "task_id": "...", "batch_id": "...",
    "province_code": "...", "province_name": "...",
    "status": "success|no_data",
    "patch_count": 3, "patches_scanned": 1200,
    "province_overlap_area": 8000.0, "province_union_area": 7500.0,
    "by_type": [...], "matches": [...], "warnings": [...],
    "worker_id": "slave-0", "elapsed_ms": 123.4
  }

可靠性策略：
- 手动 ACK：任务（含 GPKG 空间筛选、OGR 相交）完成后才确认
- prefetch_count=1：未 ACK 前不预取新任务，多个 Slave 之间公平分发
- 有限重试：失败消息带 x-retries 计数重新投递，超过上限进入死信队列
- 结果回传：成功/最终失败均发送结果消息，Master 可查询任务状态
- 断线重连：连接异常退出后等待固定间隔重新连接
"""
import json
import logging
import os
import socket
import time

import pika

from ophub.config import settings
from ophub.calculator import run_task
from ophub.mq import connect, declare_topology

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("slave")

_WORKER_ID = f"slave-{socket.gethostname()}-{os.getpid()}"


def _publish_result(channel: pika.channel.Channel, result: dict) -> None:
    channel.basic_publish(
        exchange=settings.result_exchange,
        routing_key=settings.result_routing_key,
        body=json.dumps(result, ensure_ascii=False).encode("utf-8"),
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2,
            message_id=result.get("task_id") or result["batch_id"],
        ),
    )


def _make_callback(channel: pika.channel.Channel):
    def on_task(ch: pika.channel.Channel, method, properties, body: bytes) -> None:
        try:
            task = json.loads(body)
        except json.JSONDecodeError:
            logger.error("Invalid message body, reject to DLQ: %r", body[:200])
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            return

        batch_id = task.get("batch_id", "unknown")
        task_id = task.get("task_id", batch_id)
        province_code = task.get("province_code")
        province_name = task.get("province_name", province_code)
        headers = dict(properties.headers or {})
        retries = int(headers.get("x-retries", 0))
        logger.info(
            "Received task %s (attempt %s): batch=%s province=%s(%s)",
            task_id, retries + 1, batch_id, province_name, province_code,
        )

        if not province_code or not task.get("input_wkt"):
            logger.error("Task %s missing province_code or input_wkt, reject to DLQ", task_id)
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            return

        try:
            result = run_task(task)
            # run_task 返回的是 overlay_province 的结果（缺 batch_id/task_id/worker_id），补上
            result.update({
                "batch_id": batch_id,
                "task_id": task_id,
                "worker_id": _WORKER_ID,
                "status": result.get("status", "success"),
            })
            _publish_result(ch, result)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info(
                "Task %s done: %s(%s) status=%s patches=%s elapsed=%sms",
                task_id, province_name, province_code,
                result["status"], result.get("patch_count", 0), result.get("elapsed_ms", 0),
            )
            return
        except Exception as exc:
            logger.exception("Task %s failed on attempt %s", task_id, retries + 1)
            error = f"{type(exc).__name__}: {exc}"

        # ---- 失败处理：未超上限则带计数重新投递，超过上限进死信队列 ----
        try:
            if retries < settings.task_max_retries:
                headers["x-retries"] = retries + 1
                ch.basic_publish(
                    exchange=settings.task_exchange,
                    routing_key=settings.task_routing_key,
                    body=body,
                    properties=pika.BasicProperties(
                        content_type="application/json",
                        delivery_mode=2,
                        message_id=task_id,
                        headers=headers,
                    ),
                )
                logger.info("Task %s re-queued (retry %s/%s)", task_id, retries + 1, settings.task_max_retries)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                logger.error("Task %s exceeded max retries, send to DLQ: %s(%s)",
                             task_id, province_name, province_code)
                _publish_result(ch, {
                    "batch_id": batch_id,
                    "task_id": task_id,
                    "province_code": province_code,
                    "province_name": province_name,
                    "status": "failed",
                    "worker_id": _WORKER_ID,
                    "error": error,
                    "patch_count": 0,
                    "patches_scanned": 0,
                    "overlap_area": 0.0,
                    "warnings": [],
                })
                ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
        except (pika.exceptions.AMQPError, OSError):
            logger.exception("Failed to publish retry/result for task %s, nack requeue", task_id)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    return on_task


def run_slave() -> None:
    logger.info("Slave worker_id=%s starting ...", _WORKER_ID)
    while True:
        conn = None
        try:
            logger.info("Slave connecting to RabbitMQ %s:%s ...", settings.mq_host, settings.mq_port)
            conn = connect()
            channel = conn.channel()
            declare_topology(channel)
            channel.basic_qos(prefetch_count=settings.prefetch_count)
            channel.basic_consume(settings.task_queue, _make_callback(channel))
            logger.info(
                "Slave consuming queue %r via %r (prefetch=%s), waiting for tasks ...",
                settings.task_queue, settings.task_binding_key, settings.prefetch_count,
            )
            channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Slave stopped by user")
            break
        except Exception:
            logger.exception("Slave connection error, reconnect in %ss", settings.reconnect_interval)
            time.sleep(settings.reconnect_interval)
        finally:
            try:
                if conn is not None and conn.is_open:
                    conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    run_slave()
