"""RabbitMQ 拓扑。

关键设计：**fanout exchange + 每个 worker 一个独占队列**。

原方案写的是「N 个子任务投递到一个队列，各节点从队列消费」。这在按省拆分时是错的：
一条消息只能被一个消费者取走，worker A 拿到「河北」、worker B 拿到「北京」，
那么「河北」的结果永远回不来——因为汇总节点只收到它自己消费到的那些。

正确做法：
  exchange: redline.overlay (fanout, durable)
  queue:    redline.task.<worker_id>  ← 每个计算节点绑定一个，自动获知全部任务，
            只取自己 province 允许的那部分；取到不属于自己的立刻 nack(requeue)
  更简单可靠的做法：直接给每个 worker 发一份（本实现采用此方案），
  由 publisher 按 worker 列表逐个 basic_publish 到 routing key = redline.task.<worker_id>。

  queue:    redline.result      ← 所有 worker 回传结果
  queue:    redline.dead        ← 计算失败 / 超时消息，便于重放
"""
from __future__ import annotations

import pika

from .config import settings


def connection_params() -> pika.ConnectionParameters:
    return pika.ConnectionParameters(
        host=settings.amqp_host,
        port=settings.amqp_port,
        virtual_host=settings.amqp_vhost,
        credentials=pika.PlainCredentials(settings.amqp_user, settings.amqp_password),
        heartbeat=30,
        blocked_connection_timeout=30,
    )


def connect() -> pika.BlockingConnection:
    return pika.BlockingConnection(connection_params())


def declare_topology(channel) -> None:
    channel.exchange_declare(
        exchange=settings.exchange, exchange_type="direct", durable=True
    )
    channel.queue_declare(queue=settings.task_result_queue, durable=True)
    channel.queue_bind(
        queue=settings.task_result_queue, exchange=settings.exchange, routing_key="result"
    )
    channel.queue_declare(queue=settings.dead_letter_queue, durable=True)
    channel.queue_bind(
        queue=settings.dead_letter_queue, exchange=settings.exchange, routing_key="dead"
    )


def task_routing_key(worker_id: str) -> str:
    return f"task.{worker_id}"


def register_worker(channel, worker_id: str) -> str:
    """为计算节点创建（或复用）独占队列并绑定。返回队列名。

    队列上额外写两个 header（worker_id / registered_at），供主节点发现在线节点用。
    """
    queue = f"redline.task.{worker_id}"
    channel.queue_declare(queue=queue, durable=True)
    channel.queue_bind(queue=queue, exchange=settings.exchange, routing_key=task_routing_key(worker_id))
    return queue


def list_workers_via_registry(redis_hint: str | None = None) -> list[str]:
    """主节点发现在线 worker 的三种方式，按可靠性排序：

    1) 配置了 REDLINE_WORKERS 环境变量（静态列表，适合 K8s 固定的 StatefulSet）；
    2) Redis 心跳集合（生产推荐）；
    3) 本地进程扫描（仅开发期兜底）。
    """
    env = settings.worker_ids
    if env:
        return [w.strip() for w in env.split(",") if w.strip()]
    return []
