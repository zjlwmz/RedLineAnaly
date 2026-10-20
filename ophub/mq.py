"""RabbitMQ 连接与拓扑声明（主/从节点共用，保证双方看到的拓扑一致）。

共享队列拓扑：
  task_exchange (topic, durable)
    └─ task_queue (durable, 绑定 task_binding_key)，失败消息经 DLX 进死信队列
       所有 slave 共享此队列竞争消费（prefetch=1 天然负载均衡）
  result_exchange (topic, durable)
    └─ result_queue (durable, 绑定 result_binding_key)
  dead_letter_exchange (fanout, durable)
    └─ dead_letter_queue (durable)
"""
import logging

import pika

from ophub.config import settings

logger = logging.getLogger(__name__)


def connection_params() -> pika.ConnectionParameters:
    credentials = pika.PlainCredentials(settings.mq_user, settings.mq_password)
    return pika.ConnectionParameters(
        host=settings.mq_host,
        port=settings.mq_port,
        virtual_host=settings.mq_virtual_host,
        credentials=credentials,
        heartbeat=settings.mq_heartbeat,
        blocked_connection_timeout=30,
        connection_attempts=3,
        retry_delay=3,
    )


def connect() -> pika.BlockingConnection:
    return pika.BlockingConnection(connection_params())


def declare_topology(channel: pika.channel.Channel) -> None:
    """声明交换机/队列，主节点和从节点启动时都调用一次（幂等）。"""
    # ---- 死信：任务队列里被 reject(requeue=False) 的消息进入这里 ----
    channel.exchange_declare(settings.dead_letter_exchange, exchange_type="fanout", durable=True)
    channel.queue_declare(settings.dead_letter_queue, durable=True)
    channel.queue_bind(settings.dead_letter_queue, settings.dead_letter_exchange, routing_key="")

    # ---- 任务：Topic 交换机 + 共享持久化队列 ----
    channel.exchange_declare(settings.task_exchange, exchange_type="topic", durable=True)
    channel.queue_declare(
        settings.task_queue,
        durable=True,
        arguments={"x-dead-letter-exchange": settings.dead_letter_exchange},
    )
    channel.queue_bind(
        settings.task_queue, settings.task_exchange, routing_key=settings.task_binding_key
    )

    # ---- 结果：Slave 计算完成后回传，Master 后台线程消费 ----
    channel.exchange_declare(settings.result_exchange, exchange_type="topic", durable=True)
    channel.queue_declare(settings.result_queue, durable=True)
    channel.queue_bind(
        settings.result_queue, settings.result_exchange, routing_key=settings.result_binding_key
    )

    logger.info("RabbitMQ topology declared")
