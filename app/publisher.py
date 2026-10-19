"""任务分发与结果回传（主节点侧）。"""
from __future__ import annotations

import json
import threading
import time
import uuid
from typing import Optional

import pika

from . import topology
from .config import settings


class Publisher:
    """持有长连接，避免每次发布都握手。连接断了自动重连。"""

    def __init__(self) -> None:
        self._conn: Optional[pika.BlockingConnection] = None
        self._chan = None
        self._lock = threading.Lock()

    def _ensure(self):
        if self._conn is None or self._conn.is_closed:
            self._conn = topology.connect()
            self._chan = self._conn.channel()
            topology.declare_topology(self._chan)
        return self._chan

    def publish_task(self, worker_id: str, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode()
        with self._lock:
            ch = self._ensure()
            ch.basic_publish(
                exchange=settings.exchange,
                routing_key=topology.task_routing_key(worker_id),
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # 持久化
                    content_type="application/json",
                    message_id=payload.get("task_id"),
                    expiration=str(60 * 1000),  # 60s 未消费即过期，防止堆积
                ),
                mandatory=False,
            )

    def publish_to_dead(self, payload: dict, reason: str) -> None:
        payload = dict(payload, reason=reason)
        with self._lock:
            ch = self._ensure()
            ch.basic_publish(
                exchange=settings.exchange,
                routing_key="dead",
                body=json.dumps(payload, ensure_ascii=False).encode(),
                properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"),
            )

    def workers(self) -> list[str]:
        from .registry import discover_workers

        ids, source = discover_workers()
        self._worker_source = source
        return ids

    def fanout(self, payload_base: dict, province_hits: list[dict]) -> tuple[list[dict], list[str]]:
        """把按省任务分发给在线 worker（按省数轮询，尽量均摊）。

        返回 (tasks, workers)。没有在线 worker 时返回空列表，由调用方降级为本地计算。
        """
        workers = self.workers()
        if not workers:
            return [], []
        tasks = []
        for i, hit in enumerate(province_hits):
            worker_id = workers[i % len(workers)]
            task = dict(payload_base)
            task.update({
                "task_id": f"{payload_base['batch_id']}:{hit['code']}",
                "province_code": hit["code"],
                "province_name": hit["name"],
                "worker_id": worker_id,
            })
            self.publish_task(worker_id, task)
            tasks.append(task)
        return tasks, workers

    def wait_ready(self, timeout: float = 30.0) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                with self._lock:
                    self._ensure()
                return True
            except Exception:
                time.sleep(0.5)
        return False

    def close(self) -> None:
        try:
            if self._conn and not self._conn.is_closed:
                self._conn.close()
        except Exception:
            pass


_publisher: Optional[Publisher] = None
_lock = threading.Lock()


def get_publisher() -> Publisher:
    global _publisher
    with _lock:
        if _publisher is None:
            _publisher = Publisher()
        return _publisher


def new_batch_id() -> str:
    return f"B{time.strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
