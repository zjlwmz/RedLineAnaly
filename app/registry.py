"""计算节点注册表（主节点用来发现「该把任务发给谁」）。

RabbitMQ 的 AMQP 0-9-1 没有「列出所有队列/绑定」的协议方法（那是 Mgmt HTTP API 的事），
所以不能像很多人想的那样「通过 exchange 反查有哪些 worker」。

三种可行方案，按推荐度排序：

  A. 静态名单（最简单、生产最常用）
     环境变量 REDLINE_WORKERS=worker-1,worker-2,…  K8s 下用 StatefulSet 名即可。
  B. Redis 心跳（节点数动态变化时用）
     worker 每 5s 写 `redline:worker:<id>` = ts，TTL 15s；主节点扫前缀拿活跃节点。
  C. 本地进程扫描（仅开发期）
     扫 /proc 找 `app.worker --id xxx`。

本文件实现 A + B + C 的自动降级，保证 demo 单机跑通、生产可换 Redis。
"""
from __future__ import annotations

import glob
import os
import re
import subprocess
import time
from typing import Optional

from .config import settings

_REDIS_KEY_PREFIX = "redline:worker:"
_redis_client = None


def _redis_workers() -> Optional[list[str]]:
    global _redis_client
    if _redis_client is None:
        try:
            import redis  # type: ignore

            _redis_client = redis.Redis(
                host=os.environ.get("REDIS_HOST", "127.0.0.1"),
                port=int(os.environ.get("REDIS_PORT", "6379")),
                socket_connect_timeout=0.5,
                socket_timeout=0.5,
                decode_responses=True,
            )
            _redis_client.ping()
        except Exception:
            _redis_client = False  # 不可用，标记为 False 避免反复重连
    if not _redis_client:
        return None
    try:
        ids = []
        for key in _redis_client.scan_iter(match=f"{_REDIS_KEY_PREFIX}*"):
            ts = _redis_client.get(key)
            if not ts or time.time() - float(ts) > 15:
                continue
            ids.append(key[len(_REDIS_KEY_PREFIX):])
        return sorted(ids)
    except Exception:
        return None


def _local_process_workers() -> list[str]:
    """开发期兜底：扫进程命令行。"""
    ids: set[str] = set()
    try:
        out = subprocess.check_output(["ps", "-eo", "args"], text=True, timeout=2)
    except Exception:
        out = ""
    for line in out.splitlines():
        m = re.search(r"app\.worker\s+--id\s+(\S+)", line)
        if m:
            ids.add(m.group(1))

    if not ids and os.path.isdir("/proc"):
        for path in glob.glob("/proc/[0-9]*/cmdline"):
            try:
                with open(path, "rb") as fh:
                    raw = fh.read().replace(b"\x00", b" ").decode(errors="ignore")
            except Exception:
                continue
            m = re.search(r"app\.worker\s+--id\s+(\S+)", raw)
            if m:
                ids.add(m.group(1))
    return sorted(ids)


def discover_workers() -> tuple[list[str], str]:
    """返回 (worker_ids, 来源)。"""
    if settings.worker_ids:
        return [w.strip() for w in settings.worker_ids.split(",") if w.strip()], "static"

    redis_ids = _redis_workers()
    if redis_ids:
        return redis_ids, "redis"

    local = _local_process_workers()
    if local:
        return local, "local_scan"

    return [], "none"


def heartbeat(worker_id: str) -> None:
    """worker 调用：写心跳（Redis 可用时）。依赖 Redis 未装则静默跳过。"""
    redis_ids = _redis_workers()
    if redis_ids is None:
        return
    try:
        _redis_client.set(f"{_REDIS_KEY_PREFIX}{worker_id}", str(time.time()), ex=15)
    except Exception:
        pass
