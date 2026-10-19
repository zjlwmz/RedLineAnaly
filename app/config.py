"""全局配置。生产环境用环境变量覆盖。"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    # --- 数据 ---
    data_dir: Path = Path(_env("REDLINE_DATA_DIR", str(BASE_DIR / "data")))
    redline_dir: Path = field(init=False)
    province_pkg: Path = field(init=False)
    china_outline: Path = field(init=False)

    # --- 消息队列 ---
    amqp_host: str = _env("AMQP_HOST", "127.0.0.1")
    amqp_port: int = int(_env("AMQP_PORT", "5672"))
    amqp_user: str = _env("AMQP_USER", "guest")
    amqp_password: str = _env("AMQP_PASSWORD", "guest")
    amqp_vhost: str = _env("AMQP_VHOST", "/")

    exchange: str = _env("REDLINE_EXCHANGE", "redline.overlay")
    task_result_queue: str = _env("REDLINE_RESULT_QUEUE", "redline.result")
    dead_letter_queue: str = _env("REDLINE_DLQ", "redline.dead")
    #: 计算节点名单（逗号分隔）。生产用 K8s StatefulSet 名列表，留空则自动发现。
    worker_ids: str = _env("REDLINE_WORKERS", "")

    # --- 业务参数 ---
    #: 严重压盖阈值（“实际压盖面积”/输入面面积）。不同项目标准不同，必须按客户确认后配。
    severe_ratio: float = float(_env("REDLINE_SEVERE_RATIO", "0.5"))
    #: 红线缓冲距离（米）。用于规避红线在省界处被裁剪导致的漏判。
    buffer_m: float = float(_env("REDLINE_BUFFER_M", "50"))
    #: 单批次等待子任务结果的最长时间（秒）
    wait_timeout: float = float(_env("REDLINE_WAIT_TIMEOUT", "30"))
    #: 输入面面积上限（平方米），防止误传超大面把集群打满
    max_input_area_m2: float = float(_env("REDLINE_MAX_INPUT_AREA", "2.0e9"))
    #: 批次数内存保留数量
    max_jobs_in_memory: int = int(_env("REDLINE_MAX_JOBS", "200"))

    def __post_init__(self) -> None:
        object.__setattr__(self, "redline_dir", self.data_dir / "redline")
        object.__setattr__(self, "province_pkg", self.data_dir / "province" / "provinces.gpkg")
        object.__setattr__(self, "china_outline", self.data_dir / "province" / "china_outline.gpkg")


settings = Settings()

#: 红线类型枚举
REDLINE_TYPES = ("生态保护红线", "永久基本农田", "城镇开发边界")


class TaskStatus:
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    TIMEOUT = "timeout"
