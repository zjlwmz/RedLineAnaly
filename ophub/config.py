"""全局配置。

配置优先级（高 -> 低）：
  1. 环境变量（Docker 部署时使用）
  2. config.ini（本地运行时使用，就在工程根目录）
  3. 代码内默认值
"""
import configparser
import os
from dataclasses import dataclass, field

# 路径锚定：config.py 位于 ophub/ 包内，工程根目录是其上一级
# config.ini、tasks.db 等运行文件都锚定在根目录，避免从 PyCharm / 其他工作目录启动时读错
_PKG_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(_PKG_DIR)
CONFIG_FILE = os.path.join(BASE_DIR, "config.ini")

_ini = configparser.ConfigParser()
if os.path.exists(CONFIG_FILE):
    # 显式 UTF-8，保证中文注释在 Windows 下也能正常解析
    _ini.read(CONFIG_FILE, encoding="utf-8")


def _cfg(ini_key: str, env_key: str, default, cast=str):
    """按优先级取值：环境变量 > config.ini > 默认值，并做 strip 归一化。"""
    if env_key in os.environ:
        raw = os.environ[env_key]
    elif _ini.has_option("default", ini_key):
        raw = _ini.get("default", ini_key)
    else:
        raw = str(default)
    raw = raw.strip()
    return cast(raw)


def _to_bool(value) -> bool:
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def _to_list(value) -> list[str]:
    return [item.strip() for item in str(value).split(",") if item.strip()]


@dataclass(frozen=True)
class Settings:
    # 节点角色：all / master / slave
    role: str = _cfg("role", "ROLE", "all", str).lower()

    # ---------- 数据路径 ----------
    redline_data_dir: str = _cfg("redline_data_dir", "REDLINE_DATA_DIR", "data", str)

    # ---------- 业务参数 ----------
    severe_ratio: float = _cfg("severe_ratio", "REDLINE_SEVERE_RATIO", 0.5, float)
    buffer_m: float = _cfg("buffer_m", "REDLINE_BUFFER_M", 50, float)

    # ---------- RabbitMQ 连接 ----------
    mq_host: str = _cfg("rabbitmq_host", "RABBITMQ_HOST", "127.0.0.1", str)
    mq_port: int = _cfg("rabbitmq_port", "RABBITMQ_PORT", 5672, int)
    mq_user: str = _cfg("rabbitmq_user", "RABBITMQ_USER", "guest", str)
    mq_password: str = _cfg("rabbitmq_password", "RABBITMQ_PASSWORD", "guest", str)
    mq_virtual_host: str = _cfg("rabbitmq_vhost", "RABBITMQ_VHOST", "/", str)
    mq_heartbeat: int = _cfg("rabbitmq_heartbeat", "RABBITMQ_HEARTBEAT", 60, int)

    # ---------- Master REST 服务 ----------
    master_host: str = _cfg("master_host", "MASTER_HOST", "0.0.0.0", str)
    master_port: int = _cfg("master_port", "MASTER_PORT", 8001, int)

    # CORS：true 开启跨域访问（可用 cors_origins 限定来源）；false 不开启（仅同源调用）
    cors_enabled: bool = _cfg("cors_enabled", "CORS_ENABLED", True, _to_bool)
    cors_origins: list[str] = field(
        default_factory=lambda: _cfg("cors_origins", "CORS_ORIGINS", "*", _to_list)
    )

    # ---------- MQ 拓扑 ----------
    task_exchange: str = _cfg("task_exchange", "TASK_EXCHANGE", "redline_task_ex", str)
    result_exchange: str = _cfg("result_exchange", "RESULT_EXCHANGE", "redline_result_ex", str)
    task_queue: str = _cfg("task_queue", "TASK_QUEUE", "redline_task_queue", str)
    result_queue: str = _cfg("result_queue", "RESULT_QUEUE", "redline_result_queue", str)
    dead_letter_exchange: str = _cfg("dead_letter_exchange", "DLX_EXCHANGE", "redline_task_dlx", str)
    dead_letter_queue: str = _cfg("dead_letter_queue", "DLQ_QUEUE", "redline_task_dlq", str)

    # MQ 主题：路由键（投递方使用）与绑定键（队列订阅使用）
    task_routing_key: str = _cfg("task_routing_key", "TASK_ROUTING_KEY", "task.redline.calculate", str)
    task_binding_key: str = _cfg("task_binding_key", "TASK_BINDING_KEY", "task.redline.#", str)
    result_routing_key: str = _cfg("result_routing_key", "RESULT_ROUTING_KEY", "result.redline.overlay", str)
    result_binding_key: str = _cfg("result_binding_key", "RESULT_BINDING_KEY", "result.redline.#", str)

    # ---------- 消费控制 ----------
    prefetch_count: int = _cfg("prefetch_count", "PREFETCH_COUNT", 1, int)
    task_max_retries: int = _cfg("task_max_retries", "TASK_MAX_RETRIES", 2, int)
    reconnect_interval: int = _cfg("reconnect_interval", "RECONNECT_INTERVAL", 5, int)

    # SQLite 持久化：任务/子任务状态落库，重启后待处理任务可恢复（相对路径时相对工程目录）
    db_path: str = _cfg("db_path", "DB_PATH", "tasks.db", str)


settings = Settings()
