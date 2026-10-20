"""统一启动入口：同一份代码，通过 config.ini 中的 role 区分节点角色。

用法：
    python main.py            # 角色由 config.ini 的 role 决定（当前默认 all）
    python main.py master     # 仅主节点：接收/下发任务，不参与计算
    python main.py slave      # 仅从节点：只消费任务做计算
    python main.py all        # 合一节点：既接收下发，又在本进程内消费计算（单节点即可跑通）
"""
import logging
import sys
import threading

import uvicorn

from ophub.config import CONFIG_FILE, settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("main")


def run_master() -> None:
    # 以导入字符串方式启动，确保 uvicorn reload 等机制可用
    uvicorn.run(
        "ophub.master:app",
        host=settings.master_host,
        port=settings.master_port,
        log_level="info",
    )


def run_all_in_one() -> None:
    """合一节点：在 Master 进程内启动一个 Slave 消费者守护线程，再启动 REST 服务。

    消费者使用独立的 RabbitMQ 连接（pika 连接/频道不能跨线程共享），
    与独立 Slave 行为完全一致：手动ACK、prefetch=1、失败重试、断线重连。
    以后需要扩容时，再在其他机器/终端启动任意个 slave 即可自动分担任务。
    """
    from ophub.slave import run_slave

    worker = threading.Thread(target=run_slave, name="slave-worker", daemon=True)
    worker.start()
    logger.info("内置消费者线程已启动（本节点同时承担 Master + Slave）")
    run_master()


def main() -> None:
    # 命令行参数优先级最高，方便同机开多个终端分别跑 master / slave / all
    role = sys.argv[1].strip().lower() if len(sys.argv) > 1 else settings.role

    logger.info("配置文件: %s", CONFIG_FILE)
    logger.info("节点角色: %s | RabbitMQ: %s:%s", role, settings.mq_host, settings.mq_port)

    if role == "slave":
        from ophub.slave import run_slave

        run_slave()
    elif role == "master":
        logger.info("REST 服务监听: http://%s:%s (文档 /docs)",
                    "127.0.0.1" if settings.master_host == "0.0.0.0" else settings.master_host,
                    settings.master_port)
        run_master()
    elif role == "all":
        logger.info("REST 服务监听: http://%s:%s (文档 /docs)",
                    "127.0.0.1" if settings.master_host == "0.0.0.0" else settings.master_host,
                    settings.master_port)
        run_all_in_one()
    else:
        raise SystemExit(f"未知角色 role={role!r}，只支持 'master'、'slave' 或 'all'")


if __name__ == "__main__":
    main()
