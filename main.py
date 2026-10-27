"""1993 年 CLCD 土地覆盖分析的 Web 服务入口。"""
import argparse
import logging
import os

import uvicorn

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [pid=%(process)d] %(name)s: %(message)s")


def main() -> None:
    parser = argparse.ArgumentParser(description="1993 年 CLCD 土地覆盖分析")
    parser.add_argument("role", nargs="?", choices=("all", "master", "slave"),
                        help="节点角色；不填写时使用 ROLE 环境变量或 config.ini")
    args = parser.parse_args()
    if args.role:
        # 配置模块首次导入时读取环境变量，命令行角色因此能传递给 API 和 Worker。
        os.environ["ROLE"] = args.role

    from ophub.config import CONFIG_FILE, settings

    logging.info("配置文件: %s", CONFIG_FILE)
    logging.info("节点角色: %s", settings.role)
    logging.info("CLCD 栅格: %s", settings.clcd_raster)
    if settings.role not in ("all", "master", "slave"):
        raise ValueError("role 只支持 all、master、slave")
    if settings.split_level not in ("province", "city", "county"):
        raise ValueError("split_level 只支持 province、city、county")
    if settings.role == "slave":
        from ophub.landcover_jobs import run_slave
        run_slave()
        return
    uvicorn.run("ophub.landcover_api:app", host=settings.master_host, port=settings.master_port, log_level="info")


if __name__ == "__main__":
    main()
