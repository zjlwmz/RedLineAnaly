#!/usr/bin/env bash
# 单文件版直接跑（默认使用同仓库 ../data 下的演示数据）
cd "$(dirname "$0")"
exec "${PY:-/usr/bin/python3}" quickstart.py "$@"
