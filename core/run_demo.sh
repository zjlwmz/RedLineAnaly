#!/bin/zsh
# 初始化conda（mac miniconda需要）
eval "$(conda shell.zsh hook)"
# 激活你的gis环境
conda activate gis
# 单文件版直接跑（默认使用同仓库 ../data 下的演示数据）
cd "$(dirname "$0")"
exec "${PY:-/usr/bin/python3}" quickstart.py "$@"
