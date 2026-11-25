#!/usr/bin/env bash
# 一条命令完成端到端验证：冷启动回放百万观测、十万级通道改归、
# 新旧测点查询与步长 1 进度回调计数（预算 100000 次，限时 80 秒）。
# PYTHONSAFEPATH=1：不把脚本目录/cwd 自动加入 sys.path（与 python -I 同款的
# 防植入保护，数据目录里即使有 json.py 之类也不会被 import）；代码目录用
# 显式 PYTHONPATH 给出，数据目录与代码目录分离，数据路径仅以参数传入。
set -euo pipefail
DATA_DIR="${1:-/tmp/mseis-verify}"
CODE_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHONSAFEPATH=1 PYTHONPATH="$CODE_DIR" \
  exec python3 -m mseis verify --data "$DATA_DIR"
