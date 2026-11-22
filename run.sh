#!/usr/bin/env bash
# 启动并发批次发布实验台（仅依赖 Python 3.10+ 标准库）
set -euo pipefail
cd "$(dirname "$0")"
PORT="${1:-8000}"
exec python3 -m src.pointwise.web --host 127.0.0.1 --port "$PORT"
