#!/usr/bin/env bash
# 依次运行 4 个独立进程阶段，验证真实重启恢复。
set -euo pipefail
cd "$(dirname "$0")/.."
DB="${TMPDIR:-/tmp}/restart-demo.db"
echo "================ 阶段 1/4: seed（提交后退出） ================"
uv run python scripts/restart_demo.py seed --db "$DB"
echo "================ 阶段 2/4: crash（执行中被 kill -9） ================"
uv run python scripts/restart_demo.py crash --db "$DB" || true
echo "================ 阶段 3/4: restart（新进程回收并完成） ================"
sleep 1   # 越过 0.5s 租约
uv run python scripts/restart_demo.py restart --db "$DB"
echo "================ 阶段 4/4: inspect（打印状态流水） ================"
uv run python scripts/restart_demo.py inspect --db "$DB"
