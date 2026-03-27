#!/usr/bin/env bash
#
# StopAt30M 一键初始化脚本
#
# 用法:
#   chmod +x scripts/setup.sh
#   ./scripts/setup.sh              # 完整流程（数据 + 训练 + 信号 + 回测）
#   ./scripts/setup.sh --skip-train # 仅安装环境和下载数据
#   ./scripts/setup.sh --help
#
set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────
SKIP_TRAIN=false
DATA_SOURCE="qlib+baostock"
PYTHON_CMD=""
TOP_K=10

usage() {
    cat <<'USAGE'
StopAt30M 一键初始化

Usage: ./scripts/setup.sh [OPTIONS]

Options:
  --python PATH       指定 Python 解释器 (默认: 自动检测 3.11 > 3.12 > python3)
  --data-source SRC   数据源: qlib+baostock(默认,推荐) / baostock / qlib / akshare / tushare
  --skip-train        跳过训练，仅安装环境和下载数据
  --top-k N           信号/回测选股数量 (默认: 10)
  -h, --help          显示帮助
USAGE
    exit 0
}

# ── Parse args ────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --python)      PYTHON_CMD="$2"; shift 2 ;;
        --data-source) DATA_SOURCE="$2"; shift 2 ;;
        --skip-train)  SKIP_TRAIN=true; shift ;;
        --top-k)       TOP_K="$2"; shift 2 ;;
        -h|--help)     usage ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

# ── Detect project root ──────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"
echo "📁 项目目录: $PROJECT_ROOT"

# ── Detect Python ─────────────────────────────────────────────────────
if [[ -z "$PYTHON_CMD" ]]; then
    for candidate in python3.11 python3.12 python3.13 python3; do
        if command -v "$candidate" &>/dev/null; then
            PYTHON_CMD="$candidate"
            break
        fi
    done
fi

if [[ -z "$PYTHON_CMD" ]]; then
    echo "❌ 未找到 Python 3.10+，请安装后重试"
    exit 1
fi

PY_VERSION=$("$PYTHON_CMD" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
PY_MAJOR=$("$PYTHON_CMD" -c "import sys; print(sys.version_info.major)")
PY_MINOR=$("$PYTHON_CMD" -c "import sys; print(sys.version_info.minor)")

if [[ "$PY_MAJOR" -lt 3 ]] || [[ "$PY_MINOR" -lt 10 ]]; then
    echo "❌ Python $PY_VERSION 版本太低，需要 3.10+"
    exit 1
fi
echo "🐍 Python: $PYTHON_CMD ($PY_VERSION)"

# ── Step 1: Virtual environment ───────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════"
echo "  Step 1/6: 创建虚拟环境"
echo "═══════════════════════════════════════════════════"

VENV_DIR="$PROJECT_ROOT/.venv"
if [[ -d "$VENV_DIR" ]]; then
    echo "✅ 虚拟环境已存在: $VENV_DIR"
else
    "$PYTHON_CMD" -m venv "$VENV_DIR"
    echo "✅ 虚拟环境已创建: $VENV_DIR"
fi

# Activate
source "$VENV_DIR/bin/activate"
PIP="$VENV_DIR/bin/pip"
PY="$VENV_DIR/bin/python"

# ── Step 2: Install dependencies ──────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════"
echo "  Step 2/6: 安装依赖"
echo "═══════════════════════════════════════════════════"

"$PIP" install --upgrade pip -q
"$PIP" install -r requirements.txt -q 2>&1 | tail -5
echo "✅ 依赖安装完成"

# ── Step 3: Create output directories ─────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════"
echo "  Step 3/6: 初始化目录结构"
echo "═══════════════════════════════════════════════════"

for d in output/models output/signals output/trades output/logs output/backtest; do
    mkdir -p "$d"
done
echo "✅ 输出目录已就绪"

# ── Step 4: Download data ─────────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════"
echo "  Step 4/6: 下载数据 (source=$DATA_SOURCE)"
echo "═══════════════════════════════════════════════════"

QLIB_DATA_DIR="$HOME/.qlib/qlib_data/cn_data"
if [[ -d "$QLIB_DATA_DIR" ]] && [[ "$(ls -A "$QLIB_DATA_DIR" 2>/dev/null)" ]]; then
    echo "ℹ️  Qlib 数据目录已存在 ($QLIB_DATA_DIR)，跳过全量下载"
    echo "   如需增量更新，运行: $PY main.py download"
else
    echo "⏳ 首次下载，可能需要 10-30 分钟..."
    "$PY" main.py download --source "$DATA_SOURCE"
    echo "✅ 数据下载完成"
fi

if [[ "$SKIP_TRAIN" == true ]]; then
    echo ""
    echo "═══════════════════════════════════════════════════"
    echo "  ✅ 初始化完成（已跳过训练）"
    echo "═══════════════════════════════════════════════════"
    echo ""
    echo "  后续操作:"
    echo "    source .venv/bin/activate"
    echo "    python main.py train --top-k $TOP_K           # 训练模型"
    echo "    python main.py signal --model-path output/models/model_lgbm.pkl  # 生成信号"
    echo "    python main.py dashboard                      # 启动面板"
    echo ""
    exit 0
fi

# ── Step 5: Train model ───────────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════"
echo "  Step 5/6: 训练模型"
echo "═══════════════════════════════════════════════════"

MODEL_PATH="$PROJECT_ROOT/output/models/model_lgbm.pkl"
if [[ -f "$MODEL_PATH" ]]; then
    echo "ℹ️  模型已存在: $MODEL_PATH"
    echo "   如需重新训练，请先删除该文件或手动运行:"
    echo "   python main.py train --top-k $TOP_K"
else
    echo "⏳ 训练中 (LightGBM, 通常 5-15 分钟)..."
    "$PY" main.py train --top-k "$TOP_K"
    echo "✅ 模型训练完成"
fi

# ── Step 6: Generate signals & backtest ───────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════"
echo "  Step 6/6: 生成信号 & 回测"
echo "═══════════════════════════════════════════════════"

echo "⏳ 生成交易信号..."
"$PY" main.py signal --model-path "$MODEL_PATH"
echo "✅ 信号已生成"

echo ""
echo "⏳ 运行回测验证..."
"$PY" main.py backtest --model-path "$MODEL_PATH"
echo "✅ 回测完成"

# ── Done ──────────────────────────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  🎉 StopAt30M 初始化完成！"
echo "═══════════════════════════════════════════════════════════"
echo ""
echo "  启动面板:   python main.py dashboard"
echo "  增量更新:   python main.py download"
echo "  重新训练:   python main.py train --top-k $TOP_K"
echo "  生成信号:   python main.py signal --model-path output/models/model_lgbm.pkl"
echo ""
echo "  详细说明:   cat MANUAL.md"
echo "═══════════════════════════════════════════════════════════"
