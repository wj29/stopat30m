#!/usr/bin/env bash
set -euo pipefail

# One-click bootstrap: setup env → download data → train model → generate signals → build frontend

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

# Ensure virtual environment exists
if [[ ! -d ".venv" ]]; then
    echo "Virtual environment not found. Running setup_env.sh first..."
    bash setup_env.sh
fi

source .venv/bin/activate

echo "=============================================================="
echo "Step 1/4: Full data download (Qlib base + incremental append)"
echo "=============================================================="
python main.py download --full

echo "=============================================================="
echo "Step 2/4: Train model"
echo "=============================================================="
python main.py train --model-type lgbm --save-name first_run --top-k 10

MODEL_PATH="output/models/first_run_lgbm.pkl"
if [[ ! -f "$MODEL_PATH" ]]; then
  echo "ERROR: model file not found: $MODEL_PATH" >&2
  exit 1
fi

echo "=============================================================="
echo "Step 3/4: Generate latest signals"
echo "=============================================================="
python main.py signal --model-path "$MODEL_PATH"

echo "=============================================================="
echo "Step 4/4: Build frontend"
echo "=============================================================="
python main.py build

echo
echo "=============================================================="
echo "First run completed successfully."
echo "Model:   $MODEL_PATH"
echo "Signals: output/signals/"
echo ""
echo "Start the app:"
echo "  source .venv/bin/activate"
echo "  python main.py dev        # Development (hot reload)"
echo "  python main.py serve      # Production (single port)"
echo "=============================================================="
