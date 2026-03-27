#!/usr/bin/env bash
set -euo pipefail

# One-click bootstrap on a machine that already installed requirements.txt:
# 1) full data download
# 2) train model
# 3) generate latest signals

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

echo "=============================================================="
echo "Step 1/3: Full data download (Qlib base + incremental append)"
echo "Command: py main.py download --full"
echo "=============================================================="
py main.py download --full

echo "=============================================================="
echo "Step 2/3: Train model"
echo "Command: py main.py train --model-type lgbm --save-name first_run --top-k 10"
echo "=============================================================="
py main.py train --model-type lgbm --save-name first_run --top-k 10

MODEL_PATH="output/models/first_run_lgbm.pkl"
if [[ ! -f "$MODEL_PATH" ]]; then
  echo "ERROR: model file not found: $MODEL_PATH" >&2
  exit 1
fi

echo "=============================================================="
echo "Step 3/3: Generate latest signals"
echo "Command: py main.py signal --model-path $MODEL_PATH"
echo "=============================================================="
py main.py signal --model-path "$MODEL_PATH"

echo
echo "=============================================================="
echo "First run completed successfully."
echo "Model: $MODEL_PATH"
echo "Signals: output/signals/"
echo "Dashboard: py main.py dashboard"
echo "=============================================================="
