#!/usr/bin/env bash
set -euo pipefail

# Create a project-local virtual environment and install all dependencies.
# Run once on any new machine: bash setup_env.sh
#
# After setup, activate with: source .venv/bin/activate
# Then all commands use the correct Python automatically.

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

VENV_DIR=".venv"

# Find the best Python 3 interpreter
find_python() {
    # Prefer python3.11 (project's target version), then python3, then python
    for cmd in python3.11 python3 python; do
        if command -v "$cmd" &>/dev/null; then
            version=$("$cmd" --version 2>&1 | grep -oE '[0-9]+\.[0-9]+')
            major=$(echo "$version" | cut -d. -f1)
            minor=$(echo "$version" | cut -d. -f2)
            if [[ "$major" -eq 3 && "$minor" -ge 10 ]]; then
                echo "$cmd"
                return
            fi
        fi
    done
    echo "ERROR: Python 3.10+ not found" >&2
    exit 1
}

PYTHON=$(find_python)
echo "Using Python: $PYTHON ($($PYTHON --version))"

echo ""
echo "=== Creating virtual environment ==="
if [[ ! -d "$VENV_DIR" ]]; then
    $PYTHON -m venv "$VENV_DIR"
    echo "Created $VENV_DIR"
else
    echo "$VENV_DIR already exists, skipping creation"
fi

# From here on, everything uses the venv's Python
source "$VENV_DIR/bin/activate"
echo "Activated: $(which python) ($(python --version))"

echo ""
echo "=== Installing Python dependencies ==="
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
echo "Python packages installed."

echo ""
echo "=== Installing frontend dependencies ==="
FRONTEND_DIR="stopat30m/web/frontend"
if [[ -f "$FRONTEND_DIR/package.json" ]]; then
    cd "$FRONTEND_DIR"
    npm install
    cd "$ROOT_DIR"
    echo "Frontend packages installed."
else
    echo "Frontend not found at $FRONTEND_DIR, skipping."
fi

echo ""
echo "=============================================================="
echo "Setup complete!"
echo ""
echo "Every time you open a new terminal, run:"
echo "  source .venv/bin/activate"
echo ""
echo "Then:"
echo "  python main.py dev        # Start backend + frontend"
echo "  python main.py --help     # See all commands"
echo "=============================================================="
