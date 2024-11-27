#!/bin/sh

set -e
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
    echo "Virtual environment created."
fi
. .venv/bin/activate

echo "Installing dependencies..."
pip install -U syftbox diffprivlib psutil pandas bs4 lxml --quiet
echo "Dependencies installed."

echo "Running 'Health Steps Counter - Member' with $(python3 --version) at '$(which python3)'"
python3 main.py

deactivate