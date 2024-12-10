#!/bin/bash
# Set & move to home directory
. ./holoclean/set_env.sh

script="./holoclean/run/holoclean_repair.py"

if [ $# -eq 1 ]; then
  script_arg="$1"
else
  echo "Usage: ./start.sh <argument>"
  exit 1
fi

if [ -z "$VIRTUAL_ENV" ]; then
  echo "Virtual environment not active. Activating..."
  VENV_DIR="./holoclean/venv"

  if [ ! -d "$VENV_DIR" ]; then
    echo "Virtual environment not found at $VENV_DIR. Creating..."
    python3 -m venv "$VENV_DIR"
  fi

  . "$VENV_DIR/bin/activate"
  echo "Virtual environment activated."
else
  echo "Virtual environment is already active."
fi

echo "Launching example script $script with argument $script_arg"
python $script $script_arg