#!/bin/bash
# Set & move to home directory
. ./holoclean/set_env.sh

# script="./holoclean/extension/pipe.py"
script="./holoclean/run/holoclean_repair.py"


if [ -z "$VIRTUAL_ENV" ]; then
  echo "Virtual environment not active. Activating..."
  VENV_DIR="./venv"

  if [ ! -d "$VENV_DIR" ]; then
    echo "Virtual environment not found at $VENV_DIR. Creating..."
    python3 -m venv "$VENV_DIR"
  fi

  . "$VENV_DIR/bin/activate"
  echo "Virtual environment activated."
else
  echo "Virtual environment is already active."
fi

echo "Launching example script $script"
python $script