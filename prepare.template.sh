#!/bin/bash
. .venv/bin/activate

MAIN_DIR="<DIR-WITH-ALL_REPOS>"
# REPLACE NAMES BASED ON REPO-FOLDER NAMES
databases_REPO_NAME=Databases
tools_REPO_NAME=Tools

export PYTHONPATH="${PYTHONPATH}:."
export PYTHONPATH="${PYTHONPATH}:${MAIN_DIR}/$databases_REPO_NAME"
export PYTHONPATH="${PYTHONPATH}:${MAIN_DIR}/$tools_REPO_NAME"

echo "run: typer src/main.py run ..."
echo "or: typer src/main.py run --help"