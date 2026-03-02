#!/bin/bash
# Wrapper script to run photo_cleaner.py with the correct venv

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"
source venv/bin/activate
exec python photo_cleaner.py "$@"
