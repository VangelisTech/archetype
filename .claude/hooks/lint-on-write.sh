#!/bin/bash
set -euo pipefail

# PreToolUse hook for Edit and Write tools.
# Reads the tool input from stdin, extracts the file path,
# and runs ruff check + format on Python files AFTER the edit lands.
#
# This hook runs BEFORE the tool executes, so we can only validate
# the file path. The PostToolUse hook handles post-write linting.

INPUT=$(cat)

# Extract file_path from the JSON input
FILE_PATH=$(echo "$INPUT" | python3 -c "
import json, sys
data = json.load(sys.stdin)
# tool_input contains the parameters passed to the tool
tool_input = data.get('tool_input', {})
print(tool_input.get('file_path', ''))
" 2>/dev/null || echo "")

# Only lint Python files
if [[ "$FILE_PATH" != *.py ]]; then
  exit 0
fi

# Check it's in src/ or tests/
if [[ "$FILE_PATH" != */src/* ]] && [[ "$FILE_PATH" != */tests/* ]] && [[ "$FILE_PATH" != */examples/* ]]; then
  exit 0
fi

# PreToolUse: we can't lint yet (edit hasn't happened), just allow it
exit 0
