#!/bin/bash
set -euo pipefail

# PostToolUse hook for Edit and Write tools.
# After a Python file is written/edited, runs ruff to catch issues immediately.
# Prints warnings to stderr so the model sees them in context.

INPUT=$(cat)

# Extract file_path from the JSON input
FILE_PATH=$(echo "$INPUT" | python3 -c "
import json, sys
data = json.load(sys.stdin)
tool_input = data.get('tool_input', {})
print(tool_input.get('file_path', ''))
" 2>/dev/null || echo "")

# Only lint Python files in src/, tests/, or examples/
if [[ "$FILE_PATH" != *.py ]]; then
  exit 0
fi

if [[ "$FILE_PATH" != */src/* ]] && [[ "$FILE_PATH" != */tests/* ]] && [[ "$FILE_PATH" != */examples/* ]]; then
  exit 0
fi

# Check if file exists (it should, since the write just happened)
if [ ! -f "$FILE_PATH" ]; then
  exit 0
fi

# Run ruff check (lint errors)
LINT_OUTPUT=$(ruff check "$FILE_PATH" 2>&1 || true)
if [ -n "$LINT_OUTPUT" ] && [ "$LINT_OUTPUT" != "All checks passed!" ]; then
  echo "⚠ ruff lint issues in $FILE_PATH:" >&2
  echo "$LINT_OUTPUT" >&2
  echo "Fix these before continuing." >&2
fi

# Run ruff format check (formatting)
FORMAT_OUTPUT=$(ruff format --check "$FILE_PATH" 2>&1 || true)
if echo "$FORMAT_OUTPUT" | grep -q "would reformat"; then
  echo "⚠ ruff format issues in $FILE_PATH — auto-formatting..." >&2
  ruff format "$FILE_PATH" 2>/dev/null
  echo "Auto-formatted $FILE_PATH" >&2
fi
