#!/bin/bash
set -euo pipefail

# Only run in remote (web) environments
if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

# Install dev tools (linter, test runner, HTTP client)
pip install ruff pytest pytest-asyncio pytest-cov httpx

# Try editable install; may fail due to Python version or dep pins
pip install --no-deps -e . 2>/dev/null || true

# Ensure PYTHONPATH includes src/ for imports regardless
echo 'export PYTHONPATH="${CLAUDE_PROJECT_DIR}/src:${PYTHONPATH:-}"' >> "$CLAUDE_ENV_FILE"
