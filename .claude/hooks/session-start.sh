#!/bin/bash
set -euo pipefail

# Only run in remote (Claude Code on the web) environments
if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

# Install project dependencies directly (pip install -e fails due to
# pyiceberg version constraint typo in pyproject.toml: >=11.0.0 vs >=0.11.0)
pip install --quiet \
  "daft[openai,lance,iceberg]>=0.7.4" \
  "lancedb>=0.22.0" \
  "pyiceberg[daft,sql-sqlite]>=0.11.0" \
  "uuid-utils>=0.11.0" \
  "pydantic>=2.0" \
  "fastapi>=0.110" \
  "uvicorn[standard]>=0.29" \
  "typer>=0.9" \
  "psutil" \
  2>/dev/null

# Dev dependencies
pip install --quiet \
  "pytest>=8.3" \
  "pytest-asyncio>=0.26" \
  "pytest-cov>=5.0" \
  "ruff>=0.9" \
  "httpx>=0.27" \
  2>/dev/null

# Ensure PYTHONPATH includes src/
echo 'export PYTHONPATH="src:${PYTHONPATH:-}"' >> "$CLAUDE_ENV_FILE"
