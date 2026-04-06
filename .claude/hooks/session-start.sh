#!/bin/bash
set -euo pipefail

# Only run in remote (Claude Code on the web) environments
if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

cd "$CLAUDE_PROJECT_DIR"

# Bootstrap uv if the sandbox doesn't ship with it
UV_VERSION="0.8.17"
if ! command -v uv >/dev/null 2>&1; then
  pip install --user "uv==${UV_VERSION}"
  export PATH="$HOME/.local/bin:$PATH"
fi

# Project requires Python >=3.12 — uv fetches the interpreter if missing
uv python install 3.12

# Sync project + dev dependency-group from pyproject.toml / uv.lock
# This mirrors `make sync-dev` and is the same env CI uses for `make ci`.
uv sync --group dev

# Expose the venv on PATH so `ruff`, `pytest`, `archetype` work without `uv run`
echo "export PATH=\"${CLAUDE_PROJECT_DIR}/.venv/bin:\${PATH}\"" >> "$CLAUDE_ENV_FILE"
echo "export VIRTUAL_ENV=\"${CLAUDE_PROJECT_DIR}/.venv\"" >> "$CLAUDE_ENV_FILE"
