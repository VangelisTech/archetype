#!/bin/bash
set -euo pipefail

# Only run in remote (Claude Code on the web) environments
if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

# ── Dependencies ──
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

# ── Architectural Acknowledgment ──
# This prints to stderr so it lands in the session context.
# The model MUST process these constraints before writing any code.
cat >&2 << 'ARCH'

════════════════════════════════════════════════════════════════
 ARCHETYPE ARCHITECTURAL CONTRACT — ACTIVE FOR THIS SESSION
════════════════════════════════════════════════════════════════

 You are working in a DATA-CENTRIC ECS built on Daft DataFrames.
 The DataFrame is the source of truth. Processors are pure
 functions: DataFrame → DataFrame. If the data looks right at
 the end of a tick, nothing else matters.

 HARD RULES (violations will break the architecture):

 1. NEVER .collect().to_pylist() unless you need cross-row
    context (name lookups, message routing). Document why.

 2. USE @daft.func (row-wise) by default. NOT @daft.func.batch.
    @daft.func supports async natively.

 3. USE @daft.cls() for non-serializable state (API clients,
    model weights). Client in __init__, methods are row-wise.

 4. NEVER asyncio.gather over collected rows. Let Daft manage
    concurrency via async @daft.func or @daft.cls().

 5. Broker is GOVERNANCE ONLY (RBAC, quotas). Message delivery
    is MessageDeliveryProcessor. Chat structure is ChatGraphRegistry
    (a Resource).

 6. Tick boundaries are sacred. Outbox tick N → Inbox tick N+1.

 7. Resources for shared state, Components for entity data.

 Read CLAUDE.md and LEARNINGS.md before making changes.
════════════════════════════════════════════════════════════════

ARCH
