#!/usr/bin/env bash
# Wrapper around `uv lock --check`. Also fails on silent settings warnings.
#
# uv 0.8.x logs a warning and proceeds when [tool.uv] entries don't parse
# (e.g. `exclude-newer = "7 days"`). That can leave the supply-chain
# quarantine and other pyproject [tool.uv] policies silently disabled.
# Treat any "Failed to parse" warning as a hard failure here.
set -uo pipefail

output=$(uv lock --check 2>&1)
status=$?

# Surface uv's own output unchanged.
echo "$output"

if [ $status -ne 0 ]; then
    exit $status
fi

if echo "$output" | grep -qE 'warning: Failed to parse'; then
    echo "" >&2
    echo "ERROR: uv emitted a settings-discovery warning above." >&2
    echo "       Treating as failure so [tool.uv] policies stay enforceable." >&2
    echo "       Fix the offending entry in pyproject.toml and re-run." >&2
    exit 1
fi
