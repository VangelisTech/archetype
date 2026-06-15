#!/usr/bin/env bash
# One-screen sitrep: the curated narrative (STATUS.md) + live ground-truth signals.
# Run `./status.sh` any time you come back cold. Tells you what we're doing + green/red.
set -uo pipefail
cd "$(dirname "$0")"

echo "════════════════════ STATUS · $(date '+%Y-%m-%d %H:%M %Z') ════════════════════"
echo

# 1) Curated narrative (goal / health / next action / blockers)
if [ -f STATUS.md ]; then
  sed -n '/^## 🎯/,/^## ✅/p' STATUS.md | sed '$d'
else
  echo "(no STATUS.md — Claude should create one)"
fi

echo "──────────────────────────── live signals ────────────────────────────"

# 2) Git health
branch=$(git branch --show-current 2>/dev/null)
unpushed=$(git log --oneline @{u}..HEAD 2>/dev/null | wc -l | tr -d ' ')
dirty=$(git status --porcelain 2>/dev/null | wc -l | tr -d ' ')
echo "git      : branch=${branch}  unpushed=${unpushed}  dirty=${dirty} file(s)"

# 3) In-flight Modal apps (running only)
running=$(modal app list 2>/dev/null | grep -iE 'archetype-(gepa|libero|vla)' | grep -ci running || true)
echo "modal    : ${running:-0} archetype app(s) currently running"

# 4) Last submitted job (spawn pattern — runs in the cloud, decoupled from this session)
if [ -f /tmp/gepa_last_call.txt ]; then
  cid=$(cat /tmp/gepa_last_call.txt)
  state=$(uv run --with modal python bench/libero/submit_gepa.py --fetch "$cid" 2>/dev/null | grep '^STATUS:' | head -1)
  echo "last job : ${cid} → ${state:-(could not query — try --fetch directly)}"
else
  echo "last job : (none submitted via spawn yet — bench/libero/submit_gepa.py)"
fi

# 5) Recent commits
echo "commits  :"
git log --oneline -5 2>/dev/null | sed 's/^/           /'

echo "───────────────────────────────────────────────────────────────────────"
echo "full plan → docs/design/gepa-run-book.html   ·   cross-worktree board → /plots"
