#!/usr/bin/env bash
# PreToolUse(Bash) guard: block the Modal "footgun" launch pattern.
#
# `modal run` keeps the local CLI as the job driver; when this session is reaped the
# job is cancelled mid-run. Long jobs must use deploy + spawn (bench/libero/submit_gepa.py).
# Denies `modal run` only when it targets a long job (--detach or a known job script).
# Escape hatch: include ALLOW_MODAL_RUN=1 in the command to override.
#
# stdin = PreToolUse JSON; deny via hookSpecificOutput.permissionDecision.

cmd="$(jq -r '.tool_input.command // ""')"

# Escape hatch — explicit override.
case "$cmd" in
  *ALLOW_MODAL_RUN=1*) exit 0 ;;
esac

# Only inspect `modal run` invocations; everything else (deploy, app, logs…) is fine.
case "$cmd" in
  *"modal run"*) ;;
  *) exit 0 ;;
esac

# Footgun iff it's a long job: --detach, or one of the long-running pipeline scripts.
if printf '%s' "$cmd" | grep -qE -- '--detach|gepa_daft\.py|libero_plus_sweep\.py|baseline_sweep\.py'; then
  reason="Blocked: long Modal jobs must use deploy + spawn, not 'modal run' (it ties the job to this session and gets cancelled mid-run). Do: 'modal deploy bench/libero/gepa_daft.py' then 'uv run --with modal python bench/libero/submit_gepa.py …'. If this is genuinely a short/interactive run, prefix the command with ALLOW_MODAL_RUN=1 to override."
  jq -nc --arg r "$reason" \
    '{hookSpecificOutput:{hookEventName:"PreToolUse",permissionDecision:"deny",permissionDecisionReason:$r}}'
  exit 0
fi

exit 0
