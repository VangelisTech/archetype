---
title: AgentPrism Trajectory Capture
description: Capture Claude Code SDK runs as Daft-driven trajectories and visualize them with AgentPrism
---

[AgentPrism](https://github.com/evilmartians/agent-prism) is an open-source
React component library that renders OpenTelemetry-style trace trees for AI
agent runs. Archetype's `contrib` package ships a Claude Code processor and an
AgentPrism JSON exporter so you can:

1. Spawn one entity per prompt.
2. Run Claude Code on every entity in parallel inside a single tick via a
   `daft.cls`-backed processor.
3. Capture the agent's `--output-format stream-json` stdout into structured
   `Turn`s on a `ClaudeCodeRun` component.
4. Either emit live OTel spans for AgentPrism's trace explorer or export the
   stored trajectories as ready-to-render `TraceRecord` JSON.

## Prerequisites

- The `claude` CLI on `PATH` (`brew install claude` or `npm i -g @anthropic-ai/claude-code`).
- Logfire / OTel exporter configured in your environment if you want live
  span streaming. Without it the processor still records trajectories — they
  just don't ship to a backend in real time.

## Components

```python
from archetype.contrib.claude_code import ClaudeCodePrompt, ClaudeCodeRun

ClaudeCodePrompt(
    prompt="Summarize CLAUDE.md in one sentence.",
    system_prompt="",     # optional
    model="",             # empty → CLI default
    cwd="",               # empty → process cwd
    max_turns=4,          # 0 → no cap
    allowed_tools="",     # comma-separated, empty → all
)

ClaudeCodeRun()           # populated by the processor
```

`ClaudeCodeRun` carries the trajectory after a tick:

| Field | Description |
|-------|-------------|
| `run_id` / `session_id` | Claude Code session id from the first event |
| `model` | Model the CLI actually used |
| `turns_json` | List of turn dicts (same shape as the example `Turn`) |
| `total_turns` / `total_tokens` / `duration_seconds` | Aggregates |
| `outcome` | One-line summary, prefixed `success:` / `failure:` / `skipped:` |
| `exit_code` | Process exit code (0 = ok, 127 = no CLI) |
| `error` | Stderr captured on non-zero exit |

## Processor

```python
from archetype import ArchetypeRuntime
from archetype.contrib.claude_code import ClaudeCodePrompt, ClaudeCodeProcessor, ClaudeCodeRun
from archetype.contrib.logfire_observer import logfire_hooks

async with ArchetypeRuntime() as runtime:
    world = runtime.world(
        "claude-code-traces",
        processors=[ClaudeCodeProcessor()],
        hooks=logfire_hooks(),
    )

    for prompt in prompts:
        await world.spawn(ClaudeCodePrompt(prompt=prompt, max_turns=4), ClaudeCodeRun())

    await world.run(steps=1)
```

The processor:

- Filters on `claudecoderun__run_id == ""` so a second tick only runs *new*
  prompts — already-finished entities pass through untouched.
- Wraps the `claude` CLI in a `@daft.cls()` worker so the binary is resolved
  once per worker, not per row.
- Emits one logfire span per turn under a root `claude_code.run` span, with
  OpenInference-style `gen_ai.*` and `agent.*` attributes that AgentPrism
  reads natively.

## AgentPrism JSON export

For offline rendering or feeding the React components directly:

```python
from archetype.contrib.agentprism import claude_code_run_to_trace_record
import json

df = await world.query(ClaudeCodePrompt, ClaudeCodeRun)
info = await world.info()
rows = df.where(col("tick") == info.tick - 1).collect().to_pylist()

for row in rows:
    record = claude_code_run_to_trace_record(row)
    Path(f"./traces/{record['id']}.json").write_text(json.dumps(record, indent=2))
```

The `TraceRecord` shape matches AgentPrism's `<TraceList>` / `<TraceDetailView>`
props — drop the JSON in directly.

Span types follow AgentPrism's enum:

| Turn role | AgentPrism span type |
|-----------|---------------------|
| `user`, `system` | `chain` |
| `assistant` | `llm_call` |
| `tool_call`, `tool_result` | `tool_execution` |
| `result` | `agent_invocation` |
| anything else | `unknown` |

Tool-call → tool-result pairs are linked by `metadata.tool_use_id` so the
resulting tree mirrors the parent-child structure AgentPrism renders as a
git-log-style timeline.

## Replay stored trajectories as live spans

If you have trajectories sitting in LanceDB from a prior run and want them
in your live OTel pipeline (so AgentPrism picks them up via the same exporter
as in-flight runs):

```python
from archetype.contrib.agentprism import emit_replay_spans

for row in rows:
    record = claude_code_run_to_trace_record(row)
    emit_replay_spans(record)
```

## Full example

See [`examples/08_claude_code_trajectories.py`](https://github.com/VangelisTech/archetype/blob/main/examples/08_claude_code_trajectories.py).
