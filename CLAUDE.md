# Archetype

Read these before doing anything:

- `docs/guide/specification.md` plus the focused specification pages — Normative contracts. **Mandatory.**
- `LEARNINGS.md` — Daft 0.7.x patterns, UDF rules, serialization. **Mandatory before writing a processor.**
- `AGENTS.md` — Architecture, service layer, RBAC, conventions, dev workflow.

Skills in `.claude/skills/` are the framework rulebooks. Nothing fires on file paths alone: a skill loads when you invoke it by name or when the agent matches its description to the task at hand. Before editing Python under `src/`, `tests/`, or `examples/`, load the relevant skill — the deterministic footgun gate in CI reviews every PR against the same rules, so skipping them locally just moves the failure to review.

## Skills index

| Skill | How it loads | What it enforces |
|-------|--------------|------------------|
| `daft-patterns` | `/daft-patterns`, or model-invoked for Daft/DataFrame/UDF code | Daft built-in functions to reach for first, UDF decision tree, lazy DAG rules |
| `archetype-components` | `/archetype-components`, or model-invoked for Component/schema code | Component definitions, Arrow serialization, field conventions |
| `archetype-processors` | `/archetype-processors`, or model-invoked for processor/pipeline code | AsyncProcessor patterns, priority ordering, resource access |
| `footgun-detector` | `/footgun-detector` | Scans the current diff for archetype-specific runtime bugs across three perspectives: actor (code), observed (data), observer (review) |

## Agents

Agents in `.claude/agents/` are invoked autonomously — by CI, by other agents, or when their `description` matches the task.

| Agent | When to use |
|-------|-------------|
| `footgun-detector` | Review a PR diff for runtime bugs. Use when the user asks to review a PR for footguns, or invoke from CI on pull requests. |

## Layers

| Layer | Access |
|-------|--------|
| `src/archetype/core/` | Modify only after discussion. Holds the hard invariants. |
| `src/archetype/app/` | Extend carefully. Service contracts live in the specification. Lower-level interface. |
| `src/archetype/runtime/` | Recommended top-level API (`ArchetypeRuntime`). Add runtime ergonomics additively; keep public exports stable. |
| Everything else | Write freely, subject to the contracts they wrap. |

## Top-level API

`ArchetypeRuntime` is the recommended entry point for scripts and beginner docs. `ServiceContainer` / `CommandService` / broker semantics are lower-level interfaces — document them as such. Script boundary is `async with ArchetypeRuntime()` or `with ArchetypeRuntime.sync()`; process lifetime and world lifetime are separate concerns. See `docs/guide/runtime.md` for the full contract.

Examples must run in CI. LLM-backed examples need explicit credential gating or graceful degradation when keys are missing.

## Commands

```bash
make ci                         # Gate: lint + lock-check + tests with coverage
make test                       # Fast tests, no coverage
make check                      # Auto-format + lint
```
