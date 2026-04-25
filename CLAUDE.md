# Archetype

Read these before doing anything:

- `docs/guide/specification.md` — Normative contracts. Takes precedence over everything else. **Mandatory.**
- `LEARNINGS.md` — Daft 0.7.x patterns, UDF rules, serialization. **Mandatory before writing a processor.**
- `AGENTS.md` — Architecture, service layer, RBAC, conventions, dev workflow.

Skills in `.claude/skills/` enforce framework rules automatically. They fire based on file paths — you don't need to invoke them manually.

## Skills index

| Skill | How it fires | What it enforces |
|-------|--------------|------------------|
| `daft-patterns` | auto (Python files) | Daft built-in functions to reach for first, UDF decision tree, lazy DAG rules |
| `archetype-components` | auto (Python files) | Component definitions, Arrow serialization, field conventions |
| `archetype-processors` | auto (Python files) | AsyncProcessor patterns, priority ordering, resource access |
| `footgun-detector` | `/footgun` | Scans the current diff for archetype-specific runtime bugs across three perspectives: actor (code), observed (data), observer (review) |

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
| `src/archetype/sugar.py` | Recommended top-level API (`ArchetypeRuntime`). Add sugar additively; keep `World`/`Processor` exports stable. |
| Everything else | Write freely, subject to the contracts they wrap. |

## Top-level API

`ArchetypeRuntime` is the recommended entry point for scripts and beginner docs. `ServiceContainer` / `CommandService` / broker semantics are lower-level interfaces — document them as such. Script boundary is `async with ArchetypeRuntime()` or `with ArchetypeRuntime.sync()`; process lifetime and world lifetime are separate concerns. See `docs/guide/specification.md` § "Contracts Before Sugar" for the full rationale.

Examples must run in CI. LLM-backed examples need explicit credential gating or graceful degradation when keys are missing.

## Commands

```bash
make ci                         # Gate: lint + lock-check + tests with coverage
make test                       # Fast tests, no coverage
make check                      # Auto-format + lint
```
