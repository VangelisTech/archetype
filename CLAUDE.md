# Archetype

Read these before doing anything:

- `docs/guide/specification.md` plus the focused specification pages — Normative contracts. **Mandatory.**
- `LEARNINGS.md` — Daft 0.7.x patterns, UDF rules, serialization. **Mandatory before writing a processor.**
- `AGENTS.md` — Architecture, service layer, RBAC, conventions, dev workflow.

Skills in `.claude/skills/` are the framework rulebooks. A skill loads when
you invoke it by name or when the model matches its description to the task.
For the three Python rulebooks, `paths` frontmatter additionally limits that
model invocation to Python under `src/`, `tests/`, or `examples/`; it is an
eligibility fence, not a path-only trigger or a guarantee of invocation.
Before editing those files, load the relevant skill — the deterministic
footgun gate in CI reviews every PR against the same rules, so skipping them
locally just moves the failure to review.

## Skills index

| Skill | How it loads | What it enforces |
|-------|--------------|------------------|
| `daft-patterns` | `/daft-patterns`, or model-invoked for Daft/DataFrame/UDF code within its configured paths | Daft built-in functions to reach for first, UDF decision tree, lazy DAG rules |
| `archetype-components` | `/archetype-components`, or model-invoked for Component/schema code within its configured paths | Component definitions, Arrow serialization, field conventions |
| `archetype-processors` | `/archetype-processors`, or model-invoked for processor/pipeline code within its configured paths | AsyncProcessor patterns, priority ordering, resource access |
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

`ArchetypeRuntime` is the recommended entry point for scripts and beginner
docs. `ServiceContainer`, concrete services, and `CommandGateway` are internal
and are not top-level exports. The script boundary is
`async with ArchetypeRuntime()` or `with ArchetypeRuntime.sync()`; process and
world lifetimes remain separate. See `docs/guide/runtime.md`.

Examples must run in CI. LLM-backed examples need explicit credential gating or graceful degradation when keys are missing.

## Commands

```bash
make ci                         # Gate: lint + lock-check + tests with coverage
make test                       # Fast tests, no coverage
make check                      # Auto-format + lint
```

## PR flow

Open the PR and stop — never run `gh pr merge --auto`. The automerge
workflow arms auto-merge only after the deterministic review gate passes
your current head; arming earlier does not merge sooner, it only lets the
PR enter the merge queue before its review verdict (premature arms are
auto-reverted). Reply to footgun review threads with what you changed
before resolving them.
