# Archetype

Read these before doing anything:

- `docs/guide/specification.md` — Normative contracts. Takes precedence over everything else. **Mandatory.**
- `LEARNINGS.md` — Daft 0.7.x patterns, UDF rules, serialization. **Mandatory before writing a processor.**
- `AGENTS.md` — Architecture, service layer, RBAC, conventions, dev workflow.

Skills in `.claude/skills/` enforce framework rules automatically. They fire based on file paths — you don't need to invoke them manually.

## Skills index

| Skill | What it enforces |
|-------|-----------------|
| `daft-patterns` | Daft built-in functions to reach for first, UDF decision tree, lazy DAG rules |
| `archetype-components` | Component definitions, Arrow serialization, field conventions |
| `archetype-processors` | AsyncProcessor patterns, priority ordering, resource access |

## Layers

| Layer | Access |
|-------|--------|
| `src/archetype/core/` | Modify only after discussion. Holds the hard invariants. |
| `src/archetype/app/` | Extend carefully. Service contracts live in the specification. |
| Everything else | Write freely, subject to the contracts they wrap. |

## Commands

```bash
make ci                         # Gate: lint + lock-check + tests with coverage
make test                       # Fast tests, no coverage
make check                      # Auto-format + lint
```
