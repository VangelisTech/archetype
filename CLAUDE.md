# Archetype

Read these before doing anything:

- `LEARNINGS.md` — Daft 0.7.x patterns, UDF rules, serialization. **Mandatory.**
- `AGENTS.md` — Architecture, service layer, RBAC, conventions, dev workflow.

Skills in `.claude/skills/` enforce framework rules automatically. They fire based on file paths — you don't need to invoke them manually.

### Skills index

| Skill | What it enforces |
|-------|-----------------|
| `daft-patterns` | Daft built-in functions to reach for first, UDF decision tree, lazy DAG rules |
| `archetype-components` | Component definitions, Arrow serialization, field conventions |
| `archetype-processors` | AsyncProcessor patterns, priority ordering, resource access |

## Layers

| Layer | Access |
|-------|--------|
| `src/archetype/core/` | **Read-only.** Do not modify without explicit permission. |
| `src/archetype/app/` | Extend carefully. |
| Everything else | Write freely. |

## Commands

```bash
make ci                         # Gate: lint + lock-check + tests with coverage
make test                       # Fast tests, no coverage
make check                      # Auto-format + lint
```
