# CLAUDE.md — Contributing Guide for Future Sessions

You are working on **Archetype**, an AI-native ECS simulation engine. This file is your orientation. Read it before doing anything.

## Who You're Working With

Everett Kleven, founder of Vangelis Technologies. Senior engineer. He is terse, action-oriented, and expects you to ship — not explain. Use `uv` for deps, never `pip`. Verify things actually run end-to-end, not just that tests pass. Don't explore `~/.claude` or home directories without permission.

## What Archetype Is

A lazily-evaluated Virtual Data Architecture for AI agents. World state is columnar DataFrames (Daft). Every mutation flows through an RBAC-gated CommandBroker. Storage is append-only Arrow tables in LanceDB. Compute is decoupled from data — processors are pure DataFrame transforms that parallelize on Ray or Daft Cloud.

The codebase auto-evolves: processors can submit commands, worlds can fork, and the simulation can improve itself.

## Architecture

```
src/archetype/
├── core/           # ECS engine — DO NOT MODIFY without explicit permission
├── app/            # Service layer (v0.1)
│   ├── auth/       #   RBAC (ActorCtx, roles, quotas)
│   ├── broker.py   #   CommandBroker (priority queue + RBAC)
│   ├── container.py#   ServiceContainer (wires everything)
│   ├── command_service.py, world_service.py, simulation_service.py, query_service.py, storage_service.py
├── api/            # FastAPI REST layer
└── cli/            # Typer CLI
```

## How to Work

### Issue-driven development

Work is tracked in GitHub Issues. The `@claude` trigger on issues runs this agent via `.github/workflows/claude.yml`. The workflow:

1. Issue created with `@claude` or someone comments `@claude` on an existing issue
2. GitHub Actions checks out the repo, installs Python 3.12 + uv, runs Claude Code
3. Agent reads this file, understands context, implements the work
4. Agent opens a PR or comments with results
5. Everett reviews and merges

### Before writing code

1. Read relevant source files first — understand before modifying
2. Check existing tests: `uv run pytest tests/ -v`
3. Check lint: `uv run ruff check src/ tests/ examples/`

### After writing code

1. Run tests: `uv run pytest tests/ -v` — all must pass
2. Run lint: `uv run ruff check src/ tests/ examples/` — must be clean
3. Run the actual thing end-to-end — not just tests
4. If touching docs: verify code examples are accurate against source

### Key gotchas

- `daft.functions.prompt`: use `max_output_tokens` not `max_tokens` (Responses API)
- `with_columns({...})` not `with_column(...)` — Daft uses plural dict form
- Component payloads in commands need a `type` key for `Component.from_dict()` deserialization
- `core/` is sacred — read-only unless Everett explicitly asks

## @autoresearch Pattern

When an issue is tagged `@autoresearch` or mentions research, follow this loop:

1. **Read** — gather context from the codebase, issues, and external sources
2. **Propose** — comment a detailed plan on the issue before implementing
3. **Implement** — build it on a branch, open a PR
4. **Verify** — run it end-to-end, not just tests
5. **Reflect** — comment what worked, what didn't, what to try next

Each cycle should leave the codebase better than it found it. Every PR is a checkpoint. Every issue comment is a breadcrumb for the next session.

## Schemas I Want Built

These are the data structures that should scale as the project evolves. Build them as ECS Components when you need them:

### Memory Extraction (implemented: `examples/mind/`)
```
Segment     → role, content, session_id, timestamp, turn_index
Extraction  → memory_text, memory_type, confidence, is_memorable
Voice       → classification (actor/observer/observed), reasoning
Perspective → lens (objective/subjective/abjective/superjective), reasoning
```

### Decision Analysis (issue #40, not yet built)
```
Decision    → text, decision_type (commit/defer/reverse/explore), confidence
Agency      → actor (user/assistant), influence_type (propose/accept/reject/redirect)
Horizon     → references_prior, prior_turn_index, consistency_score
```

### Codebase Evolution (future)
```
Change      → file_path, change_type (create/modify/delete), lines_changed
Intent      → stated_goal, actual_outcome, alignment_score
Impact      → tests_affected, imports_affected, downstream_breakage
```

### Agent Performance (future)
```
Task        → description, outcome (success/partial/failure), turns_used
Correction  → what_was_wrong, who_caught_it (user/test/lint), severity
Efficiency  → tokens_used, time_elapsed, unnecessary_steps
```

Let the data scale. Every run appends to LanceDB. Every tick is queryable. Time-travel across the evolution of the project itself.

## Compute Infrastructure

- **Local**: `uv run` with `.venv`, OpenAI API via `.env`
- **GitHub Actions**: `anthropics/claude-code-action@v1` with `ANTHROPIC_API_KEY` secret
- **Future**: Modal as a GitHub runner for GPU workloads and long-running simulations
- **Distributed**: Daft on Ray or Daft Cloud for parallel LLM calls at scale

## Current State (v0.1.0)

- Published to PyPI as `archetype-ecs`
- 182 tests passing, ruff clean
- Simulation verified with gpt-5-mini
- Mind extraction pipeline works (`examples/mind/`)
- REST API + CLI functional
- LanceDB persistence verified

## Open Issues

Check `gh issue list --repo VangelisTech/archetype` for current work items. Key ones:
- #39 — Default persistent storage
- #40 — Decision-making analytics
- #41 — Expand mind extraction
- #42 — NLP analytics dashboard
- #44 — Product positioning copy

## Memory System

Session memory lives in `~/.claude/projects/-Users-everettkleven-git-other/memory/`. Read `MEMORY.md` there for the index. Update memories when you learn something new about Everett, the project, or how to work effectively.

## The Goal

Build the system that understands its user. Then use that understanding to build better systems. Recursion is the point.
