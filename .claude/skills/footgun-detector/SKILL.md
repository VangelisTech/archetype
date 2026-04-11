---
name: footgun-detector
description: "Detect subtle bugs that pass CI but break at runtime. Invoke with /footgun to scan the current PR diff or staged changes for archetype-specific footguns."
user_invocable: true
---

# Footgun Detector

You are reviewing code changes in the **archetype** repository for **footguns** — code that compiles, passes CI, and breaks at runtime or produces silently wrong results.

**This is not a linter.** Do not report style issues, missing tests, or documentation. Only report things that will bite the user.

## Step 1: Get the diff

Determine what to scan, in priority order:

1. If the user provided a PR number or URL, fetch that diff: `gh pr diff <number>`
2. If on a feature branch with commits ahead of `main`, use: `git diff main...HEAD`
3. If there are staged changes: `git diff --cached`
4. If there are unstaged changes: `git diff`
5. If the working tree is clean and on `main`, tell the user there's nothing to scan.

## Step 2: Load the knowledge base

Before analyzing, read these files for context — they contain the rules the diff must obey:

- `LEARNINGS.md` — Daft 0.7.x patterns, UDF rules, serialization, data-centric principle
- `AGENTS.md` — Architecture, service layer, RBAC, conventions
- `.claude/skills/archetype-processors/SKILL.md` — Processor rules
- `.claude/skills/archetype-components/SKILL.md` — Component rules
- `.claude/skills/daft-patterns/SKILL.md` — Daft DataFrame rules

You do NOT need to read these if you already have them in context from this session.

## Step 3: Scan for footguns

Check every changed file in the diff against the categories below. For each file, read enough surrounding context (the full file or relevant functions) to understand intent — don't just pattern-match on the diff lines.

### Footgun categories

Footguns come from three perspectives. All three must be checked.

**The Actor** (the code author) — structural mistakes in the code itself. Wrong patterns, wrong APIs, wrong types. These are the most common and the easiest to catch by pattern-matching against the knowledge base.

**The Observed** (the data) — assumptions the code makes about its inputs that don't hold in reality. The code is structurally correct but semantically wrong because it models the domain incorrectly. These require understanding what the data actually looks like, not just how the code processes it. When a processor branches on a field value, verify the branch covers all real values — not just the ones the author imagined.

**The Observer** (the reviewer) — blind spots in this very review process. If a finding relies on an assumption about runtime behavior, verify it. If a pattern-match flags something as safe, check whether the "safe" pattern actually holds for this specific case. The observer must audit its own confidence.

---

#### Incomplete domain modeling
**(Observed)** Code that branches on a field value without covering all values that field can take in production. Examples: checking for `"MERGED"` but not `"CLOSED"`, handling `"success"` and `"failure"` but not `"pending"`, parsing `fix:` and `feat:` prefixes but not the actual distribution of titles. The fix is always to enumerate real values (from the data source, API docs, or a sample query) and verify exhaustive coverage.

#### Unchecked external schema
**(Observed)** Code that parses external data (API responses, CLI output, file formats) based on assumed field names, types, or shapes without documenting or validating the assumption. If `gh pr list --json` returns `createdAt` but the code reads `created_at`, it silently gets empty strings. If an API changes its response shape, the code produces wrong results with no error.

#### Confidence without verification
**(Observer)** The reviewer (you) reporting a finding — or marking something safe — based on pattern recognition alone. Before reporting, verify: does the function signature actually match? Does the field actually exist? Does the API actually return that value? Before marking safe, verify: does the "correct" pattern actually apply in this context, or is this an edge case the pattern doesn't cover? When in doubt, read the source or run a query — don't trust the pattern.

---

#### Row dropping
Code that reduces the number of rows in a DataFrame representing world state. `df.limit()`, `df.filter()`, `df.where()` on entity DataFrames silently drops entities. If the intent is sampling, the pattern must preserve all rows (e.g., add a boolean `sampled` column, or cap via monotonic ID comparison).

#### Unguarded LLM calls
`daft.functions.prompt()` or LLM client calls applied to ALL rows when only a subset are relevant. If only sampled/active/filtered entities need LLM calls, the DataFrame must be split or filtered BEFORE the prompt call, not after.

#### API signature mismatch
Calling a function with wrong keyword arguments — especially `fork_world()`, `create_world()`, `WorldConfig()`, `Command()`, `ServiceContainer` methods. Check that kwargs match the actual function signature in the codebase.

#### Missing type key
`Component.from_dict()` and SPAWN command payloads require a `"type"` key for subclass hydration. Dicts without `"type"` silently fail to reconstruct the correct Component subclass.

#### Private API coupling
Code outside `core/` accessing private attributes: `_live`, `_spawn_cache`, `_despawn_cache`, `_entity2sig`, `_next_entity_id`. Use the public API (`get_components()`, `spawn()`, `despawn()`).

#### Monotonic state / predicate accumulation
Boolean or filter columns that AND onto existing state instead of recomputing from config each tick. This causes state to monotonically narrow (e.g., once `sampled=False`, always False). The fix: recompute predicates from source data each tick.

#### Shared mutable state across forks
Sharing a `ServiceContainer`, `CommandBroker`, `Resources`, or other stateful object between a parent world and its fork. Forks must get independent copies. Double-shutdown, cross-world mutation, and state leakage are the consequences.

#### Store vs live reads
Querying the persistent store (LanceDB) when `_live` / `get_components()` has the correct in-memory data. After `fork_world`, the fork's state is in memory — the store may have stale or empty data for the fork's world_id.

#### Governance bypass
Mutating world state (spawn, despawn, modify entities) without going through `CommandService.submit()` / `CommandBroker`. Direct mutations skip RBAC checks, audit history, and serialized writes.

#### Dead code contracts
Config fields, constructor parameters, or Component fields that are defined but never read or wired to behavior. These create false expectations (e.g., a `temperature` field that's ignored when building the LLM prompt).

#### Substring matching on structured data
Using `str.contains()` on JSON strings or structured text when exact match is needed. `"red" in '["fred", "red"]'` matches both. Parse the JSON first, then match exactly.

#### Wrong return values
Returning indices, booleans, or placeholder values instead of actual identifiers (UUIDs, entity IDs). Callers expecting real IDs get garbage.

#### DAG-breaking collects
`.collect()`, `.to_pylist()`, or `.limit()` mid-pipeline that materializes a separate Daft plan. Downstream columns referencing prior lazy computations may be empty or stale.

#### Non-serializable closures
`@daft.func` capturing non-picklable objects (API clients, DB connections, mocks) in closure scope. These fail at Daft serialization time. Use `@daft.cls()` with `__init__` instead.

#### with_column vs with_columns
Using `df.with_columns(expr1, expr2)` with multiple positional args — raises `TypeError` in Daft 0.7.x. Use `df.with_columns({...})` dict form or chain `df.with_column()`.

#### Deprecated Daft APIs
Using `@daft.udf` (removed in 0.8.0), `.struct.get()` (use `[]` indexing), or `Expression.if_else()` (use `@daft.func`).

#### Arrow serialization violations
Component fields containing `dict`, `list[dict]`, custom objects, or other non-Arrow types without JSON encoding. Must use `_json: str` suffix pattern.

#### Tick-boundary violations
Reading messages or state written in the same tick they were produced. Outbox at tick N should only be readable from Inbox at tick N+1.

## Step 4: Report findings

For each footgun found, output exactly this format:

```
### <CATEGORY> in `<file>:<line>`

**What it does:** <one sentence describing the code>

**What goes wrong:** <the runtime consequence — data loss, silent wrong results, crash, cost waste>

**Fix:**
<concrete code suggestion or description of the fix>
```

If no footguns are found, say so explicitly:

```
No footguns detected in this diff.
```

## Rules for this skill

- **No style nits.** No "consider adding tests." No "this could be cleaner."
- **No false positives.** If you're not confident it's a real footgun, don't report it. Uncertainty destroys trust.
- **Diff-only.** Only report issues in changed lines (with enough context to confirm). Don't audit the entire codebase.
- **Concrete fixes.** Every finding must include a specific fix, not "consider refactoring."
- **Fast.** This should take seconds, not minutes. Don't read files you don't need.
