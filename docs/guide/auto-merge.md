# Auto-Merge Policy

Auto-merge is gated by **tiered status checks**. The tier is determined by the
paths a PR touches. A PR's tier is the *highest* tier any changed path falls
into — touching `src/archetype/core/` once makes the whole PR Tier 3,
regardless of what else it changes.

The principle: **the blast radius of a change sets the bar for merging it.** A
docs typo and a scheduler rewrite should not clear the same gate.

## Tiers

### Tier 0 — Docs & Meta

**Paths:** `docs/**`, `**/*.md`, `LICENSE`, `assets/**`, `.github/ISSUE_TEMPLATE/**`

**Required checks:**
- `format` (`ruff format --check` on `src/` and `tests/`)
- `spelling` — typos-cli spell check ([`_typos.toml`](../../_typos.toml))
- `markdown-lint` — markdownlint-cli2 ([`.markdownlint.yaml`](../../.markdownlint.yaml))
- `link-check` — lychee link validation ([`lychee.toml`](../../lychee.toml))
- `mintlify-build` — Mintlify build to catch broken frontmatter/MDX

These checks run via the [`docs.yml`](../../.github/workflows/docs.yml) workflow
on PRs touching `docs/**` or `**/*.md`.

**Rationale:** Zero runtime impact. Don't gate on `ci` — there's nothing to
test. Auto-merge should be near-instant.

---

### Tier 1 — Examples & Benches

**Paths:** `examples/**`, `bench/**`

**Required checks:**
- `format`
- `ci` (the full gate — these import from `src/` and can break if APIs drift)

**Rationale:** Examples are load-bearing documentation. They must run against
the current API surface, but a broken example doesn't break user code. The
existing `ci` gate is sufficient.

---

### Tier 2 — App / API / CLI (service layer)

**Paths:** `src/archetype/app/**`, `src/archetype/api/**`, `src/archetype/cli/**`,
`tests/app/**`, `tests/api/**`, `tests/cli/**`, `tests/integration/**`

**Required checks:**
- `format`
- `ci`
- **Coverage delta ≥ 0** (no net coverage regression on changed files)
- At least one **integration test** added/modified if behavior changed
  (enforced by review, not CI — but auto-merge should require a `has-tests`
  label or a passing "changed files include tests" check)

**Rationale:** Service-layer changes ship to users as library API. Regressions
here surface as broken agents in the field. The 70% branch-coverage floor
(already in `make ci`) is the backstop; the delta check prevents slow rot.

---

### Tier 3 — Core Engine

**Paths:** `src/archetype/core/**`, `tests/core/**`, `tests/aio/**`,
`tests/sync/**`, `tests/storage/**`

**Required checks:**
- `format`
- `ci`
- `typecheck` **(planned:** promote to required by removing
  `continue-on-error` for core/; currently non-blocking)
- **Coverage delta ≥ 0** on changed files
- **Benchmark regression check** (if/when added; `bench/` runs and >10%
  regression blocks)
- **Human approval required** — auto-merge disabled or requires CODEOWNERS
  approval from a core maintainer

**Rationale:** Per `CLAUDE.md`: "Do not modify [core/] without explicit
approval." The ECS engine is the invariant-holder of the whole framework.
Pickling semantics, lazy-DAG preservation, Resources DI — all live here. A
silent change in core can break every downstream processor.

**This tier should rarely auto-merge.** Auto-merge is appropriate for
dependency bumps and trivial refactors within core; architectural changes
require synchronous human review.

---

### Tier D — Dependencies (special case)

**Paths:** `pyproject.toml`, `uv.lock` only (Dependabot/Renovate PRs)

**Required checks:**
- `ci` (lock-check + full test suite)
- **Security audit** (requires a PR-triggered variant of
  `daily-security-audit.yml`; the current workflow is schedule-only and cannot
  report PR status — see #65 for adding this)
- **Semver discrimination:**
  - Patch bumps (`x.y.Z`): auto-merge if `ci` green
  - Minor bumps (`x.Y.z`): auto-merge if `ci` green *and* no core/ imports
    from the bumped package
  - Major bumps (`X.y.z`): **never auto-merge** — human review required

**Rationale:** Dependency updates are the most common auto-merge case and the
most common source of supply-chain risk. Patch-level churn should not burn
maintainer attention; major-version jumps should never land unseen.

---

## Determining a PR's Tier (proposed)

> **Status:** This section describes the target implementation. The labeler
> workflow and `auto-merge-gate` check do not exist yet. Today, `ci` and
> `format` are the only required status checks.

The plan is a labeler workflow that reads changed paths and assigns tier labels:

```yaml
# .github/labeler.yml (proposed)
tier-0-docs:
  - docs/**
  - "**/*.md"
tier-1-examples:
  - examples/**
  - bench/**
tier-2-app:
  - src/archetype/app/**
  - src/archetype/api/**
  - src/archetype/cli/**
tier-3-core:
  - src/archetype/core/**
tier-d-deps:
  - pyproject.toml
  - uv.lock
```

A companion workflow would pick the **highest** tier label present and configure
the matching required-checks set via a status check named `auto-merge-gate`.
Branch protection on `main` would require `auto-merge-gate` to pass; the gate
resolves to the correct check matrix per tier.

**Tier precedence** (highest to lowest): Tier 3 > Tier 2 > Tier 1 > Tier 0.
Tier D is evaluated independently — if a PR touches *only* `pyproject.toml` /
`uv.lock`, Tier D applies. If it also touches source code, the numeric tier
takes precedence and Tier D's semver rules are ignored (the full code-change
gates apply instead).

## Disabling Auto-Merge

- **Tier 3 (core):** auto-merge disabled by default; re-enabled per-PR by a
  maintainer with the `auto-merge-approved` label.
- **Any PR with the `manual-review` label:** auto-merge disabled regardless
  of tier.
- **Any PR failing a non-required check twice:** auto-merge disabled, flagged
  for review (likely flaky test or genuine regression).

## What Auto-Merge Does NOT Replace

- Architecture decisions (see `LEARNINGS.md`)
- Breaking-change discussion
- Release-note curation
- Judgement about whether a feature belongs in the framework at all

Auto-merge is for **keeping the main branch moving on work that has already
been decided**. It is not a substitute for deciding.
