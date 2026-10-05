# Engineering history and agent debriefs

**Document type:** Historical index, non-normative.

Archetype keeps selected engineering debriefs, readiness inventories, and
architecture reports when the path to a capability is itself useful evidence.
These records explain what was known, what was proven, and what remained open
at a particular revision. They are neither release notes nor an alternative
specification.

When a historical record conflicts with current behavior, use this authority
order:

1. the focused normative specification;
2. executable contracts and evals;
3. the umbrella [Specification](../guide/specification.md);
4. current guides and examples;
5. the dated historical record.

Historical records are intentionally not rewritten to make old decisions look
inevitable. Corrections should be appended with a date and a link to the newer
authority.

## Agent debriefs

| Date | Record | Implementation anchor | Why it is retained |
|---|---|---|---|
| 2026-07-18 | [From sandbox prototype to resumable coding-agent missions](agent-debriefs/2026-07-18-coding-agent-missions.md) | Draft PR [#487](https://github.com/VangelisTech/archetype/pull/487), commit `db9d52a7` | Establishes the mission/sandbox/artifact boundaries, retained provider capability, durability evidence, benchmark result, and honest limitations after the application-family refactor. |

## Readiness inventories

| Date | Inventory | Scope |
|---|---|---|
| 2026-07-18 | [Coding-agent mission production-readiness inventory](readiness/2026-07-18-coding-agent-missions.md) | One deduplicated, priority-ordered list combining the original production checklist, architectural seams found during formalization, and observability/evaluation follow-ups. |

## Architecture and program reports

These older reports remain useful for understanding why later contracts were
introduced. Their recommendations may have been superseded.

- [Security program review — 2026-03-28](../reports/2026-03-28-security-program-review.md)
- [Service-layer redesign — 2026-04-25](../reports/2026-04-25-service-layer-redesign.md)

## What belongs here

A record belongs in this section when it binds a consequential implementation
or operational result to a date, revision, and evidence set. Routine PR
summaries, speculative designs without an implementation anchor, and current
normative contracts belong elsewhere.

Every new agent debrief should state:

- its date, status, scope, revision, and related issues/PRs;
- what actually ran and what was only proposed;
- systems of record and failure semantics;
- quantitative evidence with the workload and caveats;
- known gaps and the durable issues that own them; and
- the current specifications that supersede it when behavior evolves.
