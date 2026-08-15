# Research

| | |
|---|---|
| Distribution | `archetype-research` |
| Import package | `archetype.research` |
| Typed adapter | `Research` |
| Dependency | `archetype-ecs` |

Research is the minimal AutoResearch world library. It owns candidates,
evaluators, experiment identity, ledger state, views, admission, and the
directly awaited optimization workflow.

Coding-agent sessions, transcripts, and trajectory schemas remain Missions
behavior. Research may retain bounded external evidence without importing or
duplicating another library.

## Start

- [AutoResearch](../guide/autoresearch.md)
- [AutoResearch example](../guide/examples.md#10-autoresearch)
- [Research Python API](../reference/python/autoresearch.md)

## Framework evaluation

Generic graders, outcomes, contracts, and durable evaluation receipts belong
to `archetype-ecs`, not Research. Research consumes those framework contracts
when an optimization workflow needs them. See the
[Framework evaluation Python API](../reference/python/evaluation.md) for that
separate surface.

For installation and trusted extension behavior, see
[World libraries](../guide/world-libraries.md).
