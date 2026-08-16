# Physical AI

| | |
|---|---|
| Distribution | `archetype-physical-ai` |
| Import package | `archetype.physical_ai` |
| Typed adapter | `PhysicalAI` |
| Dependency | `archetype-ecs` |

Physical AI owns physical state, local policy and environment adapters, hosted
episode choreography, provider configuration, recovery, and completeness
evidence.

It does not place hosted-episode behavior on the generic framework world and
does not depend on Missions or Research.

## Start

- [Run a hosted episode](../guide/physical-ai.md#run-a-hosted-episode)
- [Committed-state sequence](../guide/physical-ai.md#committed-state-sequence)
- [Canonical episode contract](../guide/physical-ai.md#canonical-episode-contract)

## Reference

- [Physical AI API](../reference/python/physical-ai.md)
- [Host configuration](../reference/python/physical-ai-host.md)
- [Optimization API](../reference/python/physical-ai-optimization.md)

For installation and trusted extension behavior, see
[World libraries](../guide/world-libraries.md).
