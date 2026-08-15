# Missions

| | |
|---|---|
| Distribution | `archetype-missions` |
| Import package | `archetype.missions` |
| Typed adapters | `Missions`, `MissionWorld` |
| Dependency | `archetype-ecs` |

Missions owns coding-agent mission state and behavior: task graphs, transition
processors, author and critic Activities, sandbox resources, sessions,
transcripts, trajectory evidence, and result projections.

It does not add mission methods to `ArchetypeRuntime` or `RuntimeWorld`, and it
does not depend on Physical AI or Research.

## Start

- [Agent Missions](../guide/agent-missions.md)
- [Mission Activity recovery](recovery.md)
- [Coding-agent mission example](../guide/examples.md#11a-coding-agent-mission)
- [Mission Factory assets](../guide/mission-factory-assets.md)

## Evidence

- [Trajectories](../guide/trajectories.md)
- [Transcript ingestion](transcripts.md)
- [Python API: Missions](../reference/python/missions.md)
- [Python API: transcripts](../reference/python/transcripts.md)
- [Missions REST API](../reference/rest-api-missions.md)

## Composition

A software-research application may use Missions to produce bounded coding
evidence and Research to optimize candidates. The application composes those
public adapters; neither library imports the other.

For installation and trusted extension behavior, see
[World libraries](../guide/world-libraries.md).
