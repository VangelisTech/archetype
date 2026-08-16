# Archetype 0.6

Archetype 0.6 is an intentional pre-1.0 boundary reset. It separates the
generic ECS framework from three independently installable world libraries and
removes APIs and persisted shapes that put domain behavior in the wrong owner.

This is a clean break. There are no world-library import shims, dynamic runtime
or world method aliases, deprecated world-library spellings, or automatic
Research-ledger migration. Update an application to the canonical 0.6 surface
before upgrading its environment.

## Distribution layout

| Install | Owns |
|---|---|
| `archetype-ecs` | Generic ECS execution, storage, commands, Activities, artifacts, evaluation, runtime, API, and CLI |
| `archetype-missions` | Coding-agent missions, sandboxes, sessions, transcripts, and trajectory evidence |
| `archetype-physical-ai` | Physical state, local policies, and hosted physical episodes |
| `archetype-research` | Generic AutoResearch values, ledger state, views, and workflow |

Install only the libraries an application uses, or install the complete
first-party set with `archetype-ecs[all]`. Each world library depends on the
compatible framework release; world libraries do not depend on one another.

## Required source changes

Use family-qualified imports and explicit typed adapters:

| Removed pre-0.6 surface | 0.6 surface |
|---|---|
| Domain values imported from `archetype` | Import from `archetype.missions`, `archetype.physical_ai`, or `archetype.research` |
| `runtime.missions(...)` or `RuntimeMissions(...)` | `Missions(runtime, ...)` |
| `world.query_trajectory(...)` or `world.grade_trajectory(...)` | `MissionWorld(world).query_trajectory(...)` or `.grade_trajectory(...)` |
| `world.ingest_claude_transcript(...)` or `world.transcript_rows()` | `MissionWorld(world).ingest_claude_transcript(...)` or `.transcript_rows()` |
| `world.run_hosted_episode(...)` | `PhysicalAI(world).run_hosted_episode(...)` |
| `world.autoresearch(...)` | `Research(world).autoresearch(...)` |
| `CandidateContext` | `ResearchCandidateContext` |
| `archetype.episodes` transcript and trajectory imports | `archetype.missions.trajectories` |
| Research runner-state loading | `archetype.missions.sessions.load_runner_sessions(...)` |

World-library adapters are async in 0.6. A synchronous framework world does
not grow domain methods when a library is installed.

## Research ledger reset

Pre-0.6 Research ledgers are unsupported and are not migrated in place. Start
a new 0.6 experiment ledger. If historical evidence matters, export it with the
older release before upgrading and retain it as application-owned evidence.

The 0.6 ledger is deliberately generic:

- `RunStatus` is `RUNNING`, `SUCCEEDED`, or `FAILED`; the runner-shaped
  `BOOTING`, `STOPPING`, `STOPPED`, and `CRASHED` states are gone.
- `Experiment` no longer persists repository URL or branch fields.
- `Run` no longer persists VM, harness, repository, branch, task, agent,
  workspace, or commit fields. It records the candidate world instead.
- `BranchHead` records the incumbent candidate world rather than a Git commit.
- SQLite coding-runner ingestion belongs to Missions and returns
  `RunnerSession` values.

Research can still optimize code. An application composes `Missions` and
`Research` through their public adapters and stores bounded coding evidence in
the opaque Research result envelope.

## Episode ownership

There is no `archetype-episodes` distribution and no `archetype.episodes`
package:

- execution `EpisodeConfig` and `EpisodeResult` remain in `archetype.world`;
- coding-session transcripts and trajectories live in
  `archetype.missions.trajectories`;
- hosted physical episodes live in `archetype.physical_ai`; and
- dataset episode identity remains in `archetype.evaluation`.

Shared episode identifiers permit joins without inventing a universal episode
facade.

## Release unit

The 0.6 release is one coordinated set: four wheels and four source
distributions at version `0.6.0`. Release evidence installs the base framework,
each individual library with the framework, and the complete stack from those
exact artifacts. A partial four-project publication is not a completed release.
The first release requires pending Trusted Publishers for the three new project
names on both PyPI and TestPyPI; see
[Repository harness](repository-harness.md) for the exact OIDC identities.

For current architecture and installation contracts, continue with
[World libraries](world-libraries.md), [Runtime](runtime.md), and
[API stability](api-stability.md).
