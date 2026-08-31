# Temporal Responsibility Audit

Date: 2026-08-30  
Scope: Archetype ECS and first-party world libraries  
Status: migration boundary and sequencing recommendation

## Executive conclusion

Temporal should become Archetype's durable orchestration layer. It should not
become the authority for ECS state, storage visibility, or provider-side
exactly-once effects.

The generic Activity implementation should be reduced substantially:

- Temporal replaces queues, worker claims, leases, attempt fences, expiry
  scans, retry timers, and process-local recovery loops.
- Archetype initially retains a slim, strongly consistent Activity
  admission/settlement index. It records the exact source tick receipt,
  bounded result reference, exact later observation receipt, and the
  `has_unsettled(world_id)` lifecycle gate.
- Family Activities retain provider operation IDs, provider reconciliation,
  result validation, and domain observation semantics.

This avoids dual orchestration authority while preserving the evidence that
Temporal history cannot supply.

## Target Activity seam

1. A required projector observes an exact committed ECS receipt and starts or
   signals a deterministic Workflow.
2. Temporal owns waiting, retries, cancellation, timers, worker replacement,
   and long-running progress.
3. Family-owned Activities perform provider calls and reconciliation.
4. Large results remain outside Workflow history as bounded references.
5. A later ECS tick commits the result observation and signals settlement.
6. Fork and destroy remain forbidden until exact settlement is known.

Temporal visibility or search attributes must not initially become the
authoritative `has_unsettled(world_id)` query. A per-world coordinator Workflow
may replace the slim index later only after its admission/query consistency is
proven against the world lifecycle lock.

## Responsibility map

| Disposition | Responsibility | Main seams | Expected impact |
|---|---|---|---|
| Replace | Generic Activity execution mechanics | `archetype/activities/service.py`; `archetype/storage/activity_catalog/`; family claim/drain loops | Roughly 2.7k ECS LOC in scope; estimated 1.8–2.8k net deletion after cutover |
| Replace | MissionRun lifecycle and process-local supervision | `missions/run_catalog.py`, `run_lifecycle.py`, `run_supervisor.py`, lifecycle portions of `run_contracts.py`, `_extension.py` | Roughly 1.2–1.5k production LOC replaced |
| Replace after portable-contract work | Evaluation leases, polling, heartbeat, and recovery | `evaluation/handlers.py:66-335`; evaluation catalog methods in `storage/catalog/sqlite.py` and remote equivalents | Estimated 300–420 net LOC |
| Replace after portable-contract work | Rollout fork fan-out, cancellation, compensation, and ordered failure handling | `world/simulation.py:411-727` | Estimated 280–360 LOC |
| Replace after portable-contract work | AutoResearch single-flight admission and iteration loop | `research/handlers.py:51-95,198-424` | Estimated 180–250 LOC rewritten; ledger authority remains |
| Integrate; retain Archetype authority | Activity source receipt, bounded result ref, later-tick settlement, lifecycle gate | `activities/contracts.py`, `world/projectors.py`, `world/lifecycle.py` | Retain the slim settlement index initially |
| Integrate; retain Archetype authority | Required post-commit projection | `world/simulation.py:90-249`, `world/registry.py`, `world/projectors.py` | Temporal may drive retries; ECS retains the no-next-tick invariant |
| Integrate; retain Archetype authority | Commands and command outbox | `commands/scheduler.py`, `commands/audit.py`, command/outbox catalog tables and `publish_manifest` | Temporal may later replace lease/retry supervision; logical tick ordering and manifest-atomic settlement stay in Archetype |
| Integrate; retain Archetype authority | World create, fork, resume, cleanup orchestration | `world/lifecycle.py`, `world/registry.py`, `world/resume.py` | Temporal orchestrates; catalog state, writer epochs, lineage, locks, and reconstruction remain Archetype |
| Integrate; retain Archetype authority | Storage migration sequencing | `migration/handlers.py:380-559,677-886` | An estimated 350–550 sequencing LOC may move; plan and activation evidence remain |
| Integrate; retain Archetype authority | Modal execution | `missions/modal_author.py`, `modal_critic.py`, `sandboxes/modal_barrier.py`, `sandboxes/modal.py:2651-3101`, `physical_ai/hosted_modal.py:247-605` | Keep provider markers, first-result registers, reconciliation, and cleanup identity |
| Uncertain; spike first | Full command scheduler replacement | `commands/scheduler.py:203-702` | Potential 350–550 LOC, but `scheduled_tick` and manifest settlement are ECS semantics |
| Uncertain; spike first | Artifact ingestion orchestration | `artifacts/handlers.py:134-198`, `artifacts/pipeline.py` | Make occurrence identities deterministic before enabling retries |
| Retain | Tick compute, flush, manifest publication, writer fencing, differential reconstruction | `core/aio/async_world.py`, `storage/commit.py`, `world/resume.py` | No deletion |
| Retain | Runtime process ownership and phased shutdown | `runtime_resources.py`, `runtime/world.py` | Temporal Workers still require this host ownership |
| Retain | Iceberg CAS, ambiguous-commit reconciliation, transport retries, cache timers | `storage/service.py`, `storage/hardened_sqlite.py`, `core/aio/async_cached_store.py` | These remain inside bounded Activities |
| Retain | Smol teaching ECS | `smol/world.py` | No Temporal dependency |

## Current Mission Temporal slice

The existing slice is transitional rather than a production backend:

- `missions/temporal/workflow.py:82-96` wraps execution in one 24-hour
  Activity with retries disabled.
- `missions/temporal/activities.py:113-126` heartbeats only a phase; it does
  not persist a Modal operation handle for reattachment.
- `missions/temporal/workflow.py:120-129` records cancellation intent but does
  not stop or compensate running provider work.
- `missions/temporal/workflow.py:155-164` retains an unbounded event tuple.
- `missions/_extension.py:668-795` still constructs the SQLite lifecycle and
  process-local supervisor.

Therefore the next production slice is not merely routing the REST submit
handler to the current Workflow. Mission execution must first be split into
durable `submit/start/poll/collect/cleanup` phases with stable provider IDs.

## Modal reattachment gate

Temporal retries are at-least-once. They do not make Git pushes, Modal spawns,
hardware actions, or artifact publication exactly once.

Before retrying a long-running lifecycle Activity, prove:

1. A Modal call can be reconstructed from a durable provider ID across
   processes.
2. That provider ID is persisted against the stable Archetype operation
   identity before later polling.
3. Crash behavior is correct before spawn, after spawn but before ID
   persistence, after ID persistence, after provider result publication, and
   before Archetype result settlement.
4. A lost ID remains a fail-closed `Unknown` outcome if the provider cannot
   prove whether the spawn happened.
5. Existing permanent start markers, first-result registers, and cleanup
   identities remain in force.

`physical_ai/hosted_modal.py:639-655` currently calls
`function.spawn.aio(...)` without durably retaining a reattachable call handle.
That is the concrete blocking seam.

## Provider-neutral package boundary

`archetype.activities.temporal` is acceptable while it serves only
between-tick Activities. It should not become the universal dependency for
commands, evaluation, migration, rollout, and research.

Before those families adopt Temporal, move generic Workflow identity, client,
Worker construction, payload policy, and host lifecycle into a
provider-neutral `archetype.orchestration.temporal` package. Keep each Workflow
family-owned and let `archetype.wiring` own Temporal client and Worker lifetime.

## Recommended migration order

1. Establish the provider-neutral orchestration package and JSON/ref-only
   contracts.
2. Complete the hard-kill world proof and define idempotent world operations.
3. Replace generic Activity execution mechanics while retaining the slim
   admission/settlement oracle.
4. Split Mission execution into `submit/start/poll/collect/cleanup` and replace
   the MissionRun SQLite supervisor.
5. Migrate Mission author/critic and Physical-AI Activity workers.
6. Replace evaluation leases.
7. Move rollout and AutoResearch orchestration.
8. Wrap storage migration sequencing.
9. Spike a per-world command Workflow; remove scheduler leases only if
   manifest-atomic settlement remains intact.
10. Consider artifact ingestion last.

Deletion follows cutover. `migration/handlers.py:215-223` currently rejects any
nonempty Activity history, so legacy work requires a freeze/drain/archive plan
or an importer for unresolved provider-bound attempts. Dual admission must
never be active.

## Hard-kill proof requirements

The acceptance test must:

- Commit and settle a trivial ECS Activity before forking, and prove a fork is
  refused while work remains unsettled.
- Fork from an exact committed head.
- Advance the fork to an absolute target tick, never “N more steps.”
- Put the runtime/world owner in a child process, wait for a committed fork
  manifest, then hard-kill that process.
- Start a replacement Worker/process against the same Temporal server and
  storage, then call `ResumeWorld`.
- Assert the same `world_id` and `run_id`, preserved lineage, a higher writer
  epoch, unchanged parent, one visibility token and one visible row set per
  committed tick, and no tick beyond the target.
- Assert the fork remains `active` and resumable and that `OnDestroy` did not
  run.
- Separately call `DestroyWorld`, assert terminal `destroyed` status and one
  `OnDestroy`, then prove `ResumeWorld` refuses it.

Two production APIs are needed before lifecycle retries are safe:

- `fork_world()` currently mints the destination ID inside
  `world/lifecycle.py:375`. A Workflow must choose the destination ID so a lost
  Activity response cannot create a second fork.
- `Run(num_steps=N)` is relative. Add an idempotent
  `advance_world_to_tick(absolute_target)` Activity so a lost response cannot
  replay committed ticks.
