# From sandbox prototype to resumable coding-agent missions

**Document type:** Agent engineering debrief, historical and non-normative.

| Field | Value |
|---|---|
| Date | 2026-07-18 |
| Status at capture | Implemented, pushed, and open as draft PR [#487](https://github.com/VangelisTech/archetype/pull/487) |
| Submitted revision | `db9d52a7543cc895f7f6f058f967635f0e4d4810` |
| Architectural base | Application-family refactor [#475](https://github.com/VangelisTech/archetype/pull/475), merged as `a6c6a812` |
| Prototype lineage | Draft PR [#474](https://github.com/VangelisTech/archetype/pull/474) |
| Boundary issue | [#477](https://github.com/VangelisTech/archetype/issues/477) |

This record captures the point at which Archetype's coding-agent work stopped
being a fake sandbox or an example-local state machine and became a tested
application capability. Current contracts in [Agent
Missions](../../guide/agent-missions.md),
[Artifacts](../../guide/artifacts.md), and [Application
Architecture](../../guide/application-architecture.md) supersede this record.

## 1. Outcome and lineage

The initial work proved real Apple Container and Modal Sandbox execution,
Codex, Claude Code, OpenCode, subscription authentication, monitoring,
checkpoint recovery, artifact collection, and paid fanout. That capability was
spread across experimental modules and one large example.

While it was being developed, Archetype's application layer was reorganized
around explicit families, an actor-free `RuntimeApplication`, an authorized
`CommandGateway`, a durable command scheduler, and machine-enforced dependency
direction. Discarding the working sandbox code would have lost expensive
behavioral evidence. Merging it in its old shape would have recreated the flat
service architecture the refactor removed.

The formalization therefore preserved the provider behavior mechanically while
moving ownership into new `missions`, `sandboxes`, and `coding_agents`
families and extending `artifacts` for full attempt bundles. The resulting
vertical slice changed 64 files with 13,860 additions and 19 deletions.

The branch was first rebased onto the published head of #475. While final
verification ran, #475 was squash-merged and its source branch was deleted.
The publication guard stopped the push, verified that the published head and
merged commit had identical Git trees, then transplanted the single agent
mission commit onto the resulting `main`. The verified source tree did not
change during that history rewrite.

## 2. The authority split

The central architectural result was separating four concerns that had been
combined in the prototype:

| Family | Authority | Explicit non-authority |
|---|---|---|
| `missions` | Stable mission/task/attempt identity, retry/exhaustion, validator policy, finalization gates, task advancement | Provider credentials and sandbox mechanics |
| `sandboxes` | Provider registry, create, restore, authenticated resume, monitor, checkpoint, close, cleanup | Attempt acceptance and task advancement |
| `coding_agents` | Repository-oriented composition of one mission attempt and one coding harness session | World persistence and gateway authorization |
| `artifacts` | Durable publication claims, portable objects, content identity, queryable indexes, reconciliation | Provider-native checkpoint lifecycle and mission success |

Existing boundaries remained intact:

```text
trusted script -> ArchetypeRuntime -> RuntimeApplication

untrusted client -> API authentication -> CommandGateway authorization
                                      -> RuntimeApplication

RuntimeApplication -> family workflow ports -> durable storage / core
```

Concrete services and provider clients stayed internal. The container remained
the sole cross-family construction root.

## 3. Attempt semantics

A world tick became a **durable orchestration opportunity**, not a synonym for
one tool call or one state transition. The first processor still creates at
most one model submission per entity per tick, but a future tick may only
recover, reconcile, or finalize an existing attempt.

`MissionService` became the sole task-transition authority. It derives a
deterministic attempt key, validates receipt identity, requires non-vacuous
validator evidence, persists accepted and rejected outcomes, and advances only
when the accepted attempt has the required commit, restorable checkpoint, and
finalization phase.

A validator rejection does not abort the tick. It becomes durable data. The
task gate stays on the current task and a later tick may create another
attempt. This distinction removed the earlier ambiguity between “abort the
tick” and “do not transition the task.”

Exactly-once model execution was deliberately not claimed. Deterministic
attempt identity detects replay, but a crash before a durable pre-execution
claim may repeat a model submission.

## 4. Real sandbox and harness capability

Two concrete isolation providers were retained:

- **Apple Container:** local, non-Docker execution with rehydratable root
  filesystem exports.
- **Modal Sandbox:** remote isolated execution with filesystem snapshots,
  restorable image references, live file access, and authenticated continuation.

Three coding harnesses were retained:

- Codex;
- Claude Code; and
- OpenCode against an OpenAI-compatible endpoint.

Codex and Claude subscription credentials use dedicated broker volumes. The
broker stages only the selected credential for execution, persists a refresh,
and removes the mission copy before validators, manifests, or snapshots.
Credential-free recovery receives no model credential. Authenticated resume
resolves it again from the named secret or broker volume.

The live monitor can attach by sandbox ID without a model credential. It reads
`session.json`, follows `events.jsonl`, preserves byte offsets across temporary
snapshot interruptions, and reports explicit reconnect or disconnect results.
Heartbeats distinguish a quiet but running CLI from a stuck monitor.

## 5. Three complementary recovery records

The implementation made a deliberate distinction among three records:

| Record | System of record | Use |
|---|---|---|
| Archetype component rows | World storage | Mission, task gate, validator, commit, attempt, and finalization facts |
| Provider checkpoint | Modal image or Apple rootfs export | Complete resumable sandbox substrate |
| Portable artifact bundle | Object storage plus Iceberg index | Independently readable manifests, patches, Git bundle, `.context`, session logs, traces, and declared outputs |

The full provider checkpoint is referenced in the artifact index but is not
blindly copied into Parquet. Portable files are hashed, typed, uploaded, and
indexed separately.

Artifact publication gained a durable control-catalog state machine:

```text
PENDING -> UPLOADED -> INDEXED
    |
    +-> EXPIRED  (only after the retry window while still provider-dependent)
```

The publication claim is recorded before external I/O. Deterministic bundle
and artifact identities make upload and index replay idempotent. SQLite, the
remote catalog client, and the Cloudflare Durable Object implementation share
the same protocol and schema version.

This gave teardown a concrete meaning: persist attempt facts, record a
recovery checkpoint, hand artifacts to a durable publication claim, collect
the live event files, then close the provider handle. Telemetry remained
operator evidence rather than the state authority.

## 6. Queryability

The example no longer treats terminal stdout as the result. It queries mission
components and artifact index rows through the runtime/application path.

The artifact index carries world, run, entity, tick, attempt, bundle, content,
MIME, checkpoint, lifecycle, acceptance, and retention coordinates. R2 uses an
S3-compatible Daft `IOConfig`; MIME detection and upload use Daft functions.
Credentials remain process-local configuration and do not enter the request,
catalog, manifest, or index.

At this revision the example still initiated artifact publication after the
episode. The artifact service was available through `RuntimeApplication`, but
`indexed` had not yet become an authoritative in-transition task gate.

## 7. Parallelism and the paid calibration

The coding-agent processor materializes each mission row only at the explicit
external-side-effect boundary and fans independent mission entities out with
concurrent async tasks. The practical topology at this point was one mission,
one sandbox, and one session per agent. Same-sandbox collaborative subagents
were not implemented.

A paid Modal/OpenCode calibration exercised concurrency levels 1, 4, 8, 16,
24, and 32 against a Qwen3.6 35B-A3B FP8 endpoint declared to use one H200 and
one maximum model replica.

Across 85 agents, 75 passed the authoritative validator. Every OpenCode process
exited successfully, but ten repository outcomes were rejected. Concurrency 24
was the observed useful-throughput peak for that short workload. Moving to 32
reduced accepted throughput and raised median and p95 latency. The result was
retained as a calibration, not a production capacity guarantee; levels 16,
24, and 32 still required repeated samples.

See the [dated benchmark
report](../../guide/benchmark-runs/modal-opencode-single-h200-2026-07-18.md)
for configuration, timing, correctness, and caveats.

## 8. Evidence at submission

The exact submitted tree passed:

- static formatting, lint, type, lock, architecture, API-boundary, lazy-DAG,
  contract, and benchmark registry checks;
- 1,303 tests with 19 credentialed/external skips;
- 86.52% branch coverage;
- all 15 regression and 8 specification conformance tasks;
- all 6 capability evals, including mission transition authority;
- package installation smoke;
- normal example smoke, with the external coding-agent example explicitly
  delegated to its path-gated workflow; and
- the full documentation build.

The Modal PR workflow detected that repository secrets were absent and skipped
the paid steps. Its green result meant “credential-aware skip,” not a new live
Modal proof. Paid evidence in this record came from the earlier explicit run.

## 9. Honest limitations at capture

The implementation was a substantial capability, not a finished production
platform:

- provider-neutral primitives were still physically located in the Modal
  module and imported by Apple;
- `run_attempt` still combined execution, validation, commit, evidence, and
  checkpoint phases;
- mission states were validated strings rather than one persisted enum graph;
- artifact indexing was not yet part of the authoritative transition;
- model calls remained at-least-once around a pre-claim crash;
- reconciliation was bounded per world, without a fleet operator or garbage
  collector;
- there was no sandbox supervisor, network proxy, durable external event bus,
  or sandbox-side OTel pipeline;
- secrets had strong process isolation but no unified pre-durability redaction
  scanner;
- CLI/image versions were not yet pinned as evidence; and
- six new lazy materializations marked real external-I/O boundaries that
  deserved later decomposition rather than cosmetic removal.

Those gaps are consolidated in the [dated production-readiness
inventory](../readiness/2026-07-18-coding-agent-missions.md). The observability
and evaluation designs were split into implementation-ready follow-ups:

- [#488 — sandbox-side span semantics](https://github.com/VangelisTech/archetype/issues/488)
- [#489 — Pydantic Evals integration](https://github.com/VangelisTech/archetype/issues/489)
- [#490 — policy-controlled L7 traffic capture](https://github.com/VangelisTech/archetype/issues/490)
- [#491 — durable external live-event bus](https://github.com/VangelisTech/archetype/issues/491)
- [#492 — redacting Logfire/OTel collector](https://github.com/VangelisTech/archetype/issues/492)

## 10. Why this record is retained

Several architectural rules came from operating a real agent rather than
designing an abstract service:

- progress must be observable while the agent is running;
- resumability and portable evidence are different requirements;
- a rejected attempt is still valuable durable state;
- task transition, tick commit, model submission, checkpoint, artifact index,
  and PR merge are distinct state machines;
- credentials are capabilities, not serializable configuration;
- trace availability cannot become correctness authority; and
- concurrency without authoritative validators measures process completion,
  not useful agent throughput.

That is the historical value of the work: the contracts were extracted from a
working system and its failure modes, not inferred from a toy interface.
