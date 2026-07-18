# Agent missions and sandbox execution

This specification formalizes coding-agent execution as application services
without promoting provider clients into Archetype's supported public API. It
preserves the behavior proven by the original Modal and Apple Container
prototype while aligning it with the family architecture.

## 1. Boundary and dependency graph

Three families divide the authority:

| Family | Owns | Must not decide |
|---|---|---|
| `missions` | mission/task/attempt identity, retry and exhaustion policy, validator and finalization gates, terminal state | provider credentials or sandbox mechanics |
| `sandboxes` | provider selection, create, credential-free restore, authenticated resume, monitoring, checkpoint recovery, close | acceptance or task advancement |
| `coding_agents` | repository-oriented composition of a mission and one live sandbox session | world persistence or gateway authorization |

The existing families retain their authority:

- `application` is the actor-free canonical operation facade.
- `runtime` is the trusted scripting facade over that application port.
- `gateway` adds identity, authorization, quotas, and audit at untrusted ingress.
- `artifacts` publishes and indexes structured facts and portable attempt
  bundles; it does not own provider checkpoints.
- `evaluation` owns graders and durable evaluation receipts.
- `container.py` is the only concrete cross-family composition root.

Concrete providers implement `iSandboxBackend`. Mission and coding-agent
services depend on protocols, not Modal or Apple Container clients. Provider
implementations may share internal utilities, but that shared code must move to
a provider-neutral module rather than making one provider inherit another
provider's module boundary.

## 2. Attempt and task-transition contract

A world tick is a durable orchestration opportunity. The current coding-agent
processor creates at most one model submission in a tick; future processors may
also use a tick only to reconcile finalization or recover a session. A tick is
never synonymous with a tool call.

`MissionService` is the sole task-transition authority:

1. Parse the immutable task at `TaskGate.step_index`.
2. Derive the next positive attempt index and deterministic idempotency key
   from world, run, entity, task, and attempt identity.
3. Preserve the preceding agent session and validator details for a retry.
4. Validate that the sandbox receipt matches the requested attempt and contains
   non-vacuous validator evidence.
5. Persist accepted or rejected attempt, checkpoint, commit, evidence, and
   friction facts.
6. Advance only when the attempt is accepted, a commit SHA exists, the
   checkpoint is restorable, and the configured finalization threshold is met.

Validator rejection and checkpoint/finalization failure are ordinary committed
states. They do not abort the tick. Reaching `max_attempts` marks the mission
terminal and failed. Invalid identities, missing validator evidence, unknown
finalization phases, and accepted receipts without a commit fail closed as
contract violations.

The initial implementation represents states as validated component fields.
Before missions become an untrusted API surface, these strings should graduate
to enums and one explicit transition graph so every invalid combination has a
single auditable rejection point.

## 3. Sandbox lifecycle and resumability

`SandboxService` owns a registry of provider adapters and process-local live
handles. The live-handle map is a cache, never mission state. A process restart
reconstructs an episode from durable mission components and a provider
checkpoint.

The lifecycle operations are intentionally distinct:

- `create(provider, spec)` starts an isolated repository session.
- `restore(provider, spec, checkpoint_ref)` is credential-free recovery for
  evidence extraction and inspection.
- `resume(provider, spec, checkpoint_ref)` re-resolves the selected harness
  credential and continues the agent session.
- `close(sandbox_id)` releases one live handle.
- `shutdown()` stops admission, drains the registry, and closes every retained
  session even when individual closes fail.

Provider checkpoints are complete recovery objects. Apple Container exports a
rehydratable root filesystem. Modal records a provider image reference. The
checkpoint TTL must exceed the artifact publication retry window; the default
policy is 30 days for Modal checkpoints and seven days for publication retry.
A checkpoint still referenced by a resumable mission or non-indexed publication
must not be garbage-collected.

Exactly-once model execution is not claimed. A crash before a durable
pre-execution claim may repeat an attempt. Deterministic attempt identity makes
the repetition detectable, while checkpoint and artifact reconciliation never
implicitly creates another model submission.

## 4. Evidence, finalization, and teardown

Every identified attempt should leave three complementary records:

| Record | Authority |
|---|---|
| Archetype component rows | mission, task gate, validator, commit, and finalization facts |
| Provider checkpoint | complete resumable sandbox state |
| Portable artifact bundle | independently readable traces, manifests, patches, Git bundle, `.context`, and declared outputs |

`ArtifactBundleService` claims the publication before external I/O, uploads
content-addressed objects, stores the deterministic record set, then indexes it.
Its durable phases are `pending`, `uploaded`, `indexed`, and `expired`.
Reconciliation is bounded and idempotent; fleet scheduling can shard it by
world. See [Artifact finalization](artifact-finalization.md) for the crash
matrix, R2 prefix, query schema, and explicit lifecycle policy.

Teardown is successful only after the latest attempt facts are persisted, a
recovery checkpoint is recorded when one was produced, declared artifacts have
been handed to durable publication or left in a retryable claim, live event
files have been collected, and the provider handle has been closed. Teardown
errors remain observable and must not silently turn a failed finalization into
mission success.

The example currently publishes recoverable attempts after the episode. The
next service increment should project `ArtifactBundleRequest` inside the
mission/application path, allowing a task policy to require `indexed` without
an example-owned teardown loop.

## 5. Credentials and telemetry

Provider, model, endpoint, and Git credentials are process capabilities. They
must never appear in component rows, validator processes, filesystem
manifests, checkpoints, artifact bytes or metadata, generated OpenCode config,
logs, or traces.

Codex and Claude subscription credentials use dedicated broker volumes. The
broker stages only the selected CLI credential during execution, persists any
refresh, and removes the mission copy before validation and snapshotting.
Credential-free restore receives no model credential; authenticated resume
re-resolves it from the named secret or broker volume.

Live observation is required for remote sessions. Status, append-only events,
stdout, stderr, phase changes, and heartbeats carry `world_id`, `run_id`,
`entity_id`, `tick`, attempt identity, harness, and sandbox identity. The same
keys belong on host and sandbox OpenTelemetry spans. Telemetry is operator
evidence, not the state authority; a trace outage cannot advance a task or
erase a durable receipt.

## 6. Harness tiers and execution policy

The harness deliberately separates correctness from cost and capacity:

| Tier | Purpose | Normal CI |
|---|---|---|
| unit/contract tests | provider conformance, transition gates, shutdown races, catalog replay | yes |
| deterministic eval | graded end-to-end mission transition capability | yes |
| Apple Container integration | real local isolation and restore/resume | no; explicit local target |
| Modal integration | live Codex, Claude Code, OpenCode, monitoring, and resume | no; credential-gated and mission-path-triggered |
| paid benchmark | endpoint throughput and one-sandbox-per-agent fanout | no; explicit paid confirmation |

External tests must skip cleanly when their credentials or local provider are
absent. Normal CI runs example 11's credential-free `--dry-run` construction
path. The paid Modal workflow is separate and triggers when the executable
example, mission/sandbox/coding-agent families, live tests, or benchmark harness
change. Benchmark reports bind results to workload configuration, Git and
runner context, and correctness evidence; timing is advisory because the
runner is not stable.

## 7. Known limitations and migration plan

The retained providers are intentionally ported mechanically before being
split further. The Modal adapter is still large, Apple Container imports a
provider-neutral base currently housed in the Modal module, and one provider
attempt still performs execution, validation, commit, evidence capture, and
checkpointing in a single method. Those are reviewable follow-ups, not reasons
to discard proven behavior.

The next increments are:

1. extract common harness, validator, commit, evidence, and checkpoint records;
2. separate provider execution from mission finalization/reconciliation;
3. move bundle projection and `indexed` gating into the application path;
4. persist an explicit transition graph and durable pre-execution claim;
5. add an operator reconciler that discovers due worlds and renews leases;
6. add sandbox-side OTel export after checkpoint and artifact durability are
   trustworthy.
