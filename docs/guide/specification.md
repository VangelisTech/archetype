# Archetype 0.7 specification

## Ownership

Archetype maps declared DDlog relations to typed ECS components and complete
Iceberg results. The existing DDlog WorldManager owns live admission, workers,
revisions, barriers, checkpoints and recovery. Archetype adds no second live
manager, scheduler, admission ledger or tick engine. Physical storage owns
verified cut visibility and artifact indexes. Family workflows use that storage
port; concrete wiring remains private composition.

## Programs and values

Programs are immutable versions or compositions with complete declared input
coverage. Worlds bind a program, logical world/run identity and explicit component
projections. Entity keys are exact Int64. Required live cells support Int64,
string, Bool and finite Float64. Nullability, schema evolution, additional live
Arrow types and arbitrary relation-to-component inference are not supported.
Float64 preserves finite bits and canonicalizes signed zero to positive zero.
Wire integers are canonical decimal strings; Float64 is canonical lower-case
16-digit big-endian hexadecimal IEEE-754 bits; Bool is a JSON boolean.

## Complete cuts and recovery

A completed native boundary is distinct from a durable complete cut. Publish
all declared components, including zero-row outputs, verify the cut manifest and
native receipt binding, then confirm the exact receipt to release the admission
barrier. A lost acknowledgement requires reconciliation of that retained result;
it never authorizes replaying inputs. History selects complete receipts before
reading rows. Forks pin an explicit source cut, isolate destination identity and
preserve inherited history without reading unrelated latest rows.

## Lifetime and cancellation

Runtime construction and context entry do not load native code or open storage.
First use owns one supervised opening task. World shutdown drains that world's
work and stops it; siblings remain usable. Process shutdown rejects new work,
drains retained tasks and closes the native owner. Caller cancellation preserves
supervision and capacity until the operation settles. Sync wrappers retain their
Runner and owner after failed close for retry. Inherited handles reject use in
another PID before touching inherited runtime state. Server teardown keeps the
SDK lifespan open until ingress draining and native close settle.

## Artifact facts and batches

Artifacts have content-addressed originals and UUIDv7 occurrence identities.
Hosted and artifact-only contexts have immutable identities without invented
simulation ticks. Cutless occurrences remain cutless. Exact cut attribution is
verified against the published context and cut. Prepare/publish file batches use
the existing family-owned Daft graph outside execution and physical storage port.
Retained preparation preserves exact publication metadata for retry. Public cold
reads expose common and six typed index families: images, audio, video, PDF,
text and diff. Nullable metadata remains unknown; it is not coerced to a live
cell. Physical source/object locations, table proofs and checkpoint bytes are
private. Logical paths and semantic strings such as PDF titles are facts.

## Transport and grants

Python, HTTP, MCP and CLI use one closed operation contract. HTTP and MCP verify
real principals and both capability and exact resource grants before resolving
native bindings. Fork needs source and destination grants. Program compositions
need every referenced program grant. Hosted artifact publication needs context
and world grants. Public responses omit native paths, native identities and
internal diagnostics. Failure outcomes are `not_dispatched` or `unknown`;
error text never proves rollback or safe replay. Public limits are 64 KiB request,
16 KiB response, 32-row pages and 32 KiB inline upload. Oversize responses fail
with a bounded unknown outcome; clients reduce page sizes rather than replaying
mutations. Native read budgets remain independent of transport envelopes.

## Compatibility and release

0.7 is a deliberate breaking runtime contract. The old Daft `spawn/run/step`,
mutable processors, old FastAPI operation hosts and application composition are
not fallback execution paths. Retained 0.6 specifications, contracts and teaching
material are explicitly versioned under compatibility. Research requires matched
0.6 source/wheels. Smol is independent. Gateway and Holocron need tested consumer
migrations before acceptance. Candidate publication and hosted exact-head evidence
remain separate from local test success. Routine simulated compiler evidence
cannot replace installed actual DDlog/Iceberg acceptance.

## Executable validation

The current runtime, CLI and server contracts live in `packages/archetype-ecs/tests`.
Native value, durability, logical resource and official SDK tests live in the
native and transports packages. Architecture/lazy audits protect retained family
boundaries. Installed acceptance asserts product origins, exact wheel/native
hashes, real compilation, full and empty cuts, historical fork, audio facts,
registry/build/driver/source-independent cold reads and HTTP/MCP parity.

## Idempotency matrix

Request identity never makes changed input safe to replay. The native owner and
verified storage receipts retain authority. This current matrix is checked
against `quality/native_idempotency.json`; the former workflow-family matrix
remains in the matched 0.6 specification archive.

| Scope | Contract | Executable oracle |
| --- | --- | --- |
| `program_version` | Immutable definition and complete composition coverage | `packages/archetype-native/tests/test_logical_binding.py::LogicalBindingTests::test_fresh_exact_retries_cold_identity_conflicts_and_no_compiler` |
| `world_creation` | Logical scope plus request key binds one exact definition | `packages/archetype-native/tests/test_logical_binding.py::LogicalBindingTests::test_fresh_exact_retries_cold_identity_conflicts_and_no_compiler` |
| `historical_fork` | Request key binds exact source cut and destination; changed payload conflicts without allocation | `packages/archetype-ecs/tests/test_native_runtime_binding.py::RuntimeBindingTests::test_two_program_full_empty_cut_historical_fork_and_storage_only_cold_reads` |
| `publication` | Lost acknowledgement reconciles the retained boundary and receipt without replay | `packages/archetype-native/tests/test_binding.py::BindingTests::test_recovery_contract` |
| `artifact_occurrence` | Retained occurrence UUID and metadata retry exactly; new preparation creates new occurrence | `tests/artifacts/test_context_attachments_native.py::ContextArtifactTests::test_collection_has_no_execution_side_effects_and_cold_reads` |
| `artifact_cut` | Retained preparation pins exact historical cut after live head advances | `tests/artifacts/test_cut_attachments_native.py::CutArtifactTests::test_historical_cut_two_occurrences_and_exact_retry` |
