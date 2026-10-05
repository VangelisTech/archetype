# Logical creation and resolution

Status: implemented local migration contract; supported runtime migration and
final installed native acceptance remain subsequent stages.
The native manager remains the sole owner of world reservation, materialization,
activation, input admission and recovery. The existing processor registry owns
program identity and immutable versions. CutStore owns published context
visibility. Neither Python nor the hosted adapter adds an authoritative birth
catalog, execution manager or scheduler.

## Immutable identity and publication

A logical destination contains resource, world and run. Its identity excludes
programs, component mappings and other mutable caller payload. A request binds
that destination to an immutable, bounded payload: an exact program reference,
world definition, persistent component declarations and any specialized fork
source evidence. Same request key and payload return the same native identity;
a changed payload conflicts. Another key cannot allocate a second world at an
occupied destination. Resource identity and world/run ownership cannot be
bypassed with aliases. Cold resolution requires no caller-held native ID or
request key.

Fresh and fork creation share the current manager's durable reservation and
materialization primitive. New tagged records must preserve retained v1 fork
identity, source proofs and materialization markers. Legacy fork entry points
remain exact compatibility wrappers; old records are not silently reinterpreted
as fresh creation. A fresh world stays created at generation zero without
starting a compiler or admitting input.

Validate exact program/declarations and compatible storage scope before
avoidable effects. A durable reservation precedes world control materialization.
Retries fence retained control bytes before accepting the materialization
marker; missing progressed control state fails closed. Hosted context
publication then completes the data binding. Until exact context readback is
confirmed, public activation and input remain blocked by the native owner.
Storage publication and native readiness are independently recoverable phases;
an interrupted or lost response cannot imply absence, rollback or safe replay.

Program publication follows the same principle under the registry's existing
update lock. The registry reserves logical destination, request digest and exact
processor identity before immutable version publication. Retry reconciles the
same pin. Display labels and mutable library associations are not identity.
Composition accepts bounded closed declarations and exact retained references;
raw registration is never blindly repeated after lost acknowledgment.

## Shared operations and authority

Typed local C ABI create/reserve/resolve and bounded program description/list/
composition operations use the existing owners. Raw Open, Register, Bind and
native paths remain operator machinery. Logical handles are inert references
which borrow that process owner; a Python cache is not durable truth.

Authenticated ingress checks capability and exact destination/program-reference
grants before any native or storage lookup, including creation of an absent
name. Composition checks every protected reference before resolving any of
them. One bounded operation contract supplies Python, HTTP and MCP outcomes;
retained call supervision owns cancellation, capacity and shutdown draining.
Results expose logical coordinates, exact references and factual phase/receipt
identities while excluding native IDs, source programs, paths, checkpoints and
internal diagnostics.

## Executable oracles and validation

The implementation must prove concurrent creation yields one reservation/world;
lost acknowledgment after reservation, control publication or context visibility
resolves the same identity; cold resolution recovers without the original key;
changed payload/scope or occupied aliases conflict; invalid declarations and
unauthorized requests cause no avoidable effects or lookups; incomplete context
publication blocks activation/input; program publication retries preserve exact
pins; and cancellation retains capacity until real work finishes.

Validation includes deterministic native fault tests, retained fork and registry
regressions, shared ingress HTTP/MCP parity, a fresh installed-wheel run with
all product imports audited, focused static checks, and independent read-only
review. Actual compiler acceptance remains distinct from simulated-driver tests.
Broader Bool/finite Float64 types, supported runtime migration, consumer changes
and final public documentation remain separate subsequent stages.

The shared wire operations are `create`, `resolve`, `program_create`,
`program_compose`, `program_resolve`, and `program_describe`, with exact closed
arguments and grants documented in [shared ingress](ddlog-ingress-preview.md).
Global bounded program listing remains a trusted local registry operation;
it is not exposed as an unrestricted authenticated inventory.

Executable contracts live in `crates/archetype-ddlog/tests/published_contexts.rs`
and the preview's `test_logical_binding.py` and `test_logical_ingress.py`.
The deterministic origin-write fault proves a context-confirmed but unready fork
survives cold resolution and resumes the same reservation. Lost or corrupt
origin evidence after readiness blocks resolution. Public fork projections
check both the native reservation and exact analytical source receipt.

Normative sources: the accepted Final Surface, the root-owned logical-creation
contract, [published contexts](ddlog-published-contexts.md), and
[historical forks](ddlog-historical-forks.md). If the retained 0.6 runtime guides
disagree, this focused migration contract owns the new operation; it does not
silently redefine the old supported facade.
