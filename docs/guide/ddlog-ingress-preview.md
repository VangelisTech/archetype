# DDlog shared ingress preview

Status: version 0.1.2 local transport-neutral contract over the tested Python `Host` and its
existing Rust owner. Optional [local HTTP/MCP adapters](ddlog-transports-preview.md)
now consume it. This shared package supplies no listener, production credential
provisioning, supported-runtime replacement or consumer migration.
The upstream pinned DDlog revision remains unpublished, blocking clean external
builds. Native compilation is not required to validate this Python-only change
against an existing compatible library.

## Ownership and authority

`archetype_ddlog_preview.wire` owns versioned immutable request values and the
strict JSON codec. `archetype_ddlog_preview.ingress` owns exact capability and
resource authorization, immutable operator configuration, public projections,
and outstanding-call supervision. Both remain inside the audited stdlib-only
isolated preview. The retained runtime's `ActorCtx`/`CommandDispatcher` contract
continues to apply to its own ingress; this explicitly scoped preview does not
import that runtime or create another durable command scheduler.

The operator passes one already-owned `Host`, a configured real
`archetype.api.principals.PrincipalDirectory` through its structural verifier
port, `Resource.from_binding(...)` configurations, and exact `Grant` values.
The existing verifier is stdlib-only and checks SHA-256 credential verifiers,
expiry and revocation. This package neither imports its framework package nor
substitutes a permissive verifier. Its enclosing composition must supply that
real implementation; ordinary tests use it directly from repository source.
A standalone host distribution still needs a reviewed dependency arrangement
for it. Installing the existing framework distribution brings its existing
dependencies; this preview does not claim an extracted authentication wheel.

`Ingress.invoke(credential, raw_json_bytes)` authenticates each call, decodes an
exact operation, checks both the principal's capability and its explicit
resource grant, then resolves configuration. Denied or unknown resources share
the same denial result and cause no native lookup. No default principal, role
label, wildcard grant, permission inheritance, or caller-supplied actor exists.
Credentials are out-of-band and never passed to native work or responses.
Verifier configuration and grants are process-owned snapshots; this slice adds
no access provisioning, reload service, tenant database or durable auth ledger.

`Ingress.authenticate(credential)` exposes the same verifier and principal
validation for transport authentication/context only. It authorizes no operation;
`invoke` re-verifies on every invocation before checking exact grants.

Resource bindings retain native world, analytical world/run, component
declarations and allowed input predicate schemas. Native-world and analytical
world/run aliases must be unique. Configured declarations come from trusted
`Host.bind`; configuration alone is not native validation. Ingress checks
restore receipt world/run before dispatch and constructs native boundary world
identity from the authorized binding. Native fences and verified Rust tickets
remain authoritative, including latest-head restore and cut confirmation.

## Shared operation and value contract

All transports must pass the original JSON bytes through this codec, including
duplicate-key rejection. A transport that parses JSON first must preserve those
checks before information is lost. Version 1 has exactly four envelope fields:

```json
{"version":1,"operation":"admission_status","resource":"experiment","arguments":{"generation":"1","admission_key":"first"}}
```

| Operation | Exact arguments | Required capability |
| --- | --- | --- |
| `status` | none | `simulation:read` |
| `admission_status` | `generation`, `admission_key` | `simulation:read` |
| `start`, `stop` | none | `simulation:control` |
| `admit` | `generation`, `revision`, `admission_key`, `expected_head`, `changes` | `simulation:submit` |
| `publish` | `boundary` | `simulation:publish` |
| `reconcile` | `boundary`, `tick`, `expected_parent` | `simulation:publish` |
| `confirm` | `boundary`, `tick`, `expected_parent` | `simulation:confirm` |
| `restore` | `receipt`, `expected_generation` | `simulation:restore` |

Every capability also requires an exact `(principal_id, resource)` grant.
`start` can invoke the operator's existing compiler driver. `stop` controls only
that configured world; neither operation controls the shared process. Grant
these and restore separately from read/submit. No operation takes a native ID,
program definition, driver, path, storage config, binding or component schema.
Registration, world creation, binding, host construction/close and raw requests
remain trusted local operations. Inventory, history, component reads, step/run,
fork and artifact registration are absent.

All signed Int64 cells and unsigned 64-bit control values use canonical decimal
strings on the wire. For example an input row is
`[{"int64":"9007199254741109"},{"string":"ready"}]`; a change contains exactly
`op` (`insert`/`delete`), `predicate`, and `values`. The decoder converts integer
strings directly to exact Python integers for `Host`. It rejects JSON numeric
cells, bool/null, floats, exponent notation, signs on unsigned values, leading
zeros, negative zero and overflow. Strings reject control characters and exceed
neither 4096 UTF-8 bytes nor the total request cap. Input predicate and cell kinds
must match operator configuration. Entity IDs remain component Int64 cells.

`boundary` contains exactly `{generation, admission_key, request_sha256}`;
the resource supplies its native world. `receipt` contains exactly
`{world, run, tick, cut_id}`. Digests are lowercase SHA-256 hex. Optional parent
or head values are a digest or explicit null. Generation/revision, request
digest, tick and parent are forwarded exactly. Publish, reconcile and confirm
stay separate calls. Ingress never confirms implicitly, mints retry keys,
replays input, polls completion, or turns response JSON into publication authority.

Requests are capped at 64 KiB, 256 changes, and 64 cells per change. Public
responses are capped at 16 KiB. Status projects state, generation, revision and
an error-presence flag. Admission results project retained state (including
`applied_but_unpublished`), publication state, exact admission identity,
applied revision and a compact boundary. Publication returns a compact receipt
and parent. Physical paths, source definitions, process telemetry, logs,
checkpoint objects, full manifests and diagnostic error text are never exposed.

Success has `{version, ok, resource, operation, value}`. Failure has
`{version, ok, error: {code, outcome}}`. Pre-dispatch authentication, request,
authorization, capacity and closing failures report `not_dispatched`. Owned
native read-boundary facts may report `resource_limit`, `corrupt_data`,
`invalid_request` or `unsupported_format`. Other native failures and
post-dispatch projection failures report `operation_failed`. Every failure after
dispatch retains `unknown` outcome. Native text is never parsed into
not-found/conflict/retry semantics.
A failed or lost response is not rollback evidence. Retain the original
admission key and query its exact identity through authorized admission status.

## Cancellation, shutdown and limits

One ingress belongs to one process and one event loop. It borrows the existing
Host. At most `max_inflight` calls (default 4, allowed 1–16) are retained;
additional authorized calls receive `busy` without a queue. Blocking methods
run in worker threads. Cancelling a caller cancels only its waiter: the owned
task retains capacity, the Host reference and eventual completion. Responses
are not retained as a second result ledger. Callers use native admission/cut
identity to resolve ambiguity.

For shutdown, call `stop_accepting()`, run the existing `Host.close()` off the
event loop so its independent shutdown signal can unblock native work, then
await `drain()` before ending the event loop. `drain()` itself stops ingress;
cancelling it does not cancel owned calls, so it can be awaited again. Ingress
never closes the borrowed Host. Failed native close retains its existing owner
for explicit repair/retry; no new process-teardown mechanism is introduced.

Response caps and concurrency limits do **not** bound individual backend cost
or latency. Native status performs diagnostic work; admission status may adopt
and persist a completed boundary. Hosted operations also bind through native
status and may scan catalog history. They are not pure, cheap read promises or
hard-deadline services. Native storage now enforces
[read limits v1](ddlog-python-preview.md#storage-read-limits-v1), including exact
component page decoding and bounded metadata scans. History and component read
transport operations remain outside this ingress's current operation allowlist.

Before broader hosting, work remains: history and component read transport
operations preserving exact snapshot verification; broader typed native
errors where callers need conflict/not-found distinctions; lean diagnostic
projections if status cost must be bounded; and production transport/authentication
composition beyond the local static-credential adapters. No new manager, scheduler, tick loop, admission ledger
or core change is needed for this local adapter. Multi-process/distributed
ownership and native work cancellation are not claimed.

## Executable oracle

`packages/archetype-ddlog-preview/tests/test_ingress.py` tests the real verifier,
deny-by-default grants before resource/native lookup, cross-world selectors,
strict integer/JSON/byte limits, immutable configuration, safe projections,
known-apply failure states, and cancellation retaining capacity and drain.
One focused integration uses the existing C ABI library and real local Iceberg
with an explicitly simulated native driver, exercising the separate admission,
publication, confirmation and restore operations. It is not native acceptance.

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=packages/archetype-ddlog-preview/src \
DDLOG_PYTHON_LIBRARY=/absolute/existing/libarchetype_ddlog_python.dylib \
/absolute/isolated/python -m unittest discover \
  -s packages/archetype-ddlog-preview/tests -p test_ingress.py -v
```

The existing import audit, Ruff and type checks cover both new modules. Prior
real-DDlog acceptance remains evidence for the unchanged native binding; this
slice does not rerun native compilation or claim a deployed authenticated host.
