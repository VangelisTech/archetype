# DDlog Python preview

Status: local, trusted, opt-in Python binding over DDlog Runtime
`3451df0ce968c6c2dc2a24265cf6434d108edccf` and the
[hosted cut publisher](ddlog-hosted-publisher.md). This is a separately installed
preview, not a drop-in replacement for `ArchetypeRuntime`. The retained 0.6
runtime still uses its existing engine. A separate local
[shared ingress contract](ddlog-ingress-preview.md) now adapts a real principal
verifier and explicit resource grants over this Host. HTTP/MCP listeners and
consumer migration remain unimplemented.

## Ownership and installation

`packages/archetype-ddlog-preview` ships `archetype_ddlog_preview`, a standard
library-only distribution with no Python dependencies. Its classification in
`quality/ddlog-preview.toml` is isolated runtime preview infrastructure. The
required `scripts/check_ddlog_preview.py` audit permits only standard library
and self imports. It stays outside the default UV workspace and the existing
`archetype` family DAG; lint and type checks include its source. Importing or
using it never enters the retained ECS, runtime or Daft path.

`crates/archetype-ddlog-python` builds a separate C ABI library. Its process
resource holds the existing `WorldManager`, `CutStore`, and a Tokio executor.
The opaque handle registry controls foreign-resource lifetime only. It stores
no second world inventory, admission ledger, generation, analytical head,
scheduler or tick loop. World/job/admission authority stays upstream; cut
visibility and verification stay in the hosted adapter and store.

Build with Rust 1.95 and the cached, pinned dependencies:

```sh
cargo +1.95.0 build --offline --locked -p archetype-ddlog-python
python -m pip wheel --no-index --no-deps --no-build-isolation \
  --wheel-dir /tmp/ddlog-wheels packages/archetype-ddlog-preview
python -m venv /tmp/ddlog-preview-env
/tmp/ddlog-preview-env/bin/python -m pip install --no-index --no-deps \
  /tmp/ddlog-wheels/archetype_ddlog_preview-0.1.1-py3-none-any.whl
```

Source-wheel building requires locally available setuptools >=83 and wheel.
The wheel contains Python only. Supply the exact native library explicitly
(`target/debug/libarchetype_ddlog_python.dylib` on macOS, `.so` on Linux), plus
absolute private registry/build/store roots and an installed DDlog build driver.
There is no import-time library loading, automatic build/download or fallback
implementation. The driver is trusted executable process configuration.

The DDlog dependency commit remains unpublished. Local Git-cache objects permit
offline validation; a clean external build cannot fetch this revision until
separately authorized upstream publication. Neither the wheel nor local tests
remove that release blocker.

## Explicit operation sequence

```python
from archetype_ddlog_preview import Host

with Host(
    library="/absolute/build/libarchetype_ddlog_python.dylib",
    registry_root="/absolute/private/registry",
    build_root="/absolute/private/worlds",
    store_root="/absolute/private/storage",
    driver="/absolute/operator/build-ddlog.sh",
) as host:
    # program_definition and composition_definition use upstream DDlog schemas.
    program = host.register("Labels", program_definition)
    composition = host.register("Composition", composition_definition)
    pin = {name: composition[name] for name in ("processor_id", "version")}
    native_id = host.create("Experiment", pin, ["labels", "statuses"])
    binding = host.bind(
        {"native_world": native_id, "world": "experiment", "run": "run_a"},
        component_declarations,
    )
    status = host.start(native_id)  # delegates start_async; may be starting
    # Caller observes host.status(native_id) until running, with its own deadline.
```

Native creation returns its identity before separate analytical binding.
`bind` validates component declarations against that pinned composition without
activating it. Bindings are caller-owned configuration snapshots, not durable
execution state. The trusted application must retain this configuration and
bind exactly one native world per analytical world/run, including after reopen.

After observing running status, call `admit(binding, expected_head=...,
generation=..., revision=..., key=..., changes=...)`. Native generation/revision,
the analytical parent and admission key are explicit fences. Keep the original
world/generation/key before submitting. The response carries the exact native
boundary key. Poll `admission_status` for that original identity; a lost response
is not permission to mint a new admission key or replay input.

`publish(binding, boundary_key)` captures immutable frozen evidence one page at
a time, then publishes the complete cut through real Iceberg. It returns a cut
receipt without confirming the native boundary. `confirm(binding, boundary_key,
tick=..., expected_parent=...)` requires the already-visible exact latest cut
and reconstructs private verified confirmation authority. Caller JSON never
constructs a `PublishedCut` or native external acknowledgment.

Use `history(world, run, offset=0, limit=100)` and
`read(receipt, component, offset=0, limit=1000)` for published cuts. Reads return
logical component rows, schema, total row count and a next offset. Integers stay
exact beyond 2**53. Historical and explicitly empty cuts remain readable.
Pagination bounds the returned JSON, not Iceberg's internal scan memory; reads
currently materialize the selected component through the existing store.

Read/restore consume only `{world, run, tick, cut_id}` from the returned receipt
(or accept that compact reference directly). The Rust boundary resolves the
exact canonical receipt from the catalog and verifies it through the store;
extra caller receipt metadata confers no authority. This also avoids requiring
wide receipt schemas to fit back through the request-size limit.

## Stop, recovery and resource lifetime

`stop(native_id)` delegates to upstream process control. It affects that world;
other worlds remain available. Stop can leave uncertain native work and an
unresolved publication barrier. It does not roll back a native commit or cancel
an analytical publication.

Reopening `Host` with the same roots reconstructs upstream durable inventory;
it does not activate worlds or replay inputs. Rebind the saved scope/components.
If only native frozen evidence exists, call `publish` with its exact boundary
key. If the analytical journal exists, `reconcile(binding, key, tick=...,
expected_parent=...)` verifies and publishes that exact journal, or returns its
already-visible cut after a lost response. Confirm explicitly afterward.

`restore(binding, receipt, expected_generation=...)` verifies the latest exact
same-world/run cut through `prepare_restore`, then starts the upstream bound
checkpoint restore asynchronously. A stale head or generation fails. Plain
`start` is also explicit; it must not be confused with restoring a saved cut.
There is no absence-based adoption or automatic retry owner here.

Use a context manager or call `close()` explicitly. Garbage collection does not
perform process control. Calls lease the native resource; manager locks cover
one upstream call or one capture page and are released throughout storage work.
Close rejects new calls, signals the independent upstream shutdown handle,
drains existing calls, observes startup/boundary completion, and explicitly
persists stopped state before releasing roots. Concurrent closes converge on
that teardown. Historical persistence error text does not override a successful
fresh durable stop. Failed close retains its handle/library and closing
ownership for a subsequent explicit close; other operations remain rejected.

The 30-second cooperative drain deadline is not a hard bound on blocking
filesystem calls, upstream joins or runtime destruction. A caught Rust panic
poisons the host and requests shutdown; a poisoned manager is never resumed.
Its retained owner may require process exit and durable recovery in a new
process. Signals interrupting successful construction adopt and close the
returned handle before propagating the exception. If that cleanup also fails,
`ConstructionCleanupError.host` retains the adopted owner: repair the reported
failure and call `error.host.close()` again. Retain that exception/host until
cleanup succeeds or the process exits.

`ctypes.CDLL` releases the GIL. In asynchronous applications, use
`asyncio.to_thread` for blocking methods on an already-owned host. Cancelling a
waiter does not cancel its native call: the call retains its lease until it
finishes, and close drains it. Construct the host synchronously, or retain and
drain the constructor future even if its waiter is cancelled; discarding an
asynchronous constructor's successful result loses explicit close ownership.
After `fork`, inherited library/host calls and teardown are rejected before
any inherited lock is acquired. Use a fresh exec/spawn process.

## ABI and value contract

ABI 1 exports version, open, call, close and buffer-free functions. Operators
supply an absolute trusted library path and the wrapper checks the version.
Handles are monotonic opaque integers and are never reused. Input is one bounded
UTF-8 JSON object, at most 1 MiB; responses are at most 16 MiB. Unknown operation
fields, duplicate keys, trailing JSON, floats/nonfinite request numbers and
excessive nesting fail. Component cells are non-null signed Int64 or strings:
Python bool, float, numeric coercion and out-of-range integers are rejected.
Control fields preserve unsigned 64-bit integers; finite float status telemetry
is preserved on responses. Upstream string/CLI restrictions still apply,
including rejection of NUL/control characters.

C callers keep input and output metadata valid for each call. Returned buffers
belong to this library; copy before its explicit free, once per owned allocation.
The wrapper frees in `finally`, including errors and interruption. It never
retains a borrowed Python pointer or uses thread-local last-error state.
Exported boundaries catch unwinding panics; invalid raw pointers, allocation
abort and process termination are outside the safe C contract.

`TypeError`/`ValueError` report wrapper input failures; `OSError` and missing
symbol errors report library loading; `ProtocolError` reports ABI/envelope
violations. `NativeError.kind` distinguishes request, operation, open, close,
closed, forked, poisoned, panic and response-limit failures. Upstream errors
remain contextual text without string-based retry classification. A native
operation or oversized response may have completed before an error reaches
Python: inspect exact retained identity instead of assuming rollback.

## Validation

`make ddlog-python-check` runs the isolated import audit, rustfmt, strict Clippy,
Rust tests, native-library build and ordinary Python contracts. Ordinary tests
use explicitly simulated native transport and real local Iceberg. They cover
strict raw ABI requests, exact integers, fork/stale handles, interruption during
open, use after close, native isolation, cancellation during catalog publication,
concurrent close, close during compilation/restore, persistence failure repair,
published-cut history and latest bound restore.

The installed-wheel end-to-end test additionally requires the actual compiler:

```sh
DDLOG_PYTHON_LIBRARY=/absolute/build/libarchetype_ddlog_python.dylib \
ARCHETYPE_DDLOG_DRIVER=/absolute/operator/build-ddlog.sh \
/tmp/ddlog-preview-env/bin/python -m unittest discover \
  -s packages/archetype-ddlog-preview/tests \
  -k test_actual_ddlog_iceberg_recovery -v
```

It executes two registered/composed DDlog programs and two worlds, verifies
actual downstream `ready` values and an entity ID above 2**53, exercises
published-but-unconfirmed stop/reopen/reconciliation and native bound restore,
and reads nonempty → empty → nonempty cuts plus historical data. Its evidence
is distinct from simulated transport; a skipped opt-in is not native evidence.

The original environment, old runtime, suspended duplicate-manager prototype
and prior checkpoints remain separate. Broader query/Arrow types, sanctioned
Daft analytical reads, authenticated hosts, fork lineage, world-context
artifacts and versioned consumer migration remain future scoped work.
