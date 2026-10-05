# Migration to 0.7

0.7 replaces the supported Daft live runtime with the existing DDlog hosted owner.
It is a breaking version. Immutable program definitions and declared projections
replace mutable processors; changes and explicit admission/boundary/publication
replace `spawn`, `run` and `step`. History means complete cuts, not audit frames.
The CLI and default server expose the same closed native operations. Old FastAPI
world-library routes and stale entry points cannot restore the old default owner.

The [0.6 guide archive](../compatibility/0.6/guide/runtime.md) and
`compatibility/0.6/quality/contracts.toml` retain their actual contracts and tests.
Run those checks against matching 0.6 source/wheels. They are not 0.7 acceptance
oracles. Generic engine/storage/family tests remain useful where their owning
modules and semantics are unchanged. The independent `archetype-smol==0.6.3`
teaching engine remains runnable. Research and generic Biome/inference examples
stay on explicitly matched 0.6 source/wheels until their own migrations are tested.
No package-index availability claim is made for those compatibility wheels.

Gateway's previous 0.5.0 hosting internals require a deliberate native runtime
adapter. Holocron's previous 0.6.3 artifact internals require public context,
prepare/publish, pagination and typed cold-fact reads. Their installed consumer
migration oracles and version decisions are required release evidence. The
installed gate builds the selected source ports in `tests/consumers`, installs
both consumer wheels and all four product artifacts, copies only tests into a
neutral fixture directory, and verifies source/wheel/loaded module hashes plus
the native library and upstream revision. Gateway uses the public operator
verifier and retained tenant lifespan; Holocron uses artifact-only collections.
The older Holocron provenance/provider replay API remains on its explicitly
matched 0.6.3 source line, with a version guard in the new package. A version-1
collection binding requires explicit export/import and is never reinterpreted.

Missions and Physical AI evaluation products are excluded from active exports,
entry-point discovery, extras, docs navigation and release lanes. The independent
Eventual physical-ai-evals repository is outside this change.


## Verification profiles

`make ci` requires source static checks, current Python/native/storage/transport
contracts, C ABI format/Clippy/Rust tests and package smoke. The artifact source
contracts include all six typed index families, corruption and cold-process
checks. They use a simulated compiler with the real C ABI and Iceberg.

`make verify-full` additionally requires the explicit current lifecycle/recovery
inventory, scoped runtime/native/transport coverage evidence, strict docs, and
the installed actual-DDlog contract, documented example, consumer migration and
real loopback CLI success/authorization checks. The CLI and server processes
record installed module hashes; only the server loads the native library.
The recovery inventory covers process ownership, fork/PID rejection, cancelled
opening and waits, sibling isolation, failed close retry, interrupted restore
and compile, partial publication/reconciliation and storage-only reopen. Actual
native worker/checkpoint semantics remain owned by the pinned DDlog dependency;
its exact-head upstream acceptance is retained separately rather than counted as
new Archetype tests.

`make verify-release` also records and verifies exact wheel/sdist hashes, commit,
clean source and all four package identities. Smol retains its independent 0.6.3
version and separate installed Daft environment. The three current native surface
packages are 0.7.0. Package smoke rebuilds each sdist and checks wheel content,
licenses, exports and discovery without source imports. The installed example is
the credential-free operator smoke. Failed or skipped actual proof is never a
release pass. Current coverage is evidence; no global 0.6 coverage denominator
or threshold is reused for this different surface.

The current manual release workflow retains the original operator and immutable
tag authorization, builds matched native/toolchain inputs and retains this
credential-free evidence. The old package-index publishing configuration is
versioned under `compatibility/0.6/release`; current package-index delivery and
external deployment require separately reviewed configuration. Docs builds do
not deploy unless the operator explicitly selects the manual deploy input.
