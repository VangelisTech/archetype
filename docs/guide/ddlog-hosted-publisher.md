# Hosted DDlog cut publisher

Status: implemented local Rust integration; the Python runtime remains on its
existing engine. This slice consumes the upstream hosted external-publication
port and real local Iceberg storage. It adds no manager, worker, admission ledger,
Python binding, API route, MCP tool, or cross-repository consumer migration.

## Dependency and ownership

`archetype-ddlog` pins DDlog Runtime commit
`3451df0ce968c6c2dc2a24265cf6434d108edccf` through the canonical Git dependency and
lockfile. That dependency commit is local and unpublished. Local evidence uses a
Cargo Git cache seeded from its exact local repository; no filesystem dependency
or path override is committed. External clean builds cannot fetch this revision
until the dependency is made available under separate publication authorization.

The host creates and owns `ddlog_runtime::worlds::WorldManager`, registered
composition definitions, native worlds and their lifecycle. `HostedCutAdapter`
holds an immutable analytical world/run, native world ID, component declarations,
exact compiled composition identity and publication policy. `CutStore` owns its
local catalog, immutable objects, journal, and publication serialization. The
host constructs the storage root; callers cannot select a path through an
admission or receipt. Authentication belongs to a later transport boundary.

One native binding per analytical world/run is a host composition invariant.
CutStore's cooperative process lock does not provide distributed admission
fencing across independently configured native worlds. The old standalone
`world::World` is a separate preview used by the earlier native test; the hosted
path never calls or wraps it. The suspended duplicate-manager experiment is not
part of this implementation.

## Trusted host sequence

1. Register a pure composition with DDlog Runtime. Create a native world with
   `publication_policy(&components)` before activation. Bind `HostedCutAdapter`
   with that native ID, analytical world/run and component declarations.
2. Await `prepare_admission` using the expected latest analytical cut ID. It
   verifies the complete prior cut through CutStore and binds the next tick,
   declarations/program, analytical parent and DDlog receipt parent. Submit the
   returned immutable ticket to the caller-owned manager. Repeating the ticket
   performs the upstream exact-key lookup and never reapplies inputs.
3. Poll the existing upstream admission status. Once frozen, start `capture`.
   Each `Capture::advance` reads at most one 4 MiB immutable page; the manager
   borrow ends at the call boundary. The caller can yield or serve another world
   between pages. `finish` rejects incomplete assembly and validates exact sizes,
   digests, ordered types, row counts, entity uniqueness, full output inventory,
   checkpoint attribution and activation provenance.
4. Await `publish` with the owned frozen cut and CutStore. No native manager is
   borrowed during asynchronous catalog work. It validates the exact analytical
   successor, saves its immutable journal before table mutations, publishes
   component snapshots, then publishes one final FULL cut manifest. Empty
   outputs remain explicit zero-row entries.
5. Publication verifies exact final-receipt equality, its complete attribution
   and inventory against the journal, and every pinned component table UUID,
   schema, snapshot, object digest and full-cut contents. Only then does it
   return a privately constructed `PublishedCut` ticket.
6. Call the ticket's synchronous `confirm` with the native manager. Confirmation
   records the exact upstream acknowledgment before admitting another native
   batch. A failed write retains the same candidate and barrier; retry that
   ticket or reconstruct it through exact reconciliation.

This is an embedding protocol. It does not authenticate Rust callers, make a
remote catalog transactional, or authorize bypassing WorldManager's trusted API.

## Evidence and identities

The hosted FrozenCut stores the upstream managed checkpoint bytes unchanged and
the entire upstream FrozenManifest. It uses a distinct validation path from the
standalone preview's flat checkpoint metadata. Invalid hosted evidence cannot
fall back to preview validation. Preserve native row order: the retained upstream
output digest hashes that exact JSON row-array order.

The program digest binds adapter ABI, DDlog dependency revision, processor and
dependency pins, generated source/lowering, public mapping and component
schemas. The managed checkpoint retains the origin native generation, revision
and executable/build provenance. A restore activation has its own build identity;
recompilation does not establish binary identity.

The final CutReceipt includes the canonical upstream frozen-manifest digest.
Archetype `parent` is the prior analytical **cut ID**. DDlog
`parent_receipt_sha256` is the previous canonical **external receipt envelope
hash**. These are distinct, checked identities.

The compact upstream acknowledgment body contains adapter ABI, analytical
world/run/tick, cut ID, and the canonical digest of the complete verified
CutReceipt. This binds every component schema, UUID, snapshot and object while
keeping the native acknowledgment below its 1 MiB bound even for wide schemas.
The surrounding upstream receipt also binds the FrozenManifest digest. Upstream
hashes canonicalize through `serde_json::Value`; they do not use raw Rust struct
field order.

## Failures and recovery

A component-table append alone cannot advance the analytical head. Readers must
use the final manifest and exact component snapshots; never select latest rows
across cuts. A rejected future cut cannot reserve a journal coordinate: parent
validation precedes that coordinate claim, and the journal precedes table writes.

If catalog access fails before journaling, the upstream owner still retains the
immutable native freeze and admission barrier. Reopen it and recapture the exact
key; no native input is replayed. Once an analytical journal exists, `reconcile`
requires exact world/run/tick, program, boundary key and prior cut ID. It can
publish the next cut or adopt its already-visible immediate successor after a
lost catalog acknowledgment. It cannot adopt arbitrary plausible history,
replace journal payloads, skip a parent, or interpret absence as publication.

The verified `PublishedCut` carries the same deterministic confirmation on every
recovery. Upstream confirmation failure cannot authorize a second batch. Native
uncertainty without a complete frozen boundary remains blocked; this slice adds
no discard or automatic replay mechanism.

`prepare_restore` accepts only the exact verified latest same-world/run receipt.
It returns a private ticket containing the retained manifest, checkpoint bytes
and external receipt. Its `restore` call uses the existing native manager and an
explicit target-generation fence. Old historical receipts, forged inventories,
wrong contexts, incompatible policies, and live/unresolved target worlds reject.

## Executable evidence and limits

The ordinary `tests/hosted_cut.rs` contracts use a clearly labeled simulated
native transaction transport with **real local Iceberg storage**. They cover
partial and final-manifest failures, confirmation persistence failure, owner
reopen, missing analytical journal, no input replay, full-cut visibility,
nonempty/empty/nonempty history, exact latest restore, stale parents, forged
receipts, mismatched contexts, corrupt component objects, held native work with
independent sibling publication/stop, and multi-page capture.

`installed_compiler_hosted_iceberg_recovery` is a separately invoked ignored test
using the operator-installed real DDlog compiler. It executes two composed
programs and independent native worlds, verifies a downstream derived value,
and runs publication failure/reopen/confirmation failure/restore/retraction
through the same hosted publisher and actual local Iceberg catalog. An ignored
entry in an ordinary run is not native evidence.

```sh
CARGO_NET_OFFLINE=true make ddlog-check
ARCHETYPE_DDLOG_DRIVER=/absolute/path/to/native-driver \
  cargo +1.95.0 test --offline --locked -p archetype-ddlog --test hosted_cut \
  installed_compiler_hosted_iceberg_recovery -- --ignored --nocapture
```

The adapter currently accepts checkpointable pure compositions and non-null
Int64/string components. Policy bounds are 64 outputs, one million rows per
output, 64 MiB aggregate output JSON and the upstream 64 MiB checkpoint bound.
Upstream input, context, manifest, JSON-depth and admission-retention bounds also
apply. The store remains local and cooperatively owned. Immutable data and
receipts must be retained; remote recovery, retention/GC, distributed fencing,
fork lineage and cross-world checkpoint import are not implemented here.

Next work is the thin Python/runtime and authenticated transport boundary, with
explicit cancellation, lifecycle and consumer migration contracts. Sanctioned
Daft queries, forks and world-context artifact receipts remain separate work.
This local publisher does not complete the overall execution migration or grant
publication authorization.
