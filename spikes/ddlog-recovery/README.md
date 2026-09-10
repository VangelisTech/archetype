# DDlog → Iceberg controlled checkpoint restart

An executable acceptance prototype: persist every input of one real DDlog
connected-components program through immutable Parquet and an Iceberg v3 SQLite
catalog, restore the exact program into a fresh native host, and compare the
maintained outputs against an uninterrupted host and independent BFS.

This is a controlled clean-shutdown experiment. It does not install a production
persistence backend in Archetype or ddlog-runtime. See [RESULTS.md](RESULTS.md)
for the historical result and current verification.

## Run

Python 3.11+, Rust 1.94+, and a Unix local filesystem are required. The adapter
has its own Cargo workspace and lockfile; it does not change root dependencies.

```sh
cd spikes/ddlog-recovery
cargo build --locked -j 1
python3 -m unittest discover -s . -p 'test_*.py'
cp artifact.example.json artifact.local.json
# Replace host_binary and native_artifact with operator-owned absolute paths.
# Keep the recorded SHA-256s: they select the tested binaries, not arbitrary code.
python3 checkpoint_restart.py artifact.local.json /tmp/ddlog-recovery-fresh-run
```

The final command requires the historical DDlog host and native artifact
identified in `artifact.example.json`. Both are intentionally **not** distributed.
It validates their hashes and every generated program/operator source hash before
native activation. Missing or changed artifacts fail closed. This is a retained
artifact reproduction path, not a portable fresh DDlog compilation claim.
The host derives from runtime commit
`3c377e6915bb640b2e6cb3ebf8c7fa0433ecc5c4`; a newly compiled binary need not have
the same hash. Supporting fresh compiler output is separate work.

Use `DDLOG_RECOVERY_PERSIST=/absolute/path/to/adapter` to reuse a separately built
adapter. No sibling spike or external Python helper is required. Native reuse
hardlinks verified bytes, or copies them across filesystems, without compiling.
Use a new output directory on each run; paths containing spaces, `%`, `?`, or `#`
are unsupported by this local URI fixture. Socket creation must be permitted.
The run requires 900 MiB free and uses bounded subprocess waits. Output contains
local paths, process identities, full manifests and hashes; keep raw run receipts
local rather than publishing them unreviewed. Do not run Python with `-O`.

## Contract

The exclusive fixture ledger admits finite-set mutations and updates only after
native acknowledgment. Sequence reuse with identical content is a local no-op;
changed content fails. The pinned runtime does not export inputs, so this ledger
supplies all seven vertices and three edges at boundary 2. Compiled input inventory
is checked to contain exactly those two relations. Derived labels are never used
as restore inputs.

One tagged Parquet schema carries relation, arity and integer values. An immutable
manifest binds complete inputs, source transaction digests, logical boundary,
registry records, exact program pin and artifact hashes. Iceberg `fast_append`
publishes the file through the real SQL catalog. A fresh catalog reload and
pinned-snapshot Arrow scan verify exact rows and hashes before acknowledging the
checkpoint. This readback is necessary because the pinned SQL catalog discards
its database COMMIT error; that failure mode itself is not injected here.

The oracle applies an additional unpublished transaction, cleanly stops the
original host, reads the committed checkpoint again, reconstructs registry files,
and installs the exact program in a new empty host. Replayed inputs must reproduce
boundary 2 while excluding unpublished changes. Bridge insertion and deletion
must then match both the uninterrupted host and BFS (eight full-set comparisons).

## Limits

No crash injection, WAL/tail replay, distributed/cloud durability, concurrent
writers, multiple checkpoints, power-loss test, general schema evolution,
compaction, inference or exactly-once external effects. Registry restoration is a
bounded file adapter. Internal Timely progress/arrangements are recomputed, not
serialized. This is JSON/file adaptation, not Arrow-native FFI. Empty whole
checkpoints are outside the experiment. Publication/CAS fault testing from the
prior Iceberg experiment is not part of this oracle.

The preserved historical dependency lock uses Arrow/Parquet 58 and Thrift 0.17.0,
which do not meet the later Rust kernel experiment's Arrow/Parquet 59 floor.
Use only self-generated local files. This isolated, unpublished Cargo package is
not approved for untrusted data or production use; upgrading and revalidating its
Iceberg dependency stack remains separate work.

See [provenance and helper licensing](PROVENANCE.md).
