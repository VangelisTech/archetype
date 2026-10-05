# Performance Benchmarking

Archetype's benchmarks answer narrow performance questions on a controlled
machine. They are local measurements, not CI gates or claims about product
capability. Each workload defines its timed boundary and checks the result so
that a fast but incorrect run is never reported.

Benchmarks are the measurement arm of the
[Repository Harness](repository-harness.md). They stay outside the shipped
`archetype` package because they consume the runtime rather than implement it.

## Run a benchmark

```bash
make bench        # five ECS microbenchmarks, one simulation step
make bench-full   # the same workloads, three simulation steps
make bench-query  # four materialized durable-world read shapes
make bench-daft-attribution  # version-pinned lazy execution characterization
```

The ECS commands write `bench-results.json`; the query command writes
`query-bench-results.json`; and the Daft characterization writes
`daft-attribution-results.json`. All three are gitignored local snapshots.
Pass a different `--out` path to the corresponding Python module when
retaining a named run.

Set `ARCHETYPE_BENCH_RUNNER` to a stable machine name when snapshots may be
compared manually:

```bash
export ARCHETYPE_BENCH_RUNNER=mac-mini-m2-pro
make bench
```

Each snapshot contains the suite configuration, timestamp, Git revision and
dirty state, machine and dependency context, and the workload's raw results.
The envelope is intentionally small. Individual suites own the meaning of
their result fields instead of forcing every workload into one metric model.

## Current workloads

The commands above define the supported Python benchmark inventory. A
supported workload has a documented entry point, correctness oracle,
reproduction-context snapshot, and focused tests. Specialized MuJoCo and Rust
measurements remain development probes owned by their respective guides until
they adopt this same contract.

### ECS

`make bench` runs packed iteration, simple iteration, fragmented iteration,
entity cycling, and component add/remove. Fixture creation is outside the
timed region. Each workload uses its own storage namespace because several
fixtures deliberately reuse component class names.

The one-step and three-step commands are different workloads; compare like
configurations on the same machine and dependency set.

### Query latency

`make bench-query` creates one durable world with three archetype signatures,
then measures:

- latest-tick state for one exact signature;
- an early historical tick for that signature;
- a component-subset union across all matching signatures;
- that union restricted to one entity.

Setup is outside the timed region. Lazy plan construction and terminal
`collect().count_rows()` materialization are inside it. Every warmup and
measured query must return the expected row count before the snapshot is
written. The default workload uses 100 entities per signature, three history
ticks, one warmup, and five measured repetitions.

### Daft execution attribution

`make bench-daft-attribution` retains the synthetic oracle from issue #518. It
pins the Daft version, records the configured runner, proves that the synthetic
delayed UDF remains deferred until `DataFrame.collect`, separates Python
conversion time, and reduces experimental Subscriber events to bounded
booleans and counts. It does
not retain raw plans, operator names, query/node identifiers, trace identifiers,
or temporary marker paths. The workload measures no persistence backend and
does not make the Subscriber API a production dependency.

## Trend tracking is an operational decision

The repository does not currently archive benchmark history or label timing
changes as regressions. A useful automated trend gate first needs all of the
following:

- a stable dedicated runner;
- durable artifact retention;
- a named cadence and baseline window;
- an owner and response policy for reported regressions.

Without those pieces, a local history directory and statistical threshold add
machinery without producing a trustworthy decision. The snapshots preserve
the context needed to revisit comparison once those prerequisites exist.

Remaining workload coverage and the runner/retention decision stay tracked in
[issue #141](https://github.com/VangelisTech/archetype/issues/141).

## Add a workload

Keep setup, measurement, and correctness checks visibly separate. Time only
the boundary named by the workload, emit fields natural to that question, and
reuse `bench.core.report.build_report` only for the small reproduction-context
envelope. Add a trend rule only when retained data demonstrates that the rule
is meaningful.

Do not retain an unreferenced parameter sweep as benchmark infrastructure.
Keep it as a local experiment until its question, oracle, command, and owner
are clear; then promote the smallest repeatable workload.
