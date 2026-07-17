# Performance Benchmarking

Archetype's benchmark harness records enough provenance to decide whether two
runs are comparable before it compares their timings. It does not gate normal
CI: shared GitHub-hosted runners are too noisy to establish performance
regressions.

## Run and retain a measurement

Give a stable machine a durable identity, then run the suite:

```bash
export ARCHETYPE_BENCH_RUNNER=mac-mini-m2-pro
make bench
```

`make bench` writes the latest report to `bench-results.json` and archives the
same content-addressed report under `.bench_out/history/`. Both locations are
gitignored. Copy the history directory to durable artifact storage when the
machine is ephemeral; the repository does not pretend that a local ignored
directory is durable storage.

Use `make bench-full` for the three-step profile. A different step count is a
different benchmark configuration and is not mixed into the same comparison.

### Query latency

`make bench-query` records four materialized `QueryService` read shapes:

- latest-tick state for one exact archetype signature;
- an early historical tick for that signature;
- a component-subset union across three matching signatures;
- the same component union restricted to one entity.

Setup is outside the timed region. Each measurement includes lazy query-plan
construction and the terminal `collect().count_rows()` materialization, and it
fails before reporting if the materialized row count differs from the workload
oracle. The default workload uses 100 entities per signature, three history
ticks, one warmup, and five measured repetitions.

Query reports use `query-bench-results.json` and
`.bench_out/query-history/`, separate from the ECS microbenchmark history:

```bash
make bench-query
make bench-query-compare
```

For this suite, `steps_per_sec` means materialized queries per second and
`entities_per_sec` means materialized entity rows per second. Configuration
changes such as entity count, history depth, repetitions, warmups, or backend
produce a different compatibility identity.

## Compare compatible history

After at least three earlier runs on the same machine and configuration:

```bash
make bench-compare
```

The comparator aligns rows by benchmark name and dimensions. A historical
report is admitted only when all of these match the current report:

- suite and benchmark configuration;
- runner identity, operating system release, machine, and processor;
- Python implementation and version;
- Archetype, Daft, LanceDB, and PyIceberg versions.

Incompatible runs are reported but never folded into the distribution. Duplicate
report IDs are counted once, and the current report's archived copy is excluded.
Malformed or hand-edited history fails closed because every report ID is a hash
of its contents.

## Regression rule

For each metric, the comparator uses the 20 most recent compatible reports that
predate the current report, then computes their median and population standard
deviation. A result is a regression only when it lies strictly more than two
standard deviations beyond the median in the worse direction:

- `elapsed_s`: higher is worse;
- `steps_per_sec`: lower is worse;
- `entities_per_sec`: lower is worse.

Fewer than three compatible samples produces `insufficient`, not a pass or a
regression. When historical variance is zero, any strictly worse value crosses
the zero-width threshold. This is intentionally literal evidence rather than an
invented tolerance.

Comparison is advisory by default. A stable dedicated runner may opt into a
failing exit code after its history is established:

```bash
uv run python -m bench.core.compare \
  --current bench-results.json \
  --history-dir .bench_out/history \
  --fail-on-regression
```

Do not add that flag to shared-runner CI. A performance gate becomes meaningful
only after the runner, retention location, cadence, and response policy have
named owners.

## Report contract

Schema version 1 separates three kinds of data:

- `environment` and `config` decide comparability;
- benchmark `name` and `dimensions` decide row identity;
- `metrics` carry measured values.

World and run IDs remain available under `provenance`, but they do not change a
benchmark's identity. `revision` records the measured commit and whether tracked
files were dirty. This keeps execution receipts useful without treating a new
world UUID as a new benchmark.

The current ECS and query suites remain focused benchmarks. End-to-end step
scaling, fork, contention, memory, recovery, and backend-parity coverage remain
tracked in [issue #141](https://github.com/VangelisTech/archetype/issues/141).
