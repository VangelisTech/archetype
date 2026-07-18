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
make bench-query  # four materialized QueryService read shapes
```

The ECS commands write `bench-results.json`; the query command writes
`query-bench-results.json`. Both files are gitignored local snapshots. Pass a
different `--out` path to the corresponding Python module when retaining a
named run.

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

Paid agent and inference benchmarks are separate explicit targets:

```bash
make bench-opencode-endpoint CONFIRM_PAID_BENCH=1
make bench-opencode-agents CONFIRM_PAID_BENCH=1
```

They are never part of normal CI. They require a protected Modal endpoint,
named endpoint credentials, and an operator declaration of the deployed GPU,
maximum containers, and target concurrency.

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

### Modal OpenCode capacity

The endpoint workload sends streaming OpenAI Chat Completions requests at an
increasing concurrency curve and reports success rate, tokens per second, and
time to first token. A warmup probe is outside the measured boundary.

The agent workload starts one independent Modal Sandbox and OpenCode session
per unit of concurrency. Before fanout it proves that a new sandbox can resume
the same OpenCode session from a provider checkpoint. During the measured
sweep, snapshots and full-filesystem manifests are disabled so checkpoint I/O
does not contaminate inference timing. An independent exact-file validator,
not CLI exit status, determines acceptance.

Both workloads require `--confirm-paid-run` (the Make targets supply it after
`CONFIRM_PAID_BENCH=1`), capture a secret-free reproduction envelope, and use
advisory comparison policy because Modal placement and the client runner are
not stable. The retained single-H200 calibration is documented in
[Modal OpenCode single-H200 calibration](benchmark-runs/modal-opencode-single-h200-2026-07-18.md).

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
