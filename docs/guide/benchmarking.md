# Performance Benchmarking

Archetype's benchmarks answer narrow performance questions on a controlled
machine. They are local measurements, not CI gates or claims about product
capability. Each workload defines its timed boundary and checks the result so
that a fast but incorrect run is never reported.

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

## Current workloads

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
An explicitly reviewed calibration note does not establish automated history
or change this policy.

## Modal endpoint and coding-agent capacity

The paid Modal/OpenCode benchmark is intentionally separate from `make ci` and
from the path-gated live example integration. It has two suites because raw
inference saturation and useful coding-agent concurrency are different
questions:

- `endpoint` sends streaming OpenAI Chat Completions requests with a stable,
  repeated repository prefix. It records time to first token, total latency,
  request rate, token throughput, success rate, and raw request samples for
  each concurrency level. A bounded warmup retries transient zero-to-one 503s;
  its attempts and elapsed time are reported but excluded from measured load.
- `agents` creates one Modal Sandbox and one OpenCode session per concurrent
  unit. It times the authoritative edit/validate/commit attempt after sandbox
  setup, and records setup and teardown separately. Before load, it terminates
  one sandbox, starts another from the checkpoint, and requires the same
  OpenCode session to complete a second validated task.

The default step curve is `1,4,8,16,24,32`. Override it with
`BENCH_ARGS="--levels 1,8,16"`. Both make targets require an additional paid-run
acknowledgement:

```bash
# Direct endpoint calls need these two values in the local process. The
# MODAL_PROXY_* aliases shown by Modal's endpoint docs are also accepted.
export MODAL_ENDPOINT_TOKEN_ID=...
export MODAL_ENDPOINT_TOKEN_SECRET=...
make bench-opencode-endpoint CONFIRM_PAID_BENCH=1

# Real agents resolve credentials inside Modal from this named Secret.
make bench-opencode-agents CONFIRM_PAID_BENCH=1
```

The defaults target `Qwen/Qwen3.6-35B-A3B-FP8` at the configured endpoint and
use the Modal Secret `archetype-modal-endpoint`. Use `BENCH_ARGS` to change the
endpoint, model, Secret, workload levels, or output dimensions. A cheap
continuation-plus-one-agent preflight is:

```bash
make bench-opencode-agents CONFIRM_PAID_BENCH=1 \
  BENCH_ARGS="--levels 1"
```

For a single-H200 saturation experiment, first set the endpoint's maximum
containers to one and confirm the deployed revision is healthy. Keep target
concurrency at 32. Modal documents target concurrency as a soft autoscaler
input, while maximum containers is the hard replica bound. A maximum of one
also prevents overlapping old and new containers during rolling deployment,
so use it only for a controlled benchmark window and do not deploy while the
sweep runs. Restore the production scaling policy afterward.

Deployment metadata is operator-declared because the protected endpoint does
not expose its autoscaler configuration through the OpenAI API. Record the
verified hard bound in both reports with
`BENCH_ARGS="--declared-max-containers 1"`. An omitted value is serialized as
`null`; it must not be interpreted as a single-replica result. GPU and target
concurrency declarations default to `H200` and `32` and can also be overridden.

The endpoint snapshot is written to `opencode-endpoint-bench-results.json`;
the agent snapshot is written to `opencode-agent-bench-results.json`. Both are
gitignored. Reports include the Git revision, runner identity, complete
non-secret workload configuration, aggregate metrics, and raw samples. They do
not contain endpoint credential values. See Modal's
[endpoint benchmark guidance](https://modal.com/docs/guide/endpoint-benchmarks),
[server scaling semantics](https://modal.com/docs/guide/servers), and
[Sandbox snapshot contract](https://modal.com/docs/guide/sandbox-snapshots)
when interpreting results.

Explicitly promoted calibration notes may summarize a run without checking in
its transient sample-level snapshot. The first such note is the
[2026-07-18 single-H200 OpenCode fanout calibration](benchmark-runs/modal-opencode-single-h200-2026-07-18.md).
It records the raw report checksum and workload declaration, but it is not a
trend baseline.

Remaining workload coverage and the runner/retention decision stay tracked in
[issue #141](https://github.com/VangelisTech/archetype/issues/141).

## Add a workload

Keep setup, measurement, and correctness checks visibly separate. Time only
the boundary named by the workload, emit fields natural to that question, and
reuse `bench.core.report.build_report` only for the small reproduction-context
envelope. Add a trend rule only when retained data demonstrates that the rule
is meaningful.
