# Modal OpenCode single-H200 calibration — 2026-07-18

This is a curated calibration note, not a regression baseline or production
capacity guarantee. The run measured one independent Modal Sandbox and
OpenCode session per agent against a Qwen endpoint declared to have one H200,
one maximum container, and target concurrency 32.

## Configuration

| Field | Value |
|---|---|
| Model | `Qwen/Qwen3.6-35B-A3B-FP8` |
| Workload | `one-sandbox-one-opencode-session-v1` |
| Concurrency curve | `1, 4, 8, 16, 24, 32` |
| Declared GPU | H200 |
| Declared maximum containers | 1 |
| Declared target concurrency | 32 |
| Git revision | `64333356d028ff48eb62a7253f02f0940ba84868` |
| Client runner | Darwin arm64, CPython 3.12.12 |
| Raw report SHA-256 | `8c869a3d8a04c7b3e664b59938a2a2350b761a19f4eb2fe50568cd9e0a60e294` |

Sweep snapshots and whole-filesystem manifests were disabled so provider
checkpointing did not contaminate inference timing. The resumability preflight
kept snapshots enabled.

## Resumability preflight

The preflight passed in 603.32 seconds. Phase A and phase B used distinct Modal
sandboxes, continued the same OpenCode session, produced separate validated
commits, and created two restorable filesystem checkpoints. The endpoint cold
start consumed roughly seven minutes: `/v1/models` initially returned 503 and
later returned 200 with the expected model. A one-token Chat Completions probe
then returned 200 before the measured fanout levels.

## Fanout results

| Concurrency | Accepted | Execution (s) | p50 (s) | p95 (s) | Max (s) | Gross agents/s | Accepted agents/s |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1/1 (100.00%) | 30.72 | 30.72 | 30.72 | 30.72 | 0.0325 | 0.0325 |
| 4 | 4/4 (100.00%) | 46.76 | 34.31 | 45.18 | 46.75 | 0.0856 | 0.0856 |
| 8 | 6/8 (75.00%) | 280.70 | 58.59 | 213.25 | 280.70 | 0.0285 | 0.0214 |
| 16 | 13/16 (81.25%) | 65.33 | 44.98 | 62.47 | 65.33 | 0.2449 | 0.1990 |
| 24 | 22/24 (91.67%) | 93.66 | 75.90 | 90.20 | 93.66 | **0.2562** | **0.2349** |
| 32 | 29/32 (90.63%) | 164.05 | 107.66 | 127.20 | 164.05 | 0.1951 | 0.1768 |

Across all measured levels, 75 of 85 agents passed (88.24%). All 85 OpenCode
processes exited with return code 0. The authoritative validator rejected ten:
eight did not create the required file, one created an additional misspelled
file, and one produced incorrect content. Every sandbox teardown succeeded.

## Interpretation

For this short coding workload, concurrency 24 was the observed
useful-throughput peak. Moving from 24 to 32:

- reduced gross completion throughput by about 23.9%;
- reduced accepted completion throughput by about 24.7%;
- increased median latency by about 41.8%;
- increased p95 latency by about 41.0%.

The concurrency-8 point was an agent-behavior outlier rather than a monotonic
capacity result. Seven agents reached terminal processing well before the final
one. The outlier wrote the target, attempted an unnecessary `xxd` verification,
found that `xxd` was unavailable, and entered additional model turns. Its
280.70-second tail dominated the level.

Acceptance did not degrade monotonically with concurrency, so strict task
success is better treated as model-quality variance than direct evidence of
endpoint overload. The throughput reversal and latency increase from 24 to 32
are the stronger saturation signals. Repeat levels 16, 24, and 32 before
treating 24 as a stable production limit.

The raw JSON snapshot remains excluded from Git under the benchmark retention
contract. Its checksum binds this note to the locally retained sample-level
evidence without committing transient sandbox, session, and checkpoint IDs.
