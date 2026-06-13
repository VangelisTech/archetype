# RoboSemanticBench Modal Runbook

This runbook is for running RoboSemanticBench through Archetype's Modal-native
runner. It does not use LIBERO.

## Checkouts

Archetype runner:

```bash
cd /Users/darin/vendor/github.com/VangelisTech/archetype
```

RoboSemanticBench source checkout:

```bash
/Users/darin/src/vendor/github.com/ZGC-EmbodyAI/RoboSemanticBench
```

Override the RSB checkout with `RSB_SOURCE_DIR=/path/to/RoboSemanticBench` when
needed.

## Bootstrap Data On Modal

Install or verify the HF CLI first:

```bash
modal profile current
modal secret list | grep hf-token
```

Populate the Modal volumes directly from Hugging Face:

```bash
modal run bench/robosemantic/bootstrap_modal.py
```

This is the preferred path. It keeps the laptop off the gigabyte data path and
caches the payloads in:

- `robosemantic-rsb-data` mounted at `/rsb/data`
- `robosemantic-rsb-gsm8k-data` mounted at `/rsb/gsm8k/data`
- `robosemantic-rsb-mmluqa2-data` mounted at `/rsb/mmluqa2/data`
- `robosemantic-model-cache` mounted at `/models`

Use `force=True` only when you intentionally want to refresh existing volume
contents:

```bash
modal run bench/robosemantic/bootstrap_modal.py --force
```

`modal run` uses an ephemeral app, so Modal memory snapshots are not enabled in
that mode. Use `modal deploy bench/robosemantic/bootstrap_modal.py` if you want
the bootstrap functions themselves to benefit from snapshot restore. Volume
contents are persistent either way.

## Local Bootstrap Fallback

If you need a laptop-local copy for inspection, this also works:

```bash
uv run --with pyarrow python bench/robosemantic/bootstrap_payloads.py \
  --rsb-source /Users/darin/src/vendor/github.com/ZGC-EmbodyAI/RoboSemanticBench \
  --upload-modal-data-volume \
  --force-upload
```

The bootstrap is resume-friendly. It downloads:

- `VLyb/RSB-Math` into `data/rsb_math/rsb_math_train_500`
- `VLyb/RSB-Math-10blocks` into `data/rsb_math_10blocks/rsb_math_10blocks_train_500`
- `openai/gsm8k` into `gsm8k/data/{train,test}.json`
- `cais/mmlu` into `mmluqa2/data/{train,test}.json`

For emergency simulator smoke tests only, add
`--allow-mmlu-test-train-fallback` if the public MMLU auxiliary-train parquet is
unreachable. Do not use that fallback for paper-protocol reporting.

The Math payload excludes videos and `_traj_data`; eval needs `data/*.hdf5`,
`instructions/*`, `scene_info.json`, and `seed.txt`.

## Upload DP Checkpoints

The DP policy loader expects checkpoint directories at the root of the Modal
volume mounted as `/rsb/policy/DP/checkpoints`.

```bash
modal volume create robosemantic-rsb-checkpoints
modal volume create robosemantic-model-cache

for d in /Users/darin/src/vendor/github.com/ZGC-EmbodyAI/RoboSemanticBench/policy/DP/checkpoints/*; do
  test -d "$d" || continue
  modal volume put --force robosemantic-rsb-checkpoints "$d" "/$(basename "$d")"
done
```

For the default DP args, one checkpoint path should look like:

```text
/rsb/policy/DP/checkpoints/rsb_math-default-50-0/600.ckpt
```

Change `--ckpt-setting`, `--expert-data-num`, `--policy-seed`, and
`--checkpoint-num` to match the checkpoint directory name.

## Smoke Run

Use a fresh `run_id`; the runner refuses to write into a non-empty shard result
directory.

```bash
RUN_ID="rsb-smoke-$(date +%Y%m%d-%H%M%S)"

modal run bench/robosemantic/runner.py \
  --suites RSB-Math-4 \
  --episodes-per-suite 4 \
  --shards-per-suite 4 \
  --run-id "$RUN_ID" \
  --policy-name DP \
  --ckpt-setting default \
  --expert-data-num 50 \
  --policy-seed 0 \
  --checkpoint-num 600
```

## Full Paper-Protocol Eval

The paper scores 500 eval episodes per suite. `--shards-per-suite` controls
Modal task-level parallelism; each shard carries a distinct `episode_start`, so
semantic rows are not repeated across shards.

```bash
RUN_ID="rsb-full-$(date +%Y%m%d-%H%M%S)"

modal run bench/robosemantic/runner.py \
  --suites RSB-Math-4,RSB-Math-10,RSB-HardMath-4,RSB-HardMath-10,RSB-General-4,RSB-General-10 \
  --episodes-per-suite 500 \
  --shards-per-suite 20 \
  --run-id "$RUN_ID" \
  --policy-name DP \
  --ckpt-setting default \
  --expert-data-num 50 \
  --policy-seed 0 \
  --checkpoint-num 600
```

Results are committed per episode by default and summarized in:

```text
robosemantic-results:/$RUN_ID/aggregate.json
robosemantic-results:/$RUN_ID/<task_name>/shard<N>/_result.txt
robosemantic-results:/$RUN_ID/<task_name>/shard<N>/episode_metadata/episode<M>.json
```

Fetch them locally with:

```bash
modal volume get robosemantic-results "/$RUN_ID" "./$RUN_ID"
```

## Weight Cache And Snapshots

The runner follows the Modal cache convention from
`Eventual-Inc/daft-examples/models`:

- `/models` is backed by `robosemantic-model-cache`
- `HF_HOME`, `HF_HUB_CACHE`, and `TRANSFORMERS_CACHE` point into `/models`
- `hf_xet` is installed and `HF_XET_HIGH_PERFORMANCE=1`
- shard functions use `enable_memory_snapshot=True`
- both bootstrap and eval functions attach the `hf-token` Modal secret

This caches Hugging Face model repos and files across cold starts. The DP
baseline checkpoints are not HF snapshots by default; they live in
`robosemantic-rsb-checkpoints` and are mounted directly at
`/rsb/policy/DP/checkpoints`.

Important: the current shard path loads `get_model()` inside a Modal function
body. That means the memory snapshot helps imports, CUDA/library initialization,
and cache reuse, but it does not guarantee a resident DP model snapshot. To
snapshot already-loaded DP weights, refactor the runner into a `modal.Cls` and
load the policy in `@modal.enter(snap=True)`, then run shard methods against
that class instance.

## Metrics

Each shard returns:

- `task_success_rate` (`TSR`)
- `grasp_success_rate` (`GSR`)
- `normalized_semantic_grounding` (`nSG`)

`nSG = ((TSR / GSR) - (1 / N)) / (1 - (1 / N))`, where `N` is the number of
answer choices in the suite.

## Operational Notes

- `robosemantic-rsb-data` is mounted at `/rsb/data`, so the top-level RSB
  `data/` tree must live in that volume.
- `gsm8k/data` and `mmluqa2/data` are separate Modal volumes, not baked into the
  image. Re-run `bootstrap_modal.py` after changing their generation logic.
- The public HF datasets currently expose the RSB-Math and RSB-Math-10blocks
  payloads. HardMath and General eval can run from GSM8K/MMLU source JSONs, but
  DP baselines still need matching checkpoint dirs.
- Pass `--commit-every-episodes 0` only if you intentionally want to trade away
  per-episode result durability for fewer Modal volume commits.
