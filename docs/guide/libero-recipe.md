# The Blessed LIBERO Run Recipe

> Roadmap item **G** / failure-mode **D5** ("Environment is rediscovered, never
> captured"). LIBERO and VLA-JEPA are genuinely broken research code, but the
> *recurrence* of solving their packaging from scratch every deploy is the
> disease this page cures. Solve it once; freeze it; never thrash again.

This is the **single source of truth** for running LIBERO under Archetype. If you
find yourself re-deriving torch pins or arguing about a Modal interpreter split,
stop and read this. The recipe is pinned, tested, and owned by Archetype.

---

## TL;DR — the two commands

```bash
modal run bench/libero/image.py             # builds the image, runs the in-process smoke
modal run bench/libero/image.py::eval_task  # full batched control-plane eval on real LIBERO
```

Both run on a Modal GPU container (`A10G`). Modal is used **only** for the one
real constraint — a Linux host with EGL offscreen rendering and a GPU. Everything
else runs in one Python 3.12 interpreter.

---

## The modernized approach: LIBERO runs IN-PROCESS

The premise of the old `bench/libero/modal_worker.py` was that LIBERO "can never
live in the Archetype process" because of its dependency pins, so it had to sit
behind a Modal `.remote()` boundary. **That premise was laziness, not law.**

The modern recipe rebuilds LIBERO on a current stack so it imports as a normal
in-process dependency, in the **same Python 3.12 interpreter as Archetype**:

| File | Role |
|------|------|
| `bench/libero/image.py` | Builds the one-env image (Python 3.12 + modern torch/mujoco + robosuite 1.4.1 + LIBERO at a pinned SHA + Archetype editable-installed on top). Exposes `libero_smoke` and `eval_task`. |
| `bench/libero/in_process.py` | `InProcessLiberoEnvClient` — the in-process `EnvClient`. Drives LIBERO's `OffScreenRenderEnv` directly; no `.remote()`, no container split, no env-state-loss. |
| `bench/libero/eval_run.py` | `run_task_eval` — the batched control-plane orchestration. Env-client-agnostic. |

`import archetype` and `import libero` share one interpreter. The env client owns
a dict of envs keyed by `env_key` (the trial index), so a control-plane world
with N trial entities batch-steps N envs in a single `step` call. There is **no
Modal interpreter split** — the `modal_worker.py`/`vla_jepa_worker.py` RPC path
is legacy.

---

## The one real constraint vs the lazy pins we removed

There is exactly **one** non-negotiable constraint, and it is environmental, not
a dependency pin:

> **Linux host + EGL offscreen rendering + GPU.** MuJoCo needs a GL context to
> render the agentview/wrist cameras. `image.py` provides it: `apt`-installs
> `libegl1`/`libgl1`/`libosmesa6-dev` and sets `MUJOCO_GL=egl` /
> `PYOPENGL_PLATFORM=egl`. On macOS, MuJoCo's native context works for small
> local smoke tests; for the real eval you want the Modal GPU container.

Everything the old worker treated as a constraint was actually a lazy pin we
removed:

| Lazy pin (old worker) | Why it existed | What we did instead |
|-----------------------|----------------|---------------------|
| `torch<2.6` | torch 2.6 flipped `torch.load` to `weights_only=True`, which rejects LIBERO's numpy-object init-state pickles. **One unpatched call**, not a real constraint. | Modern `torch>=2.6`. `in_process.py::_patch_torch_load_for_libero()` restores `weights_only=False` once at import. LIBERO's init-states are part of the trusted benchmark image, not untrusted input. |
| Python 3.8–3.10 | Inherited from upstream LIBERO's `setup.py`. | **Python 3.12** — same interpreter as Archetype. Python 3.12 itself is free; nothing in the robot stack actually needs the old runtime. |
| `robosuite==1.4.1` | Upstream LIBERO froze it. | **Kept at `==1.4.1`, but on Python 3.12.** robosuite 1.5 removed `SingleArmEnv` and `load_controller_config`, which LIBERO @ this SHA imports/subclasses — so 1.5 fails at `import libero` (LIBERO #49). 1.4.1 has no py3.12 exclusion; the real win is the *interpreter*, not the robot lib. We float 1.4.1's transitive pins (its exact numpy/numba/scipy/opencv have no cp312 wheels) and keep `numpy<2`. Verified against openpi/OpenVLA/LeRobot — all three pin robosuite 1.4.1. |

LIBERO itself is installed with `pip install --no-deps -e /opt/LIBERO` at a pinned
SHA, so its `requirements.txt` cannot drag the env back to the lazy pins.

### Verified dependency matrix

The pins frozen in `bench/libero/image.py`. Treat this table as the artifact;
update it (and the SHA) only with a passing `modal run bench/libero/image.py`.

The pins in `bench/libero/image.py`, research-verified against openpi / OpenVLA /
LeRobot (all three pin robosuite 1.4.1). The move is Python **3.12**, not new
robot libs: keep robosuite 1.4.1, but **float its py3.8-era transitive pins
upward** (numpy 1.22 / numba 0.53 / scipy 1.10 / opencv 4.6 have no cp312
wheels), and keep `numpy<2` so robosuite 1.4.1's `np.float`/`np.bool8` aliases
survive. Install LIBERO `--no-deps` so its `requirements.txt` can't drag the env
back. Update the table (and SHA) only with a passing `modal run`.

| Dependency | Pin | Notes |
|------------|-----|-------|
| Python | `3.12` | One interpreter, shared with Archetype. Replaces 3.8–3.10. **The actual win.** |
| `torch` | `>=2.6` | env layer is torch-agnostic; `torch.load` patched in-process. |
| `robosuite` | `==1.4.1` | 1.5 removed `SingleArmEnv`/`load_controller_config` → LIBERO import-fails. |
| `mujoco` | `==3.2.3` | openpi-verified; cp312 wheels. Avoid 3.1.1. |
| `bddl` | `==1.0.1` | LIBERO-vendored API; bddl 3.x (BEHAVIOR-1K) is **not** a drop-in. |
| `numpy` | `>=1.26,<2` | cp312 wheels **and** keeps `np.float`/`np.bool8` aliases robosuite 1.4.1 uses. |
| `numba` | `>=0.59` | 0.53.1 has no cp312 wheel. |
| `scipy` | `>=1.11` | 1.10.1 is `<3.12`-only. |
| `opencv-python-headless` | `>=4.8` | 4.6.0.66 has no cp312 wheel. |
| `gym` | `==0.25.2` | Required at import time (`libero.libero.envs.venv`); rollout still goes through robosuite. |
| `robomimic` | **omitted** | Training-only, no cp312 wheel. |
| LIBERO | SHA `8f1084e3132a39270c3a13ebe37270a43ece2a01` | Pinned 2026-06-12; installed `--no-deps -e`. |
| GL stack (apt) | `libegl1`, `libgl1`, `libglew-dev`, `libosmesa6-dev`, `libglfw3`, `patchelf`, `libglib2.0-0`, `libsm6`, `libxext6`, `libxrender1` | The EGL/offscreen-render constraint. |
| Render env | `MUJOCO_GL=egl`, `PYOPENGL_PLATFORM=egl` | Offscreen GL. |
| GPU | `A10G` (Modal) | The only reason Modal is in the loop. |

**Residual unknown:** robosuite 1.4.1 *on py3.12 specifically* (the ecosystem
stays on 3.8–3.10). No declared py3.12 exclusion exists; expect `numpy<2`
deprecation noise, not a wall. The smoke entrypoint confirms it.

---

## The architecture: one control-plane world, N trial entities

`bench/libero/eval_run.py::run_task_eval` runs the eval the Archetype-native way.
This is the clean replacement for the old `eval_driver.run_episode`, which
produced untrustworthy paper numbers (one ephemeral world per trial, orphaned
ledger rows, a hand-rolled episode loop, a manual quota reset).

```text
                one control-plane world per (suite, task_id)
   ┌──────────────────────────────────────────────────────────────┐
   │  trial entity 0 (env_key=0)  ─┐                                │
   │  trial entity 1 (env_key=1)  ─┤  PolicyActionProcessor (pri 1) │ writes ManipAction
   │  ...                          ├─ EnvStepProcessor      (pri 10) │ steps env, writes
   │  trial entity N-1            ─┘   InProcessLiberoEnvClient      │  ManipProprio/Status
   └──────────────────────────────────────────────────────────────┘
        one tick batch-steps all live trials (keyed by env_key)
                         ↓ persisted ledger
        addressable by ONE (world_id, run_id), sliced by ManipTask
                         ↓
        EvalService grades from raw ManipStatus  (no EvalTrialResult)
```

- **One control-plane world, N trial entities** keyed by `env_key`. The env
  client batches by `env_key`, so one tick steps all live trials at once.
  Finished trials freeze on the ledger (`ManipStatus.done` latches). Every
  trial's trajectory is addressable by the single `(world_id, run_id)` and sliced
  by `ManipTask` — so the eval service grades it natively (unblocks A2).
- **Reset-then-spawn:** each trial's reset obs lands as its raw tick-0 row.
- **The policy** (`PolicyActionProcessor`, priority 1) writes each tick's
  `ManipAction` from the previous observation; `EnvStepProcessor` (priority 10)
  consumes it and writes `ManipProprio` / `ManipStatus`.
- **Termination** is `SimulationService.run_episode` with the value-based
  "all entities done" contract (**B2**): `terminal_component=ManipStatus`,
  `terminal_field="done"`, `terminal_all=True`. No per-tick hand-rolled loop.
- **The RBAC quota resets inside `SimulationService.step`** (**B1**) — wired by
  the container to `auth.reset_tick_counters`. No manual `reset_tick_counters()`
  call in the driver.
- **The real instruction** is wired from `env_client.task_language()`, never
  `instruction=""` (failure mode 4d).
- **Grading is from raw `ManipStatus`** by `EvalService.query_components`, not
  computed in the driver. Success and length are recomputable from the ledger, so
  there is **no `EvalTrialResult` summary to drift** (E1: it is legacy). The
  driver reads the latest-tick row per entity at the grading boundary only.

The env client is injected, so the orchestration is identical for the in-process
LIBERO client, the legacy Modal client, or the scripted contract env — the
control plane never learns which.

---

## Running it

### In-process smoke — prove the dep upgrade works

```bash
modal run bench/libero/image.py
# or pick a suite/task:
modal run bench/libero/image.py --suite libero_spatial --task-id 0
```

`libero_smoke` resets one env and takes a few zero actions on the GPU container,
returning real proprio numbers. This is the verification that the floated
dependency set (modern torch/mujoco + robosuite 1.4.1 on py3.12) actually loads,
resets, and steps in-process on Python 3.12 — no Archetype world needed yet, just
the env client. Any residual `numpy<2` deprecation noise shows up here; that is
the only expected friction, and it is not a wall.

### Batched eval — the trustworthy numbers

```bash
modal run bench/libero/image.py::eval_task --suite libero_spatial --task-id 0 --trials 5 --max-steps 520
```

`eval_task` runs the full batched control-plane eval entirely in-process:
Archetype's `ServiceContainer`, the LIBERO env, and (when wired) the policy all
in one Python 3.12 interpreter on the GPU. It returns the graded report —
`success_rate`, `mean_length`, and the `(world_id, run_id)` that addresses the
ledger so anyone can re-grade.

> **Policy wiring is the open TODO.** `eval_task` currently passes
> `policy_client=None`; wire the in-process VLA-JEPA policy there to get real
> action numbers instead of zero actions.

---

## What's legacy (do not extend)

| Legacy | Why |
|--------|-----|
| `bench/libero/modal_worker.py`, `vla_jepa_worker.py` | The Modal interpreter-split RPC path. Superseded by the in-process client. |
| `bench/libero/eval_driver.py` | Hand-rolled per-trial-world loop with manual quota reset. Superseded by `eval_run.run_task_eval`. |
| `EvalTrialResult` component + `eval_harness.py`/`report.py` grading off it | Summary that can drift from the ledger. Grade from raw `ManipStatus` instead (E1). |
