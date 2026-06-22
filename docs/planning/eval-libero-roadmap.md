# Eval + LIBERO Roadmap (living plan)

Working plan for: dogfooding the eval service, redoing the LIBERO/VLA-JEPA
bench cleanly, and the cleanups that fall out. Iterate freely — status markers
at each item. Lens throughout: the infra failure-mode catalog
(`.context/infra-failure-modes.md`) and its 6-disease diagnosis
(`.context/infra-diagnosis.md`).

Status legend: ☐ todo · ◐ in progress · ☑ done · ⛔ blocked

---

## Goal

Run VLA-JEPA on LIBERO cleanly enough to trust the numbers — the thing the
hackathon infra thrash prevented. "Clean" = the bench is thin, the runtime owns
orchestration, results are *graded from persisted data* (not computed in a
script), and the run recipe is captured so it can't regress.

---

## Autopsy so far

### Orchestration layer — `bench/libero/eval_driver.py` (read ☑)
- Hand-rolls the episode loop (`for _tick in range(max_steps): step; query; break`,
  lines 234–255) instead of using `SimulationService.run_episode`. **D1.**
- Manually calls `reset_tick_counters()` 3× (179, 237, 367) to paper over the
  RBAC counter never being reset by the service. **Bug #1.**
- Per-tick `ManipStatus` materialization to drive the done-check (line 245) — no
  trustworthy termination contract. **D3 / wasted compute.**
- Computes `success`/`episode_length` in Python and writes `EvalTrialResult`;
  the eval service never touches the raw data. **The core dogfood gap.**
- `instruction=""` passed to the VLA policy (line 214). **Failure mode 4d — a
  paper-corrupting bug.**
- Lab-world genesis duplicated from `autoresearch_service._attach_ledger`.

### Compute layer — components & processors (read ☑ for manipulation.py)
- `ManipStatus {reward, done, success, env_step}` (success latches, done freezes);
  `ManipTask {suite, task_id, instruction, seed, env_key}`;
  `ManipProprio`, `ManipAction`, `ManipFrameRef`.
- `EnvStepProcessor` / `FramedEnvStepProcessor` wrap the env behind a
  `@daft.cls` batch UDF (`_EnvStepper`) — the sanctioned external-process escape
  hatch. Supports constructor-injection OR `EnvClientSpec` from Resources.
- `ScriptedReachEnv` — deterministic in-process env for CI (no Modal/GPU).
- LIBERO persists `Manip*`, **not** `Trajectory` (that's a separate, unused-by-LIBERO
  vocabulary in `experiments/trajectories.py`).

### Compute layer — STILL UNREAD ☐
`modal_worker.py`, `vla_jepa_worker.py`, `colocated_runner.py`, `video_rollout.py`,
`policy.py`. This is where container-split / env-state-loss (2a/2b),
lance-on-volume (2g), nested-function lifecycle (2c/2d), and packaging (1a–1k)
actually live. The compute redo (D below) cannot be specced until these are read.

---

## Key finding from starting A (dogfood)

**Eval cannot dogfood the LIBERO path as-built.** Each trial runs as a one-entity
world that is destroyed after grading. The `ManipStatus` rows persist (append-only),
but under a fresh `(world_id, run_id)` that is never recorded — `EvalTrialResult`
carries suite/task/trial/seed, not world/run ids. The eval service queries only by
`(world_id, run_id)`, so the raw trajectory data is **orphaned**. The driver only
works because it grades inline before `destroy()`.

This is D1 + D2 + the batching gap in one place. It points the redo at: **one
control-plane world, N trial entities, batch-stepped**, so `ManipStatus` is
addressable by one `(world_id, run_id)` and sliced by `ManipTask` — then eval
grades it natively.

---

## Work breakdown

### A — Dogfood eval  ◐ (A1+A2 done; A3 needs the GPU smoke)
- A1 ☑ Self-contained dogfood: scripted manipulation episodes → persist
  `ManipStatus` → eval service queries via `query_episode(world_id, run_id)` →
  graders compute success-rate/length → assert they reproduce an independent
  in-Python replay of the same scripted env. **Done** —
  `tests/experiments/test_eval_dogfood_manipulation.py` (two cases: all-success
  with early stop, and a failed trial with a 2/3 rate). Proves eval grades raw
  rows and that `EvalTrialResult`'s summary is recomputable → **E1 is evidenced**.
- A2 ☑ Full-sweep dogfood (grade a whole multi-trial run by one `(world_id,
  run_id)`) — **unblocked by C2 and proven**: `tests/bench/test_eval_run_batched.py`
  grades 4 batched trials, cross-checked against an independent sim, asserting all
  trajectories are addressable (none orphaned).
- A3 ☐ Cross-check graded success vs LIBERO's own success rate on **real** LIBERO
  — needs the one GPU smoke (`modal run bench/libero/image.py::eval_task`). This
  is the only step between here and trustworthy real numbers.

### B — Core/service fixes the driver papers over
- B1 ☑ Reset the per-tick RBAC quota at each tick boundary (bug #1). **Done** —
  `SimulationService.step` calls an injected `_reset_quota` (wired by the
  container to `auth.reset_tick_counters`), mirroring the `set_command_drain`
  seam so the service stays free of an auth import. Tests:
  `tests/app/test_tick_quota_reset.py`; eval: `regression.tick_quota_resets`.
- B2 ☑ Value-based "all entities done" termination contract in `run_episode`
  (bug #3 / D3). **Done** — `EpisodeConfig.terminal_field` + `terminal_all`:
  terminate when entities carrying `terminal_component` latch the boolean field
  (all, or any), read post-step via `world.get_components` reduced in Daft to a
  single row. Legacy structural / callable termination preserved; `terminal_field`
  suppresses the structural check. Tests: `test_episode_rollout.py`
  (`TestValueBasedTermination`); eval: `regression.episode_value_termination`.
  - *Follow-up (not blocking):* the contract still issues one boolean-column read
    per tick. It is now framework-owned, opt-in, and Daft-reduced (was an
    all-rows `.to_pylist()` in the bench). A future optimization could surface
    done-ness from the step's `PostTick` frames instead of a separate query.
- B3 ✓ **Dissolved, not deferred.** B3 was "the processor-remote boundary" — but
  that boundary only existed to bridge LIBERO's incompatible interpreter. With D
  (LIBERO in-process), there is no processor↔remote split to fix for the env: the
  env processor runs the env in the same interpreter via `@daft.cls`, statefully.
  A *general* remote-processor seam (running processors across machines for scale)
  is a real but separate future concern — not needed for LIBERO eval, and no
  longer on this critical path.

**Resolved decision (was open #1):** B1 + B2 landed *with* the eval work, each
TDD (red→green) + a BDD contract test + a CI regression eval.

### C — LIBERO orchestration redo  ☑ (new clean orchestrator)
Built as `bench/libero/eval_run.py` (`run_task_eval`), superseding the old
`eval_driver.py`. Proven green by `tests/bench/test_eval_run_batched.py` with the
scripted env+policy (no Modal/LIBERO needed). The in-process LIBERO client runs
this orchestration **unchanged**.
- C1 ☑ Thin onto `SimulationService.run_episode` (B1 quota reset + B2 all-done).
- C2 ☑ One control-plane world + N trial entities, batch-stepped by `env_key`.
  Every trial's trajectory is addressable by one `(world_id, run_id)`, sliced by
  `ManipTask` — **the A2 addressing gap is fixed** (test asserts 4 trials persist,
  none orphaned).
- C3 ☑ Real `instruction` wired from `env_client.task_language()` (kills 4d).
- C4 ☑ No lab-world genesis hand-roll at all — grading is from raw `ManipStatus`
  via the eval service, no `Experiment`/`Run`/`EvalTrialResult` bookkeeping.

### D — LIBERO compute redo  ☑ (in-process, one Python 3.12 env)
**The premise was wrong.** LIBERO's "can't share our process" pins are laziness,
not law (torch<2.6 = one `weights_only` patch; py3.8-3.10 + robosuite 1.4.1 =
upgradeable). So the compute redo is *not* a better RPC boundary — it's **no RPC
boundary**: LIBERO runs in the same interpreter as Archetype.
- `bench/libero/in_process.py` — `InProcessLiberoEnvClient` drives
  `OffScreenRenderEnv` directly (mirrors `LiberoEnvBatch` minus Modal); the
  existing `_EnvStepper` `@daft.cls` steps it statefully, exactly like the
  scripted env. Patches `torch.load` for modern torch.
- `bench/libero/image.py` — modernized Modal image: Python 3.12 + modern
  torch/robosuite/mujoco + LIBERO **and** Archetype installed into one env, with
  `libero_smoke` / `eval_task` entrypoints that run the whole eval in-process on
  a GPU container. Collapses the entire D1 disease cluster (container split,
  env-state-loss, nested `.remote()`) — those existed only to bridge two
  interpreters.
- **One empirical step remains:** `modal run bench/libero/image.py` (GPU build +
  smoke) to confirm robosuite 1.5 vs LIBERO. The compat matrix is being
  researched; fallback is robosuite 1.4.1 on py3.12 (still one interpreter).

### E — Experiments cleanup  ◐ (in progress via breadth workflow)
- E1 ◐ `EvalTrialResult` → delete. Proven legacy by A1 *and* by the new
  `eval_run` orchestrator (which grades from raw `ManipStatus`, never writes a
  summary). The old `eval_driver.py` that wrote it is superseded by `eval_run.py`.
  Being executed now (delete `EvalTrialResult` + legacy `eval_driver` + fix
  `report.py`/`eval_harness`), conservatively, gate-gated.
- E2 ☐ `Manip*` vs `Trajectory` — decide one vocabulary. (Plan only this pass.)
- E3 ☐ `Run`/`Result`/`Experiment` naming collisions. (Plan only — prefix churn.)
- E4 ☐ `BranchHead` → dies with SearchService (issue #253), not here.

### F — Relocation  ☐
- `bench/{libero,mujoco}` out of root; `archetype_data`/`archetype_db` default
  paths. Import-heavy (like the services reorg). *Decision: `src/archetype/bench/`
  vs top-level `benchmarks/`?*

### G — Capture the recipe (D5)  ☐
- A blessed, version-pinned LIBERO/VLA-JEPA run recipe + LEARNINGS so the thrash
  can't recur. The image recipe solved once, frozen.

### H — Instruction optimization (the research payload)  ☑ (CI-proven; GPU run pending)
**The thesis, made executable:** prompt optimization that lifts a coding agent's
performance also lifts a VLA's success rate. The instruction is a *per-entity*
``ManipTask`` field that already flows untouched to the policy
(``PolicyActionProcessor`` → ``_PolicyCaller.act`` → ``PolicyClient.act`` →
``VlaJepaPolicyClient`` → ``infer_refs(instruction=...)``). So optimizing it is
not a new mechanism — it is the batched eval with the per-entity instruction
varied.
- H1 ☑ **`run_instruction_sweep`** (`bench/libero/instruction_sweep.py`): V
  instruction variants × S seeds spawned as trial entities in ONE control-plane
  world, batch-stepped, graded **per variant** from the persisted ledger
  (reuses `eval_run._final_row_per_entity` → zero new lazy-audit sites).
- H2 ☑ **`optimize_instruction`**: GEPA-style hill-climb over success-rate;
  evaluator injectable (sim ground-truth now; a VLA-JEPA *latent* scorer is the
  drop-in fast inner loop later). Never regresses (incumbent re-evaluated),
  deterministic tie-break, terminates on perfect score / rounds / patience.
- H3 ☑ **CI proof** (`tests/bench/test_instruction_sweep.py`):
  `InstructionConditionedReachPolicy` (gain scales with `instruction_quality`)
  is a fully-replayable VLA stand-in. One test grades a graded ladder
  (0 → 0.2 → 0.8 → 1.0) per variant against an independent replay; the other
  shows the optimizer **climbing from a vague instruction (0% success) to a
  precise one (100%)** through partial-credit rounds. No GPU.
- H4 ☑ **GPU entrypoint** `image.py::optimize_task` — the same loop with
  `InProcessLiberoEnvClient` + `VlaJepaPolicyClient`, perturbing the task's own
  instruction words. One command → the paper numbers, graded from the ledger.
- H5 ☐ Run it on real LIBERO (needs the GPU smoke first, A3); then a richer
  (LLM/paraphrase) perturbation strategy than single-token toggles.

**Discovered while building H:** `run_episode(max_steps=N)` persists tick 0 (the
reset obs) + N−1 control steps — the reset consumes one budget slot, so a trial
gets **N−1** control steps, not N. Correct-but-offset semantics (verified by
ledger dump across max_steps∈{4,6,8}); negligible at LIBERO horizons, now
documented on `run_instruction_sweep` and accounted for in the CI replay.

---

## Dependency order (updated)

```
B1, B2 ✓ ──> C1,C2,C3,C4 ✓ ──> A2 ✓ ──┐
                                        ├──> A3 (real numbers) ── needs ──> modal smoke (GPU)
D (in-process env + image) ✓ ──────────┘
E1 (delete EvalTrialResult) ◐  ── proven legacy by A1 + eval_run
E2/E3 (vocab/naming) : plan only       F (relocate bench) : last, import-heavy
G (recipe) ◐                           compat matrix : researching
```

Everything that can be done without a GPU is done and green. The single gate to
**real LIBERO numbers** is one `modal run bench/libero/image.py` (build + smoke),
then `::eval_task`. The compute layer is now fully read (modal_worker, policy,
manipulation) — the in-process path replaced the need to "map then bridge" it.

---

## Open decisions
1. ~~Do B1/B2 land in the eval PR or a separate core PR?~~ **Resolved:** landed
   with the eval work, fully tested (see B above).
2. Relocation target for `bench/`: `src/archetype/bench/` vs `benchmarks/`?
3. SearchService `value()` semantics (mean-Q vs max) — deferred to issue #253.
4. Where does this plan doc live / get committed? (currently `docs/planning/`)

## Session log
- **This pass (A1 + B1 + B2):** the three cheap, well-understood entry points,
  each TDD (red→green) + BDD contract test + CI regression eval, per the request
  to back the core-bug fixes with TDD/BDD/evals.
  - B1: `simulation_service.py` (`set_quota_reset` seam), `container.py` (wiring).
  - B2: `models.py` (`EpisodeConfig.terminal_field`/`terminal_all`),
    `simulation_service.py` (`run_episode` + `_entities_terminal`), `lazy_audit.toml`.
  - A1: `tests/experiments/test_eval_dogfood_manipulation.py`.
  - Evals: `evals/suites/regression.py` (`tick_quota_resets`,
    `episode_value_termination`).
  - Gates green: ruff, ty, lazy-audit (11 sites), regression eval 12/12.
- **Second pass (C + D + A2, the in-process keystone):** read the full compute
  layer and rejected its premise. LIBERO's dep conflict is laziness, not law, so
  the fix is **in-process, not a better RPC boundary.**
  - D: `bench/libero/in_process.py` (`InProcessLiberoEnvClient` + spec, torch.load
    patch), `bench/libero/image.py` (py3.12 image: Archetype + modern LIBERO in
    one env; `libero_smoke`/`eval_task` GPU entrypoints).
  - C: `bench/libero/eval_run.py` (`run_task_eval`: one control-plane world, N
    trial entities batch-stepped, instruction wired, graded from raw rows).
  - A2: `tests/bench/test_eval_run_batched.py` — 4 trials, addressable, graded,
    cross-checked vs independent sim. Green.
  - B3 dissolved (no remote boundary once in-process). Gates green on new files.
  - Breadth (E cleanup, G recipe, compat matrix) running via the
    `libero-modernize-breadth` workflow.
- **The one remaining empirical step:** `modal run bench/libero/image.py` — GPU
  build + in-process smoke confirms modern robosuite vs LIBERO; then `::eval_task`
  produces the first trustworthy real-LIBERO numbers. Everything else is done.
- **Deferred:** F (relocate `bench/` — now includes the new in-process files;
  import-heavy, do last); wiring the in-process VLA-JEPA policy into `eval_task`
  (env-only smoke first).

## Issues / PRs
- PR #252 — eval service + app reorg (in progress)
- Issue #253 — SearchService design (deferred, after eval + experiments cleanup)
