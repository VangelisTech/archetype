# Paper-Readiness Definition of Done (consensus)

Source: a 22-agent adversarial review (10 dimension reviews + 3 deep
testing/evals/repro audits → 8 independent reviewer-persona panelists →
synthesis), run `wf_99544adc-08b`, 2026-06-21.

## Update 2026-06-22 — smoke + colocation landed on real GPU hardware

Two top gating items are now **closed**, verified on Modal GPUs:

- **Env smoke ✅** — `modal run image.py` (A10G): robosuite 1.4.1 + LIBERO +
  Archetype run in ONE Python 3.12 interpreter under EGL. The modernization
  "residual unknown" is resolved.
- **Colocation ✅ — the Modal RPC is gone.** VLA-JEPA's pins (torch 2.6 /
  numpy 1.26.4 / transformers 4.57, no `python_requires`) coexist with LIBERO +
  Archetype in one py3.12 image (`vla_import_smoke`: `coexist=True`). The model
  now loads **in-process** in the env container and infers via a localhost server
  — `vla_smoke` returned a real 7-dim action with **no `.remote()`**. Two precise
  dep fixes got there: pin numpy `<2` (caught by the CPU import smoke before any
  GPU spend) and the flash-attn cu12/torch2.6/cp312 wheel (the torch-2.6 analogue
  of the worker's proven cp310 wheel). `optimize_task` is migrated to the
  in-process policy; `colocated_eval_task` is the RPC-free real-eval entrypoint;
  `vla_jepa_worker.py` is marked superseded.

Remaining gating items below are unchanged EXCEPT: gate #2/#3 (frames
visibility / wire a real policy) are **resolved by colocation**, and gate #1 (a
real number) is now one `colocated_eval_task` run away. The circularity (#2 in
the original list), the perturbation strategy, statistical N, and env-freeze
items still stand.

## Verdict — NOT READY (unanimous 8/8: `not_ready`)

The framework engineering is genuine, but **the scientific contribution is
unestablished**. The central claim — *prompt/instruction optimization that lifts
coding agents also lifts physical-AI VLAs, measured as higher LIBERO success-rate*
— rests on four mutually-reinforcing defects every panelist reached independently:

1. **No real number exists.** All four GPU entrypoints
   (`libero_smoke`/`eval_task`/`optimize_task`/`vla_jepa_worker`) have never run;
   `eval_task` took zero actions (`policy_client=None`). Every reproducibility /
   capability / cost claim is unrun scaffolding.
2. **The CI proof is circular.** `InstructionConditionedReachPolicy` sets
   `eff_gain = gain * instruction_quality(instruction)` and the test oracle
   recomputes that same function — success *is* the optimized metric by
   construction. It proves the optimizer hill-climbs; it is **not** evidence for
   the physical-AI claim.
3. **The headline path could not run** — frames-volume wiring bug (env writes
   PNGs locally; the policy reads them from a Modal Volume in a separate
   container with no commit).
4. **The experiment could not express lift** — the delete-only token perturbation
   over the instruction's own words has its reachable optimum *equal to* the base.

Plus confounds that corrupt the real numbers once wiring is fixed: position-keyed
(unpaired) seeds, an un-reset VLA chunk buffer leaking across variants, zero
coverage of the headline claim in the `evals/` gate, an architecture doc that
overclaims "entirely in-process" while the policy is a cross-app RPC, floating
dep floors / unpinned model revisions, and no Limitations section.

## Fixed this session (testable, no GPU, all green)

| Gating item | Fix | Lock |
|---|---|---|
| Unpaired position-keyed seeds (8/8) | `run_instruction_sweep` seeds by per-variant `seed_slot`, not global `env_key` → variants share init-states (paired A/B), same instruction reproducible across rounds | `test_sweep_is_paired_and_position_invariant` |
| VLA chunk-buffer leak (7/8) | `VlaJepaPolicyClient.reset()`; `run_instruction_sweep` resets policy state at each sweep boundary | `test_sweep_resets_policy_state_between_runs` + client unit tests |
| Delete-only perturbation / dup emit (6/8 + polish) | `TemplatePerturbation` → true single-occurrence (Hamming-1) toggle; reachability limitation documented; `optimize_task` placeholder strategy flagged | docstring + needs H5 |
| Silent denominator shrinkage (4/8) | loud failure when graded-trial count ≠ spawned count | guard in `run_instruction_sweep` |
| `run_task_eval` fixed world name (3/8) | uuid7 suffix (matches the sweep) | — |
| VLA translation layer untested (6/8) | `tests/bench/test_vla_jepa_client.py`: gripper sign table, 8-dim state ordering, quat→axis-angle (identity/90°/clamp), pickle round-trip, chunk pop/refresh cadence, per-env buffers | 13 tests |
| Circularity not disclosed (8/8) | test module relabeled a **mechanism check**, not evidence | docstring |
| "Entirely in-process" overclaim (6/8) | `image.py` docstring: env is in-process, **policy is a cross-app RPC** | docstring |
| `eval_task` mislabeled "trustworthy numbers" (8/8) | relabeled an env-only zero-action plumbing smoke; return carries a `policy: none` note | docstring + return |
| Frames path mismatch (8/8) | `optimize_task` writes to `FRAMES_MOUNT` (necessary, not sufficient — commit still open) | partial |

## Gating DoD — must close before submission (open)

Ordered by the minimum path to a credible submission:

1. **GPU smoke first** — `modal run bench/libero/image.py` to confirm
   robosuite-1.4.1 imports/resets/steps under EGL on py3.12. If it fails, revise
   the modernization thesis before claiming any number.
2. **Resolve frames cross-container visibility** — co-locate the policy
   in-process (preferred; also closes the architecture overclaim) or
   `frames_volume.commit()` per step. Add a CI test asserting env-write path ==
   policy-read path.
3. **Wire a real policy into `eval_task`** (or keep it labeled env-only) so the
   baseline is a real number. Add a policy-routing test (priority-1 before
   priority-10, non-zero actions).
4. **Replace the placeholder perturbation** with a paraphrase/keyword-injection
   or LLM strategy (roadmap H5) whose reachable optimum can exceed base; assert
   measured lift > 0 from the ledger. Add torch/numpy seeding + deterministic
   flags on the in-process path.
5. **Break the circularity** — validate instruction→success on an objective the
   optimizer does NOT define: a real VLA on held-out LIBERO (and/or a learned /
   noisy black-box scorer).
6. **Produce ≥1 real LIBERO success-rate** (base vs optimized) and commit it as a
   **re-gradable artifact**: `(world_id, run_id)` + exact command + provenance
   manifest {SHAs, image digest, resolved deps, seeds, instruction trace}.
7. **Cross-check** ledger-graded success against LIBERO's `check_success` over a
   full per-suite horizon (roadmap A3); reconcile the single hardcoded
   `max_steps=520` against published per-suite horizons; read
   `EpisodeResult.terminated` so truncated trials aren't silently graded False.
8. **Scale to a defensible N** — un-optimized baseline + random-edit control +
   ≥1 alternative prompt-opt method, >1 suite/task, ≥20 seeds, per-variant
   success with bootstrap/Wilson CIs and a paired McNemar/bootstrap significance
   test; report run-to-run variance (mean ± std).
9. **Freeze the environment** — `>=` floors → `==`, pin LIBERO/VLA-JEPA SHAs +
   model revisions, commit a pip-freeze/image-digest lock.
10. **Docs** — write the Limitations/Threats section (below), correct every
    in-process overclaim, reconcile the `vla_jepa_worker` legacy-vs-required
    contradiction, add `libero-recipe.md` to the nav and the VLA contribution to
    the README; fix/remove the stale `docs/guide/autoresearch.md`.

## Testing & evals audit (flagged for careful examination)

**Current state.** 699 pytest tests collected (all under `tests/`; `bench/` not a
package, excluded from collection + the 70% coverage floor). Framework + the
CI-mechanism surface are genuinely well-tested with independent-replay oracles.
`evals/` (distinct from pytest) registers 20 tasks across 4 suites; `python -m
evals.run` passes 20/20. **But** the headline claim is validated ONLY by the
tautological stand-in, and **every** GPU/Modal path has zero tests and has never
executed (`import modal` fails in CI). `make ci` runs eval-reg only; the `spec`
suite is in `REQUIRED_SUITES` but never gates the build.

**Required new tests (DoD).**
- `test_vla_jepa_client.py` — **DONE this session** (13 tests).
- Frames-path equivalence (no GPU): env-write abs path == policy-read abs path
  under the `optimize_task` config.
- `eval_task` policy-routing: non-None policy runs priority-1 before priority-10
  and writes non-zero actions; None yields a labeled env-only smoke, not a rate.
- CPU import/signature smoke for `image.py`/`modal_worker.py`/`vla_jepa_worker.py`
  behind a stubbed `modal` (breaks on kwarg drift).
- `InProcessLiberoEnvClient` with fake robosuite+torch: proprio contract,
  `_write_frames` ref format, `seed % len(init_states)` selection, torch-load
  patch idempotency, obs-keys == what `VlaJepaPolicyClient` reads.
- Framed `_PolicyCaller` 10-arg path (CI covers only the no-refs branch).
- Determinism: `optimize_instruction` twice → identical trace + best; real path
  reports mean ± std over N seeds.
- Convert `e2e_*_ledger_smoke.py` into `requires_gpu`/`requires_modal`-marked
  tests (nightly GPU job) so their normative assertions gate the real path.

**Required new evals (DoD).**
- Register `vla_instruction_optimization` as a first-class pass@k eval through
  `optimize_instruction` (scripted stand-in in CI; real-VLA variant behind a
  GPU/credential gate through ONE code path) — closes roadmap A3/H5.
- A **non-circular** instruction-opt eval on an objective the optimizer doesn't
  define, with a documented threat-to-validity.
- Eval-service grading-correctness eval (lift the dogfood replay into a suite);
  ledger addressability (A2) regression; `ManipStatus.done` termination eval.
- Gate the `spec` suite in `make ci`; run real-VLA evals with `--trials > 1`.
- A cost-performance eval (wall-clock / GPU-seconds per graded trial; in-process
  vs per-trial-world) — "cost-performant" is a stated selling point with no eval.

## Threats to validity (for the paper's Limitations section)

- **Circularity** — the scripted proof is monotone in the optimized metric by
  construction; not evidence the effect transfers to a real VLA.
- **No empirical content** — no real LIBERO/VLA numbers exist at submission.
- **Search space** — token-toggle perturbation biases measured lift toward ≤0.
- **Confounds (now fixed in code, must be re-validated on the real path)** —
  paired seeds, leak-free buffers; re-confirm on GPU.
- **Underpowered** — n=3, single task/suite, no baseline/control/competing method.
- **Non-determinism** — floating dep floors, moving SHAs, unpinned model
  revisions, robosuite-1.4.1-on-py3.12 unverified, no torch/numpy seeding.
- **Architecture** — the VLA policy is an RPC to a worker the recipe marks
  "legacy — do not extend"; the in-process selling point does not hold for it.
- **Harness trust** — ledger-graded success never cross-checked against
  `check_success` over a full horizon.

## Polish (non-gating)

Cost-performance table or soften the "more cost-performant than anyone"
superlative; commit the Scarf (44s→1.6s) and table-handle-cache micro-benchmarks
with a CI perf guard; make `bench/` a package and include it in coverage; per-suite
control-step horizons; CI-run the credential-free examples; bound the
`seed % len(init_states)` aliasing; adversarial `instruction_quality` unit tests.
