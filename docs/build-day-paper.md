# A Ledger-Native, Massively-Parallel Harness for Conditioning-Space Optimization of Frozen Vision-Language-Action Policies

*Gradient-free instruction optimization of VLA-JEPA on LIBERO, made cheap and exactly reproducible with the Archetype ECS ledger.*

**Claude Build Day — Anthropic × Cerebral Valley, 2026-06-13.**
Team: Vangelis Technologies. Repo: `github.com/VangelisTech/archetype` (public).

> **Status:** method + system shipped and running; empirical cells are landing as
> batched rollouts complete. Tables marked **(filling)** are populated from the
> live ledger; see `bench/libero/out/dataset/` for the queryable artifact.

---

## Abstract

Recent work shows the *conditioning input* of a frozen vision-language-action
(VLA) policy is an optimizable variable for deployment-time gains: TTT-VLA adapts
a learned latent prompt by self-supervised gradient descent, and VLA Grounder
trains an upstream language policy with reinforcement learning to emit better
commands for a black-box VLA. Both establish the lever — a frozen action expert
is steerable through its conditioning string — but both are **gradient-based**,
both require a bespoke simulator stack (SAPIEN/ManiSkill, π0.5/OpenVLA), and
neither produces a reusable, exactly-reproducible record of the optimization
loop. We contribute a **systems** result: an ECS-ledger-native harness
(*Archetype*) that runs this class of study as **batched, append-only-logged,
fully-queryable rollouts** — every trial keyed by `(world_id, run_id, entity_id,
tick)` and replayable from `(init_state, action_sequence)`. On this harness we
run **gradient-free, black-box** reflective instruction optimization (GEPA) of a
frozen VLA-JEPA policy on LIBERO, with no weight access of any kind. The harness
turns an optimization budget that costs the prior works hundreds of thousands of
RL environment-steps (≈42 A100-hours/task) or a custom-trained latent head into a
few hundred batched rollouts whose every step is on a reproducible ledger. We
report the default→optimized instruction success gap and the rollout budget, and
release the queryable dataset and the harness.

---

## 1. Introduction

A vision-language-action model is usually treated as an end-to-end policy
conditioned on a fixed natural-language task description. In practice the policy's
behavior depends sharply on *how* the instruction is phrased — so the instruction
is not merely a label but an **optimizable conditioning input**. Two 2026 papers
make this concrete for frozen policies:

- **TTT-VLA** optimizes a *latent* soft-prompt by gradient descent through a
  frozen π0.5 backbone at test time, and finds gains arise "primarily from
  correcting a small number of critical decisions rather than globally altering
  policy behavior."
- **VLA Grounder** keeps the VLA frozen and *black-box*, and trains an upstream
  guiding VLM with RL (GRPO/LoRA) to emit a better-grounded command, with
  hidden-state probe evidence that the mechanism is grounding-*access*, not new
  motor competence.

Both are real results — and both depend on a heavyweight stack: a SAPIEN/ManiSkill
simulator (Vulkan rendering, net-new to most groups), a specific large VLA
(π0.5 / OpenVLA-7B), and either per-environment gradient training (TTT-VLA:
800–2000 transitions + 500–1000 optimization steps, 8×H100, 15–30 min/env) or
sample-hungry RL (VLA Grounder; the comparable published recipe needs ≈0.4–0.8M
env-steps and ≈42 A100-hours *per task*). None of them emits a reusable,
exactly-reproducible record of the optimization loop.

**Our contribution is a systems one.** We show that the *same genre* of study —
conditioning-space optimization of a frozen VLA — can be run on an
**ECS-ledger-native, massively-parallel, exactly-reproducible** harness, and we
do it with a **gradient-free, black-box** optimizer on a stack that already runs
end-to-end (LIBERO + VLA-JEPA on Modal). The optimizer (GEPA) is an
implementation choice, not the novelty; the **harness and its reproducibility
guarantees** are.

### 1.1 Contributions

- **C1 — A ledger-native, batched, reproducible harness (primary).** Every
  rollout is an entity in an Archetype world; a single batched tick advances *all*
  seeds of a task together (one `env.step([N],[N])` + one policy forward), and
  every step is committed to an append-only store keyed by `(world_id, run_id,
  entity_id, tick)`. Any rollout is queryable through the Archetype query service
  and replayable from `(init_state, action_sequence)`. This makes the study
  cheap, parallel, and bit-for-bit auditable.
- **C2 — Gradient-free, black-box instruction optimization on a live stack.** We
  optimize the frozen VLA-JEPA policy's natural-language instruction via GEPA's
  reflective evolution — no gradients, no weight access, no simulator port. The
  optimized object is human-readable and auditable.
- **C3 — An honest-frontier reading (secondary).** We report the
  default→optimized instruction success gap as a measure of how much the default
  phrasing *under-elicits* a fixed policy, with `pass` defined as simulator
  ground truth (never an LLM judge).

### 1.2 What we built during Build Day *(judging-rule disclosure)*

Built today, original to this event:
- the **batched rollout primitive** (`eval_driver.run_episode_batched`) — N seeds
  as N entities in one world, one batched `env.step` per tick;
- the **co-located Modal harness** orchestration for batched fan-out across tasks
  (`baseline_sweep.py` + the env/policy workers as deployed apps);
- the **query-service dataset/packaging tool** (`prepare_dataset.py`) and live
  monitor (`monitor.py`) that read the ledger only through the Archetype service;
- this paper, the design docs, and the reproducible dataset artifact.

Pre-existing (brought in, per the rules): the Archetype ECS framework itself, the
VLA-JEPA model, and the LIBERO/robosuite simulator.

---

## 2. Hypothesis

> **H.** The natural-language instruction is an optimizable, black-box
> conditioning input for a frozen VLA. Gradient-free reflective optimization
> (GEPA) of the instruction string raises a frozen VLA-JEPA policy's LIBERO task
> success over the default phrasing — with zero weight updates and no model
> internals — and an ECS ledger makes the optimization loop cheap, massively
> parallel, and exactly reproducible.

This is conditional and falsifiable: it holds only where the policy has headroom
and the failure is grounding-related (not pure long-horizon execution). A null
result — no legitimate rewrite beats the default — is itself an honest finding
about that suite/checkpoint, never a trigger to train.

### 2.1 Falsifiable predictions

- **Confirms.** An optimized instruction raises success above the default with
  non-overlapping bootstrap CIs, on a held-out split.
- **Confirms-mechanism.** The improvement concentrates in failure→success flips
  at the grounding decision while gross motor competence is unchanged — mirroring
  TTT-VLA's "few critical decisions." *(We test this directly; we do not inherit
  it — TTT-VLA's critical decisions were motor-timing, ours would be semantic.)*
- **Confirms-efficiency.** GEPA reaches its best held-out gap in dramatically
  fewer sim rollouts than a matched gradient/RL baseline (GEPA reports up to 35×
  fewer rollouts than GRPO on text tasks; we test whether that transfers to
  expensive sim episodes — it is a hypothesis, not an inherited property).
- **Kills.** No legitimate rewrite beats the default beyond CI overlap → the
  default is not under-eliciting on this suite/checkpoint (honest null).
- **Kills-as-artifact.** A length-matched neutral paraphrase moves success as
  much as the optimized rewrite → the effect is generic input perturbation, not
  grounding.

---

## 3. System: the Archetype harness *(the contribution)*

```
        Modal (one region, co-located)
  ┌───────────────────────────────────────────────┐
  │  Archetype driver (py3.12, world loop + ledger) │  one Function per (suite,task)
  │      │ EnvClient            │ PolicyClient       │  via .starmap (World axis)
  │      ▼                      ▼                    │
  │  archetype-libero-env   archetype-vla-jepa       │
  │  (robosuite/MuJoCo)     (L40S, VLA-JEPA server)  │
  └───────────────────────────────────────────────┘
       every rollout → append-only ledger (world_id, run_id, entity_id, tick)
```

**Batched tick = the core primitive.** A task's N seeds are spawned as N
*entities* in one world. Each tick, the proven processors issue exactly one
batched environment call — `env.step([N env_keys],[N actions])` — and one policy
forward over the live entities, then commit all N rows. Within an episode the tick
sequence is genuinely coupled (step *t+1* needs *t*); across seeds nothing is
coupled, so it is all data and batches into one tick. Cross-task parallelism is
the Modal `.starmap` (World axis); per-task batching is the Entity axis. We
verified the batched path end-to-end: two seeds of a task share a single batched
world wall-time (one episode duration, not two).

**Reproducibility by construction.** The store is append-only and keyed by
`(world_id, run_id, entity_id, tick)`; `run_id` is an immutable uuidv7 and the
optimization arm is a queryable component field (`run_name`), so `run_id` is never
overloaded with semantics. Every rollout is queryable through the Archetype query
service (`AsyncQueryManager.query_archetype(sig, world_id, run_id)`) — we never
read raw files — and replayable from `(init_state, action_sequence)`. `pass` is
the simulator's own `check_success`, computed from the ledger, never an LLM
opinion.

**Cost contrast.** TTT-VLA: 800–2000 transitions + 500–1000 gradient steps,
8×H100, 15–30 min *per environment*. VLA Grounder–class RL: ≈0.4–0.8M env-steps,
≈42 A100-hours *per task*. Our harness runs the comparable study as a few hundred
batched rollouts, every one logged and replayable.

---

## 4. Method: gradient-free instruction optimization

We optimize a **single global** instruction-rewrite strategy applied identically
to every task instance and frozen before the held-out pass, using GEPA's
reflective evolution: candidate rewrites are mutated from natural-language
reflection over per-rollout reports, and a Pareto/top-k frontier selects
survivors. The LLM is used **only** for diagnosis and mutation — never as the
success judge. The downstream VLA-JEPA policy is **frozen** and treated as a
**black box**: GEPA needs no gradients and no access to the policy's weights or
activations, only the instruction string fed to `set_language()` and the
simulator's pass/fail.

**Anti-gaming protocol.** Rewrites are content-preserving and answer-blind: they
may re-ground the task description but may not encode the solution. We bracket a
legitimate gain between a length-matched neutral-paraphrase *floor* and an
answer-encoding oracle *ceiling*, and evaluate on a held-out split, so any gap is
attributable to better grounding rather than perturbation or leakage.

---

## 5. Related work

Our work sits in a recent line treating the conditioning input of a frozen VLA as
the optimizable variable, rather than updating the action policy. **TTT-VLA**
(Test-Time Latent Prompt Optimization) makes a learnable latent soft-prompt `z`
the interface and adapts it per environment via self-supervised gradient descent
through a frozen π0.5 backbone (`z ← z − η∇_z L_proxy`), reporting consistent
SimplerEnv gains that arise "primarily from correcting a small number of critical
decisions." **VLA Grounder** (Language-Conditioning Space Optimization for
Black-Box VLA Models) moves the optimizable variable into natural language,
training an upstream guiding VLM with GRPO/LoRA to emit a better-grounded command
for a frozen black-box VLA, with probe evidence that the mechanism is
grounding-*access*, not new motor skill. Notably, **VLA Grounder itself benchmarks
GEPA** as a frozen-VLA command optimizer (it reports GEPA competitive with, though
below, its GRPO on VL-Think) — so *reflective prompt evolution on a frozen VLA is
not itself novel*, and we do not claim it. **GEPA** (Agrawal et al., 2025)
establishes that gradient-free reflective prompt evolution can outperform RL while
using up to 35× fewer rollouts on text tasks.

Both robot papers establish the lever our hypothesis depends on — a frozen action
expert is steerable through its conditioning string, and the corrections are few,
local, and grounding-driven — but both are gradient-based, both require a bespoke
SAPIEN/ManiSkill + π0.5/OpenVLA stack, and **neither emits a reusable,
exactly-reproducible record of the optimization loop**. Our distinct contribution
is the **ledger-native, batched, reproducible harness** that runs this study as
queryable append-only rollouts, demonstrated with a gradient-free black-box
optimizer on a stack (LIBERO + VLA-JEPA) that runs end-to-end without a simulator
port — plus the discipline that `pass` is simulator ground truth and a null gap is
an honest finding.

| Axis | TTT-VLA | VLA Grounder | **Ours** |
|---|---|---|---|
| Optimizer | gradient TTT (backprop→latent) | RL (GRPO/LoRA) | **gradient-free reflective (GEPA)** |
| Access needed | white-box | reward + sim | **black-box (string + pass/fail)** |
| Optimized object | opaque latent `z` | NL command | **NL instruction (auditable)** |
| Sample cost | 800–2000 transitions/env, 8×H100 | ≈0.4–0.8M env-steps, ≈42 A100-h/task | **hundreds of batched rollouts** |
| Reproducible loop | — | — | **append-only ledger, replayable** |
| Claim | improvement | improvement | **harness + honest-frontier gap** |

---

## 6. Experimental setup

- **Policy:** VLA-JEPA (Qwen3-VL-2B + V-JEPA2), frozen, served on Modal L40S via
  the deployed `archetype-vla-jepa` worker. No weights touched.
- **Env:** LIBERO suites on robosuite/MuJoCo, deployed `archetype-libero-env`
  (`LiberoEnvBatch`, N envs per container, batched `step`).
- **Standard LIBERO is saturated — used only to validate the harness.** VLA-JEPA
  reports ≈97.2% average across all four standard suites (spatial 96.2%, object
  99.6%, goal 97.2%, long/LIBERO-10 95.8%); there is no instruction headroom
  there. We run them only to confirm our batched Archetype harness **reproduces
  the published numbers** (a systems-validity check), not as the main experiment.
- **The headroom — and the exact match for our lever — is LIBERO-Plus.**
  LIBERO-Plus (arXiv 2510.13626) is a standardized robustness *superset* of LIBERO
  on the **same robosuite/MuJoCo base** (no SAPIEN), with ~10k tasks across seven
  controlled perturbation dimensions. Its **Language** dimension rewrites the
  instruction while keeping the scene and goal predicate byte-identical, and
  VLA-JEPA drops to **85.4%** — ≈12 points of *instruction-induced* headroom that
  is, by construction, recoverable by re-grounding the instruction. This is the
  perturbed-instruction setting in standardized, citable form (not a hand-rolled
  perturbation).
- **Primary experiment — LIBERO-Plus *Language Instructions*.** Baseline = the
  benchmark's perturbed instruction (≈85.4%); GEPA re-grounds it (answer-blind,
  content-preserving); we measure **recovery toward the unperturbed ≈97% ceiling**.
- **Stretch — non-language dimensions.** Camera (63.3%), Noise (66.3%), Robot-init
  (67.1%) offer 30+ points; we test whether instruction *grounding cues* (spatial /
  object descriptors) help a frozen policy survive *non-language* perturbation —
  the VLA-Grounder grounding-access mechanism. High risk, high reward.
- **Metric = success rate** (sim `check_success`), per task and dimension, with
  seed/episode bootstrap CIs. Optimization is on a val split; the frozen strategy
  is evaluated once on a held-out test split.

---

## 7. Results **(filling)**

**Headroom map** (default instruction, frozen VLA-JEPA):

| Suite | Horizon | Default success | Headroom for instruction opt? |
|---|---|---|---|
| libero_spatial | short | 96.7% | none (ceiling) |
| libero_object | short | 100% | none (ceiling) |
| libero_goal | medium | **(filling — probe `goal-headroom-batched`)** | TBD |
| libero_10 (LONG) | long (~520) | **(filling — probe `libero10-longhorizon-batched`, cap 700)** | TBD (execution-bound risk) |

**Main result — default → optimized instruction** *(to fill on the selected
headroom suite):*

| Arm | Instruction | Success (val) | Success (held-out test) |
|---|---|---|---|
| A — default | raw task language | (filling) | (filling) |
| B — GEPA rewrite (answer-blind) | optimized | (filling) | (filling) |
| C — oracle ceiling (answer-encoding) | upper bound | (filling) | (filling) |
| D — neutral paraphrase (floor) | length-matched | (filling) | (filling) |

Legitimate gap requires **A < D < B < C** on the held-out split.

**Efficiency — rollouts to best held-out gap** *(to fill):* GEPA budget vs a
matched gradient/RL baseline; the prior works' published budgets (≈0.4–0.8M
env-steps; 8×H100 15–30 min/env) are the external reference.

All numbers above are computed from the queryable ledger (`bench/libero/out/dataset/`).

---

## 8. Reproducibility & artifact

- **Queryable dataset:** `bench/libero/out/dataset/` — `rollouts.parquet` /
  `.csv` (one row per rollout: run_id, suite, task_id, trial_idx, seed, success,
  episode_length), `summary.csv`, and `DATASHEET.md` documenting schema,
  provenance `(world_id, run_id)`, and a snippet to re-query any rollout's full
  trajectory through the Archetype query service. Produced by
  `bench/libero/prepare_dataset.py`, which reads the ledger only through the
  service.
- **Replay:** any rollout reconstructs deterministically from its
  `(init_state, action_sequence)` on the ledger.
- **Environment pinning:** Modal images SHA-pin LIBERO and VLA-JEPA.

---

## 9. Limitations

- The headroom suite must be selected empirically; on ceiling suites the gap is
  necessarily ~0, and on pure long-horizon suites the failure is execution, which
  instruction optimization cannot fix. We report where the gap exists and where it
  does not.
- The "few critical decisions" mechanism is established for *motor-timing* in
  TTT-VLA; whether it recurs for *semantic/grounding* selection is something we
  test, not assume.
- GEPA's 35×-fewer-rollouts efficiency is a text-task result; transfer to
  expensive sim episodes is a hypothesis under test here.
- We do not claim GEPA-on-frozen-VLA as novel (VLA Grounder benchmarks it). The
  novelty is the reproducible, batched, ledger-native harness.

---

## 10. Conclusion

Conditioning-space optimization of frozen VLAs has been shown to work with
gradients (TTT-VLA) and with RL (VLA Grounder), each on a heavyweight bespoke
stack and without a reusable record of the loop. We contribute the missing
*systems* piece: an ECS-ledger-native, massively-parallel, exactly-reproducible
harness that runs the same study as queryable append-only rollouts, demonstrated
with a gradient-free, black-box optimizer on a stack that runs end-to-end today.
The optimizer is swappable; the reproducible ledger is the point.
