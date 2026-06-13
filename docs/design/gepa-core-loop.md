# GEPA Core Loop — LIBERO-Para × VLA-JEPA

**Document type:** Core-loop specification. This is the *isolated* reference for
everything that touches the GEPA optimization and VLA-JEPA. Orchestration, infra,
ownership, and the day plan live in [`libero-para-day-plan.md`](./libero-para-day-plan.md).
If a decision is about *what the optimization does*, it belongs here. If it's
about *how we run it*, it belongs there.

Status: **contract locked 2026-06-13** (the make-or-break scorer gate cleared —
LIBERO exposes native subgoal predicates; see §5). Cleared to build.

---

## 1. Hypothesis

**Sufficiency:** test-time prompt optimization can *elicit latent VLA capability
that the default instructions under-measure*. The model may already be able to do
a task; the canonical phrasing just fails to unlock it.

The deliverable number is the **gap**:

> success(VLA-JEPA | fair default instruction)  →  success(VLA-JEPA | GEPA-optimized paraphrase)

That gap is "honest frontier − benchmarked frontier." It tests whether test-time
optimization techniques that work for coding agents (GEPA) **transfer to VLAs**.

Secondary framing: benchmarking a model on un-optimized prompts *under-measures*
it. Prompt optimization gives an **honest assessment** of the model's true
capability ceiling — a prerequisite to knowing whether training changes help.

---

## 2. What LIBERO-Para is

LIBERO-Para = LIBERO with the **language instruction made mutable**. The simulator,
tasks, objects, and success criteria are identical to LIBERO; only the text layer
the policy conditions on changes. So a vanilla LIBERO run *is* a LIBERO-Para run
at the identity paraphrase — we already know it runs (the end-to-end demo solved
`libero_spatial` task 0).

The **mutable knob is a single field**: `ManipTask.instruction`, which flows
through the policy caller to VLA-JEPA's `infer_refs(..., instruction=...)`.

**Fairness rule:** the baseline is the **raw LIBERO instruction**, never the empty
string. (The eval driver currently spawns `instruction=""`; that must become the
canonical task language for an honest baseline.)

---

## 3. The optimization target (GEPA candidate)

**Candidate = a global paraphrase *strategy*** (a small evolvable system prompt /
rewrite policy) that maps any raw LIBERO instruction → a grounded paraphrase.

- **Not** per-task memorized strings. Per-task strings make the held-out test set
  meaningless (you'd just re-optimize on it).
- A global strategy **transfers**, so the test number is real generalization —
  which is the entire point of the train/val/test discipline below.

This is the "small thing that trains." Everything else in GEPA (mutation, Pareto
selection) is deliberately naive and self-correcting; **only the feedback stage
makes it improve.**

---

## 4. Train / Val / Test split

No usable historical failure data exists (the only prior eval artifacts are the
internal regression suite + one smoke). So the split is produced by a **baseline
sweep**, not mined.

1. **Baseline sweep:** run a validated suite (start `libero_spatial`, 10 tasks)
   with the *raw* instruction, a few seeds each → per-task success rate.
2. **Val set = the hardest tasks** (lowest baseline success = most room to climb).
   This is "find the ones that fuck up." The reflector sees **only** val.
3. **Test set = held-out, distribution-matched** tasks. Never seen by the
   reflector. Measures whether the evolved paraphrase strategy generalizes.
4. Choose the hardest tasks deliberately so there is **room to climb** — a val set
   already at ceiling proves nothing.

Leakage guard: the reflector/mutator touches val rollouts only; test is evaluated
once, at the end, with the frozen strategy.

---

## 5. The SCORER (per-rollout feedback) — the crux

**This is the only part of GEPA that matters.** Hill-climbing is luck unless, for
any rollout, we can say *exactly where it failed, why, and the root cause.*

### Ground truth is natively available (gate cleared)

Verified on a live LIBERO env (`libero_introspect.py`):

- `env.env._eval_predicate(pred)` evaluates **arbitrary** predicates over the
  scene, per state — not just the goal. Confirmed returning
  `{["on","akita_black_bowl_1","plate_1"]: false}` at reset.
- `_check_success` = conjunction of `_eval_predicate` over `parsed_problem["goal_state"]`.
- `object_states_dict` exposes every **object and every named region**
  (`main_table_between_plate_ramekin_region`, `wooden_cabinet_1_top_region`, …).
- obs carries each object's pose **and eef-relative pose**
  (`akita_black_bowl_1_to_robot0_eef_pos`).
- `parsed_problem["obj_of_interest"]` names the task-relevant objects.

**Consequence:** even when a task's `goal_state` is one coarse predicate
(`on(bowl, plate)`), the scorer can **construct and evaluate intermediate
subgoals itself** — `grasped(bowl)`, `lifted(bowl)`, `in(bowl, between_region)` —
via the same native evaluator. Fine-grained root cause on any task.

### Substrate: an agentic REPL scorer (deterministic replay)

Rollouts are deterministic given `(init_state, action_sequence)`, both on the
ledger. So the scorer **replays** any rollout exactly and can restore MuJoCo state
at any tick. The append-only ledger *is* the replay substrate.

The scorer is a **multi-step agent** (dspy.RLM / ReAct Claude) in a Python REPL
with `mujoco`/`libero` + curated helpers:

```
replay_to(t)            object_states(t)       contacts(t)
eval_subgoals(t)        eef_to(obj, t)         frame(t, cam)
find_grasp_attempts()   eval_predicate(pred, t)
```

It investigates adaptively per failure mode (replay → find the tick a subgoal
*should* have flipped and didn't → render that frame → root cause), which is
strictly more powerful than handing it pre-distilled fixed features.

### Scorer output contract (per rollout)

```
{
  "pass": bool,                 # GROUND TRUTH from sim _check_success — never the LLM's opinion
  "subgoals": [                 # goal + constructed intermediate predicates
    {"predicate": [...], "satisfied": bool, "flipped_at_tick": int | null}
  ],
  "failure_phase": "approach|grasp|lift|transport|place|none",
  "root_cause": str,            # NL: what happened and why
  "key_frames": [tick, ...]     # ticks the human UI should render
}
```

Metric (`pass` + subgoal completion) and NL feedback are **always returned
together**; both flow to the mutator.

### Scorer properties (answered)

- **Smart:** yes, Claude-class — reasoning about manipulation failure.
- **Multimodal:** yes — renders & reads frames on demand.
- **Multi-step:** yes — this is where the agentic loop lives.
- **`pass` is sim ground truth, not LLM judgment.** The LLM produces diagnosis +
  constructed-subgoal reasoning only.

---

## 6. The MUTATOR (reflection → strategy evolution)

Reads the **batch** of scorer reports across val rollouts (cross-task, for a
global strategy) → proposes an improved paraphrase strategy. Deliberately simple:
the intelligence is in the scorer's reports, not here.

- Maintains a **Pareto frontier** of strategy candidates (per-task best preserved,
  not a single scalar-best) to keep diversity.
- One mutation per round; sample parents from the frontier; loop.

---

## 7. The loop

```
baseline sweep (raw instruction) ── pick val (hardest) + test (held-out)
        │
        ▼
  ┌─ evaluate current strategy on val  → rollouts on the ledger
  │         │
  │         ▼
  │   SCORER (agentic REPL) per rollout → {pass, subgoals, root_cause}
  │         │
  │         ▼
  │   MUTATOR reads batch → new paraphrase strategy → Pareto-keep
  └─────────┘   repeat K rounds (or until val plateaus)
        │
        ▼
  freeze best strategy → evaluate ONCE on test → report the gap
```

---

## 8. Reproducibility (non-negotiable)

Every rollout — baseline, val, test — is **already** recorded on the Archetype
ledger: `(init_state via seed, action_sequence, per-tick proprio/frames/status)`,
keyed by `(world_id, run_id)`. This is not something we add — it is the append-only
invariant. `destroy_world` is **in-memory teardown only; storage is preserved**
(verified in code + by the persisted episode table surviving destroyed worlds). The
scorer therefore **queries** any rollout's full trajectory by `(world_id, run_id)`
via the query service; replay is reconstructable from the persisted seed + actions.
Any rollout is exactly replayable and the whole study is packageable. The scorer's reports and
the mutator's strategy lineage are recorded alongside. The paper's numbers come
from queryable ledger data, not loose logs.

---

## 9. Human sanity-check UI

A minimal page rendering, per rollout: key frames + the subgoal trace + the
scorer's `pass`/root-cause. We verify the scorer is *correct* on a handful of
rollouts **before** trusting it to drive optimization. Scoring correctness is the
prerequisite to every downstream gain.

---

## 10. Open build-time item

Enumerate LIBERO's full predicate vocabulary (`on`/`in`/`grasp`/`up`/`open`/…) so
the scorer's constructed-subgoal helper is concrete rather than ad hoc.
