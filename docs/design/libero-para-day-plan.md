# Day Plan & Orchestration — Hackathon Sprint

**Document type:** Orchestration + big-picture plan. The optimization itself
(everything JEPA/GEPA) is specified separately in
[`gepa-core-loop.md`](./gepa-core-loop.md). This doc is *how we run the day*:
architecture, parallel workstreams, ownership, timeline, and deliverables.

**Hard deadline: paper publishable by 5:00 PM + demo video with materials.**

---

## 1. North star

Demonstrate that **GEPA-style test-time prompt optimization improves VLA-JEPA
grounding on LIBERO-Para** — measured as a success-rate gap from the fair-default
instruction to the optimized paraphrase strategy, generalizing to a held-out test
set. Everything reproducible and packageable in Archetype format.

Two benchmarks in flight (decoupled, parallel):
- **LIBERO-Para** (this team) — the primary GEPA demonstration.
- **RoboSemanticBench** (Darin) — second benchmark, same tight-loop pattern, own
  branch off `main`.

---

## 2. Architecture (the tight loop)

```
        Modal (one region, co-located)
  ┌───────────────────────────────────────────────┐
  │  Archetype driver  (py3.12, world loop+ledger) │  ← co-located runner
  │        │ EnvClient            │ PolicyClient    │
  │        ▼                      ▼                 │
  │  archetype-libero-env    archetype-vla-jepa     │
  │  (LIBERO/robosuite/      (L40S, VLA-JEPA        │
  │   MuJoCo, frames vol)     websocket server)     │
  │                                                 │
  │  end-of-rollout ──▶ Reflection agent (REPL,     │
  │                     mujoco/libero libs, replay) │
  └───────────────────────────────────────────────┘
        rollouts + scores + strategy lineage → Archetype ledger (reproducible)
```

- Archetype on Modal = the tight loop (in-region calls, not Mac↔cloud).
- Rollouts orchestrated by Archetype; **all data on the append-only ledger**.
- At the end of each rollout batch, spin up a **reflection agent** inside the
  Modal environment (robotics libs present) to do REPL-based scoring (see
  core-loop §5).

---

## 3. Workstreams (parallel) & ownership

| # | Workstream | Owner | Output | Branch |
|---|---|---|---|---|
| WS1 | **Tight loop on Modal**, merged to main (throughput pass: per-episode volume commit + `env_key` batching → <1 s/step) | Claude | mergeable tight loop | `main` via PR |
| WS2 | **GEPA core loop** — baseline sweep, split, mutator, loop | Claude | runnable optimizer | `everettVT/libero-para-gepa` |
| WS3 | **Reflection agent** — agent in the Python REPL loop, integrated into Archetype end-of-rollout (Darin has most of the code) | Darin + Claude | the scorer | `everettVT/libero-para-gepa` |
| WS4 | **RoboSemanticBench** — second benchmark on the tight-loop pattern | Darin | second result | own branch off `main` |
| WS5 | **Query persisted rollouts** by `(world_id, run_id)` via the query service to feed the scorer. *(Data is already persisted — `destroy_world` is in-memory-only, append-only invariant verified. No "don't destroy" change; destroy/fork are fine. The only orchestration gap was the eval driver not querying the trajectory back.)* | Claude | scorer reads ledger | `everettVT/libero-para-gepa` |
| WS6 | **Human sanity-check UI** — per-rollout frames + subgoal trace + scorer verdict | Claude | review page | `everettVT/libero-para-gepa` |
| WS7 | **Reproducibility & packaging** — session/data packaging in Archetype format | Claude | packaged artifact | `everettVT/libero-para-gepa` |
| WS8 | **Paper + demo video + materials** | all | publishable paper, video | `everettVT/libero-para-gepa` |

Branch strategy: **iterate fast on `everettVT/libero-para-gepa`** (don't gate on
merge-to-main). Only WS1 (the tight loop) merges to `main` so RoboSemanticBench
has a stable base. Everything else stays on the dedicated branch until the end.

---

## 4. Critical path & sequencing

```
WS5 persist-trajectory ──┐
WS1 throughput pass ─────┼──▶ WS2 baseline sweep ──▶ pick val/test ──▶ WS2+WS3 GEPA rounds ──▶ test eval ──▶ WS8 paper/video
WS3 reflection scorer ───┘                                    │
WS6 sanity UI ───────────────────(verify scorer before trusting)┘
```

- **Gate before any GEPA round:** the scorer must be verified correct on a handful
  of rollouts via the sanity UI (WS6). Garbage feedback = wasted compute.
- WS4 (RoboSemanticBench) runs fully in parallel, decoupled.

---

## 5. Timeline to 5:00 PM (adjust as we go)

| Block | Target |
|---|---|
| now → +45m | WS5 persist-trajectory + WS1 throughput pass; WS3 scorer skeleton; WS4 Darin bootstraps off main |
| +45m → +90m | Baseline sweep (libero_spatial) → val/test split; scorer verified on real rollouts via WS6 UI |
| +90m → +3h | GEPA rounds on val; watch val climb; RoboSemanticBench first numbers |
| +3h → +4h | Freeze strategy → test-set eval → final numbers; package data (WS7) |
| +4h → 5:00 | Paper writeup from ledger numbers; demo video + materials |

If throughput or scorer slips, **scope down the val set** (fewer hard tasks),
never the rigor of the scorer.

---

## 6. Reproducibility & packaging (requirement)

- **All runs and all data in Archetype format** — rollouts, scores, and strategy
  lineage on the append-only ledger; any rollout exactly replayable from
  `(init_state, action_sequence)`.
- The whole study **packageable at the end** (one artifact: ledger parquet +
  frame volume manifest + strategy lineage + the two design docs).
- Paper numbers are computed from queryable ledger data, not loose logs.
- Modal images SHA-pin LIBERO and VLA-JEPA (already done) for environment repro.

---

## 7. Paper + demo video (WS8)

- **Paper skeleton:** hypothesis (latent capability / honest frontier) → method
  (GEPA × LIBERO-Para, the scorer contract) → setup (split, fairness baseline) →
  results (val climb + test generalization gap) → reproducibility (Archetype
  ledger) → second benchmark (RoboSemanticBench) as corroboration.
- **Demo video:** the tight loop running on Modal; a side-by-side of a task
  failing under the default instruction and succeeding under the optimized
  paraphrase; the scorer's root-cause readout; the sanity UI.
- **Materials:** the two design docs, the packaged ledger, the success/failure
  rollout clips.

---

## 8. Decisions already locked (don't relitigate)

- GEPA candidate = global paraphrase strategy (transferable). [core-loop §3]
- Baseline = raw LIBERO instruction, not empty. [core-loop §2]
- Scorer = agentic REPL, deterministic replay, native subgoal predicates. [core-loop §5]
- `pass` = sim ground truth, never LLM opinion. [core-loop §5]
- Data is already persisted (append-only); scorer queries rollouts by `(world_id, run_id)`. `destroy_world` is in-memory-only — verified. No "don't destroy" change. [WS5]
- Fast iteration on `everettVT/libero-para-gepa`; only the tight loop merges to main.

---

## 9. People / access

- Darin (`darinkishore`, they/them) — added as a write collaborator (invite
  pending acceptance). Owns RoboSemanticBench + the reflection-agent code.
