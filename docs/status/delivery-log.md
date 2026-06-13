# Delivery Log — LIBERO-Para × VLA-JEPA (+ RoboSemanticBench)

**Deadline: 5:00 PM PDT, 2026-06-13.** Append-only; committed + pushed on every
update so progress is trackable from the remote. Newest entry on top.

## Deliverables (must-ship)
- [ ] **D1 — Optimized-val demo**: success gap baseline → GEPA on a hard val slice. *(the headline; minimum win)*
- [ ] **D2 — Held-out test gap**: generalization, one frozen pass. *(strong-to-have)*
- [ ] **D3 — Demo video**: fail-under-default vs win-under-optimized + scorer readout.
- [ ] **D4 — Paper**: publishable by 5pm (hypothesis → method → results → repro).
- [ ] **D5 — Packaged reproducible ledger** artifact.
- [ ] **D6 — RoboSemanticBench** (Darin): TBD per official reqs.
- [ ] **OFFICIAL HACKATHON REQUIREMENTS** — *placeholder, to be filled in when provided.*

## Advance gate (the go/no-go to GEPA)
**Idempotent runs.** We move forward only when a rollout family is:
(1) **batched** (scaling table N=1/4/16/32 shows real batching, <1 s/step),
(2) **replay-deterministic** (replayed `env.check_success()` == ledger exactly),
(3) **cleanly ledgered** (canonical store partitioned by `(world_id, run_id)`; re-runnable, same inputs → same truth).
All three = idempotent runs = green light for GEPA. If any fails: STOP and fix before spending GEPA cycles.

## On-time assessment
**Realistic for D1 (the win) + D3/D4/D5.** D2 depends on batch throughput landing.
Risk: the whole timeline hinges on the WF1 scaling gate; if batching is real we are
*ahead*, if not we fix it before anything else. D6 is Darin's parallel lane.

---

## Log

### 2026-06-13 14:40 PDT — libero_10 = 0% but CAP-CONFOUNDED; probing libero_object
- **libero_10 probe (1 seed, cap 128): 0/9, every task hit the 127-step cap.** libero_10 = LIBERO-LONG (long-horizon, ~500 steps needed) → cap-128 forces 0% regardless of policy. CONFOUNDED, not a capability floor. A true read needs cap ~512 (4x slower episodes → slow GEPA rounds).
- **Suite hunt:** spatial=96.7% (ceiling), libero_10=confounded-floor. Want a SHORT-horizon suite at a MIDDLE score (~40-80%) — room to climb AND fast episodes for many GEPA rounds.
- **Firing libero_object probe** (1 seed, cap 256). libero_goal next if needed. (libero_10 viable only at cap~512 — slow; deprioritized.)

### 2026-06-13 14:20 PDT — ⚠️ SUITE PIVOT: libero_spatial is at ceiling (96.7%)
- **Baseline result (real, on Modal):** libero_spatial raw-instruction VLA-JEPA = **29/30 = 96.7%**; only task4 sub-100%. **No headroom → GEPA cannot show a gap here.** Must pivot to a harder suite (libero_object / libero_goal / libero_10 / libero_90). The libero_spatial split is moot.
- **Next action (proposed):** data-driven headroom probe across libero_object/goal/10 (few seeds, batched) → pick the suite with the most room → freeze immutable split there → GEPA.
- **Substrate built & mostly verified** (workflow wxtim2n1j, 5 branches): para-sweep (instruction fix + GPU max_containers 1→8 + env 1→16 + baseline_sweep, ran live), para-query (ledger read by world_id/run_id, verified on real 80-tick rollout), para-replay (deterministic O(1) replay, ran live), para-vocab, para-scorer (mock only; needs anthropic-api-key for live — now available). 2 small audit fixes pending: para-query strip 3 out-of-scope files; para-replay restrict eval_subgoals to native `_eval_predicate` (grasp isn't a registered predicate).
- **WF1 (woyq6k1ej) still running:** its scaling table (throughput go/no-go) is still wanted; its libero_spatial baseline is now confirmatory/redundant.

### 2026-06-13 14:03 PDT — observability triage + logfire secret
- **WF1 in progress:** S2 (batched env reset) + S3 (batched GPU infer_refs_batch) built & pushed; `wf1-scale` has the finalized batched run_cell + scaling_probe. PENDING: scaling table (go/no-go), replay-determinism, baseline+split. No completion yet.
- **Observability scope decision:** logfire on Modal workers = DO (highest leverage); Daft logging = fold into the same `@modal.enter` configure; **hosted Daft dashboard = DEFER** (monitor.py already gives a self-refreshing web dashboard; hosting Daft's is real plumbing, momentum risk).
- **Done:** created the `logfire` Modal secret (token verified). **Queued:** worker logfire instrumentation rides ON TOP of the merged S2/S3 workers (same files, avoid conflict) — spans on env.step_batch / policy.infer_refs_batch with attrs run_name/run_id/suite/task_id/seed/batch_size.
- **Monitor live tool:** `bench/libero/monitor.py` (query-service reads, --watch/--html) ready; populates once baseline writes the ledger.

### 2026-06-13 13:38 PDT — WF1 fired (trimmed), advance-gate = idempotent runs
- **Deletion pass applied.** WF1 reduced to: batch env [S2] + batch GPU [S3] → scaling table (HARD GATE) → baseline → immutable split manifest → replay determinism → report. Cut from WF1 → deferred to GEPA workflow: programmatic scorer + subgoal/Toy B-C, Pareto search (→ bounded top-k), tiny strategy genome (prefix · paraphrase rule · failure-recovery suffix · maybe temp/ensemble).
- **WF1 running** (workflow `woyq6k1ej`, Opus builders + Codex audits). First output will be the N→sec/step scaling table + the frozen `libero_spatial` val/test split.
- **Locked invariants:** arm = `ManipTask.run_name`; `run_id` = canonical uuidv7; world=(suite,task); entity=seed (tag); pure service layer; one canonical store; `pass` = sim `env.check_success()`, never LLM; baseline = raw task language.
- **Next:** scaling table + split land → if idempotent-runs gate is green, fire GEPA (WF2).
