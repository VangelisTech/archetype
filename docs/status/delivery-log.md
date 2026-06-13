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

### 2026-06-13 13:38 PDT — WF1 fired (trimmed), advance-gate = idempotent runs
- **Deletion pass applied.** WF1 reduced to: batch env [S2] + batch GPU [S3] → scaling table (HARD GATE) → baseline → immutable split manifest → replay determinism → report. Cut from WF1 → deferred to GEPA workflow: programmatic scorer + subgoal/Toy B-C, Pareto search (→ bounded top-k), tiny strategy genome (prefix · paraphrase rule · failure-recovery suffix · maybe temp/ensemble).
- **WF1 running** (workflow `woyq6k1ej`, Opus builders + Codex audits). First output will be the N→sec/step scaling table + the frozen `libero_spatial` val/test split.
- **Locked invariants:** arm = `ManipTask.run_name`; `run_id` = canonical uuidv7; world=(suite,task); entity=seed (tag); pure service layer; one canonical store; `pass` = sim `env.check_success()`, never LLM; baseline = raw task language.
- **Next:** scaling table + split land → if idempotent-runs gate is green, fire GEPA (WF2).
