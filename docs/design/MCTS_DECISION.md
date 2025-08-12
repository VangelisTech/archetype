## MCTS Decision Document — Resolving Open Surgery Points

Scope: Binding decisions for implementation where the plan left degrees of freedom. These choices remove ambiguity and keep us within the 3–4 week timeline.

### Summary of Decisions
- Rollout serialization: RolloutPipelineProcessor (single GraphProcessor) with internal sequential steps; no GraphSystem changes.
- Priority widening: per-node PriorityList stored off-graph; computed at expansion time and treated static within a plan; lazy recompute optional.
- Simhash: 3-gram shingles over normalized text fields; 64-bit simhash; Hamming similarity ≥ 0.85.
- In-flight de-dup: async promise map keyed by request key; correlation IDs; broadcast completion or error to all waiters; deterministic merge order.
- Adaptive batch sizing: EWMA-based recent latencies; clamp with cooldown; K ∈ [K_min, K_max].
- Failure handling: retries + circuit breaker; conservative fallback; failed evals don’t increment visits; no backprop on failure; log and continue.
- Cache caps: byte-capped LRU with per-entry size accounting at insert; version fencing.
- Tombstones/alive: bounded_store keeps alive bitsets for nodes/edges; CSR built over alive subset; selection only reads CSR; compaction optional.
- Parent semantics: `parent[vid]` display-only; backprop strictly uses selected edge path; depth in algorithms = path length.
- Root stop: heuristic (non-IID) “root stop”; min visits; variance floor; optional re-judge; bounds logged as heuristics.
- Component integration: define `Action` Component schema; processors read `action__*` fields; conflict = last-writer-wins with policy hook.
- Worker pool: unified async interface over threadpool and Ray; send `StateSpec` (JSON) only; VL revert + resubmit on worker failure.
- Plateau: robust linear regression over last M points; hysteresis R; stop on sustained low slope.
- Free-list: no ID reuse; monotonic ids; tombstones only; optional offline compaction snapshot.
 - Invariants (alive subset): `V_alive == len(off)-1`, `E_alive == len(child_eids_alive)`, `g.vcount() ≥ V_alive`, `g.ecount() ≥ E_alive`; VL applied == VL reverted per tick.
 - Baselines defaults: Beam width=5; Bandit=UCB1‑Tuned with ε=0.05.
 - Cache caps defaults: LLM=512MB, Eval=256MB (configurable).
 - Time bucket default: 1 hour; configurable; disable if retrieval snapshots immutable.
 - Determinism: adaptive K recorded per tick in DecisionTrace; Replay enforces recorded K and selection order.
 - Depth cap/horizon: `max_depth=32` default; `rollout_horizon()` absolute by default.
 - Lambda calibration: one‑shot line search over {λ/2, λ, 2λ} in first N root iterations; record chosen λ.
 - Canary default: compares beam vs mcts over last N=10 ticks by tokens/point (configurable).

---

### 1) Rollout Processor Serialization — Decision
- Approach: Wrap the entire per-step rollout as a single GraphProcessor to guarantee sequential execution under GraphSystem without modifying it.
- Class:
```python
class RolloutPipelineProcessor(GraphProcessor):
    stage = UpdateStage.Update
    def __init__(self, steps: list[Callable[[DataFrame], Awaitable[DataFrame]]]):
        self._steps = steps
    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        for fn in self._steps:
            df = await fn(df)
        return df
```
- Enforcement: RolloutEngine constructs exactly one processor per rollout step (or a single multi-step pipeline) so GraphSystem sees no parallelism within a rollout.
- Test: Ensure adding more GraphProcessors does not interleave when RolloutEngine is in serialized mode.

### 2) Priority-Based Widening — Data Structures
- Store: a per-node `PriorityList` alongside NodeStats (off-graph):
  - arrays: `action_ids`, `priorities` (float32), `eligible_eids` (stable array of eids sorted by priority desc)
  - computed at action generation (expansion) using `priority = prior/(1+cost)^β*(1−similarity)^γ`.
- Selection: `eids = eligible_eids[v][:m(n)]` where `m(n)=k*n^α`; map `eids` → `child_vids` via eid lookup.
- Dynamic updates: treat priorities static within a planning session for simplicity; optional lazy-recompute hook if `est_cost/similarity` sources change.

### 3) Simhash — Specification
- Input: normalize text by lowercasing, stripping markup, compressing whitespace; extract 3-gram shingles.
- Hash: 64-bit simhash over shingles; Hamming similarity via `1 - (HammingDistance/64)`.
- Threshold: ≥ 0.85 considered near-duplicate; short-circuit judge on cache hit with this similarity.
- JSON states: if structured, extract semantically relevant text fields and keys; stable field order.
 - Similarity computation: Hamming similarity = `1 - (HammingDistance/64)`.

### 4) In-Flight De-dup (Promise Map)
- Map: `Dict[key, asyncio.Future]` owned by batch evaluator (tick‑scoped).
- Algorithm:
  - On request: if key in map: await future; else create future, place in map, start provider call.
  - On success: future.set_result(result); remove from map.
  - On failure: future.set_exception(err); remove from map; all waiters see same error.
  - Correlation IDs: assign monotonic `cid` per request batch; included in results and DecisionTrace; stable merge by input order, with (key, cid) as tie‑break.
  - Race: a duplicate arriving mid-call awaits the existing future.
  - Timeout: cancel provider task; set_exception(TimeoutError); propagate.
 - Defaults: retry backoff base=100ms, factor=2.0, max_attempts=5, jitter=20%; hedge at P95 of last 100 calls.

### 5) Adaptive Batch Sizing — EWMA Policy
- Maintain EWMA of p50 and p99 latencies with α=0.2 over the last N batches.
- Adjustment:
  - If EWMA_p99 > threshold: `K = max(K_min, K//2)`
  - Elif EWMA_p50 < threshold/4: `K = min(K_max, int(K*1.5))`
  - Cooldown: don’t adjust more than once per 3 batches to avoid oscillation.
 - Determinism: DecisionTrace records K per tick; ReplayMode reuses recorded K and enforces deterministic rollout completion order (sort by correlation_id).
 - Test/Replay: `adapt_rollouts=False` by default in reproducibility modes.

### 6) LLM/Judge Failure Handling
- Retries: exponential backoff (jitter), max_retries=3; retry on 429/5xx.
- Circuit breaker: open after ≥5 failures in 10s per provider/model; cooldown 30s; fallback to cheaper model if configured.
- Scoring on failure:
  - No visit increments and no backprop for that rollout.
  - Mark node edge with `eval_failed=True` and penalize its priority for subsequent selection.
  - If fallback succeeds: use fallback result; tag in trace.
  - If all fail: assign conservative value (e.g., prior mean − penalty) but do not backprop; log error.
 - Fallback policy order: cheaper model → cached similar state → beam baseline; configurable.

### 7) Cache Memory Management
- Per-entry size accounting at insert:
  - LLM: approx bytes = len(prompt) + len(output) * 2 + overhead(128B)
  - Eval: approx bytes = len(state_json) + 64 + overhead(64B)
- Cap by bytes (configurable); LRU by last access; evict until under cap.
- Version fencing: invalidate on judge/model/template version change.
 - Defaults: LLM=512MB, Eval=256MB; exposed via config.

### 8) Tombstones / Alive Masks — Ownership
- `bounded_store` maintains `alive_node[vid]` and `alive_edge[eid]` bitsets.
- Eviction sets tombstones (alive=False); no physical deletes in igraph.
- CSR is rebuilt over alive edges only; selection only traverses CSR, hence never sees tombstoned edges/nodes.
- Optional offline compaction can rebuild igraph/arrays to drop tombstones when paused.
 - Stats: add `NodeStats.alive: bool[]`, `EdgeStats.alive: bool[]` (default True). Eviction flips to False. CSR rebuild skips `alive=False` edges.
 - Invariants (alive subset): `V_alive == len(off)-1`, `E_alive == len(child_eids_alive)`, `g.vcount() ≥ V_alive`, `g.ecount() ≥ E_alive`.

### 9) Parent / Depth Semantics with Transpositions
- `parent[vid]` is display-only (e.g., first parent at creation).
- Backprop uses the explicit edge path produced by selection.
- Depth for horizon limits uses path length at selection time, not stored `depth[vid]`.
 - Annotate `NodeStats.parent` as "first parent (display/debug only)".

### 10) Root “Confidence” Stop — Heuristic
- Not statistically guaranteed (non-IID). We use empirical-Bernstein style bounds as a heuristic gate.
- Requirements: min visits n0=8 for top-2; variance floor ε_var=1e-3; optional double-judge recheck of top-2 before hard stop.
- DecisionTrace logs LCB/UCB and a `heuristic_root_stop=true` flag.
 - Mandatory holdout re‑judge: before executing a confidence stop, re‑judge top‑2 with holdout judge; if ranks disagree, continue sampling.

### 11) Component Integration — Action Schema
- Define `Action` as a Pydantic `Component`:
  - Fields: `action_id:int`, `priority:float`, `params_json:str|None`, `origin:str` (planner id), `tick:int`.
  - Processors read `action__action_id`, `action__params_json`.
  - Conflict policy: last-writer-wins by tick/time; optional hook to arbitrate.
 - DomainAdapter similarity source:
   - `def similarity_space(self) -> Literal["none","text-embedding-3-large"]` (default embedding name).
   - Cosine similarity normalized to [0,1]; persona/style tokens stripped before embedding if configured.

### 12) Worker Pool Abstraction
- Interface:
  - Request: `{cid, state_spec: StateSpec, seed}`
  - Response: `{cid, value, realized_cost, judge_version, cache_hit}`
- Threads: `asyncio.to_thread` wrapper; Ray: actor exposing same coro signature. Controller awaits a list of tasks uniformly.
- Serialization: send `StateSpec` (canonical JSON + fence); avoid sending DataFrames.
- Failures: on exception, revert VL for that path and resubmit (subject to breaker), or drop per failure policy.

### 13) Plateau Detection — Robust
- Maintain last M=20 samples of `(tokens_spent, best_root_score)`; fit simple linear regression (least squares). Optionally RANSAC if noisy.
- Defaults: M=20, ε=1e-3 (on normalized [0,1] score), R=3. Stop when slope < ε for R consecutive checks; log rolling slope.

### 14) Free List — Final Decision
- No ID reuse during planning: keep monotonic vid/eid; tombstones only.
- Memory bounded via caps; compaction snapshot optional offline.
- Removes CSR/array hole-management complexity and keeps indices stable.
 - Determinism: IDs monotonic; DecisionTrace does not need generation counters.

### Error Recovery Boundaries
```python
async def plan_with_recovery(self, root_state):
    try:
        return await self.plan(root_state)
    except InvariantViolation as e:
        self.emit_error_trace(e)
        return await self.fallback_policy(root_state)  # e.g., beam or cached best
```
- InvariantViolation: stop planning, emit trace, fallback.
- CSR rebuild memory error: reduce K and retry once; if persists, fallback.
- Worker crash: revert VL and resubmit or shrink K via adaptive policy.
 - Batch eval failure: propagate error to all waiters (promise map); apply fallback policy.

### Baseline Defaults
- Beam: width=5, same judge/rubric and shadow cost, token budget parity with MCTS average tick; deterministic seeds.
- Bandit: UCB1-Tuned, prior from action priors; deterministic seeds.
 - ε floor: 0.05 exploration floor.

### Implementation Notes
- CSR rebuild policy: default to full rebuild over alive edges (simpler, stable). Optional optimization via `parents_with_new` requires indirection to keep offsets stable; defer unless profiling demands.
- DecisionTrace logs only top-N (N=20) root edges to bound log volume.
- Time bucket default: 1 hour; if retrieval snapshots are versioned, the time bucket may be disabled.

### Go/No-Go Checklist (Delta)
- [ ] RolloutPipelineProcessor implemented and tested.
- [ ] PriorityList per-node with static priorities; selection by top-m(n).
- [ ] Promise map in batch evaluator with correlation IDs and error broadcast.
- [ ] Alive masks owned by bounded_store; CSR over alive subset only.
- [ ] Failure handling policy wired (retries, breaker, fallback; no-visit on failure).
- [ ] Cache byte caps enforced with LRU and version fencing.
- [ ] Adaptive K with EWMA and cooldown.
- [ ] Root stop clearly marked heuristic; min-visits + var floor.
- [ ] Plateau detection via regression + hysteresis.
- [ ] No free-list reuse; compaction optional.
 - [ ] DecisionTrace records K per tick, λ trials/chosen λ, and selection order; Replay enforces these.


