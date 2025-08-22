## Comprehension‑First MCTS for Archetype — Single Source of Truth

This document defines the full implementation plan for extending Archetype with an operable, debuggable, and measurable MCTS planner for LLM domains. It is designed so another engineer/model can implement end‑to‑end from this spec.

### Objectives
- Build an MCTS engine that:
  - Uses `igraph` for topology, CSR arrays for hot adjacency, NumPy arrays for stats.
  - Executes hypothetical rollouts through `@graph/GraphSystem` (pure DataFrame transforms).
  - Is bounded, deterministic, observable, and easy to debug.
- North‑star KPI: tokens per quality point (judge‑versioned).
- Operating principles: batch everything, use shadow (uncached) cost in selection, bounded memory, confidence stop at root, holdout judge before finalize, feature‑flag ablations, single‑writer controller, pure rollouts.

### Core constraints and seams
- No breaking changes to `AsyncWorld`, `AsyncSystem`, or `@graph/GraphSystem`.
- Integration seam:
  - Preferred: dedicated `AsyncProcessor` that triggers planning and emits an Action component for downstream processors.
  - Optional: feature‑flagged pre‑step planning hook in `AsyncWorld.step`.
- Pydantic everywhere for runtime config; state artifacts as `Component` subclasses (LanceModel-based).
- Rollouts must be serialized (no parallel processor writes within a rollout step).

---

## Architecture overview

- Topology spine: `igraph.Graph` (directed) with vertex ids (vid), edge ids (eid). Authoritative for parent/child structure and multi‑parent transpositions. Graph is append‑only; logical deletions use tombstones (alive masks).
- Hot adjacency: CSR mirror (`child_off`, `child_vids`, `child_eids`) rebuilt per commit batch with prefix‑sum; RCU versioning for readers. CSR is built over the alive subset (see Invariants).
- Hot stats: NumPy arrays keyed by vid/eid:
  - Node: visits, value_sum, virtual_loss, parent, depth, expanded.
  - Edge: prior_p, visits, value_sum, est_cost, realized_cost, virtual_loss.
- Selection: array‑only PUCT with progressive widening, action priority, shadow cost, deterministic tie‑break; virtual loss on edges.
- Expansion: uses DomainAdapter to enumerate untried actions; builds child specs; resolves transpositions; batch adds vertices/edges; single CSR rebuild per commit.
- Rollouts: `GraphSystem.execute(df, world_state=...)` with sequential processors; no store updates; returns values and realized costs; caches used.
- Backprop: path‑only updates via parent pointers; root confidence stop using empirical Bernstein bounds.
- Controller: single writer orchestrating batch select → rollout dispatch → commit (expand+CSR+stats) → backprop; enforces invariants and bounded store.
- Caches: LLM call cache, Eval (judge) cache; optional LanceDB warm starts (off hot path).
- Observability: DecisionTrace (root & session‑level), metrics (tokens/point, cache hits, branching, skew), invariants on every commit.
- Modes: mcts/beam/bandit; CanaryMode (simple vs full parallel); ReplayMode (reconstruct from traces).
- Runtime: transparent Ray/Thread rollout pool (controller API is pool‑agnostic).

---

## Runtime configuration (Pydantic)

```python
from pydantic import BaseModel, Field
from typing import Literal

class FeatureFlags(BaseModel):
    csr: bool = True
    transpositions: bool = True
    progressive_widening: bool = True
    virtual_loss: bool = True
    confidence_stop: bool = True
    bounded_store: bool = True
    batching: bool = True
    cost_in_value: bool = True  # required; cost in exploration is disabled

class MctsConfig(BaseModel):
    mode: Literal["mcts","beam","bandit"] = "mcts"
    c_puct: float = 1.4
    lambda_cost: float = 0.02
    widen_k: float = 1.0
    widen_alpha: float = 0.5
    rollouts_K: int = 32
    rollouts_K_min: int = 4
    rollouts_K_max: int = 64
    adapt_rollouts: bool = True
    adapt_p50_threshold_ms: int = 800
    adapt_p99_threshold_ms: int = 2500
    max_nodes: int = 100_000
    max_edges: int = 500_000
    delta_confidence: float = 0.05
    seed: int = 0
    vl_penalty: float = 1.0  # virtual loss penalty applied in Q

class CanaryConfig(BaseModel):
    enabled: bool = False
    divergence_threshold: float = 0.1

class ReplayConfig(BaseModel):
    enabled: bool = False
```

---

## DomainAdapter contract (Pydantic‑first)

```python
from pydantic import BaseModel
from typing import Any

class StateSpec(BaseModel):
    state_json: str        # canonical JSON (sorted, tight separators)
    fence_meta: dict[str, Any]  # model/template/params/retrieval/judge/time/tools/seeds

class EvalResult(BaseModel):
    value: float
    realized_cost: float
    judge_version: str
    cache_hit: bool

class ActionSpec(BaseModel):
    action_id: int
    prior_p: float
    est_cost: float
    features: dict[str, Any] | None = None
    similarity: float | None = None   # optional for priority

class DomainAdapter:
    async def encode_state(self, world_or_df) -> StateSpec: ...
    async def generate_actions(self, state: StateSpec, widen_budget: int) -> list[ActionSpec]: ...
    async def apply_actions(self, state: StateSpec, actions: list[ActionSpec]) -> list[StateSpec]: ...
    async def evaluate_batch(self, states: list[StateSpec]) -> list[EvalResult]: ...
    def rollout_processors(self) -> list["GraphProcessor"]: ...
    def rollout_horizon(self) -> int: ...
    def seed(self, seed_value: int) -> None: ...
    # Optional: provide similarity features source for action priority (e.g., embedding space name)
```

- Shadow cost for selection: the adapter exposes `est_cost` (expected uncached cost). Cache savings are measured separately when realized.

---

## State fence and hashing (determinism)

- Canonical JSON: `json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)`
- Hash: `sha256(canonical).digest()[:16]` saved as bytes16.
- Fence must include: model id+version, prompt template id+version, params (temperature, etc.), retrieval snapshot IDs, judge_version, time bucket (rounded), tool versions, seeds.

```python
def canonical_hash(state_json: str) -> bytes:
    import json, hashlib
    canon = json.dumps(json.loads(state_json), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canon.encode("utf-8")).digest()[:16]
```

---

## Module footprint (new package)

- `archetype/src/archetype/core/mcts/`
  - `graph_index.py`: igraph wrapper (alloc bulk, add edges bulk, counts)
  - `adjacency_index.py`: CSR mirror with prefix‑sum rebuild and RCU version
  - `stats.py`: `NodeStats`, `EdgeStats` (geometric growth; edge `virtual_loss`)
  - `bounded_store.py`: caps (`max_nodes`, `max_edges`), leaf‑only eviction, protected set, free‑list reuse
  - `transpositions.py`: canonical hash + state fence; `state_hash → vid`
  - `selection.py`: widening gates, action priority, PUCT (cost‑in‑value), deterministic ties, shadow cost, K‑batch selection
  - `controller.py`: single‑writer loop (select→rollout→commit→backprop), budgets, root confidence stop
  - `rollout_engine.py`: `GraphSystem` wrapper; serialized processors; pure transforms
  - `domain_adapter.py`: interface + minimal reference adapter
  - `batch_eval.py`: LLM/judge batch evaluator (cache→group→microbatch→retry/backoff/circuit breaker)
  - `caching.py`: in‑memory LLM and Eval caches; hit‑rate metrics; optional LanceDB persistence hooks
  - `metrics.py`: tokens/point, branching, visits/action, selection skew; Arrow/Daft snapshots; `logging_shim` emitters
  - `decision_trace.py`: root and session‑level decision traces (scores, seeds, edges, values, stop reasons)
  - `invariants.py`: per‑commit checks (CSR parity, NaNs/inf, VL balance, caps, version consistency, fence adherence)
  - `ablation.py`: feature flags, modes (mcts/beam/bandit), seed sweeps, report
  - `canary.py`: `CanaryMode` (simple vs full, divergence alerts, pick better tokens/point)
  - `replay.py`: `ReplayMode` tree reconstruction from traces
  - `types.py`: Pydantic configs (`MctsConfig`, `FeatureFlags`, `CanaryConfig`, `ReplayConfig`)

---

## Detailed algorithms and data structures

### GraphIndex (igraph spine)

Responsibilities:
- Owns `igraph.Graph(directed=True)`.
- `alloc_vertices(k) -> list[vid]`, `add_edges_bulk(pairs) -> list[eid]`.
- Sanity methods: `vcount()`, `ecount()`, `successors(v)`.

Constraints:
- Single writer. Mutations batched. No deletions (logical pruning only).

### AdjacencyIndex (CSR mirror with RCU)

Arrays:
- `child_off`: int64, shape (V+1), offsets into flat arrays.
- `child_vids`: int64, shape (E).
- `child_eids`: int64, shape (E).
- `version`: monotonically incrementing commit id.

Rebuild (prefix‑sum) once per batch:
- Compute pending counts per parent (from new pairs).
- New degrees = old_deg + pending.
- New offsets via `cumsum(new_deg)`.
- Copy old adjacency blocks into new array using target write pointers. Optimize by computing a sparse parent list `parents_with_new` to avoid unnecessary pointer updates.
- Append new edges grouped by parent.
- Swap arrays atomically; bump `version`.

Invariants:
- `V == len(child_off) - 1`
- `child_off` non‑decreasing
- `child_off[-1] == len(child_eids) == len(child_vids)`
- `g.vcount() == V`, `g.ecount() == len(child_eids)`

Readers:
- Must snapshot `version` for consistent slice reads.

### Stats arrays (geometric growth)

NodeStats:
- `visits:uint32`, `value_sum:float32|64`, `virtual_loss:int16`, `parent:int64`, `depth:int16`, `expanded:bool`.
- `extend(k)` grows capacity ×2 when needed; returns vid range.

EdgeStats:
- `prior_p:float32`, `visits:uint32`, `value_sum:float32|64`, `est_cost:float32`, `realized_cost:float32`, `virtual_loss:int16`.
- `extend(k)` grows capacity ×2 when needed; returns eid range.

Note: store `value_sum` as float32 initially; clamp Q; upgrade to float64 if precision issues observed.

### Progressive widening and action priority

- Widening budget per node: `m(n) = k * n^alpha`.
- Eligible children are the top‑m(n) by priority (not insertion order). Maintain a per‑node eligible set sorted by `priority`.
- Action priority (within eligible set): `priority = prior_p / (1 + est_cost)^β * (1 - similarity)^γ` (β, γ configurable). Use as a stable sort key before PUCT.

### PUCT with shadow cost and deterministic ties

- Q term: `q = value_sum / max(1, visits)`; clamp NaNs/inf; subtract shadow cost:
  - `q = q - lambda_cost * est_cost`
- Exploration: `bonus = c_puct * prior_p * sqrt(N_parent) / (1 + visits_child)`
- Score: `score = q + bonus`
- Ties: seeded PRNG among equal‑within‑epsilon scores.
 - Virtual loss penalty applies via `q -= vl_penalty * virtual_loss[e]` where `vl_penalty` is configured in `MctsConfig`.

### Virtual loss (edges)

- On selecting a path: assign a `batch_id` to the selection; increment `EdgeStats.virtual_loss[e] += 1` for every traversed edge; subtract `vl_penalty * virtual_loss[e]` inside Q.
- Revert after rollout returns: use the recorded edge list and `batch_id`; apply LIFO ordering and ensure no cross‑batch interference (never decrement below 0).

### Transpositions

- `TranspositionIndex`: `state_hash -> vid` dictionary.
- On expansion, for each child spec:
  - If seen: do not create new vertex; add edge `(parent, existing_vid)`.
  - If new: allocate vid and later add edge.
- Multi‑parent is natural: CSR will contain same `child_vid` under multiple parents with distinct `eids`.
 - Depth and parent pointers: treat `parent[vid]` as display‑only; algorithms use the exact edge path returned by selection for backprop.

### Selection (array‑only over CSR slices)

- Inputs: `parent vid`, CSR arrays, NodeStats, EdgeStats, widening budget.
- Steps:
  - If node not expanded or has `outdegree == 0`: stop.
  - Compute allowed children window `[lo, lo+allowed)`.
  - Vectorize Q, bonus, score over `eids = child_eids[lo:hi]`.
  - Choose argmax deterministically; continue until an unexpanded leaf or allowed window is empty.

### Batch selection

- Select up to `K` paths:
  - Apply virtual loss on edges per path to reduce collisions.
  - Return `[(path_vids, path_eids, leaf_vid)] * K`.

### Expansion and commit

- For each leaf with untried queue:
  - Build `ChildSpec` list: `(parent_vid, action_id, child_state_hash, prior_p, est_cost)`.
- Resolve transpositions: map to existing vids or allocate new ones; dedupe duplicate pairs.
- Bounded store:
  - If caps exceeded, evict leaves only using value‑weighted LRU; protected set includes root, ancestors of top‑K, and in‑flight batch (parents and children). Eviction pass runs before `add_edges`.
  - Reuse freed ids via free‑list.

Eviction priority (defaults):
```
priority = 1.0 * age_rank - 0.5 * normalized_Q - 0.2 * sqrt(visits)
```
Tune weights per domain; never evict protected nodes.
- Graph add:
  - `g.add_vertices(new_count)`, `g.add_edges(pairs)`.
- CSR rebuild once; RCU swap.
- Stats extend/init for new nodes/edges; set parent pointers and depth; set `expanded=False` for new nodes.

### Rollout engine (pure; serialized)

- `RolloutEngine` wraps `GraphSystem`, adding a serialization contract:
  - Either wrap the whole per‑step pipeline as a single `GraphProcessor`, or ensure per‑stage writes are disjoint so `GraphSystem`’s simple merge is safe.
- `simulate(state_df, steps, world_state)` runs the staged processors sequentially; returns values and realized costs per state.
- No store updates; workers never touch igraph or arrays; only return deltas.

### Backprop and root confidence stop

- Backprop path‑only:
  - Use the selected edge path (not `parent[]`) from leaf to root; update node and edge `visits`, `value_sum`.
  - Clamp numerics; avoid NaNs/inf.
- Root confidence stop:
  - Maintain running mean/variance per root edge (Welford). Heuristic (non‑IID); require min visits per top‑2 (e.g., n≥8), apply variance floor, and log stop decisions.
  - Compute empirical Bernstein bounds per edge; stop when `LCB(best) > UCB(second)` within `delta_confidence`. Optionally re‑judge top‑2 before hard stop.

### Controller orchestration (single writer)

Loop:
1. Select K paths (apply virtual loss).
2. Dispatch K rollouts to worker pool (async tasks or Ray).
   - Adaptive batching (optional):
     - If recent P99 rollout latency > `adapt_p99_threshold_ms`, halve K (down to `rollouts_K_min`).
     - If recent P50 < `adapt_p50_threshold_ms`/4, increase K by 1.5× (up to `rollouts_K_max`).
3. Commit:
   - Resolve transpositions.
   - Bounded store evictions (if needed).
   - Add vertices/edges; CSR rebuild; RCU swap.
   - Extend/init stats; backprop; revert virtual loss.
4. Check budgets and stopping criteria (time/nodes/plateau/confidence).
5. Emit DecisionTrace (root) and metrics.

Concurrency:
- One controller task/actor owns all mutations.
- Workers are pure functions returning deltas.

---

## Caching and batch evaluation

- LLM cache: keyed by `(provider, model_id, canonical_prompt, params, seed)`.
- Eval (judge) cache: keyed by `(state_hash, judge_version)`; include simhash/minhash for near‑dup short‑circuit.
  - Simhash parameters: 3‑gram shingles, 64‑bit hash, similarity threshold 0.85.
- `batch_eval.py`:
  - De‑dup by key → cache probe → in‑flight de‑dup (promise map) → group by provider/model → microbatch by token limits → retry/backoff → circuit breaker → hedged requests if enabled.
  - Attach correlation IDs to maintain deterministic merge order.
  - Returns `EvalResult` with `value`, `realized_cost`, `judge_version`, `cache_hit`.

- Shadow cost policy & λ:
  - Selection always uses expected uncached cost (`est_cost`), independent of real cache hits. Log cache savings separately.
  - Normalize judge to [0,1]. Calibrate `lambda_cost` per plan (light line search or target slope). Record chosen λ in DecisionTrace.

- Cache caps:
  - Cap LLM and Eval caches by bytes; LRU eviction; fence by judge/model/template versions. Emit cache memory metrics.

---

## Observability, invariants, and modes

### DecisionTrace (session‑level and root)

- Per iteration: seed, selected path eids, per‑edge scores `{q, bonus, prior_p, est_cost, score}`, chosen vs runner‑up, stop reason, expansions `{(u,v,eid)}`, values, realized costs, cache hits.
- Root summary: bounds `{best_lcb, second_ucb}`, final choice, tokens‑per‑point.

DecisionTrace schema (root snapshot):

```json
{
  "root_id": 7,
  "candidates": [
    {"eid": 12, "child": 19, "visits": 23, "q": 0.41, "bonus": 0.12, "prior_p": 0.30, "est_cost": 240.0, "score": 0.53}
  ],
  "chosen_eid": 12,
  "runner_up_eid": 15,
  "score_gap": 0.04,
  "stop_reason": "confidence|budget|plateau|none",
  "bounds": {"best_lcb": 0.50, "second_ucb": 0.49},
  "tokens_per_point": 210.0
}
```

### Invariants (per commit)
- CSR parity (alive subset): `V_alive == len(off) - 1`, `off` non‑decreasing, `off[-1] == len(child_eids_alive)`.
- Topology parity (alive subset): `g.vcount() ≥ V_alive`, `g.ecount() ≥ E_alive`.
- No NaNs/inf in `q`, `bonus`, `score`.
- Virtual loss balanced (applied == reverted).
- Caps respected; evictions only on leaves; protected set intact.
- Versioning consistent (reader snapshot version stable within an iteration).
- State fence respected; judge/model/version consistent.

### Metrics
- Tokens per quality point (primary).
- Cache hit rates (LLM, judge).
- Effective branching factor by depth.
- Visits per action type.
- Top‑K score vs tokens curve.
- Selection skew (Gini on edge visits).
- Memory headroom; latency P95.

### Modes
- `mode = "mcts" | "beam" | "bandit"`; used by controller to run baseline comparators.
- CanaryMode: run simple and full MCTS in parallel; alert on divergence; choose better tokens/point. Module `canary.py`.
- ReplayMode: reconstruct tree from DecisionTrace; deterministic debugging. Module `replay.py`.

Baselines:
- Beam: specify beam width, judge, and prompt/token budget; deterministic seeds.
- Bandit: specify algorithm (UCB1‑Tuned or Thompson) and priors; deterministic seeds.

Plateau stopping:
- Define “no‑improve” as the slope of best root score vs tokens over last M iterations falling below ε; stop when below threshold for consecutive R iterations.

---

## Integration with Archetype

- No changes to `@graph/GraphSystem` (rollouts serialized within `rollout_engine.py`).
- `AsyncWorld` integration:
  - Preferred: an `AsyncProcessor` that triggers MCTS (using controller) when certain components/conditions are present; emits an Action component to downstream processors via `Component.to_row_dict()`.
  - Optional: add a feature‑flagged pre‑step planning hook in `AsyncWorld.step` to compute actions and pass via `input_kwargs`.
- State artifacts: use `Component` subclasses (Pydantic LanceModel) for transient action and planning outputs where needed; convert to rows via `to_row_dict()`.

RCU snapshot semantics:
- Within a planning tick, selection uses a single CSR `version` snapshot; CSR swaps only at commit.

Rollout serialization enforcement:
- Enforce via a single `RolloutPipelineProcessor` that encapsulates per‑step transforms, or an explicit `execution_mode="sequential"` honored by `RolloutEngine`. Add a test to ensure processors do not interleave during rollouts.

---

## Tests and benchmarks

Write this first:
- E2E smoke: `test_smoke_mcts_beats_random()`
- Determinism: `test_mcts_deterministic()` same seed produces identical tree and DecisionTrace.
  - Toy domain (e.g., tic‑tac‑toe).
  - Simplest possible MCTS vs random; assert wins > 70/100.

Unit and subsystem tests:
- CSR correctness (naive vs optimized rebuild) and throughput.
- Alive‑mask CSR: evict random leaves; CSR excludes tombstones; invariants pass; selection never returns tombstoned children.
- Selection throughput (CSR array‑only vs dict baseline).
- Virtual loss concurrency (K‑way selection; minimal collisions at depth 1 unless forced).
- Transpositions: multi‑parent edges for the same `state_hash`; one vertex.
- Rollout purity and determinism (seeds respected).
- Root confidence stop: synthetic distributions verify bounds stopping behavior.
- Bounded store: caps enforced; only leaves evicted; protected sets preserved.
- Batch evaluator: cache hits, microbatch grouping, retry/backoff, circuit breaker.
- Batch eval in‑flight de‑dup: concurrent identical requests hit provider once; stable correlation IDs.
- Deterministic replay: same seeds → identical top‑1 and DecisionTrace bytes.
- Confidence stop false‑positive guard: neck‑and‑neck top‑2 does not stop early when n is low.
- Circuit breaker: inject 429/5xx; verify fallback and recovery.

Benchmarks:
- Nodes/sec, selections/sec, tokens‑per‑point vs baselines; scaling with K rollouts and widening parameters.

Ablations:
- Feature flags on/off; mode comparisons (mcts/beam/bandit); seed sweeps; report tokens/quality deltas.

---

## Phased plan (timeline realistic)

- Phase 0 — Contracts & policy (3–4 days)
  - Canonical hashing/seeding; DomainAdapter contract; cost policy; state fence spec.
- Phase 1 — Topology & CSR (3–4 days)
  - GraphIndex; AdjacencyIndex; prefix‑sum rebuild; invariants; version/RCU.
- Phase 2 — Hot‑path stats & bounds (3–4 days)
  - NodeStats/EdgeStats; zero‑copy snapshots; bounded store.
- Phase 3 — Selection, widening, VL (1 week with tests)
  - Widening, priority; PUCT with shadow cost; deterministic ties; edge VL; array‑only selection; K‑batch selection.
- Phase 4 — Expansion & commit (2–3 days)
  - Child specs; transpositions; graph add; CSR rebuild; stats init.
- Phase 5 — Domain & rollout (3–4 days)
  - RolloutEngine; DomainAdapter v1; reward/cost plumbing; serialization contract.
- Phase 6 — Controller & stop rules (3–4 days)
  - Single‑writer loop; budgets; determinism logging; root confidence stop.
- Phase 7 — Caches & observability (3–4 days)
  - LLM/eval caches; DecisionTrace; invariants; metrics.
- Phase 8 — Integration & modes (3–4 days)
  - AsyncProcessor trigger or pre‑step hook; CanaryMode; ReplayMode; modes/ablations.
- Tests ongoing; total ~3–4 weeks to production‑ready with focused dev.

---

## HTN/DAG (high‑level tasks and deps)

- P0: T0.1, T0.2, T0.3, T0.4
- P1: T1.1 → T1.2 → T1.3 → T1.4 → T1.5
- P2: T2.1, T2.2 → T2.3 → T2.4
- P3/P5: T4.1 → T4.2, T4.4; T5.1 → T5.2 → T5.3 → T5.4 → T5.5
- P6: T6.1 → T6.2 → T6.3 → T6.4 → T6.5
- P7: T7.1 → T7.2 → T7.3
- P8: T8.1 → T8.2 → T8.3; plus T5.6 (confidence stop) after T5.4
- P3: T3.1, T3.2, T3.3, T3.5
- P11: T11.1 → T11.2 → T11.3 → T11.4 → T11.5; plus P11.6 (Canary), P11.7 (Replay), P11.8 (Trace v2)
- P10: T10.1/T10.2, T10.3/T10.4
- P12: T12.1 → T12.2 → T12.3 (optional)
- P13: T13.1, T13.2 (optional)
- P14: T14.1–T14.8 (tests/benches/ablations)

---

## Example API sketches

AdjacencyIndex (CSR with RCU):

```python
class AdjacencyIndex:
    def __init__(self):
        import numpy as np
        self.child_off = np.array([0], dtype=np.int64)
        self.child_vids = np.empty(0, dtype=np.int64)
        self.child_eids = np.empty(0, dtype=np.int64)
        self.version = 0
        self.num_vertices = 0

    def extend_vertices(self, k: int):
        import numpy as np
        last = self.child_off[-1]
        add = np.full(k, last, dtype=np.int64)
        self.child_off = np.concatenate([self.child_off, add])
        self.num_vertices += k

    def rebuild_with(self, pairs: list[tuple[int,int]], eids: list[int]):
        import numpy as np
        V = self.num_vertices
        pending = np.zeros(V, dtype=np.int64)
        for (u, _), _e in zip(pairs, eids):
            pending[u] += 1
        old_deg = self.child_off[1:] - self.child_off[:-1]
        new_deg = old_deg + pending
        new_off = np.empty_like(self.child_off)
        new_off[0] = 0
        np.cumsum(new_deg, out=new_off[1:])
        total_new = int(pending.sum())
        new_child_vids = np.empty(self.child_vids.size + total_new, dtype=np.int64)
        new_child_eids = np.empty(self.child_eids.size + total_new, dtype=np.int64)
        write_ptrs = new_off[:-1].copy()
        # copy old blocks
        for u in range(V):
            d = int(old_deg[u])
            if d == 0:
                continue
            src_lo, src_hi = int(self.child_off[u]), int(self.child_off[u+1])
            dst = int(write_ptrs[u])
            new_child_vids[dst:dst+d] = self.child_vids[src_lo:src_hi]
            new_child_eids[dst:dst+d] = self.child_eids[src_lo:src_hi]
            write_ptrs[u] += d
        # append new edges
        for (u, v), e in zip(pairs, eids):
            dst = int(write_ptrs[u])
            new_child_vids[dst] = v
            new_child_eids[dst] = e
            write_ptrs[u] += 1
        # swap (RCU)
        self.child_off = new_off
        self.child_vids = new_child_vids
        self.child_eids = new_child_eids
        self.version += 1
```

Selection (array‑only, PUCT with shadow cost):

```python
def select_path(idx, ns, es, v_root, cfg, rng):
    import numpy as np, math
    path, v = [v_root], v_root
    while True:
        if not ns.expanded[v]:
            return path, v
        lo, hi = int(idx.child_off[v]), int(idx.child_off[v+1])
        if lo == hi:
            return path, v
        # progressive widening
        allowed = min(lo + int(cfg.widen_k * (max(1, ns.visits[v]) ** cfg.widen_alpha)), hi)
        if allowed <= lo:
            return path, v
        eids = idx.child_eids[lo:allowed]
        childs = idx.child_vids[lo:allowed]
        pv = max(1, ns.visits[v])
        q = es.value_sum[eids] / np.maximum(1, es.visits[eids])
        q = np.nan_to_num(q, nan=-1e9, posinf=1e9, neginf=-1e9)
        q = q - cfg.lambda_cost * es.est_cost[eids] - 0.0 * es.virtual_loss[eids]
        bonus = cfg.c_puct * es.prior_p[eids] * (pv ** 0.5) / (1.0 + es.visits[eids])
        scores = q + bonus
        best = int(np.argmax(scores))
        v = int(childs[best])
        path.append(v)
```

Controller loop (skeleton):

```python
class MctsController:
    def __init__(self, idx, csr, ns, es, trans, adapter, rollout, cfg, flags):
        ...

    async def plan(self, root_state):
        # init tree if needed
        ...
        while not self._should_stop():
            # 1) select K paths
            batch = []
            for _ in range(self.cfg.rollouts_K):
                path, leaf = select_path(...)
                self._apply_virtual_loss(path)
                batch.append((path, leaf))
            # 2) rollout workers
            deltas = await self._dispatch_rollouts(batch)
            # 3) commit: expand, CSR rebuild, stats init, backprop, revert VL
            pairs, eids = self._expand_and_commit(deltas)
            # invariants, decision trace
            self._check_invariants()
            self._emit_trace()
        return self._best_action()
```

---

## Deliverables and done criteria

- Code modules as listed (new package) with tests and examples.
- Passing Day‑0 smoke test: MCTS beats random on toy domain.
- Invariants green under load; DecisionTrace and metrics visible.
- Memory bounded; caps enforce leaf‑only evictions.
- Root confidence stop functional; Canary and Replay modes operational.
- KPI: On canaries, `mode="mcts"` beats beam by ≥15% tokens/quality; auto‑fallback to beam if guardrails red.

---

## Risks and mitigations

- Performance hotspots: addressed via CSR array‑only selection, batch selection, deterministic ties.
- Merge semantics: rollouts serialized; no modifications to `GraphSystem` required now.
- Parallel hazards: single writer, edge‑level VL, RCU CSR versioning.
- Reward hacking: judge_version fenced; holdout checks pre‑finalize; DecisionTrace for scrutiny.
- Complexity risk: Canary and Replay provide safety rails; Ablation harness ensures incremental confidence.

---

This document is the implementation blueprint. Build phases in order, keep batched, single‑writer discipline, and always validate with the smoke test and invariants before layering features.