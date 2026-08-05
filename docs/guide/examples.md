# Examples

Every example on this page runs end-to-end with a single command. The
recommended pattern is `ArchetypeRuntime` for scripts. A small number of
examples intentionally exercise internal services as focused implementation
fixtures; application examples stay on the runtime.

## 0. Quickstart

The smallest complete simulation defines one component and one processor,
then runs through the public runtime surface. It stays below 30 non-comment
source lines and is part of the credential-free example smoke suite.

```bash
uv run python examples/00_quickstart.py
```

Source: [`examples/00_quickstart.py`](https://github.com/VangelisTech/archetype/blob/main/examples/00_quickstart.py)

The script prints `3`: the initial state is persisted first, then the
processor increments the counter on three subsequent ticks.

---

## 1. World Mutations

Demonstrates the trusted mutation surface: spawn entities with components,
inject processors at runtime, fork a world, and query state/history.

```bash
uv run python examples/01_world_mutations.py
```

Source: [`examples/01_world_mutations.py`](https://github.com/VangelisTech/archetype/blob/main/examples/01_world_mutations.py)

This example uses actor-free `ArchetypeRuntime`. RBAC is intentionally absent
from trusted scripting; the dispatcher-policy and API tests cover role policy.

**What it demonstrates:**

- **SPAWN / DESPAWN / UPDATE** through the runtime world surface
- **ADD_COMPONENT / REMOVE_COMPONENT** with archetype migration at tick boundaries
- **ADD_PROCESSOR** to inject a `MovementProcessor` at runtime
- **FORK** while preserving runtime ownership and independent world identity
- **History reads** through `world.history()` without fabricating access events

Its structured receipt checks component migration, despawn, processor
installation/removal, fork isolation, and the fact that trusted runtime calls
do not fabricate actor-aware access rows.

**Runtime operations in this example (curated, not exhaustive):**

| Runtime call | Owning behavior |
|---|---|
| `world.step()` / `world.run()` | `world.simulation`, registered through commands |
| entity/component mutations | `world.mutation`, registered through commands |
| processor mutations | `world.mutation`, registered through commands |
| `world.fork()` / `world.info()` | world lifecycle, registered through commands |
| `world.query()` | `world.query`, registered through commands |
| `world.history()` | commands-owned `AuditLog` projection |

The runtime constructs exact family operations and uses trusted dispatcher
entry; it does not construct an `ActorCtx`. See the normative
[Command Gate](command-gate.md#3-four-roles-and-permissions) for the separate
untrusted-adapter permission matrix.

---

## 2. Fork for Counterfactuals

Run three logistic-map regimes on one prime timeline, fork once, perturb each
forked value by `1e-9`, then advance both worlds and compare their append-only
histories with one Daft join.

```bash
uv run python examples/02_fork_counterfactual.py
```

Source: [`examples/02_fork_counterfactual.py`](https://github.com/VangelisTech/archetype/blob/main/examples/02_fork_counterfactual.py)

The receipt proves distinct world identity, an identical pre-fork prefix, the
exact perturbation at the branch point, aligned post-fork histories, and
regime-specific divergence. Forks share resource instances by default; trusted
scripts attach replacement resources through the runtime when per-branch
resource isolation is required.

---

## 3. Time-Travel Queries

Run ticks, rewind to any past tick by filtering the `tick` column, then fork
a counterfactual branch and diff it against the source at the same tick.
Every tick is preserved.

```bash
uv run python examples/03_time_travel.py
```

Source: [`examples/03_time_travel.py`](https://github.com/VangelisTech/archetype/blob/main/examples/03_time_travel.py)

`world.query(...)` returns the full append-only history, so a point-in-time
view is a Daft filter:

```python
df = await world.query(Position, Velocity)
at_tick_2 = df.where(col("tick") == 2)
```

The fork half of the example stages a divergent component value on the fork
(`fork.update(entity, Velocity(vx=10.0))`), steps both worlds the same number
of ticks, and prints the source-vs-fork diff at the same tick — plus the
fork's view of its pre-fork history, read through lineage.

Initial conditions are part of the ledger: an entity's first persisted row
is its raw spawn values at the tick it materializes, and processors first
apply on the following tick — the table contains `x_0, f(x_0), f^2(x_0), ...`.

---

## 4. Agent Messaging

Three agents exchange greetings through an example-local shared `Mailbox`.
Priority-ordered processors realize pending messages on the following tick,
then update mood and energy from each inbox.

```bash
uv run python examples/04_messaging.py
```

Source: [`examples/04_messaging.py`](https://github.com/VangelisTech/archetype/blob/main/examples/04_messaging.py)

**What it demonstrates:**

- **Components**: `AgentState` (name, mood, energy), `Inbox`, `Outbox`
- **Resources**: `SimConfig` for shared parameters, `Mailbox` for pending messages
- **Processors**: `GreetingProcessor` (deposits messages), `MessageRealizationProcessor` (drains the mailbox into inboxes), `MoodProcessor` (updates mood based on inbox)
- **Hooks**: `PreTick` and `PostTick` lifecycle callbacks

The structured receipt verifies the one-tick mailbox delay, six realized
messages, two received messages per agent, sender/receiver pairs, hook tick
sequences, and the final energy values without retaining temporary storage
paths.

---

## 5. LLM-Powered Agents

Three agents with different personalities, each calling an LLM every tick via `daft.functions.prompt`. The ECS handles batching automatically — all entities get LLM calls in parallel because world state is a DataFrame.

```bash
export OPENAI_API_KEY=sk-...
uv run python examples/05_llm_agents.py
```

Source: [`examples/05_llm_agents.py`](https://github.com/VangelisTech/archetype/blob/main/examples/05_llm_agents.py)

**What it demonstrates:**

- **Component**: `Agent` with name, role, a `journal_json`, and durable thought count
- **Processor**: `ThinkProcessor` uses `daft.functions.prompt` to call an LLM for every agent entity in a single DataFrame operation
- **Pattern**: spawn, persist the raw initial state with `world.step()`, run five model-processing ticks, then filter the append-only query to the latest tick
- **Receipt**: proves three latest agent rows, five processor calls per agent, and five valid JSON journal entries without retaining model text

Requires an OpenAI API key (or any provider via `daft.set_provider()`).

---

## 6. Mission Trajectory Analysis

Persist normalized turn and reward rows keyed by `episode_id`, then select
and grade one episode's evidence through the runtime-owned application
service. The example is deterministic and requires no model credentials.

```bash
uv run python examples/06_trajectory_analysis.py
```

Source: [`examples/06_trajectory_analysis.py`](https://github.com/VangelisTech/archetype/blob/main/examples/06_trajectory_analysis.py)

**What it demonstrates:**

- **Normalized evidence**: turn and reward rows remain independently queryable per episode.
- **Typed selection**: `TrajectorySelection` filters one evidence table by `episode_id`.
- **Derived view**: `trajectory(...)` reconstructs one episode's seq-ordered evidence lazily.
- **Application composition**: `query_trajectory()` uses persisted query access; `grade_trajectory()` delegates graders to evaluation.
- **No duplicate trajectory model**: the example consumes `archetype.missions.trajectories` directly.

---

## 7. Lifecycle Hooks

Record lifecycle audit events, measure tick duration, and publish per-tick metrics without putting side effects inside processors.

```bash
uv run python examples/07_hooks.py
```

Source: [`examples/07_hooks.py`](https://github.com/VangelisTech/archetype/blob/main/examples/07_hooks.py)

**What it demonstrates:**

- **Mutation audit**: `OnSpawn`, `OnDespawn`, `OnComponentAdded`, and `OnComponentRemoved`
- **Tick telemetry**: `PreTick` starts a timer and `PostTick` computes metrics from `event.results`
- **Hook handles**: unregister a temporary debug hook with `world.remove_hook(handle)`
- **Boundary discipline**: hooks emit side effects; processors keep the simulation state deterministic

---

## 8. HTN Resolution

Resolve a hierarchical task network into a fan-out AND/OR forest.

```bash
uv run python examples/08_htn_resolution.py
```

Source: [`examples/08_htn_resolution.py`](https://github.com/VangelisTech/archetype/blob/main/examples/08_htn_resolution.py)

This is a planning primitive, not the Agent Missions V1 planner. The future
mission-planning adapter may translate a resolved plan into task entities and
`DependsOn` edges; it may not advance those tasks.

---

## 9. Cloud Storage

Configure cloud-backed storage through `StorageConfig` without changing the
runtime workflow.

```bash
uv run python examples/09_cloud_storage.py
```

Source: [`examples/09_cloud_storage.py`](https://github.com/VangelisTech/archetype/blob/main/examples/09_cloud_storage.py)

The local path runs without cloud credentials. Provider-specific branches need
the matching host credentials.

---

## 10. AutoResearch

Run a multi-candidate research workflow through the runtime-owned world and
evaluation boundaries.

```bash
uv run python examples/10_autoresearch.py
```

Source: [`examples/10_autoresearch.py`](https://github.com/VangelisTech/archetype/blob/main/examples/10_autoresearch.py)

AutoResearch is a sibling workflow, not a coding-agent mission subfamily. It
may consume an agent callback without inheriting mission transition authority.
Its transient `ResearchCandidateContext` is not the persisted missions
`Candidate` review subject.

---

## 11. Coding-Agent Mission

Submit a two-task repository mission: first prove a regression is red, then
implement the fix only after that predecessor is accepted.

```bash
# Inspect the typed graph without creating Modal resources.
uv run --extra coding-agent python examples/11_coding_agent_mission.py --dry-run

# Run the credentialed dogfood.
uv run --extra coding-agent python examples/11_coding_agent_mission.py
```

Source: [`examples/11_coding_agent_mission.py`](https://github.com/VangelisTech/archetype/blob/main/examples/11_coding_agent_mission.py)

**What it demonstrates:**

- typed `AgentTask` and `CommandValidator` authoring;
- a temporal `DependsOn` relationship instead of a JSON plan cursor;
- an expected-nonzero validator for the red regression;
- committed dispatch admitted as a durable author Activity after its tick;
- revision-bound validation and exact-head publication producing an immutable
  candidate rather than acceptance;
- independent review of that exact candidate in a distinct critic sandbox;
- processor-owned acceptance only after a complete candidate-bound critic
  receipt; and
- same-worktree repair carrying durable validator failures or blocking critic
  findings into a new dispatch and candidate.

See [Agent Missions V1](agent-missions.md) for the complete state machine,
sequence diagram, ownership map, dogfood result, and explicit limits.

---

## 11. Graph Relationships

Represent hierarchy edges as temporal ECS entities, traverse a bounded command
tree, read it at an earlier tick, and cascade cleanup after a despawn.

```bash
uv run python examples/11_graph_relationships.py
```

Source: [`examples/11_graph_relationships.py`](https://github.com/VangelisTech/archetype/blob/main/examples/11_graph_relationships.py)

The receipt proves traversal order, temporal edge visibility, and the remaining
unit/edge counts after cascade.

---

## 12. PreFabs

Author a prefab subtree, instantiate isolated copies with overrides and `IsA`
lineage, then edit the template and re-instantiate without mutating prior
instances.

```bash
uv run python examples/12_prefabs.py
```

Source: [`examples/12_prefabs.py`](https://github.com/VangelisTech/archetype/blob/main/examples/12_prefabs.py)

---

## 13. Biome-Inspired RTS

Compose a prefab asset catalog into a live RTS command hierarchy with
registered processors, minimap and fog-of-war projections, and a
possessed-unit view.

```bash
uv run python examples/13_biome_rts.py
```

Source: [`examples/13_biome_rts.py`](https://github.com/VangelisTech/archetype/blob/main/examples/13_biome_rts.py)

The hosted physical-AI episode path has no numbered example script; its
runnable snippet and contract live in [Physical AI](physical-ai.md).
