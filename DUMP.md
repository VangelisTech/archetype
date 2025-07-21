Key questions: 
1. Should our Command class be sub-class of the archetype Component with the additional serialization methods since it already supports being translated to and from arrow schemas? This may not be optimal given the fine control we have already.  




In our earlier conversation we talked about how a a “non-naïve” command queue that scales from your laptop demo all the way to a Ray cluster running dozens of LLM-driven agent terminals would need to have:

Below is a **blue-print** for a *“non-naïve”* command queue that scales from your laptop demo all the way to a Ray cluster running dozens of LLM-driven agent terminals.

---

## 1 Conceptual Layers

| Layer                     | Purpose                                                        | Key Guarantees                                   |
| ------------------------- | -------------------------------------------------------------- | ------------------------------------------------ |
| **Command Model**         | Canonical, versioned dataclass + Pydantic schema               | Deterministic, serialisable, backward-compatible |
| **Broker Interface**      | Abstract façade (`enqueue`, `dequeue_batch`)                   | Hides storage details; easy swap                 |
| **Durable Log**           | Append-only Arrow/Parquet bucket                               | Crash recovery, replay, analytics                |
| **Fast Priority Heap**    | In-RAM view of “due” commands, ordered by `tick`★ + `priority` | O(log n) pops, constant-time peek                |
| **Validator / Guardrail** | Inline policy checks (RBAC, budget, safety)                    | Commands never mutate invalid state              |
| **World Integrator**      | Pulls batches each tick; applies, emits events                 | Idempotent, deterministic                        |
| **Event Stream**          | SSE / WebSocket for observers (UI, MCP)                        | Real-time feedback, back-pressure aware          |

★ `tick` = logical “apply no later than”.

---

## 2 Key Data Structures

```python
# async_command.py
from pydantic import BaseModel, Field
from uuid import UUID, uuid4
from typing import Literal, Any, Dict

class Command(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    tick: int
    actor_id: UUID
    op: Literal[
        "add_component", "remove_component",
        "add_processor", "remove_processor",
        # domain-specific ops…
    ]
    payload: Dict[str, Any]
    priority: int = 0
    version: int = 1
```

---

## 3 Broker Interface

```python
class CommandBroker(Protocol):
    async def enqueue(self, cmd: Command) -> None: ...
    async def enqueue_bulk(self, cmds: list[Command]) -> None: ...
    async def dequeue_due(self, *, tick: int, limit: int) -> list[Command]: ...
    async def ack(self, cmd_ids: list[UUID]) -> None: ...
```

### Optional capabilities

* `subscribe()` → async generator of *future* commands (for real-time dashboards)
* `fail(cmd_id, error)` → mark irrecoverable

---

## 4 Concrete Brokers

| Name                 | Use-case              | Storage                                         | Ordering                                  |
| -------------------- | --------------------- | ----------------------------------------------- | ----------------------------------------- |
| **`AsyncCommandQueue`** | Unit tests, local dev | `asyncio.PriorityQueue`                         | Heap key = `(tick, priority, seq)`        |
| **`RayBroker`**      | Single-cluster prod   | Ray actor holding heap **+** Arrow write-behind | Exact same key; offers actor-to-actor RPC |
| **`DLQBroker`**      | Cross-cluster, HA     | Append-only **Kafka** / Pulsar topic            | Partition key = world-id                  |

*Swap by DI or `BrokerFactory.from_env()`.*

---

## 5 Durable Log Write-Path (for RayBroker)

```
enqueue(cmd)
 ├─ 1. pydantic.validate()
 ├─ 2. guardrail_allow?  ⟶  4xx if not
 ├─ 3. append_row("command_log/{world_id}/{date}.parquet", cmd)
 └─ 4. heap.put((tick, priority, seq, cmd))
```

*The Arrow/Parquet file is flushed in async batches (e.g. every 100 cmds or 500 ms).*

---
---

## 7 Policy / Guardrail Plug-in

```python
async def guardrail_allow(cmd: Command) -> bool:
    if (budget := budgets[cmd.actor_id]) < estimate_cost(cmd):
        return False
    if cmd.op == "delete_account" and not is_admin(cmd.actor_id):
        return False
    # …LLM-based semantic filter etc.
    return True
```

*Broker calls this **before** log append to avoid writing rejects.*

---

## 8 Metrics & Observability

| Metric                    | Type      | Notes                  |
| ------------------------- | --------- | ---------------------- |
| `commands_enqueued_total` | Counter   | label `op`, `world_id` |
| `commands_rejected_total` | Counter   | label `reason`         |
| `command_queue_depth`     | Gauge     | size of heap           |
| `step_duration_seconds`   | Histogram | label `world_id`       |

Prometheus scraper on Ray head; Grafana board for live sims.

---

## 9 MCP / Agent Terminal API

```python
POST /worlds/{id}/commands
{
  "tick": 0,
  "actor_id": "...",
  "op": "add_component",
  "payload": { "entity_id": "...", "component": "Cart", "data": {...} }
}
```

*Returns `202 Accepted` with `command_id`.*
Agents poll **`/events?since=<tick>`** (SSE) or subscribe to WebSocket to see when their command is applied, enabling **recursive planning** and “explore-alternative-reality” branching by cloning a world snapshot and replaying a filtered command log.

---

## 10 Failure & Replay Story

* On crash, RayBroker re-hydrates heap from the Parquet log (`world_id` & `tick` index).
* To replay:

  ```python
  cmds = read_log("command_log/world-123/*.parquet")
  world.reset()
  for cmd in cmds: await world.apply(cmd)
  assert world.hash == expected_hash
  ```

---

### TL;DR

1. **Abstract Broker** with pluggable back-ends.
2. **Append-only log** + in-RAM heap for low-latency pops.
3. Deterministic apply order: data-cmds → processors → meta-cmds.
4. Guardrails, metrics, ACKs, and HA built in.
5. Exposed over HTTP/WebSocket so MCP-driven LLMs can spawn worlds, enqueue commands, and branch timelines at will.

Drop-in the `AsyncCommandQueue` today, keep the interface, and you can promote to the Ray or Kafka broker when load or durability demands—without touching `World.step()` again.


Where the guardrails live in the stack
pgsql
Copy
Edit
┌─────────────────────────────────────────────────────────┐
│  Agent / LLM / Terminal                                │  ←— caller supplies identity token or actor_id
├─────────────────────────────────────────────────────────┤
│  MCP HTTP API (FastAPI, gRPC…)                          │  ←— does *authentication* (token → actor_id)
├─────────────────────────────────────────────────────────┤
│  **CommandBroker.enqueue()**                           │
│    ├── 1️⃣ **validate(cmd, actor_ctx)**  ← RBAC lives here
│    │       · cheap cache-lookup or OPA/Rego policy
│    ├── 2️⃣ append-to-Parquet (audit trail)
│    └── 3️⃣ heap.push(cmd)                              │
└─────────────────────────────────────────────────────────┘
Guardrails run inside the broker—before the command touches the log or heap—so an invalid request never pollutes state, never needs a compensating rollback, and never shows up in replay.

1 Identity & role sources
Source	How the broker sees it	Typical storage
Human terminal (CLI login)	OAuth2 access-token ➜ actor_id	Redis session or JWT
LLM agent sandbox	actor_id hard-wired when you spin up the sandbox	In-memory map
K8s workload	GKE/KSA → projected JWT ➜ aud=...	Verify sig, map to service_account_id
Ray task/actor	Ray’s internal worker_id header	Ray’s key-value store

The MCP API layer converts whatever credential it got into a single UUID‐ish actor_id plus a set of roles (strings) and passes those to the broker with every enqueue().

python
Copy
Edit
actor_ctx = ActorCtx(
    id=UUID("..."),
    roles={"sales-bot", "writer"},
    org="acme-corp",
)
await broker.enqueue(cmd, actor_ctx)
2 RBAC data model (minimal but expressive)
text
Copy
Edit
role_bindings      [actor_id, role]
role_permissions   [role, op, component, entity_selector]
entity_selector can be:

'*' → any entity

'owner' → entities where entity.owner_id == actor_id

'tag:shop' → entities labelled with a tag

'expr:health < 50' → declarative filter

Store these two tables in Daft (or DuckDB, Postgres—doesn’t matter). They’re tiny and get cached by the broker for millisecond reads.

3 Validator flow
python
Copy
Edit
def validate(cmd: Command, ctx: ActorCtx) -> bool:
    # 1. fetch all roles for actor (cached set lookup)
    roles = ctx.roles or lookup_roles(ctx.id)

    # 2. gather all permissions for those roles (cached)
    perms = get_permissions_for_roles(roles)

    # 3. find matches
    for p in perms:
        if p.op != cmd.op:           # fast reject
            continue
        if not entity_allowed(p, cmd.payload, ctx):
            continue
        return True                  # first allow wins
    return False                     # fallback deny
entity_allowed() inspects entity_selector:

'*'   → always true

'owner' → check entity.owner_id == ctx.id (the World already keeps owner_id as a column)

'tag:…' → set-membership on entity’s tag column

'expr:…' → evaluate a Daft predicate if you want fancy filters

Why before the heap?
Latency: the check is a dict/tuple lookup—not a database scan—so it costs microseconds.

Safety: we never record disallowed commands in the audit log, keeping replays clean.

Back-pressure: if an LLM loops badly the broker rejects immediately → no heap growth.

4 Tying entities to identity
Add two standard columns to every entity table:

python
Copy
Edit
owner_id: UUID      # who “owns” this entity (can be a user, svc acct, agent)
group_id: UUID      # optional tenant / org / project
A spawn command must set owner_id (defaults to caller). That’s enough for:

Multi-tenant worlds (filter by group_id)

“owner only” ops (selector = 'owner')

Service accounts (grant bots role = world-maintainer)

No heavyweight IAM is required; you can still integrate with cloud IAM by mapping KSA → group_id.

5 Example in code
python
Copy
Edit
# guardrails.py
from async_command import Command
from models import ROLE_CACHE, PERM_CACHE, WORLD_STATE

async def guardrail_allow(cmd: Command, ctx: ActorCtx) -> bool:
    for perm in PERM_CACHE[ctx.roles]:
        if perm.op != cmd.op:
            continue
        if perm.component and perm.component != cmd.payload.get("component"):
            continue
        if match_entity_selector(perm.selector, cmd.payload, ctx):
            return True
    return False

def match_entity_selector(sel, payload, ctx):
    if sel == "*":
        return True
    if sel == "owner":
        eid = UUID(payload["entity_id"])
        owner = WORLD_STATE.owner_of(eid)
        return owner == ctx.id
    if sel.startswith("tag:"):
        tag = sel.split(":", 1)[1]
        return tag in WORLD_STATE.tags_of(payload["entity_id"])
    if sel.startswith("expr:"):
        expr = sel[5:]
        # delegate to Daft filter engine
        return WORLD_STATE.test_expression(expr, payload["entity_id"])
    return False
The broker calls guardrail_allow() right before log-append/heap-push:

python
Copy
Edit
async def enqueue(self, cmd, ctx):
    if not await guardrail_allow(cmd, ctx):
        raise HTTPException(403, "RBAC deny")
    await log_append(cmd)
    heapq.heappush(self._heap, cmd)
6 How this plays with Daft-lazy processors
Nothing changes:

Processors run after the authoritative state mutations, so owner_id, tags, etc. are already updated and visible inside DataFrame queries.

RBAC only inspects metadata—never forces an early .collect(). So laziness and vectorised compute stay intact.

Cheat-sheet
Auth (who are you?) → MCP layer (JWT, OAuth, mTLS)

ActorCtx attaches id + roles to every enqueue()

RBAC Guardrail lives inside the broker, runs in <1 ms, decides Allow/Deny

Entities carry owner_id & optional group_id for selectors

Permissions live in tiny cached tables; OPA/Rego optional if you prefer declarative policy files

This pattern survives local dev → Ray actor → multi-cluster scale with zero API changes.

Once you wire this in, your agent terminals can be as wild as they like—every command still passes through the same unified choke-point, so violations are blocked early and audited consistently.

In the new command-queue world, an “agent” isn’t a first-class Python class that lives inside the ECS loop.
It’s a policy-plus-identity sitting outside the World, talking to it only through the broker’s enqueue() and (optionally) a read-only query API.
Think of an agent as a configurable adapter that turns observations → commands.

1 Canonical agent anatomy
Facet	Stored where	Purpose
Actor ID (UUID)	comes in every enqueue()	Identity for RBAC, quotas, audit
Role set ({string})	RBAC tables	What ops/components it may touch
Observation contract	“view” definition in World API	Which slice of state it can read (e.g. own entities only)
Policy impl	Anything callable → List[Command]	LLM, rules engine, PPO model…
Sampling & runtime params	YAML / JSON manifest	temperature, max_tokens, batch size
Budget / quota	per-tick or per-day counters in broker	Stops runaway loops
Startup blueprint (optional)	spawn-time Commands	Gives the agent an initial entity/body

TL;DR: An agent is just agent.yaml + a small runner; the World never sees the code—only its commands.

2 Typical agent.yaml
yaml
Copy
Edit
id: 94b34d7e-f40b-44e3-9d43-8c53f3a2c6cf
name: cart-bot
roles: ["buyer", "movement"]
startup:
  - op: add_component
    payload:
      entity_id: "${AGENT_ID}"
      component: Wallet
      data: { balance: 100.0 }
  - op: add_component
    payload:
      entity_id: "${AGENT_ID}"
      component: Position
      data: { x: 0, y: 0 }
policy:
  type: openai-chat
  model: gpt-4o-mini
  system_prompt: |
    You control a shopping agent in a grid world…
  sampling:
    temperature: 0.3
    max_tokens: 128
observe:
  query: |
    SELECT entity_id, item_price
    FROM Market
    WHERE distance(Position, $self) < 3
budget:
  commands_per_tick: 5
  daily_tokens: 50_000
3 How the runner works (pseudo-code)
python
Copy
Edit
async def run_agent(cfg: AgentCfg, world_api: WorldAPI, broker: Broker):
    await broker.enqueue_bulk(render_startup(cfg.startup, cfg.id))

    while True:
        obs = await world_api.fetch_view(cfg.observe.query, actor_id=cfg.id)
        cmds = await cfg.policy(obs)          # LLM or rule
        limited = cmds[: cfg.budget.commands_per_tick]
        await broker.enqueue_bulk(limited)
        await asyncio.sleep(world_api.tick_interval)
That’s all the orchestration you need.
Multiple agents = multiple such tasks or Ray actors, each streaming commands into the same broker.

4 How this differs from a processor
Aspect	Processor	Agent
Lives where	Inside World.step()	Outside, any runtime
Determinism	Pure function on DF; no network	May call LLMs, random, network
RBAC	Runs with full authority	Scoped by roles/quotas
Timing	Always each tick, semaphore-bounded	Any cadence; async
Failure	Fails world tick	Only its own task; commands can be retried

Processors mutate mechanics of the world (physics, economy).
Agents mutate strategy inside the rules the processors enforce.

5 Why “unique configs” is enough—for now
Because:

Command schema is the only write surface → agents never need direct object references.

Query/views give controlled read access → no risk of leaking entire state.

RBAC + budgets enforce safety before heap/log.

Hot-swap: update agent.yaml, restart runner — no world deploy required.

When you later want marketplace plugins or third-party skills, you can wrap these same YAML+policy binaries behind a thin MCP HTTP layer, but the core definition stays exactly the same.

Cheat-sheet
Agent = Config + Policy + Broker client

World only knows actor_id and validated commands.

RBAC, budgets, and views isolate behaviour.

You can ship v0.1 with agents as “just launch scripts” and evolve toward MCP if/when external plugins appear.

That gives you clear boundaries, replayability, and a very low-friction path to spin up “recursive simulations exploring alternative realities”—each agent runner simply targets a cloned World ID and its own command log.