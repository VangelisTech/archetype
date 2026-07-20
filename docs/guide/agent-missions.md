# Agent Missions V1

**Document type:** Normative V1 contract.

**Status:** Accepted target; implementation migration is in progress.

Agent Missions is Archetype's first software factory. An author submits a
repository, a branch, and a graph of coding tasks guarded by the repository's
own validators. Archetype records the graph, commits every decision as world
state, and advances work only when current evidence permits the transition.

Archetype does not own how an agent writes code. It owns **when work may
start, which observations may advance it, and why every transition occurred**.

> **The V1 contract**
>
> Tasks and validators are entities. Dependencies are relations. Processors
> are the transition authority. A dispatch is committed intent. Agent and
> sandbox activity are observations. Only validator results bound to the
> current dispatch and repository revision can accept a task.

## 1. The contract in one view

| Primitive | Responsibility |
|---|---|
| World | The durable state machine for missions, tasks, relations, dispatches, executions, and outputs. |
| Mission | Names one repository objective and rolls its task graph into one result. |
| Task | Holds one atomic goal, workflow state, retry policy, and repository coordinates. |
| Validator | Describes one executable acceptance check; `Guards` relates it to a task. |
| Relations | Express membership, dependencies, execution placement, and provenance without serialized plans. |
| Processors | Decide readiness, dispatch, retry, failure, acceptance, and mission rollup. |
| `TaskDispatch` | Records committed permission to perform a particular task revision. It is intent, not an attempt object. |
| `AgentExecution` | Records what an agent process did for a dispatch. It never says whether the task was accepted. |
| Sandbox | Records the lifecycle of an isolated filesystem and process container. It never says whether the task was accepted. |
| Outputs | Record validator results, commits, checkpoints, manifests, friction, and published artifacts. |
| Sandbox service | Owns backend selection and live session lifetime; it has no workflow authority. |
| Application service | Materializes the graph, crosses committed I/O boundaries, stages observations, and returns projections. |

The governing separation is:

```text
Archetype owns transitions as data.
The sandbox owns isolated filesystem and process capabilities.
The agent execution records what happened.
The repository harness owns acceptance.
```

Daft evaluates state transforms and joins. It does not keep an agent process
alive or schedule a Modal sandbox. External work begins only after the tick
containing its dispatch has committed.

## 2. Public authoring surface

Configuration happens once. Authors submit typed values; they do not construct
Components, wire processors, manage `GraphView`, or serialize a plan.

```python
import asyncio

from archetype import ArchetypeRuntime
from archetype.missions import AgentMissionConfig, AgentTask, CommandValidator
from archetype.missions.sandboxes.modal import ModalSandboxBackend


MISSION_CONFIG = AgentMissionConfig(
    sandbox_backend=ModalSandboxBackend(
        auth_volume_name="archetype-codex-auth",
        github_secret_name="archetype-github",
    ),
    max_ticks=40,
)

TASKS = (
    AgentTask(
        name="regression",
        prompt="Add a deterministic regression test. Do not change production code.",
        validators=(
            CommandValidator(
                name="regression_is_red",
                command=("uv", "run", "pytest", "-q", "tests/app/test_bug.py"),
                expected_returncode=1,
            ),
        ),
    ),
    AgentTask(
        name="implementation",
        prompt="Make the regression pass with the smallest layer-correct fix.",
        validators=(
            CommandValidator(
                name="focused_contract",
                command=("uv", "run", "pytest", "-q", "tests/app/test_bug.py"),
            ),
            CommandValidator(
                name="architecture",
                command=("uv", "run", "python", "scripts/check_architecture.py"),
            ),
        ),
        depends_on=("regression",),
    ),
)


async def main() -> None:
    async with ArchetypeRuntime() as runtime:
        async with runtime.missions(
            "fix-bug",
            config=MISSION_CONFIG,
            storage=".context/agent-missions/data",
        ) as missions:
            mission = await missions.submit(
                repository="VangelisTech/archetype",
                branch="agent/fix-bug",
                tasks=TASKS,
            )
            result = await missions.run(mission)

    print(result.status)
    for task in result.tasks:
        print(task.name, task.status, task.dispatches, task.commit_shas)


asyncio.run(main())
```

`CommandValidator` and `AgentTask` are authoring values. Submission compiles
them into Validator and Task entities plus relations. The convenient surface
does not turn validator definitions or task dependencies back into JSON blobs.

### Submission contract

`missions.submit(...)` accepts `list[AgentTask]` or any finite sequence of
tasks. V1 requires:

- non-empty repository, branch, base ref, and task sequence;
- unique, non-empty task and validator names;
- non-empty prompts and at least one validator per task;
- positive dispatch budgets;
- dependencies that name tasks in the same submission;
- an acyclic dependency graph; and
- a pinned sandbox environment plus an explicit publication policy.

The sequence is already the planner seam. A later planner may take one large
task and emit many tasks and relationships. Task decomposition is not part of
V1.

## 3. Architecture and ownership

```mermaid
flowchart LR
    Author[Mission author] --> Runtime[RuntimeMissions]
    Runtime --> App[MissionService]
    App --> World[Mission world]

    subgraph Domain[archetype.missions]
        State[Components + relations]
        Behavior[Processors + transitions]
        Agents[Coding-agent harness]
        Sandboxes[Sandbox service + backends + sessions]
    end

    World --> State
    World --> Behavior
    App --> Agents
    Agents --> Sandboxes
    Sandboxes --> Provider[Modal / future providers]
    App --> World
```

`archetype.missions` owns the reusable family:

- mission, task, validator, sandbox, execution, and output Components;
- relations and pure DataFrame transition logic;
- built-in processors and projections;
- coding-agent protocols and harness behavior;
- sandbox Service, Backend, and Session contracts; and
- capability-scoped provider adapters such as Modal.

`archetype.app.missions` owns application composition:

- reserve identities and materialize submitted graphs;
- configure a world with the built-in mission behavior;
- step the state machine;
- cross the post-commit boundary and invoke the coding-agent harness;
- stage factual observations through the mutation path;
- compose artifact publication and evaluation when requested; and
- return supported mission projections.

The application service does not decide readiness, retry, acceptance, or
mission success. Those decisions remain visible in processors and persisted
state.

### World topology is not sandbox topology

A world is a state machine, not a sandbox. The same contract permits one world
with many sandboxes, many worktrees in one sandbox, several agents in separate
worktrees, or cooperating agents in one worktree. Placement policy may change
without changing task readiness.

V1 may choose one persistent repository session per mission and serialize
tasks that share its worktree. That is a provider policy, not an ECS invariant.

## 4. State and transition protocol

```mermaid
sequenceDiagram
    autonumber
    actor Author
    participant Service as MissionService
    participant World
    participant Processors
    participant Harness as CodingAgentHarness
    participant Sandboxes as SandboxService

    Author->>Service: submit(repository, branch, tasks)
    Service->>World: stage mission, tasks, validators, relations

    loop until mission is terminal
        Service->>World: step()
        World->>Processors: evaluate N from committed N-1
        Processors-->>World: state changes + TaskDispatch
        World->>World: commit tick N
        World-->>Service: PostTick(committed dispatches)
        opt a current dispatch is ready for external work
            Service->>Harness: execute(dispatch)
            Harness->>Sandboxes: acquire session and run tools
            Sandboxes-->>Harness: process + filesystem observations
            Harness-->>Service: execution, validation, Git, optional recovery outputs
            Service->>World: stage observations
        end
    end

    Service-->>Author: terminal projection
```

The PostTick boundary is a safety property: no sandbox sees speculative work.
If tick persistence fails, its dispatch cannot leak an external side effect.

### Task state

```mermaid
stateDiagram-v2
    [*] --> pending
    pending --> ready: all prerequisites accepted at N-1
    ready --> dispatched: commit TaskDispatch
    dispatched --> accepted: current revision passes every guard
    dispatched --> ready: evidence fails and budget remains
    dispatched --> failed: evidence fails and budget is exhausted
    accepted --> [*]
    failed --> [*]
```

The built-in pipeline has four concerns:

| Order | Processor | Authority |
|---:|---|---|
| 10 | Task decision | Consume observations for the current dispatch and accept, retry, or exhaust. |
| 20 | Task readiness | Make a task ready only when every prerequisite was accepted in the previous committed tick. |
| 30 | Task dispatch | Convert ready state into one durable dispatch identity. |
| 40 | Mission rollup | Succeed only when every member task is accepted; fail when a member task is terminally failed. |

There is no durable `Attempt` aggregate. Retrying produces a new
`TaskDispatch`; history preserves every prior dispatch and observation.

### Intent, observation, decision

These concepts never collapse into one status field:

| Kind | Examples | May decide task state? |
|---|---|---:|
| Intent | `TaskDispatch` | No |
| Runtime observation | `Sandbox`, `AgentExecution` | No |
| Work output | `ValidationResult`, `Commit`, `Checkpoint`, `FilesystemManifest`, `FrictionLog`, artifact refs | No |
| Decision | `TaskState` written by the task decision processor | Yes |

`TaskDispatch` identifies the task, dispatch sequence, requested repository
base, and policy. `AgentExecution` records process lifecycle such as
`starting`, `running`, `exited`, `errored`, or `interrupted`.

Sandbox lifecycle is separate: `provisioning`, `ready`, `errored`,
`interrupted`, or `closed`. A sandbox is never `accepted`, `rejected`, or
`completed`; it is a container that may host zero or many executions.

### Relations and previous-tick visibility

At minimum, V1 materializes:

```text
PartOfMission(source=task, target=mission)
DependsOn(source=task, target=prerequisite)
Guards(source=validator, target=task)
Executes(source=execution, target=task)
RunsIn(source=execution, target=sandbox)
ProducedBy(source=output, target=execution)
```

Readiness and rollup use `GraphView`, which is strictly previous-tick. If task
A is accepted at N, dependent task B may become ready no earlier than N+1.
Edges are temporal entities, so dependency, provenance, and fork inheritance
remain queryable without decoding a plan blob.

## 5. Sandbox and validator protocol

The sandbox vocabulary describes a resource, not a workflow:

```python
class SandboxBackend(Protocol):
    async def create(self, spec: SandboxSpec) -> SandboxSession: ...
    async def restore(
        self, spec: SandboxSpec, checkpoint: CheckpointRef
    ) -> SandboxSession: ...


class SandboxSession(Protocol):
    @property
    def identity(self) -> SandboxIdentity: ...

    async def status(self) -> SandboxStatus: ...
    async def exec(self, request: ProcessRequest) -> ProcessResult: ...
    async def checkpoint(self) -> CheckpointRef: ...
    async def close(self) -> None: ...


class SandboxService:
    async def acquire(self, key: SandboxKey, spec: SandboxSpec) -> SandboxSession: ...
    async def close(self, key: SandboxKey) -> None: ...
    async def close_all(self) -> None: ...
```

- Backend creates or restores provider resources.
- Session is the live handle for process and snapshot capabilities.
- Service selects a backend, reuses sessions according to policy, and owns
  shutdown.

The coding-agent harness works through `SandboxSession`. It owns clone and
branch preparation, agent invocation, validator execution, Git publication,
and translation into factual Components. Provider adapters do not know task
state and do not return an acceptance verdict.

Checkpointing is optional resumability. A checkpoint is a lightweight,
content-addressed reference to a sandbox snapshot; a filesystem manifest is a
queryable observation of captured state. Neither is required to accept a task.

### Repository validators are authority

Submission materializes each `CommandValidator` as an entity related to its
task. Every execution emits one `ValidationResult` per guard containing:

- validator, task, dispatch, execution, and repository-revision identity;
- the expected and observed return codes;
- bounded stdout/stderr or artifact references; and
- timing and error observations.

`passed` is derived from `actual_returncode == expected_returncode`. It is
never trusted from a sandbox or agent response. Expected nonzero codes are
valid—for example, a predecessor can prove a regression test is red.

The task decision processor accepts only when all guards have a result for the
**current dispatch and exact final repository revision**. Evidence from a
prior dispatch or pre-repair tree is stale by construction.

### Git and publication

Git is part of the coding contract:

1. the harness records the dispatch's starting revision;
2. the agent may create commits during its work;
3. validators run against the final working tree;
4. if validated work remains dirty, the publisher creates one final commit;
5. every commit created during the dispatch is recorded; and
6. the configured branch policy publishes the validated final revision.

The publisher never resets valid agent-authored commits merely to manufacture
one synthetic result. Acceptance binds validation and publication evidence to
the same final revision.

### Friction and artifacts

`FrictionLog` is one timestamped observation entity, not an append-only JSON
field. It may reference a task, dispatch, execution, validator, path, or commit
so later analysis can group failure modes across sessions.

Large outputs use content-addressed artifact references: digest, media type,
size, and a storage hint. The mission application service may compose the
Artifacts family to persist or ingest them; the sandbox protocol does not
become a storage system.

## 6. Prefabs and planning

V1 submission directly materializes tasks, validators, and relations. That is
enough for correct execution order and remains the simplest dogfood surface.

A later mission prefab may author the same graph. Prefab instantiation does not
decide readiness or replace the processors. Registration code installs
Components and behavior; durable prefab library data describes the graph.
Manifests may declare allowlisted behavior-module requirements but never
auto-import executable code.

A planner will have the same output boundary: `list[AgentTask]` plus
relationships. HTN decomposition is useful, but is not a V1 correctness gate.

## 7. V1 boundary

### Included

- explicit task and validator entities;
- temporal membership, dependency, guard, placement, and provenance relations;
- previous-tick readiness joins;
- processor-owned acceptance, retry, exhaustion, and rollup;
- post-commit dispatch;
- separate sandbox and agent-process lifecycle;
- repository validators with expected nonzero support;
- revision-bound validation and Git publication evidence;
- first-class commits, checkpoints, manifests, friction, and artifact refs;
- a Modal backend; and
- terminal result projection and cleanup.

### Deliberately not included

- task decomposition or HTN planning;
- prefab-driven readiness;
- claims, leases, fences, receipts, or a mission-specific control catalog;
- a six-phase sandbox kernel;
- an `Attempt` aggregate;
- PR creation, CI watching, review, merge, or deployment;
- a general relationship-to-sandbox placement scheduler; and
- a requirement that checkpoints or manifests gate acceptance.

The retired claim/fence/finalization subsystem is not a compatibility layer for
this contract. Cleanup stops creating or consuming its tables and routes while
leaving existing persisted tables inert; deleting historical operator data is
a separate, explicit migration decision.

### Current hardening gaps

| Gap | V1 treatment | Later seam |
|---|---|---|
| Cold process resume | A live dogfood may recover from an explicit checkpoint; no fleet claim is implied. | Reinstall registration bundles and reconcile committed dispatches through a reviewed outbox contract. |
| Sandbox placement | Use a simple configured policy. | Add a scheduler only when multiple topologies require one. |
| Task decomposition | Authors submit the graph. | Planner emits the same typed graph. |
| Terminal interaction | `exec` is the required capability. | Add optional PTY/tmux/ttyd capabilities without widening workflow authority. |
| Artifact ingestion | Record content-addressed refs. | Compose `archetype.app.artifacts` for typed ingestion. |
| Prefab mission libraries | Direct materialization remains authoritative. | Author reusable graphs after generic prefab registry contracts settle. |

## 8. File and responsibility map

This is the destination layout for the cleanup:

| File | Owns |
|---|---|
| `archetype/missions/contracts.py` | Supported authoring, configuration, and result values. |
| `archetype/missions/components.py` | Mission, task, validator, dispatch, sandbox, execution, and output Components. |
| `archetype/missions/relations.py` | Membership, dependency, guard, placement, and provenance Relations. |
| `archetype/missions/transitions.py` | Small persisted status vocabularies and transition tables. |
| `archetype/missions/processors.py` | Task decision, readiness, dispatch, and mission rollup authority. |
| `archetype/missions/projections.py` | Supported mission/task/execution result projections. |
| `archetype/missions/coding_agents/contracts.py` | Coding-agent request and driver protocols. |
| `archetype/missions/coding_agents/harness.py` | Repository preparation, agent invocation, validation, Git publication, and observation translation. |
| `archetype/missions/sandboxes/contracts.py` | Sandbox Backend, Session, process, status, and snapshot value contracts. |
| `archetype/missions/sandboxes/service.py` | Backend registry and live-session lifetime. |
| `archetype/missions/sandboxes/modal.py` | Modal Backend and Session implementation only. |
| `archetype/app/missions/service.py` | Graph materialization, tick/I/O composition, cross-family workflows, and projections. |
| `archetype/runtime/missions.py` | Mission-author runtime handle and lifecycle. |
| `examples/11_coding_agent_mission.py` | Real typed dogfood script. |
| `tests/missions/` | Family contract, transition, sandbox, harness, and provider oracles. |
| `tests/integration/test_agent_mission_service.py` | End-to-end graph, retry, revision binding, and cleanup oracle. |

No author imports a Component, processor, `GraphView`, application service, or
provider SDK to run the built-in workflow.

During migration, existing `coding_agents/components.py`,
`coding_agents/processors.py`, `coding_agents/resources.py`, and
`app/missions/agent_service.py` are temporary source locations—not an
alternative contract.

## 9. Family direction after V1

Agent Missions establishes the repository convention: reusable state, pure
behavior, and capability-scoped resources live in the named family; the app
layer owns durable application authority and cross-family composition.

```text
archetype.missions
├── components.py
├── relations.py
├── transitions.py
├── processors.py
├── projections.py
├── coding_agents/
├── sandboxes/
├── planning/
└── trajectories/

archetype.app.missions
└── service.py
```

The orphan cleanup follows the same rule:

| Capability | Owner |
|---|---|
| Planning / former HTN | `archetype.missions.planning` |
| Mission trajectories | `archetype.missions.trajectories` with app query/evaluation composition |
| Artifact and transcript ingestion | `archetype.app.artifacts` consuming family-owned value contracts |
| Physical-AI state and behavior | `archetype.physical_ai` |
| Physical-AI workflows | `archetype.app.physical_ai` |
| Research state and pure decoding | `archetype.research` |
| Research workflows | `archetype.app.research` |
| Vendor-neutral observability vocabulary | `archetype._obs` |

Datasets, Experiments, Contrib, and a production RTS vocabulary are not
families. The Biome prefab remains an example; its reusable pattern belongs in
generic prefab machinery.

## 10. Verification

The credential-free contract lane must prove:

- graph materialization and cycle rejection;
- previous-tick dependency ordering;
- post-commit-only dispatch;
- retry with a new dispatch identity;
- stale-revision evidence rejection;
- expected-nonzero validator derivation;
- agent-authored and publisher-authored commit preservation;
- sandbox/agent/task lifecycle separation;
- terminal cleanup; and
- example dry-run execution.

The live Modal dogfood must additionally prove real repository preparation,
agent execution, validation, commit/push publication, evidence-carrying repair,
and sandbox teardown. It is paid and credentialed, so it remains an explicit
operation rather than an ordinary CI test.

## Companion contracts

- [Runtime](runtime.md)
- [Application Architecture](application-architecture.md)
- [Service Protocols](service-protocols.md)
- [Repository Harness](repository-harness.md)
- [Prefab Libraries](prefab-libraries.md)
- [Graph system design](../design/graph-system.md)
