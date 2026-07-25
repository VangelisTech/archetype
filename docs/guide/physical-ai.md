# Physical AI

**Document type:** Contract and user guide.

Archetype treats a physical-policy evaluation as one durable world containing
many trial entities. The runtime submits a typed request; the physical-AI
family handler creates the world, installs the standard processors, runs the
episode, and derives a report from persisted state.

```text
one evaluation request
        │
        ▼
one control-plane world ── one durable (world_id, run_id)
        │
        ├── trial entity 0 ── ManipTask + observation + action + status
        ├── trial entity 1 ── ManipTask + observation + action + status
        └── trial entity N ── ManipTask + observation + action + status
```

The world is the evidence. A report is a typed projection of its terminal
`ManipStatus` rows, not a second summary component that can drift away from
the ledger.

## Run one task evaluation

Use `ArchetypeRuntime`; do not assemble world, mutation, simulation, and
evaluation services in application code.

```python
from archetype import ArchetypeRuntime, PhysicalTaskEvalConfig, StorageConfig
from archetype.physical_ai.manipulation import (
    ManipStatus,
    ManipTask,
    ScriptedReachEnv,
)
from archetype.physical_ai.policy import ScriptedReachPolicy

storage = StorageConfig(uri="./data", namespace="physical-evals")
targets = {
    0: (0.10, 0.0, 0.5),
    1: (0.20, 0.0, 0.5),
}

env = ScriptedReachEnv(targets=targets, tolerance=0.02)
policy = ScriptedReachPolicy(targets=targets, gain=0.5, max_step=0.05)

async with ArchetypeRuntime() as runtime:
    report = await runtime.evaluate_physical_task(
        PhysicalTaskEvalConfig(
            suite="scripted-reach",
            task_id=0,
            trials=2,
            max_steps=40,
            storage=storage,
        ),
        env_client=env,
        policy_client=policy,
    )

    print(report.success_rate, report.mean_length)

    # The report points back to the evidence that produced it.
    world = runtime.attach(report.world_id, storage=storage)
    rows = await world.query(ManipStatus, ManipTask)
```

`env_client.task_language()` supplies the instruction when the provider offers
it. Otherwise `PhysicalTaskEvalConfig.instruction` is used. Omitting the policy
client leaves the spawned default `ManipAction` in place, which is useful for
an environment-only baseline.

The synchronous facade has the same operation without `await`:

```python
with ArchetypeRuntime.sync() as runtime:
    report = runtime.evaluate_physical_task(
        config,
        env_client=env,
        policy_client=policy,
    )
```

## Compare instructions on paired seeds

An instruction sweep changes only the language supplied to the policy. Every
variant receives the same seed slots, making the comparison paired instead of
confounding the instruction with different initial states.

```python
from archetype import InstructionSweepConfig

config = InstructionSweepConfig(
    suite="scripted-reach",
    task_id=0,
    variants=(
        "reach",
        "reach the red block",
        "reach the red block precisely",
    ),
    seeds_per_variant=5,
    max_steps=40,
    storage=storage,
)

async with ArchetypeRuntime() as runtime:
    report = await runtime.sweep_physical_instructions(
        config,
        env_client=env,
        policy_client=policy,
    )

print(report.scores)
print(report.best)
```

For task `T` and seed slot `S`, the seed is `T * 1000 + S`. `env_key` remains
unique because it routes processor rows to a particular environment instance;
it does not choose the initial state. Reordering variants therefore cannot
change the seeds used to grade one instruction. Duplicate instruction strings
collapse to one condition.

`max_steps` includes the raw reset-observation tick. A budget of `N` therefore
permits at most `N - 1` policy-controlled environment steps.

## Execution sequence

```mermaid
sequenceDiagram
    participant Host
    participant Runtime as ArchetypeRuntime
    participant Dispatcher as CommandDispatcher
    participant Owner as RuntimeResources
    participant Physical as physical_ai.handlers
    participant Providers as Env + Policy Providers
    participant World as World + Processors
    participant Simulation as world.simulation
    participant Query as world.query + StorageService

    Host->>Runtime: evaluate request + env/policy providers
    Runtime->>Dispatcher: apply(EvaluatePhysicalTask)
    Dispatcher->>Physical: registered handler
    Physical->>Owner: register + lease each unique provider identity
    Physical->>World: create one uniquely named world
    Physical->>World: install policy and environment processors
    Physical->>Providers: reset each environment by seed
    Physical->>World: batch-spawn trial entities
    Physical->>Simulation: run_episode(all ManipStatus.done)
    loop each committed tick
        Simulation->>World: process every live trial
        World-->>World: append component state
    end
    Physical->>Query: query ManipStatus + ManipTask by world/run
    Query-->>Physical: lazy persisted frame
    Physical->>World: retire live writer; retain durable rows
    Physical-->>Dispatcher: ledger-derived typed report
    Dispatcher-->>Runtime: report
    Runtime-->>Host: report with world_id + run_id
    Host->>Runtime: exit runtime context
    Runtime->>Owner: aclose()
    Owner->>Providers: async aclose once per identity
```

## Ownership

| Location | Responsibility |
| --- | --- |
| `archetype.physical_ai.interfaces` | Canonical environment, policy, and lifetime-registration protocols |
| `archetype.physical_ai.models` | Operation models, configuration values, outcomes, and reports |
| `archetype.physical_ai.contracts` | One-release object-identical compatibility re-exports for moved value contracts |
| `archetype.physical_ai.manipulation` | ECS Components, scripted environment, and internal environment-step processors |
| `archetype.physical_ai.policy` | Scripted policy and internal action processor |
| `archetype.physical_ai.optimization` | Pure, callback-driven instruction search |
| `archetype.physical_ai.views` | Storage-backed terminal report projection |
| `archetype.physical_ai.handlers` | Free world/processor/episode/query workflows over declared storage and world ports |
| `CommandDispatcher` | Exact-operation admission and registered handler dispatch |
| `RuntimeResources` | Process-scoped ownership and retryable close of live providers |
| `ArchetypeRuntime` | Supported trusted Python entry point and sync parity |
| world/simulation/storage families | Lifecycle, tick execution, and persisted reads |

The separation is intentional:

- Components and processors decide per-tick state transitions.
- `archetype.world.simulation` owns episode execution and value-based
  termination.
- `archetype.physical_ai.handlers` owns the workflow meaning but delegates
  lifecycle, mutation, simulation, query, and physical execution to the
  declared world and storage ports.
- External simulator and model implementations own their resource behavior,
  while `RuntimeResources` owns their process lifetime after transfer.
- The runtime exposes the capability without exposing a concrete service or
  live `AsyncWorld`.

`EvaluatePhysicalTask` and `SweepPhysicalInstructions` are exact,
application-scoped registrations. Both are trusted-only, direct,
non-durable, and zero-token operations. There is currently no REST operation
for physical evaluation. A future remote surface must add an explicit
actor-aware registration and API contract rather than treating the trusted
runtime method as an authentication boundary.

## Provider lifetime

`EnvClient` and `PolicyClient` are live host resources, not portable command
payloads. Construction must be inert, and every provider must expose
`async aclose()`. Passing a provider to either registered physical operation
transfers that exact object to the runtime's process owner synchronously before
world creation, policy reset, environment reset, or any other workflow or
provider effect.

Ownership is deduplicated by object identity for the runtime lifetime. Reusing
one provider across operations, or supplying the same object as both
environment and policy, creates one owner and one close. Cancelling the
operation does not discard that ownership. `RuntimeResources.aclose()` is the
authoritative host-side close boundary; if a provider close fails, the failed
owner is retained and retried on the next close while providers that closed
successfully are not repeated.

The registrar also returns an exclusive, identity-ordered lease held for the
complete evaluation or sweep. Two operations sharing an environment, a policy,
or a dual-role provider therefore cannot overlap mutable provider state.
Operations whose provider identity sets are disjoint may still progress
concurrently. Raw-client step/action processors are internal implementation
details, so generic processor installation is not a supported route around the
lease and ownership boundary.

Before either handler releases its provider lease, it retires the live world
writer through an exact-lease retirement handle returned by the provider-scoped
lifetime token. Registered `DestroyWorld` and that handle join the same
process-owned reconcile, command-cancel, and lifecycle-close transaction; the
handle never resolves a later replacement by world ID. A provider close first
joins every evidence-world retirement associated with that provider identity,
so failed cleanup retains both owners for shutdown retry. The returned
`world_id` and `run_id` remain valid durable query coordinates, including for
nonterminal trials that hit `max_steps`, but attaching that identity cannot
execute the retained provider processors because no live writer remains.

Daft 0.7.19 provides no deterministic teardown hook for `@daft.cls` instances.
Consequently, worker-local environment or policy Specs are unsupported and
have been removed. A serialized processor handle may reconnect to the
host-owned backing resource, but it must not construct an independently owned
closeable or I/O-backed worker-local provider, socket, or process. The exact
`_CartpoleStepper` exception constructs only non-I/O MuJoCo model/data scratch
from embedded XML, exposes no application-controlled close, and dies with the
worker. A future safe worker-local provider construction contract is tracked
separately in issue #667.

## Instruction optimization

`optimize_instruction` is pure orchestration over an injected evaluator:

```python
from archetype.physical_ai.optimization import (
    TemplatePerturbation,
    optimize_instruction,
)

async def evaluate(instructions: list[str]) -> dict[str, float]:
    config = InstructionSweepConfig(
        suite="scripted-reach",
        task_id=0,
        variants=tuple(instructions),
        seeds_per_variant=5,
        max_steps=40,
        storage=storage,
    )
    report = await runtime.sweep_physical_instructions(
        config,
        env_client=env,
        policy_client=policy,
    )
    return report.scores

result = await optimize_instruction(
    evaluate=evaluate,
    base="reach",
    strategy=TemplatePerturbation(("red", "block", "precisely")),
    rounds=4,
    neighbors=3,
)
```

The callback is the boundary. It can execute real paired episodes, or a future
model-based scorer can evaluate candidates without changing the search
algorithm. The deterministic template strategy exists to verify the mechanism;
it is not evidence that language optimization improves a real policy.

## Evidence invariants

The workflow enforces these contracts:

1. One request produces one control-plane world and one active run identity.
2. Every requested trial must have a terminal ledger row before a report is
   returned; a reduced denominator fails loudly.
3. Task evaluation seeds are deterministic by trial index.
4. Sweep seeds are paired by seed slot and independent of variant position.
5. Stateful policy clients are reset at each evaluation boundary when they
   expose `reset()`.
6. Success and episode length come from the latest persisted `ManipStatus` row.
7. The workflow binds and retires its exact live writer before releasing
   provider leases while preserving durable evidence addressable by the
   returned identifiers.
8. Every unique provider identity transfers to process ownership before the
   first effect, is exclusively leased for the full workflow, waits for its
   associated exact-world cleanup, and remains owned across cancellation until
   authoritative host shutdown succeeds.

The credential-free contracts live under `tests/physical_ai/`. Real LIBERO,
VLA, GPU, and Modal adapters remain external provider implementations and paid
dogfoods; the physical-AI family does not import them.
