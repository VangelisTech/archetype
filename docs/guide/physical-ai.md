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

The direct path's `max_steps` includes the raw reset-observation tick. A budget
of `N` therefore permits at most `N - 1` policy-controlled environment
transitions. The hosted whole-episode contract names that action budget
`max_transitions` instead: reset is trajectory row `0`, and transitions are
rows `1..max_transitions`. The exact bridge is therefore
`max_transitions = max_steps - 1` (and `max_steps = max_transitions + 1`).
A zero-transition hosted request is valid and produces one terminal reset row.

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
| `archetype.physical_ai.hosted_episode` | Canonical whole-episode Arrow schemas, codecs, identities, digests, completeness validation, and the family-owned hosted choreography target |
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

### Accepted hosted-episode Activity target

The current direct path above remains supported until its owning cutover
passes. It supplies environment and policy clients to internal per-step
processors and owns those providers for the full workflow. That lifetime
machinery proves cleanup; it does not make a provider mutation inside a
retryable tick recoverable after process loss.

The accepted hosted path uses the [Activity](activities.md) boundary:

```text
committed physical trial intent
        |
        v
whole-episode Activity outside the world lock
        |
        v
durable Arrow trajectory reference + digest
        |
        v
later committed physical observation
        |
        v
pure terminal projection and report
```

`archetype.physical_ai` owns request/result schemas, physical meaning, provider
protocols, pure processors, views, adapter-specific recovery, and the
choreography that projects committed intent, invokes the Activity coordinator,
publishes large results, stages factual observations, and binds settlement to
the later committed receipt. It declares the lower-family ports that
choreography needs; no parallel application-layer mirror is created.

The family-owned hosted data contract is
`archetype.physical_ai.hosted_episode`, version
`archetype.physical-ai.hosted-episode/v1`. It contains four canonical Arrow IPC
payloads:

| Payload | Cardinality and authority |
| --- | --- |
| Request | One row per logical trial, seed, and physical episode; one operation may batch many episodes |
| Trajectory | Reset row `0` plus at most `max_transitions` action rows for every admitted episode |
| Episode results | One terminal row derived from each complete episode trajectory |
| Manifest | One row binding all request, trajectory, and episode-result digests and exact completeness counts |

`operation_id` is the caller-stable provider-operation identity shared by the
batch. `episode_id` is derived separately from `(operation_id, trial_id)`, so a
batched provider operation never collapses several trials into one episode.
`episode_result_id` identifies the complete result for one episode, while
`step_id` independently identifies every trajectory row. Both identities are
contract-version-domain-separated.

Reset has a null action and does not consume the transition budget. Actions,
end-effector position and quaternion, and gripper joint position use fixed-size
vectors. Provider termination is recorded as `environment_done`; Archetype's
complete-episode terminal is separate and uses the closed reason vocabulary
`success`, `environment_done`, or `transition_budget`. Success may terminate a
physical evaluation even when a provider does not also assert its own
`environment_done` signal.

Configuration is finite canonical JSON. Recursive activation, placement,
timing, credential, and host-path metadata is rejected rather than admitted to
replay identity. Optional camera evidence uses content-addressed frame
references (`sha256:<digest>`, media type, and size), never provider-local
paths. Schemas carry the contract version and payload domain in Arrow metadata;
request, trajectory, per-episode result, and manifest digests additionally
domain-separate the exact canonical IPC bytes.

The completeness validator accepts a result only when every admitted episode
appears exactly once, its rows are contiguous from reset through exactly one
terminal row, no row exceeds its transition budget, every echoed request field
and digest agrees, the per-episode results are the exact trajectory derivation,
and the one manifest binds the other three payloads and their counts. The
validator then requires that exact canonical manifest as the fourth payload. A7
must publish the complete trajectory and manifest before recording a bounded
Activity result reference; a partial trajectory is never a settleable result.

External simulator and robot adapters should migrate to this module rather than
copying its schemas. The proof-era names map as follows:

| Proof-era field | Canonical v1 field or rule |
| --- | --- |
| `max_steps` | `max_transitions`; reset is not charged |
| `done` | `environment_done`; not the same as Archetype `terminal` |
| per-episode `publication_id` | `episode_result_id` |
| per-step `publication_id` | `step_id` |
| frame path or bare frame hash | nullable content-addressed frame reference |
| raw SHA-256 of IPC | version- and payload-domain-separated canonical IPC digest |

A seeded simulator may retrieve the first durable result by stable provider
operation identity. Correctness must not depend on a second GPU execution being
byte-identical. Real hardware is consequential: its adapter must reconcile or
require operator intervention and may never treat lease expiry as permission
to repeat an action.

`EvaluatePhysicalTask` and `SweepPhysicalInstructions` are exact,
application-scoped registrations. Both are trusted-only, direct,
non-durable, and zero-token operations. There is currently no REST operation
for physical evaluation. A future remote surface must add an explicit
actor-aware registration and API contract rather than treating the trusted
runtime method as an authentication boundary.

## Provider lifetime

`EnvClient` and `PolicyClient` are live host resources, not portable command
payloads. Construction must be inert, and every provider must expose
`async aclose()`. Before any ownership transfer or effect, admission validates
every supplied role and serializes the exact object with Daft's serializer.
Accepted objects are serializable non-owning handles to host-owned backing
resources; an opaque failure later in tick execution is not the admission
boundary. Passing accepted providers to either registered physical operation
then transfers those exact objects to the runtime's process owner synchronously
before world creation, policy reset, environment reset, or any other workflow
or provider effect.

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

The provider-scoped token contains a cleanup-owner reservation created before
the handler may create its private evidence world. World creation durably marks
the identity `writer_mode="cleanup_only"` while leaving it active for tick
materialization. Before the first registration write, world lifecycle
synchronously binds the exact catalog and complete cleanup-only `WorldRecord`
to that reservation. Immediately after the private binding is inserted into
the registry, lifecycle promotes the same owner to its sticky
`WorldCleanupLease` without awaiting. The handler therefore never crosses a
durable or live ownership boundary without retained cleanup authority.

Activation cleanup is cancellation-resistant and retryable. Before promotion
it performs exact identity-safe registration retirement, including a destroyed
tombstone when an ambiguous remote write is absent at reconciliation; after
promotion it revalidates and executes only through canonical `WorldCleanup`.
There is no direct lifecycle-destroy bypass. A failed attempt remains owned in
the `workflow-handles` shutdown phase, and provider close joins it before any
backing resource is released.
Caller cancellation and cleanup-originated cancellation remain distinguishable
when cleanup also fails; multiple failures preserve all causes. A hard process
crash can leave active evidence rows, but mutable resume rejects their
cleanup-only identity before storage or fence effects.

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
8. Every provider passes exact Daft-serialization admission before ownership or
   effects. Every unique accepted identity then transfers to process ownership,
   is exclusively leased for the full workflow, waits for its associated
   exact-world cleanup, and remains owned across cancellation until
   authoritative host shutdown succeeds.
9. Cleanup ownership is reserved before private-world creation, and the
   exact catalog registration is bound before its first write. The same owner
   is promoted to canonical world cleanup after registry insertion, and the
   world's immutable cleanup-only writer mode prevents crash recovery from
   reactivating provider processors.
10. Every physical-AI `@daft.cls` constructor is covered by the closed lifetime
    inventory: host-backed classes only retain an admitted serialized client
    handle, and `_CartpoleStepper` is the sole reviewed worker-local
    constructor, limited to embedded-XML MuJoCo model/data scratch.

The credential-free contracts live under `tests/physical_ai/`. Real LIBERO,
VLA, GPU, and Modal adapters remain external provider implementations and paid
dogfoods; the physical-AI family does not import them.
