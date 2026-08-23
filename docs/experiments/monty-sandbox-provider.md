# Monty as a Python-only Agent Mission sandbox

**Spike date:** 2026-07-27

**Upstream examined:** `pydantic/monty` at
`c8de3ea24b14c5f6252259d138fb9a98500c60c9`

**Packages exercised:** stable `pydantic-monty==0.0.18` and prerelease
`pydantic-monty==0.0.19b3`

## Decision

Monty is **not a viable implementation of the current
`missions.SandboxBackend` contract**, even for repositories whose authored
files are all Python.

Monty **is promising as a separate, narrow Python code-mode capability**
inside an agent. That would be a new execution profile, not another provider
behind the existing process-sandbox abstraction.

The distinction matters. The current Agent Mission harness requires an
isolated persistent repository plus arbitrary process execution for:

1. the Codex CLI;
2. Git clone, inspection, commit, and push;
3. authored command validators such as `pytest`; and
4. provider-scoped secret leases for agent authentication and publication.

Monty interprets a deliberately limited Python language and virtualizes host
capabilities. It does not provide an OS shell or subprocess module. A mounted
repository is a controlled file tree, not a normal importable Python
environment.

## What worked

Across the direct probes and inspected APIs, Monty:

- read files from an explicit virtual mount;
- wrote through a bounded read-write mount;
- denied path traversal and unmounted host paths;
- denied ambient host environment access;
- rejected subprocess use;
- retained interpreter globals across feeds;
- enforced interpreter duration limits; and
- captured stdout/stderr through the host API.

Monty can also serialize interpreter state. This is useful evidence for a
code-mode Activity, but it is not equivalent to the current sandbox checkpoint
contract: the snapshot does not make an independently mounted repository or
external provider effects durable.

The newer worker-pool API isolates interpreter crashes in replaceable worker
subprocesses and adds a parent-side hard request timeout. The tested
`0.0.19b3` prerelease also changed overlay mounts to per-feed copy-on-write:
overlay writes disappeared on the next feed. A retained writable workspace
therefore needs a read-write mount or an explicit export/apply transaction.

Mount I/O is serviced by the parent side of the worker pool. Upstream's own
implementation warns against mounting directories writable by untrusted local
peers: a concurrent replacement of a regular file with a FIFO can block the
host thread servicing the mount. A mission design would need exclusive
workspace ownership in addition to Monty's virtual path checks.

## What failed

| Mission requirement | Monty observation | Disposition |
|---|---|---|
| `SandboxSession.exec(ProcessRequest)` | No arbitrary argv or subprocess capability | Contract mismatch |
| Codex driver | Cannot launch `codex exec` | Blocked |
| Git lifecycle | Cannot launch `git` | Blocked |
| Command validators | Cannot launch `pytest`, `uv`, or repository commands | Blocked |
| Ordinary project imports | Mounted source is not added as a normal importable module tree | Blocked for existing repositories |
| Third-party dependencies | Pure Monty intentionally exposes a fixed language/stdlib subset | Blocked for normal Python projects |
| Symbolic process secrets | No current mapping to `ProcessRequest.secret_names` | New capability design required |
| Filesystem checkpoint | Interpreter snapshots do not snapshot read-write host mounts | New export/checkpoint design required |
| Activity recovery | No provider operation binding/reconciliation adapter | Required before mission admission |

“Python-only” is therefore insufficiently narrow. A compatible profile would
have to mean **single-feed Monty-language source with no normal project imports,
no third-party dependencies, no shell validators, and host-mediated
publication**. That is materially different from an Archetype coding mission.

## Best-fit architecture

Use Monty one level inside the sandbox contract:

```text
Mission Activity
  -> existing fail-closed sandbox provider
     -> coding agent
        -> Monty code-mode tool
           -> allowlisted host functions
           -> read-only or overlay mounts
```

This preserves the current provider's Git, validator, secret, publication, and
Activity-recovery responsibilities while giving the agent a cheap, restricted
Python scratchpad.

### Full separation of concerns

The complete design has seven different owners. Collapsing any two of them makes
the recovery or isolation story dishonest:

| Concern | Owner | Durable? |
|---|---|---|
| Task readiness, dispatch, retry, and acceptance | Mission Components and processors | Yes, as committed world state |
| Admission, claim, fence, reconciliation, and result delivery | Mission author Activity | Yes, in the Activity catalog and value store |
| Linux process and repository-code isolation | Apple Container sandbox | No; it is a reconstructible live Resource |
| Base revision, exclusive writable checkout, diff, and publication | Attempt-bound Git/worktree broker | Repository bytes and exact revision are durable evidence; the live lease is not |
| Model conversation and generated action protocol | Serialized agent row using Pydantic value models | Only when the row/transcript is written durably |
| Restricted Python globals and mounted file edits | One Monty snapshot and one exclusive worktree mount per row | Snapshot bytes can be durable; mount bytes require separate Git/export evidence |
| Parallel model inference and backpressure | Daft frontier plus the shared model endpoint | No; the next frontier is reconstructed from serialized rows |

That produces two nested loops:

```text
Archetype committed-state loop
  tick T commits TaskDispatch
    -> required projector admits Mission author Activity
      -> worker claims attempt and binds provider operation
        -> Apple Container pool/session is acquired
          -> worktree broker provisions attempt/<id> at exact base SHA
            -> Daft frontier loop
               prompt(active rows)                 # one model turn per row
               -> validate AgentAction
               -> restore Monty snapshot per row
               -> mount only that row's worktree
               -> execute one bounded Python action
               -> capture observation + Git evidence
               -> serialize the next row
            -> approved validators run outside Monty
            -> broker captures exact patch/head/result
      -> bounded result reference and digest are recorded
  tick U commits factual observations and settles the Activity
```

The current frontier experiment deliberately lifts the inner model loop out of
`pydantic_ai.Agent.run()`. A stock Pydantic AI run owns its own sequence of
model requests and tool calls. Wrapping many complete runs in a Daft UDF can
schedule them, but it does not let `daft.functions.prompt` see one shared
frontier. To let Daft vectorize inference, the durable row becomes the agent:

```python
class AgentState(BaseModel):
    mission_id: str
    task: str
    base_sha: str
    workspace_path: str
    turn: int
    transcript: list[dict[str, Any]]
    monty_snapshot_b64: str | None
```

Each materialization advances that value exactly once:

```python
advanced = (
    frontier
    .with_column("raw_action", prompt(col("prompt_text"), ...))
    .with_column(
        "next_state_json",
        apply_monty_row(col("state_json"), col("raw_action")),
    )
    .collect()
)
```

Pydantic still owns validation of `AgentState` and `AgentAction`. Monty owns the
restricted code execution. Daft owns frontier scheduling. A future
batch-aware Pydantic AI `Model` adapter could recover more of the stock agent
graph, but the current experiment does not claim that integration.

### Where `daft.File` belongs

`daft.File` is a lazy, read-only file reference. It can open, seek, inspect, and
stream local or remote content, and `daft.functions.prompt` accepts File
expressions directly as model inputs. It is not a writable repository tool and
does not replace the Monty mount.

Use it before inference for vectorized, bounded context construction:

```python
from daft.functions import file, prompt

frontier = (
    frontier
    .with_column("context_file", file(col("context_path")))
    .with_column(
        "raw_action",
        prompt(
            [col("prompt_text"), col("context_file")],
            system_message=SYSTEM_PROMPT,
            provider=provider,
            model=model,
        ),
    )
)
```

The useful division is:

- `daft.File`: lazily read many selected files or remote artifacts across the
  frontier and pass bounded content to inference;
- Monty `pathlib` over `MountDir`: inspect and edit one exclusive attempt
  worktree;
- an attempt-bound host capability: run approved validators and return bounded
  facts;
- the Git broker: create/remove worktrees, compute diffs, commit, and publish.

Wrapping `daft.File(path).open()` in a Pydantic tool is possible, but doing it
once per agent call loses Daft's vectorization and duplicates the Monty mount.
Such a tool is justified for a logical remote artifact reference, not for
arbitrary repository paths. It must resolve the reference outside Monty,
enforce the attempt identity and byte budget, and return plain bounded
text/bytes rather than a live `daft.File` object.

### One outer Apple Container

One disposable Apple Container can technically own the full inner frontier.
The local probe ran:

```bash
container run --remove \
  --name archetype-monty-frontier-probe \
  --cpus 4 \
  --memory 8g \
  --cap-drop ALL \
  --volume "$PWD:/workspace/source:ro" \
  archetype-agent:codex-42e603199b6a9697 \
  uv run --script \
    /workspace/source/experiments/daft_monty_agent_frontier.py \
    --demo --dry-run
```

It completed with Daft `0.7.19` and Monty `0.0.18`: two rows created two
detached in-container worktrees, retained independent interpreter globals
across three frontiers, and emitted two distinct patches. `--remove` deleted
the container after the run.

This proves process topology, not production multi-tenant isolation. All
worktrees in one container share a Linux trust domain. Monty prevents one
generated Python action from leaving its mount, but repository validators and
other native processes can access sibling worktrees, the orchestrator, and any
ambient container credentials. Therefore:

- one outer container is acceptable for a credential-free cooperative batch
  when repository code is not trusted to isolate itself;
- secrets must remain operation-scoped and absent from the long-lived pool;
- untrusted validators need a separate strong sandbox per attempt; and
- mutually untrusted missions still require one Apple Container per attempt,
  or an equivalent nested execution boundary.

The existing `AppleContainerSandboxSession.exec()` also holds a session lock,
so multiple host-side `exec()` calls against one session are serialized. The
successful probe uses one orchestrator process inside the container; parallelism
occurs below that one process invocation.

A more ambitious Monty-native mission should begin with a new family-owned
port such as `PythonCodeSession.run(source, mounts, limits)`. It should not
implement `SandboxSession.exec()` by parsing a special `python -c` argv, because
that would advertise process semantics it cannot honor.

Before such a profile can advance beyond a spike, it needs:

1. a typed source-and-mount request instead of `ProcessRequest`;
2. immutable input mounts and an explicit bounded output export;
3. host-owned Git materialization/publication with credentials never exposed
   to interpreted code;
4. Monty-native validators bound to exact exported bytes;
5. an exact Activity operation identity and cold-recovery story;
6. package/version pinning after the worker-pool API has a stable release; and
7. adversarial tests for symlink races, mount write quotas, worker crashes,
   snapshot compatibility, and cancellation.

## Reproduce

The repository contains a self-validating PEP 723 script, so the experimental
dependency does not enter Archetype's production lock:

```bash
uv run --script experiments/monty_sandbox_spike.py
```

The script exits nonzero if a required isolation or resource-limit assertion
changes. It prints the complete observation matrix as JSON.

The companion frontier experiment makes one complete agent turn a Daft row:
Daft sends every active row through `daft.functions.prompt`, then a no-retry
process UDF restores that row's Monty snapshot and executes the returned Python
action against its dedicated worktree mount. Git worktree provisioning remains
host-owned and happens before the initial DataFrame is built.

Run the self-contained, zero-credential demonstration:

```bash
uv run --script experiments/daft_monty_agent_frontier.py --demo --dry-run
```

The deterministic policy follows the same DataFrame path as real inference. It
creates two detached worktrees, inspects both mounts, restores independent
interpreter state on the next frontier, writes separate files, and prints both
patches before cleaning up.

To use an OpenAI-compatible Modal Endpoint instead:

```bash
export MODAL_BASE_URL=https://YOUR-ENDPOINT.modal.run/v1
export MODAL_MODEL=Qwen/Qwen3.5-4B
export MODAL_PROXY_TOKEN_ID=wk-...
export MODAL_PROXY_TOKEN_SECRET=ws-...

uv run --script experiments/daft_monty_agent_frontier.py \
  --repo . \
  --task "Add a short architecture note to docs/agent-note.md" \
  --rounds 8
```

This frontier driver intentionally does not call `pydantic_ai.Agent.run()`.
That method owns its model loop; letting Daft own the inference frontier
requires lifting the loop into data. Pydantic models still own the serialized
agent and action contracts. Recovering the full Pydantic AI graph semantics
would require a batch-aware `Model` adapter or a lower-level graph driver.
