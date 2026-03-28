# Sandboxed Background Agents

## Goal

Private background agents with fully autonomous permissions, running inside isolated VMs, with guaranteed trajectory capture and evaluation collection. Think **GitHub Actions you fully own** — with trajectory capture, fast boot, security control, and local inference fallback.

This is not a new orchestration daemon. The VM lifecycle is masterblaster. The experiment loop is a fork of karpathy/autoresearch. This spec defines the **lifecycle contract** — what happens at boot, what happens at teardown, and how provenance is guaranteed.

## Scope

**In scope:**

- CLI with a clear agent session lifecycle: boot → work → teardown
- jcard.toml config for OpenCode + LM Studio (or any OpenAI-compatible endpoint)
- Tapes proxy inside the VM capturing all LLM traffic to SQLite
- Teardown migration: tapes SQLite → host, eval collection
- Agent has git identity, clones repo, pushes to experiment branches
- Autoresearch fork as the experiment/iteration loop
- N agents in parallel on local hardware

**Out of scope:**

- Recursive Archetype-on-Archetype interaction
- New orchestration daemon (masterblaster handles this)
- Custom stereosd/agentd changes
- API server for experiment management (CLI first)
- Changes to archetype core/

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Host                                                    │
│                                                          │
│  CLI (archetype agent run)                               │
│    ├── writes jcard.toml from config                     │
│    ├── mb up → boots stereOS VM                          │
│    ├── waits for agent to finish / timeout               │
│    ├── teardown:                                         │
│    │     ├── migrate tapes SQLite from VM → host         │
│    │     ├── collect git diff from experiment branch     │
│    │     ├── run eval harness on trajectory + diff       │
│    │     └── persist results (trajectory, scores, diff)  │
│    └── mb destroy                                        │
│                                                          │
│  LM Studio (port 1234)                                   │
│    └── serves inference to all VMs via host network      │
│                                                          │
│  autoresearch fork (experiment loop)                     │
│    └── iterates: launch agent → eval → keep/discard      │
│        → inject feedback → re-launch                     │
│                                                          │
├──────────────────────────────────────────────────────────┤
│  VM (stereOS + masterblaster)                            │
│                                                          │
│  stereosd (vsock control plane)                          │
│    └── injects secrets (git creds, API keys)             │
│                                                          │
│  agentd (process manager)                                │
│    └── runs agent in gVisor sandbox                      │
│                                                          │
│  tapes proxy                                             │
│    └── intercepts all LLM traffic → SQLite               │
│                                                          │
│  agent (opencode)                                        │
│    ├── clones repo from git remote                       │
│    ├── reads task (issue ref or experiment file)          │
│    ├── works (calls LM Studio on host)                   │
│    ├── commits + pushes to experiment branch              │
│    └── exits when done or timeout                        │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

## Lifecycle Contract

Every agent session follows this lifecycle. The CLI enforces it.

### Phase 1: Boot

1. Resolve task input (issue URL, experiment file path, or inline task string)
2. Generate jcard.toml from config:
   - mixtape, resources, network egress, secrets (git creds, API endpoint, task ref)
3. `mb up --config {jcard_path}`
4. Wait for VM ready (stereosd health check over vsock)
5. Agent process starts via agentd

### Phase 2: Work

The agent runs autonomously. The host does not interfere.

- Agent clones repo, reads task, works
- Agent calls LM Studio on host for inference
- Tapes proxy captures all LLM traffic to SQLite inside the VM
- Agent commits and pushes to experiment branch
- agentd enforces timeout

### Phase 3: Teardown (guaranteed)

This phase MUST run even if the agent crashes or times out. It is the provenance guarantee.

1. **Migrate tapes:** `scp` or `mb exec` to copy tapes SQLite from VM to host. Merge into host tapes database (Merkle DAG dedup — safe append from multiple VMs).
2. **Collect git state:** fetch experiment branch, compute diff from branch head before agent ran.
3. **Run eval harness:** reward signals on trajectory + diff (task completion, code quality, LLM-as-judge, composite score).
4. **Persist results:** write experiment record (spec, trajectory ID, diff, scores, timestamps) to append-only log. This is the provenance record.
5. **Destroy VM:** `mb destroy {name} --yes`. The VM is ephemeral — all state has been extracted.

### Failure Modes

| Failure | Teardown behavior |
| ------- | ----------------- |
| Agent completes normally | Full teardown: migrate tapes, collect diff, eval, persist, destroy |
| Agent times out | agentd kills process. Same teardown — partial trajectory and diff are still collected |
| Agent crashes | Same teardown — trajectory up to crash point, no diff if nothing was pushed |
| VM crashes hard | Tapes SQLite may be lost. Experiment record logs "vm_crash" with no trajectory. Provenance preserved (we know it ran and failed), data may be incomplete |
| Host crashes | masterblaster VMs survive daemon restarts. On host restart, scan for orphaned VMs via `mb list`, run teardown for each |

## Reference jcard.toml

```toml
mixtape = "coder:latest"

[resources]
cpus = 4
memory = 8192
disk = 20

[network]
mode = "nat"
egress_allowlist = ["host.lima.internal", "github.com"]
port_forwards = { 2222 = 22 }

[secrets]
OPENAI_API_KEY = "${LMSTUDIO_API_KEY}"
OPENAI_BASE_URL = "http://host.lima.internal:1234/v1"
GIT_AUTHOR_NAME = "archetype-agent"
GIT_AUTHOR_EMAIL = "agent@archetype.dev"
GH_TOKEN = "${GH_TOKEN}"
REPO_URL = "https://github.com/papercomputeco/archetype.git"
TASK_REF = "issues/47"

[[agents]]
name = "researcher"
harness = "opencode"
type = "sandboxed"
timeout = 1800
```

## Key Design Decisions

**LLM sits outside the sandbox.** The agent calls an OpenAI-compatible API (LM Studio on host). The sandbox has egress only to the host. This means:

- The LLM is never at risk from agent-generated code
- `eval()` or arbitrary execution stays inside the VM
- The host controls model selection, rate limiting, and cost
- If cloud APIs have downtime, swap to a local model without changing the agent
- No external API keys need to enter the sandbox

**Agent clones the repo, not a shared mount.** Closer to GitHub Actions semantics — the VM is ephemeral, the git remote is the coordination point. The agent gets a git identity (username, email, token) injected via stereosd secrets.

**Task delivery is issue-based or file-based.** The agent's prompt references a GitHub issue or an experiment file in the repo. This is injected as a secret (`TASK_REF`). The agent's harness prompt tells it to look at that reference. Context discovery is the agent's job — it reads the issue, reads the codebase, and decides what to do.

**Commits to experiment branches, PRs to main.** The agent commits directly to experiment branches. No human review — the eval harness is the gatekeeper. The agent can fork or explore/exploit within a branch. PRs are only for merging back to main. The git tree IS the MCTS rollout artifact.

**Tapes proxy inside the VM.** All LLM traffic routes through tapes → SQLite. On teardown, SQLite is migrated to host. Tapes uses Merkle DAG trees, so merging from multiple VMs is dedup-safe. Tapes should ship in the mixtape (verify this).

**gVisor by default.** Double isolation: VM + gVisor. Use `type = "native"` (tmux, attachable) only for debugging.

## CLI Surface

```bash
# Launch a background agent
archetype agent run \
    --task "issues/47" \
    --repo https://github.com/papercomputeco/archetype.git \
    --branch autoresearch/dedup-perf \
    --model lmstudio \
    --timeout 1800

# List running agents
archetype agent list

# Check status
archetype agent status <id>

# View logs (tapes trajectory)
archetype agent logs <id>

# Attach to agent's tmux (if type=native)
archetype agent attach <id>

# Force stop
archetype agent stop <id>

# Run autoresearch loop (fork of karpathy/autoresearch)
archetype autoresearch run \
    --repo . \
    --branch autoresearch/dedup-perf \
    --max-iterations 10 \
    --eval "pytest tests/ -x"
```

## Experiment Loop (autoresearch fork)

The iteration loop is a fork of karpathy/autoresearch, adapted to use masterblaster for sandboxing and tapes for trajectory capture. The core loop:

```
for iteration in range(max_iterations):
    1. resolve current branch head
    2. launch agent VM (mb up) with task + any feedback from prior iteration
    3. wait for agent to finish
    4. teardown: migrate tapes, collect diff
    5. evaluate: run eval harness (tests, quality, judge)
    6. if score >= threshold:
         keep — branch head advances (agent's push stands)
    7. else:
         discard — reset branch to prior head
         extract feedback from eval results
         inject into next iteration's task context
```

The autoresearch fork handles the iteration logic, branch tracking, and keep/discard decisions. The archetype CLI handles the per-agent lifecycle (boot, teardown, migration). They compose — autoresearch calls `archetype agent run` for each iteration.

## Advantages Over GitHub Actions

| Concern | GitHub Actions | This |
| ------- | -------------- | ---- |
| Trajectory capture | Logs only | Full LLM traffic via tapes |
| Boot time | 30-60s | <3s (stereOS direct-kernel boot) |
| Parallelism | Runner pool limits | N VMs on local hardware |
| Security | Third-party runner trust | You own the VM boundary + gVisor |
| Inference | API costs + rate limits | LM Studio on host, swap models freely |
| Eval harness | Custom scripts in workflow | Pluggable reward signals (completion, quality, judge, composite) |
| Provenance | Workflow logs | Merkle DAG trajectory + git tree |

## Phased Implementation

### Phase 0: Get masterblaster working (no archetype code)

- Build `mb` from source
- Build or pull `coder` mixtape
- Verify tapes is included in mixtape (if not, figure out extension)
- Write jcard.toml for OpenCode + LM Studio
- `mb up`, `mb ssh -u agent`, verify: agent can reach LM Studio, can clone repo, can push
- **Deliverable:** working jcard.toml, verified agent execution

### Phase 1: CLI lifecycle wrapper

- `archetype agent run` — generates jcard, calls `mb up`, waits, runs teardown
- `archetype agent list` — wraps `mb list`
- `archetype agent stop` — wraps `mb down`
- Teardown logic: tapes migration, git diff collection, eval, persist, `mb destroy`
- **Deliverable:** one complete boot → work → teardown cycle via CLI

### Phase 2: Autoresearch fork

- Fork karpathy/autoresearch
- Replace execution backend with `archetype agent run`
- Wire tapes trajectory into eval inputs
- Wire reward signals into keep/discard decisions
- **Deliverable:** `archetype autoresearch run` iterates until threshold met or max iterations

### Phase 3: Parallelism + polish

- Run N agents concurrently (one VM per experiment branch)
- Concurrency limits (based on host resources)
- Orphan detection on host restart (`mb list` → stale VMs → run teardown)
- `archetype agent attach` for debugging (tmux attach via `mb ssh`)
- **Deliverable:** parallel autonomous agents with clean lifecycle guarantees

## Resolved Questions

1. **Task delivery:** Issue-based or file-based. Injected as `TASK_REF` secret. Agent discovers context by reading the issue/file. Not an env var for the task body — a reference the agent follows.

2. **Write-back:** Agent pushes to experiment branches. Eval harness decides keep/discard. PRs only for merge to main. Git tree is the search artifact.

3. **Trajectory capture:** Tapes proxy inside the VM, SQLite, migrated on teardown. Merkle DAG merging across VMs.

4. **Crash recovery:** Lifecycle is atomic — teardown is guaranteed. Individual task loss acceptable if VM crashes hard. Provenance (experiment ran, result status) is always persisted even if trajectory data is incomplete. masterblaster VMs survive host daemon restarts; orphan scan handles the rest.

## Open Questions

1. **Does `coder:latest` mixtape include tapes?** If not, what's the mechanism to add it — custom mixtape, extra_packages in agentd config, or a new mixtape definition?

2. **Agent prompt construction:** How much does the harness prompt tell the agent beyond "look at TASK_REF"? Does it include eval criteria, coding style guidance, branch naming conventions? Or is all of that in the repo (AGENTS.md, CLAUDE.md, etc.) and the agent discovers it?

3. **Autoresearch fork scope:** How much of karpathy/autoresearch is reusable vs. needs rewriting? Is the core loop portable, or is it coupled to its current execution backend?
