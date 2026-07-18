# Examples

Runnable examples demonstrating Archetype's core features. Each example is self-contained and numbered to match the recommended onboarding order.

```bash
uv run python examples/<filename>.py
```

| # | Example | Description | Requires |
|---|---------|-------------|----------|
| 0 | [`00_quickstart.py`](00_quickstart.py) | Smallest complete component + processor + runtime simulation | None |
| 1 | [`01_world_mutations.py`](01_world_mutations.py) | Every mutation type: spawn, despawn, add_processor, RBAC, fork, audit history | None |
| 2 | [`02_fork_counterfactual.py`](02_fork_counterfactual.py) | Fork a world three times, run each branch, compare results | None |
| 3 | [`03_time_travel.py`](03_time_travel.py) | Rewind to any past tick by filtering the `tick` column, then fork a counterfactual branch and diff it against the source | None |
| 4 | [`04_messaging.py`](04_messaging.py) | Agent-to-agent messaging via an application-local mailbox resource, priority-ordered processors, and lifecycle hooks | None |
| 5 | [`05_llm_agents.py`](05_llm_agents.py) | LLM-powered agents — each entity gets a parallel LLM call every tick via `daft.functions.prompt` | `OPENAI_API_KEY` |
| 6 | [`06_trajectory_analysis.py`](06_trajectory_analysis.py) | Trajectory analysis — ingest, label, and compare agent trajectories using world forking | Optional: `OPENAI_API_KEY` |
| 7 | [`07_hooks.py`](07_hooks.py) | Lifecycle hooks for audit logs, tick metrics, and temporary debug traces | None |
| 8 | [`08_htn_resolution.py`](08_htn_resolution.py) | HTN plan resolution as a fan-out AND/OR forest | None |
| 9 | [`09_cloud_storage.py`](09_cloud_storage.py) | Cloud storage configurations through `StorageConfig` and the runtime API | Optional cloud credentials |
| 10 | [`10_autoresearch.py`](10_autoresearch.py) | AutoResearch loop: fork candidates, score episodes, advance BranchHead on a lab ledger | None |
| 11 | [`11_coding_agent_mission.py`](11_coding_agent_mission.py) | Coding-agent mission: Apple Container locally or Modal remotely, one submission per tick, task advancement gated by validators + a restorable checkpoint | Apple Container or optional Modal credentials |

The coding-agent example defaults to a real local Apple Container lightweight
VM. It clones the repository inside the VM and does not mount the host
workspace. For Codex, start Apple Container and complete a one-time ChatGPT
device login. The resulting OAuth state lives in the named Apple Container
volume `archetype-codex-auth`, not in the repository or a host bind mount.

```bash
container system start
uv run python examples/11_coding_agent_mission.py --codex-login
# Open the printed URL and enter the one-time code, then:
uv run python examples/11_coding_agent_mission.py
```

The checked-in mission currently targets Archetype issue #457. The persisted
login is staged only while Codex runs, removed before validators run, and never
included in workspace snapshots. To use OpenAI Platform API billing instead,
set `CODEX_API_KEY` and
`CODING_AGENT_CODEX_AUTH_ENV=CODEX_API_KEY`. Claude Code uses
`ANTHROPIC_API_KEY` and is selected with
`CODING_AGENT_HARNESS=claude-code`.

Set `CODING_AGENT_BACKEND=modal` for the remote backend. Its default Modal
Secrets are `archetype-codex` containing `CODEX_API_KEY` and
`archetype-claude-code` containing `ANTHROPIC_API_KEY`. OpenCode uses
`archetype-modal-endpoint`, containing only `MODAL_ENDPOINT_TOKEN_ID` and
`MODAL_ENDPOINT_TOKEN_SECRET`. Set
`GITHUB_MODAL_SECRET` to a Modal Secret containing `GITHUB_TOKEN` together with
`CODING_AGENT_PUSH=1` when verified commits should be pushed. Both backends
inject agent credentials only into the agent process; validator commands do not
receive them.

OpenCode is Modal-only in this example and targets an operator-supplied
OpenAI-compatible endpoint. Put the two endpoint credentials in a dedicated,
gitignored dotenv file (do not reuse a dotenv file containing unrelated
secrets), create the named Secret, and provide the deployed API root:

```bash
mkdir -p .context
# Create .context/modal-endpoint.env with only:
# MODAL_ENDPOINT_TOKEN_ID=...
# MODAL_ENDPOINT_TOKEN_SECRET=...
modal secret create --from-dotenv .context/modal-endpoint.env \
  archetype-modal-endpoint

CODING_AGENT_BACKEND=modal CODING_AGENT_HARNESS=opencode \
  CODING_AGENT_MODEL=Qwen/Qwen3.6-35B-A3B-FP8 \
  CODING_AGENT_OPENCODE_BASE_URL=https://REPLACE-ME/v1 \
  uv run --extra coding-agent python examples/11_coding_agent_mission.py
```

The default wire protocol is `/v1/chat/completions`; set
`CODING_AGENT_OPENCODE_WIRE_API=responses` for a `/v1/responses` endpoint. The
generated OpenCode config stores environment placeholders rather than token
values, disables repository-level OpenCode config, and is safe to include in a
full sandbox snapshot. The named Secret is attached only to the OpenCode
process; validators receive no endpoint credentials.

Modal can instead use the Codex or Claude subscription attached to an OAuth
login. Bootstrap each harness once; Codex prints a device code, while Claude
prints a browser URL and may ask you to paste the returned code:

```bash
CODING_AGENT_BACKEND=modal CODING_AGENT_HARNESS=codex \
  CODING_AGENT_MODAL_AUTH_MODE=oauth uv run --extra coding-agent \
  python examples/11_coding_agent_mission.py --modal-login

CODING_AGENT_BACKEND=modal CODING_AGENT_HARNESS=claude-code \
  CODING_AGENT_MODAL_AUTH_MODE=oauth uv run --extra coding-agent \
  python examples/11_coding_agent_mission.py --modal-login
```

The default durable Volumes are `archetype-codex-auth` and
`archetype-claude-code-auth`; override them with `CODEX_MODAL_AUTH_VOLUME` and
`CLAUDE_MODAL_AUTH_VOLUME`. The Volume is mounted only into a credential-broker
Sandbox. The mission receives the credential file only for the agent CLI
process; the refreshed file is returned to the broker and removed before any
validator, filesystem manifest, or provider snapshot runs. Run the mission with
the same environment and omit `--modal-login`.

The Modal driver prints its `sb-...` ID as soon as the mission sandbox exists,
streams Codex, Claude Code, or OpenCode JSONL in the launching terminal, and emits phase
events plus a heartbeat every 15 seconds. A second terminal can attach directly
to the running sandbox without a model credential:

```bash
uv run --extra coding-agent python examples/11_coding_agent_mission.py \
  --monitor-sandbox sb-REPLACE_ME
```

The monitor polls the fixed live status/event files and the active stdout and
stderr traces without executing a command in the sandbox. If the run used a
custom `CODING_AGENT_WORKSPACE`, pass the same environment value when attaching.
Heartbeats expose stdout/stderr byte counts and time since the last agent output.
Modal filesystem reads can pause while a snapshot is created; the monitor keeps
its offsets and retries those interruptions for 180 seconds by default instead
of declaring the mission dead. Override that bound with
`--monitor-disconnect-grace-seconds`. The terminal live files are included in
the attempt's artifact bundle before the sandbox is torn down.

Each tick records an accepted or rejected `Attempt` instead of retrying inside
the sandbox client. Before the task can advance, the client captures a complete
provider checkpoint: a Modal filesystem image or an Apple Container rootfs
export that can be rehydrated into a new VM. The attempt manifest inside that
checkpoint carries world/run/entity correlation, validator evidence, canonical
CLI JSONL, Git status, a binary patch, a sanitized Git bundle, `.context` when
present, and whole-filesystem start/end/diff manifests. This deliberately
separates explicitly queryable artifacts from the complete recovery snapshot.
If provider checkpointing fails, that error is persisted in the same tick and
the task does not advance; it is not disguised as a validator rejection.

Modal checkpoints default to a 30-day TTL. Local rootfs exports stay under
`.context/apple-container-snapshots` until an operator removes them. The task
transition remains gated at the `checkpointed` phase; after the episode, the
driver sends every recoverable attempt's declared evidence through the additive
`ArtifactService`, queries the dedicated Iceberg artifact index, and only then
tears down the sandbox. Its default object/index store is local under
`.context/coding-agent-artifacts`. Production deployments can point the same
contract at R2 with Daft's S3-compatible `IOConfig`; the control catalog makes
the `PENDING → UPLOADED → INDEXED` publication retryable and idempotent.

```bash
uv sync --extra coding-agent
modal setup
CODING_AGENT_BACKEND=modal uv run --extra coding-agent \
  python examples/11_coding_agent_mission.py
```

`make test-modal-sandbox` runs the live, keyless Modal infrastructure proof for
all three CLIs. `make test-modal-agent` additionally performs a real agent edit,
validation, commit, and snapshot for Codex and Claude Code. It also covers
OpenCode when `ARCHETYPE_OPENCODE_ENDPOINT_BASE_URL` is set (with optional
`ARCHETYPE_OPENCODE_INTEGRATION_MODEL`, `ARCHETYPE_OPENCODE_MODAL_SECRET`, and
`ARCHETYPE_OPENCODE_WIRE_API` overrides). Set
`ARCHETYPE_MODAL_AGENT_AUTH_MODE=oauth` to use initialized Codex/Claude
subscription Volumes instead. `make test-modal` runs both tiers.
The live suite is excluded
from normal `make test`, `make test-all`, and `make ci` runs; CI invokes it only
when `examples/11_coding_agent_mission.py` changes.

`make test-apple-container` runs the opt-in local infrastructure proof for both
CLIs without invoking either model API. It is also excluded from normal tests.

## Supplementary

| Example | Description |
|---------|-------------|
| [`pr_triage.py`](pr_triage.py) | PR triage agent that dogfoods Archetype |
| [`simulation_script.py`](simulation_script.py) | Standalone simulation script for quick prototyping |
