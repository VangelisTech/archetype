# Sandbox execution

**Document type:** Normative.

**Scope:** Provider-neutral coding-agent execution and process-local sandbox
lifetime under `src/archetype/app/sandboxes/`.

## 1. Ownership and boundary

The sandbox family owns the common attempt kernel, provider selection, live
handle retention, and provider checkpoint references. A provider adapter owns
SDK calls, command transport, filesystem writes, checkpoint creation,
provider URIs, and resource teardown.

The common kernel imports no provider adapter or provider SDK. Modal, Apple
Container, and future adapters point inward to the common kernel. Docker is not
a dependency or fallback of this architecture.

The sandbox family does not own mission/task advancement, durable submission
claims, artifact indexing, evaluation, PR policy, or fleet scheduling.

## 2. Live handles and durable state

`SandboxService` keeps a process-local map of live `iSandboxSession` handles.
The map is an optimization, not mission state. A provider checkpoint and
persisted Archetype facts are the recovery inputs after process loss.

The container owns shutdown of retained handles. Closing one handle is
idempotent. Shutdown stops new admission, attempts every close, and reports all
close failures after the drain. A sandbox close failure does not prevent audit
or owned world/storage shutdown; the composition root drains every later step
and raises one aggregate failure afterward. Cancellation and other
`BaseException` failures are included in that aggregate only after every later
shutdown step has been attempted.

## 3. Six-phase attempt protocol

After validating the request and reading the repository baseline,
`CodingAgentSandboxClient.run_attempt` executes these phases in order:

1. **Execution** — run exactly one agent submission, preserve stdout/stderr and
   session identity, and treat the process result as untrusted evidence.
2. **Validation** — execute every authoritative validator in a separate command
   without agent secrets. A nonzero agent exit does not bypass validators.
3. **Repository finalization** — only after every validator passes, create a
   commit for the verified tree and optionally push the configured branch. A
   passing validator set with no commit or tree change is rejected. The agent
   may edit the worktree but may not move `HEAD`; an agent-authored commit is
   rejected rather than accepted or pushed, and its changes are returned to the
   worktree on top of the trusted baseline. The outer gate supplies the commit
   identity and message.
4. **Evidence** — capture the attempt manifest, agent trace, validator details,
   Git status, binary patch, Git bundle source, filesystem manifests and diff,
   live-event paths, and `.context` when present. These sources remain
   non-durable until the redaction gate accepts them.
5. **Checkpoint** — after evidence exists, request a provider checkpoint. A
   checkpoint failure is returned as evidence and does not erase the attempt.
   `expires_at_ms=None` means a captured checkpoint has no configured expiry;
   epoch zero is never used for that meaning. Capture outcomes use
   `ready`/`failed`/`disabled`; the mission boundary explicitly projects those
   into its durable checkpoint vocabulary instead of sharing family internals.
6. **Artifact handoff** — declare checkpoint-qualified or live source
   references and store the sandbox-local replay receipt. This phase does not
   upload or index artifacts; authoritative mission finalization owns that
   transition.

Every phase has one typed result in `app/sandboxes/models.py`. The ordering is a
contract so later telemetry can create one correlated span per phase without
guessing at control flow.

## 4. Failure semantics

| Failure | Remaining work | Returned meaning |
|---|---|---|
| Agent transport raises | Stop before validation | No completed attempt; an external durable claim must recover submission ambiguity |
| Agent exits nonzero | Continue all phases | Agent friction plus authoritative validator outcome |
| Validator fails | Continue evidence and checkpoint | Rejected, resumable attempt |
| Validators pass but tree is unchanged | Continue evidence and checkpoint | Rejected `git_tree_change` gate |
| Agent moves repository `HEAD` | Continue evidence and checkpoint | Rejected `git_tree_change` gate; untrusted commit is never pushed |
| Commit or push transport fails | Raise; do not claim completed handoff | External supervisor resumes from durable state |
| Evidence capture fails | Raise before checkpoint | Incomplete finalization; never task success |
| Checkpoint fails | Continue artifact handoff | Captured but non-restorable evidence, never checkpoint-qualified advancement |

## 5. Secret boundary

Only the selected agent process and authenticated Git transport may receive
their respective secret handles. Validator commands, evidence commands, and
checkpoint metadata receive none. Provider adapters must preserve this process
boundary. Durable redaction is a separate required gate before any trace,
archive, event, or artifact is published.

Codex, Claude Code, and OpenCode are executable harnesses in the common
kernel. OpenCode writes a sandbox-local config outside the repository, disables
project configuration and sharing, and stores only environment placeholders
for endpoint headers. The provider adapter injects the selected secret into the
OpenCode process; the config never contains the credential values. Its endpoint
URL, provider identifier, wire API (`chat-completions` or `responses`), model,
and header-to-environment bindings come from the provider specification.
OpenCode resume uses the prior session ID through `opencode run --session`.

## 6. Idempotency and explicit non-claims

The kernel creates the gate and manifest directories before agent execution,
then writes two receipts under `.archetype-agent/gates/`. A repository-phase
receipt is stored immediately after trusted commit/push finalization. If
evidence, checkpoint, or handoff then crashes, replay reconstructs the prepared
attempt and repository result from that receipt and resumes at evidence without
another model call, validator run, commit, or push. A separate final receipt
suppresses all work once artifact handoff completes. Corrupt repository-phase
state fails closed instead of replaying model execution.

Both receipts bind the key to a hash of the complete attempt request and reject
key reuse with changed inputs. They are not a durable pre-execution claim. A
crash after model submission but before the repository-phase receipt is stored
is therefore **possibly submitted**, and the architecture makes no exactly-once
model-execution claim.

Durable claim ownership and recovery policy belong above this transport. The
claim must be acquired before entering the execution phase.

## 7. Artifact handoff

Handoff values are declared source references, not proof of publication.
Checkpoint-backed paths use `checkpoint-ref#absolute-path`; terminal live-event
paths remain live sandbox URIs so events written after checkpoint creation are
still ingestible.

Mission finalization may advance only after the artifact service durably
publishes, indexes, and correlates the required evidence. A sandbox-local
`finalization_phase` of `captured` or `checkpointed` is not an indexed receipt.

## 8. Provider adapter requirements

An adapter implements:

- stable `sandbox_id` and live path URI construction;
- command execution with isolated secret injection;
- UTF-8 text writes;
- checkpoint creation with explicit disabled/failure behavior;
- create, restore, resume, authentication, and idempotent close;
- pinned CLI, SDK, image, collector, and proxy versions where applicable.

Provider integration tests are opt-in and live outside the normal CI profile.
The common kernel contract and fake-provider tests remain credential-free PR
gates.
