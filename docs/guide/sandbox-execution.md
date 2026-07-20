# Sandbox execution

**Document type:** Normative.

**Scope:** Provider-neutral coding-agent execution and process-local sandbox
lifetime under `src/archetype/app/sandboxes/`.

> **Legacy compatibility subsystem.**
>
> This page specifies the six-phase kernel retained under
> `archetype.app.sandboxes`. Agent Missions V1 does not call it. V1 sandbox
> resources live under `archetype.missions.sandboxes` and implement the
> smaller family-owned `AgentMissionSandbox` protocol described in
> [Agent Missions V1](agent-missions.md#5-sandbox-and-validator-protocol).

## 1. Ownership and boundary

The sandbox family owns the common attempt kernel, provider selection, live
handle retention, and provider checkpoint references. A provider adapter owns
SDK calls, command transport, filesystem writes, checkpoint creation,
provider URIs, and resource teardown.

The common kernel imports no provider adapter or provider SDK. It consumes the
application mission family's immutable `FencedExecutionAuthorization` and
typed `AttemptRecoveryAction`, not its claim service or storage authority.
Those values remain app-internal control-plane contracts even though the
sandbox consumes them through a mission-owned port; they are not part of the
reusable `archetype.missions` world domain. Modal, Apple Container, and future
adapters point inward to the common kernel. Docker is not a dependency or
fallback of this architecture.

The sandbox family does not own mission/task advancement, durable submission
claims, artifact indexing, evaluation, PR policy, or fleet scheduling.

## 2. Live handles and durable state

`SandboxService` keeps a process-local map of live `iSandboxSession` handles.
The map is an optimization, not mission state. The durable attempt claim,
provider checkpoint, sandbox receipts, and persisted Archetype facts are the
recovery inputs after process loss.

The container owns shutdown of retained handles. Closing one handle is
idempotent. Shutdown stops new admission, attempts every close, and reports all
close failures after the drain. A sandbox close failure does not prevent audit
or owned world/storage shutdown; the composition root drains every later step
and raises one aggregate failure afterward. Cancellation and other
`BaseException` failures are included in that aggregate only after every later
shutdown step has been attempted.

## 3. Six-phase attempt protocol

After validating a mission-issued fenced authorization, the request, and the
repository baseline, `CodingAgentSandboxClient.run_attempt` executes these
phases in order:

The mission authority has already canonicalized validator names, commands,
return codes, timeouts, and defaults before claim acquisition. The kernel
revalidates that canonical boundary and fingerprints the exact normalized
invocation; malformed or differently normalized commands cannot reach grant
consumption.

Before phase preparation, an attempt without a matching recovery receipt calls
the mission-provided execution-authorization callback. In the supported
`MissionAttemptExecutionService` path, that callback atomically consumes the
authorization's single-use nonce through the control catalog. Only successful
consumption permits preparation or the provider call; stale, expired,
duplicate, mismatched, or settled grants stop before provider work. This is an
admission boundary for the execution phase, not a seventh attempt phase.

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
   into the top-level mission domain's durable checkpoint vocabulary instead
   of sharing sandbox internals.
6. **Artifact handoff** — declare checkpoint-qualified or live source
   references and store the sandbox-local replay receipt. This phase does not
   upload or index artifacts; authoritative mission finalization owns that
   transition.

Immediately after the execution phase returns and before validation begins,
the kernel invokes the mission-provided acknowledgement callback with the
provider session/request identity it has. Callback failure stops the attempt
before validation. This boundary makes a crash after provider return but before
later evidence distinguishable from a wholly unacknowledged submission whenever
the provider supplies an identity. The claim authority accepts that
acknowledgement only after the execution grant was consumed.

Every phase has one typed result in `app/sandboxes/models.py`. The ordering is a
contract so later telemetry can create one correlated span per phase without
guessing at control flow.

## 4. Failure semantics

| Failure | Remaining work | Returned meaning |
|---|---|---|
| Execution-grant consumption fails | Stop before preparation and provider I/O | No authorized provider call; stale, expired, duplicate, or settled grants fail closed |
| Lease heartbeat fails while the runner is active | Cancel and await the runner | No outcome is applied or settled; recovery waits for a valid lease |
| Caller cancels orchestration | Cancel and await local runner and heartbeat tasks | No local child task is orphaned; remote work may remain `possibly_submitted` and requires adapter-specific cancellation or reconciliation |
| Agent transport raises | Stop before validation | No completed attempt; the mission claim remains `possibly_submitted` until reconciliation |
| Provider acknowledgement callback fails | Stop before validation | Provider return has occurred; the durable claim remains uncertain unless the acknowledgement committed |
| Agent exits nonzero | Continue all phases | Agent friction plus authoritative validator outcome |
| Validator fails | Continue evidence and checkpoint | Rejected attempt with recovery evidence |
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

Mission control uses that gate before its own durability as well.
`MissionAttemptClaimService` requires `iRedactionService`: canonical request
JSON and this runner's provider capabilities are scanned before a claim exists,
and provider acknowledgement identity is scanned before its catalog CAS. A
semantic finding quarantines rather than rewriting identity.

The sandbox outcome remains untrusted when the runner returns. Before
`MissionService` can project it or the catalog can settle it, the claim
authority quarantines secret-bearing IDs, fingerprints, session/checkpoint
identity, references, validator command identity, and result keys. It
deterministically redacts narrative validator output, friction, messages, and
errors. The execution service supplies only that sanitized outcome to
`MissionService`; settlement stores its typed outcome/error receipts and
preserves the original finding receipt from the first narrative scan. A clean
defensive rescan of already-sanitized text cannot erase the finding evidence.

The policy ID participates in immutable claim identity. Non-terminal work may
continue only under that exact active policy; drift fails closed. Settled
sanitized outcomes remain readable and replayable after a policy rollout. A
sandbox-local receipt or checkpoint is therefore recovery input, never proof
that its contents may bypass the mission or artifact redaction authorities.

Codex, Claude Code, and OpenCode are executable harnesses in the common
kernel. OpenCode writes a sandbox-local config outside the repository, disables
project configuration and sharing, and stores only environment placeholders
for endpoint headers. The provider adapter injects the selected secret into the
OpenCode process; the config never contains the credential values. Its endpoint
URL, provider identifier, wire API (`chat-completions` or `responses`), model,
and header-to-environment bindings come from the provider specification.
OpenCode resume uses the prior session ID through `opencode run --session`.

## 6. Fenced execution, idempotency, and recovery

Every call carries the app-owned `FencedExecutionAuthorization` issued from a
durable mission claim. The supported mission orchestrator derives the claim's
provider identity and request fingerprint from this runner's
`provider_execution_capabilities`; callers cannot supply metadata for a
different runner. Before reading a receipt or performing any sandbox mutation,
including reconciliation, the kernel requires:

- the authorization's idempotency key and deterministic attempt ID to match the
  request;
- a non-empty claim key and request fingerprint;
- the exact normalized sandbox invocation fingerprint to match prompt,
  validators and defaults, task name, attempt index, prior session and validator
  evidence, and correlation;
- correlation world and run IDs, entity-derived mission identity, and task step
  to match the claim;
- a positive fence epoch and claimant identity;
- an unexpired lease at sandbox admission, before any mutation; and
- for `execute`, a non-empty, per-fence execution nonce.

`execute` is the only action that may enter the model phase. `reconcile` may
continue only from a matching repository-phase or final receipt and otherwise
fails closed. `replay_idempotent` and `resume_session` are rejected even when
their capability metadata is present, because no corresponding provider
transport is implemented. A settled claim is replayed by
`MissionAttemptExecutionService` without entering the sandbox. The kernel does
not choose recovery policy; the mission claim authority does.

The supported execution service supervises the runner with a durable-lease
heartbeat from runner start through runner completion. If renewal fails, it
cancels and awaits the runner before propagating failure. If the caller is
cancelled, it cancels and awaits both runner and heartbeat. On successful
completion it stops and awaits the heartbeat, then renews the active claim once
more before outcome application or settlement. Sandbox mutation and lease
renewal therefore cannot continue in orphaned local tasks. Async cancellation
does not prove a remote Modal or CLI operation terminated; external work may
remain `possibly_submitted`, and remote cancellation is adapter-specific or
handled through reconciliation.

For `execute` without a receipt, the kernel consumes the nonce through the
injected authorization callback immediately before attempt preparation. The
catalog compare-and-swap requires `possibly_submitted`, the exact claimant,
fence and nonce, an unconsumed grant, and an unexpired lease, then records
`execution_consumed_at`. A second caller cannot consume the same grant, and a
provider acknowledgement cannot be persisted before consumption. Recovery
from an existing receipt performs no provider work and therefore consumes no
new grant.

The kernel creates the gate and manifest directories before agent execution,
then writes two receipts under `.archetype-agent/gates/`. A repository-phase
receipt is stored immediately after trusted commit/push finalization. If
evidence, checkpoint, or handoff then crashes, replay reconstructs the prepared
attempt and repository result from that receipt and resumes at evidence without
another model call, validator run, commit, or push. A separate final receipt
suppresses all work once artifact handoff completes. Corrupt repository-phase
state fails closed instead of replaying model execution.

Both receipts bind the key to a hash of the complete sandbox request and reject
key reuse with changed inputs. They complement rather than replace the durable
claim: the claim answers whether provider execution is permitted, while the
receipts answer which completed sandbox phases can be skipped.

With a live lease and repository-phase receipt, a `reconcile` authorization may
resume at evidence capture without another model call, validator run, commit,
or push. With a live lease and final receipt, reconciliation returns the
completed outcome. Corrupt or mismatched receipt state fails closed. A crash
after model submission but before the repository-phase receipt remains
`possibly_submitted`; provider capability flags never convert missing local
evidence into permission for a blind replay.

Grant consumption proves one provider call was authorized to begin; it does
not prove an exactly-once external side effect. Consumption and provider
transport are not atomic, so a crash immediately afterward may leave the
request unsent or its provider result unknown. Recovery remains
`reconcile`-only in either case.

Before the outcome crosses the mission boundary, the execution service also
requires an `execute` result to retain consumed-grant evidence and requires the
outcome's agent session to equal the claim's provider acknowledgement. Claim
settlement applies the stronger provider binding for every terminal result:
checkpoint provider must equal the claimed runner provider, and any
provider-accepted result—authoritatively `accepted` or `incomplete` after
mission gates—requires the consumed grant.

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
- stable `provider_execution_capabilities` derived from the adapter identity
  and effective execution specification;
- command execution with isolated secret injection;
- UTF-8 text writes;
- checkpoint creation with explicit disabled/failure behavior;
- create, restore, resume, authentication, and idempotent close;
- CLI, SDK, image, collector, and proxy versions resolved from the pinned
  environment inventory (section 9), plus a `_runtime_version_evidence()`
  override reporting those effective identities.

Provider integration tests are opt-in and live outside the normal CI profile.
The common kernel contract and fake-provider tests remain credential-free PR
gates.

## 9. Pinned environment inventory

`src/archetype/app/sandboxes/versions.toml` is the one machine-readable
inventory of every executable dependency that can affect a coding-agent
attempt: the Codex, Claude Code, and OpenCode CLIs, sandbox SDKs and runtime
identities, collector and proxy images, and evaluation packages such as
`pydantic-evals`. The rendered operator view is
`docs/reference/version-inventory.md`, generated by
`scripts/generate_version_inventory.py` and kept current by the
`version-inventory-audit` static gate.

Every pinned artifact carries an exact version, a credential-free `https`
source, and an immutable digest: an npm integrity hash, a wheel or installer
`sha256`, or an OCI image digest reference. Loading through
`archetype.app.sandboxes.versions` validates all of that fail-closed:
version ranges, floating tags such as `latest`, missing digests, URL
userinfo or query strings, and credential-shaped values are rejected with
`VersionPinError`. `resolve()` and `harness_pin()` raise instead of falling
back, and every supported agent harness must map to exactly one pinned CLI.
Installers and image builds must consume the resolved pin and verify its
digest; installing an unpinned or floating version is a contract violation.

A `planned` row declares a pinning obligation whose concrete artifact is not
selected yet, such as the L7 egress proxy for #490. Planned rows carry no
version and cannot be resolved; they must become pinned before their
consumer graduates from a spike.

The evidence phase records safe effective-version evidence in the attempt
manifest under `environment`: the inventory content digest, the pinned
harness identity with an observed `--version` probe, the model, the
checkpoint provider, and the configuration digest already used for claim
identity. The probe executes in the same non-secret controlled environment
as the pinned agent invocation — auto-updates stay disabled and the managed
config location applies — so the observed version describes the install
that ran the attempt, and probing cannot trigger updater side effects. The
provider-neutral kernel records an empty `runtime` map; a concrete provider
adapter (#563, #564) must override `_runtime_version_evidence()` with its
SDK, runtime image, collector, and proxy identities before its attempts can
claim runtime attribution. Evidence values are names, exact versions, and
digests only; install sources, tokens, and private registry credentials
never enter version evidence, and the manifest still passes the durable
redaction gate like every other artifact source.

Upgrading a pin is an explicit procedure, not a background drift:

1. Update the artifact's version and immutable digest in `versions.toml`
   from the upstream registry.
2. For an agent CLI, re-affirm or update its recorded `harness_interface`
   (invocation tokens, machine-output flags, resume tokens, and session
   identity schema); the compatibility tests bind the kernel's command
   construction and session parsing to that record.
3. Run `make static` and the sandbox test lane; regenerate the operator
   page with `scripts/generate_version_inventory.py`.
4. Roll back by reverting the inventory commit. Attempt manifests store the
   inventory digest that was effective at capture time, so history across an
   upgrade or rollback remains attributable to exact versions.
