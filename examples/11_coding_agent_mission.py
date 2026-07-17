# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Coding-agent mission as an Archetype episode
============================================

Maps the software-factory mental model onto Archetype's ECS primitives:

  Lab / AutoResearch   → outer "did this candidate advance the frontier?" (sketched at end)
  Episode world        → one mission, one sandbox
  World tick           → exactly one agent submission / attempt
  Processor            → records the attempt; advances only accepted, checkpointed work
  Hooks                → sandbox lifecycle + observability (not gates)
  Resources            → picklable SandboxSpec + live SandboxClient
  Components           → Mission, TaskGate, Attempt, Checkpoint, Finalization, evidence

Default backend is a real Apple Container lightweight VM with no host workspace
mount. Set ``CODING_AGENT_BACKEND=modal`` for remote execution. Both backends
support ``CODING_AGENT_HARNESS=codex`` or ``claude-code``.

First-time local Codex setup uses your ChatGPT OAuth entitlement, not an OpenAI
Platform API key. Start Apple Container and complete Codex's device-code flow:

    container system start
    uv run python examples/11_coding_agent_mission.py --codex-login

Open the printed URL, enter the one-time code, and then run the mission:

    uv run python examples/11_coding_agent_mission.py

The login is stored in the named Apple Container volume
``archetype-codex-auth``. It is not copied from the host's browser/session
token, mounted from the host filesystem, included in workspace snapshots, or
passed to validator processes. To use a Platform API key instead, set
``CODEX_API_KEY`` and ``CODING_AGENT_CODEX_AUTH_ENV=CODEX_API_KEY``.

Claude Code currently uses ``ANTHROPIC_API_KEY``. Set
``CODING_AGENT_HARNESS=claude-code`` to select it.

Tick grain: NOT physics / NOT every tool call. One tick = one coding-agent
submission, whether accepted or rejected. Validators never abort an ordinary
tick. ``TaskGate.step_index`` advances only when the submission is accepted and
the configured finalization phase is durable enough. This example requires a
provider checkpoint; R2 publication can later raise that requirement to
``indexed`` without changing tick semantics.

Every attempt snapshot contains an attempt manifest, canonical CLI JSONL,
validator results, Git status + binary patch + sanitized bundle, ``.context``
when present, and start/end/diff manifests for the entire sandbox filesystem.
Apple Container exports and can rehydrate the complete rootfs; Modal snapshots
and restores the complete filesystem as an image. These provider-native
checkpoints establish resumability. After the episode, this example publishes
the declared evidence for every recoverable attempt through ``ArtifactService``
and queries its Iceberg index.
The default artifact store is local; production can point the same contract at
R2 with a caller-configured Daft ``IOConfig`` and managed Iceberg session.
Publication is idempotent and leaves a durable reconciliation record across the
``pending → uploaded → indexed`` phases. Host-side publication spans carry the
world, run, entity, tick, and attempt correlation keys.

Usage:
    uv run python examples/11_coding_agent_mission.py
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol, cast

import daft
from daft import DataFrame, col

from archetype import (
    ArchetypeRuntime,
    ArtifactBundleRequest,
    ArtifactCandidate,
    ArtifactStoreConfig,
    Component,
)
from archetype.app.models import EpisodeConfig
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.config import StorageConfig
from archetype.core.hooks import OnDestroy, PostTick, PreTick
from archetype.core.resources import Resources
from archetype.experiments import (
    AgentHarness,
    AppleContainerSandboxClient,
    AppleContainerSandboxSpec,
    ModalArtifactSourceResolver,
    ModalSandboxClient,
    ModalSandboxSpec,
    ValidatorSpec,
)

# ── Mission plan (would later be HTN-compiled into TaskGate rows) ────────────

PLAN: list[dict[str, Any]] = [
    {
        "step": 0,
        "name": "fix_issue_342",
        "prompt": (
            "Fix https://github.com/VangelisTech/archetype/issues/342: GET /signatures "
            "currently treats a blank storage_uri query value as a real URI, so "
            "?storage_uri=&namespace=custom resolves to the process working directory instead "
            "of StorageConfig().uri. Add a focused regression test in tests/api/test_routes.py "
            "that proves a blank storage_uri uses the default URI while preserving the supplied "
            "namespace, then implement the smallest API-layer fix. Do not modify core/."
        ),
        "validators": [
            {
                "name": "signatures_api_contracts",
                "command": [
                    "uv",
                    "run",
                    "pytest",
                    "-q",
                    "tests/api/test_routes.py",
                    "-k",
                    "signatures",
                ],
            },
            {
                "name": "ruff",
                "command": [
                    "uv",
                    "run",
                    "ruff",
                    "check",
                    "src/archetype/api/routes/query.py",
                    "tests/api/test_routes.py",
                ],
            },
            {"name": "tests", "command": ["make", "test"], "timeout_seconds": 1800},
        ],
    },
]


# ── Components ───────────────────────────────────────────────────────────────


class Mission(Component):
    """Episode-level mission. ``finished`` is the terminal latch."""

    name: str = ""
    repo: str = ""
    branch: str = "agent/mission"
    plan_json: str = "[]"
    finished: bool = False
    succeeded: bool = False
    failure_reason: str = ""
    pr_ready: bool = False
    pr_url: str = ""


class TaskGate(Component):
    """Current task. Attempts advance every tick; the step advances only at its gate."""

    step_index: int = 0
    step_name: str = ""
    prompt: str = ""
    validators_json: str = "[]"
    attempts: int = 0
    max_attempts: int = 5
    status: str = "ready"
    required_finalization_phase: str = "checkpointed"
    passed: bool = False


class Attempt(Component):
    """Exactly one coding-agent submission, persisted whether accepted or rejected."""

    attempt_id: str = ""
    attempt_index: int = 0
    status: str = "pending"
    harness: str = ""
    agent_session_id: str = ""
    validator_details_json: str = "[]"


class Checkpoint(Component):
    """Provider-native recovery point for the sandbox after this attempt."""

    provider: str = ""
    status: str = "pending"
    state_ref: str = ""
    restorable: bool = False
    created_at_ms: int = 0
    expires_at_ms: int = 0


class Finalization(Component):
    """Progress toward portable artifacts and indexing; checkpointed in this slice."""

    phase: str = "pending"
    idempotency_key: str = ""
    manifest_ref: str = ""
    error: str = ""


class Commit(Component):
    """Git identity produced by the gate. Empty until the tick commits."""

    sha: str = ""
    message: str = ""
    pushed: bool = False


class Evidence(Component):
    """Queryable pointers into the portable evidence captured for this attempt."""

    results_json: str = "{}"
    trace_ref: str = ""
    traces_ref: str = ""
    sandbox_state_ref: str = ""
    filesystem_start_ref: str = ""
    filesystem_end_ref: str = ""
    filesystem_diff_ref: str = ""
    git_status_ref: str = ""
    git_patch_ref: str = ""
    git_bundle_ref: str = ""
    context_ref: str = ""


class FrictionLog(Component):
    """Egocentric agent friction — what was hard, what to revisit later."""

    entries_json: str = "[]"


# ── Resources: picklable spec + live client ──────────────────────────────────


@dataclass(frozen=True)
class SandboxSpec:
    """Picklable sandbox config. Safe for Resources / Daft worker boundaries."""

    backend: str = "local"  # "local" | "modal"
    harness: str = "codex"
    model: str = ""
    workspace: str = "/workspace/repo"
    repo_url: str = "https://github.com/VangelisTech/archetype.git"
    base_ref: str = "main"
    branch: str = "agent/issue-342-blank-storage-uri"
    codex_secret_name: str = "archetype-codex"
    claude_secret_name: str = "archetype-claude-code"
    github_secret_name: str = ""
    local_image_name: str = ""
    local_state_dir: str = ".context/apple-container-snapshots"
    local_codex_auth_env: str = ""
    local_codex_auth_volume: str = "archetype-codex-auth"
    local_claude_auth_env: str = "ANTHROPIC_API_KEY"
    push: bool = False


class SandboxClient(Protocol):
    """Live sandbox handle — built from SandboxSpec, not pickled through Daft."""

    sandbox_id: str

    async def run_attempt(
        self,
        *,
        prompt: str,
        validators: list[ValidatorSpec | dict[str, Any]],
        step_name: str,
        attempt_index: int,
        idempotency_key: str,
        previous_session_id: str = "",
        previous_validator_details: Sequence[dict[str, Any]] = (),
        correlation: dict[str, Any] | None = None,
    ) -> dict[str, Any]: ...

    async def close(self) -> None: ...


async def build_sandbox(spec: SandboxSpec) -> SandboxClient:
    if spec.backend == "modal":
        return await ModalSandboxClient.create(_modal_spec(spec))
    if spec.backend == "local":
        return await AppleContainerSandboxClient.create(_local_spec(spec))
    raise ValueError(f"unknown sandbox backend: {spec.backend!r}")


def _local_spec(spec: SandboxSpec) -> AppleContainerSandboxSpec:
    return AppleContainerSandboxSpec(
        repo_url=spec.repo_url,
        base_ref=spec.base_ref,
        branch=spec.branch,
        harness=cast(AgentHarness, spec.harness),
        model=spec.model,
        workspace=spec.workspace,
        image_name=spec.local_image_name,
        state_dir=spec.local_state_dir,
        codex_auth_env=spec.local_codex_auth_env,
        codex_auth_volume=spec.local_codex_auth_volume,
        claude_api_key_env=spec.local_claude_auth_env,
        push=spec.push,
    )


def _modal_spec(spec: SandboxSpec) -> ModalSandboxSpec:
    return ModalSandboxSpec(
        repo_url=spec.repo_url,
        base_ref=spec.base_ref,
        branch=spec.branch,
        harness=cast(AgentHarness, spec.harness),
        model=spec.model,
        workspace=spec.workspace,
        codex_secret_name=spec.codex_secret_name,
        claude_secret_name=spec.claude_secret_name,
        github_secret_name=spec.github_secret_name,
        push=spec.push,
    )


def _artifact_bundle_from_row(row: dict[str, Any]) -> ArtifactBundleRequest:
    """Translate one persisted attempt row into its immutable evidence bundle."""
    candidates = [
        ArtifactCandidate(
            source_ref=row["finalization__manifest_ref"],
            logical_path="attempt/manifest.json",
            kind="attempt_manifest",
        ),
        ArtifactCandidate(
            source_ref=row["evidence__trace_ref"],
            logical_path="attempt/agent-output.jsonl",
            kind="agent_trace",
        ),
        ArtifactCandidate(
            source_ref=row["evidence__filesystem_start_ref"],
            logical_path="recovery/filesystem-start.jsonl",
            kind="filesystem_manifest",
        ),
        ArtifactCandidate(
            source_ref=row["evidence__filesystem_end_ref"],
            logical_path="recovery/filesystem-end.jsonl",
            kind="filesystem_manifest",
        ),
        ArtifactCandidate(
            source_ref=row["evidence__filesystem_diff_ref"],
            logical_path="recovery/filesystem-diff.jsonl",
            kind="filesystem_diff",
        ),
        ArtifactCandidate(
            source_ref=row["evidence__git_status_ref"],
            logical_path="recovery/git-status.txt",
            kind="git_status",
        ),
        ArtifactCandidate(
            source_ref=row["evidence__git_patch_ref"],
            logical_path="recovery/worktree.patch",
            kind="git_patch",
        ),
        ArtifactCandidate(
            source_ref=row["evidence__git_bundle_ref"],
            logical_path="recovery/repository.bundle",
            kind="git_bundle",
        ),
    ]
    if row["evidence__context_ref"]:
        candidates.append(
            ArtifactCandidate(
                source_ref=row["evidence__context_ref"],
                logical_path="context",
                kind="context",
                recursive=True,
                required=False,
            )
        )
    return ArtifactBundleRequest(
        world_id=str(row["world_id"]),
        run_id=str(row["run_id"]),
        entity_id=int(row["entity_id"]),
        tick=int(row["tick"]),
        attempt_id=row["attempt__attempt_id"],
        idempotency_key=row["finalization__idempotency_key"],
        checkpoint_ref=row["checkpoint__state_ref"],
        checkpoint_provider=row["checkpoint__provider"],
        checkpoint_restorable=bool(row["checkpoint__restorable"]),
        checkpoint_created_at_ms=int(row["checkpoint__created_at_ms"]),
        checkpoint_expires_at_ms=int(row["checkpoint__expires_at_ms"]),
        accepted=row["attempt__status"] == "accepted",
        retention="run" if row["attempt__status"] == "accepted" else "attempt",
        artifacts=tuple(candidates),
    )


# ── Processor: the tick gate ─────────────────────────────────────────────────


class CodingAgentProcessor(AsyncProcessor):
    """One processor = state-transition authority for the coding mission.

    For the active TaskGate:
      1. Read prompt + validators for this step
      2. Run exactly one sandbox-agent attempt
      3. Persist accepted or rejected Attempt / Checkpoint / Finalization evidence
      4. Advance only when validation and finalization requirements both pass

    A validator rejection is normal data. Infrastructure failures may still
    raise before an attempt can be identified, but a provider checkpoint
    failure is persisted as finalization data: the tick commits and the task
    remains gated.
    """

    components = (
        Mission,
        TaskGate,
        Attempt,
        Checkpoint,
        Finalization,
        Commit,
        Evidence,
        FrictionLog,
    )
    priority = 10

    _FINALIZATION_PHASES = {
        "pending": 0,
        "captured": 1,
        "checkpointed": 2,
        "indexed": 3,
        "published": 4,
    }

    async def process(
        self,
        df: DataFrame,
        resources: Resources | None = None,
        tick: int = 0,
        **kwargs: Any,
    ) -> DataFrame:
        if resources is None:
            raise KeyError("CodingAgentProcessor requires world resources")
        sandbox: SandboxClient | None = resources.get(AppleContainerSandboxClient)
        if sandbox is None:
            sandbox = resources.get(ModalSandboxClient)
        if sandbox is None:
            raise KeyError(
                "no SandboxClient in resources (AppleContainerSandboxClient or ModalSandboxClient)"
            )

        # Driver-side collect: one mission entity. Gates live here, not in hooks.
        rows = df.collect().to_pylist()
        if not rows:
            return df

        updated: list[dict[str, Any]] = []
        for row in rows:
            if row.get("mission__finished"):
                updated.append(row)
                continue

            # Column prefixes are Component.__name__.lower() + "__"
            # (TaskGate → taskgate__, FrictionLog → frictionlog__).
            plan = json.loads(row["mission__plan_json"] or "[]")
            step_index = int(row["taskgate__step_index"])
            if step_index >= len(plan):
                row = dict(row)
                row["mission__finished"] = True
                row["mission__succeeded"] = True
                row["mission__pr_ready"] = True
                updated.append(row)
                continue

            step = plan[step_index]
            validators = [ValidatorSpec.from_dict(value) for value in step["validators"]]
            attempt_index = int(row["taskgate__attempts"]) + 1
            gate_material = json.dumps(
                {
                    "world_id": str(row["world_id"]),
                    "run_id": str(row["run_id"]),
                    "entity_id": str(row["entity_id"]),
                    "step_index": step_index,
                    "attempt_index": attempt_index,
                    "step": step,
                },
                sort_keys=True,
            )
            previous_details = (
                json.loads(row["attempt__validator_details_json"] or "[]")
                if attempt_index > 1
                else []
            )
            outcome = await sandbox.run_attempt(
                prompt=step["prompt"],
                validators=validators,
                step_name=step["name"],
                attempt_index=attempt_index,
                idempotency_key=hashlib.sha256(gate_material.encode()).hexdigest(),
                previous_session_id=row["attempt__agent_session_id"],
                previous_validator_details=previous_details,
                correlation={
                    "world_id": str(row["world_id"]),
                    "run_id": str(row["run_id"]),
                    "entity_id": str(row["entity_id"]),
                    "tick": tick,
                    "step_index": step_index,
                },
            )

            prior_friction = json.loads(row["frictionlog__entries_json"] or "[]")
            prior_friction.extend(outcome.get("friction") or [])

            row = dict(row)
            row.update(
                {
                    "taskgate__step_name": step["name"],
                    "taskgate__prompt": step["prompt"],
                    "taskgate__validators_json": json.dumps(step["validators"]),
                    "taskgate__attempts": attempt_index,
                    "taskgate__status": outcome["status"],
                    "taskgate__passed": False,
                    "attempt__attempt_id": outcome["attempt_id"],
                    "attempt__attempt_index": attempt_index,
                    "attempt__status": outcome["status"],
                    "attempt__harness": outcome["harness"],
                    "attempt__agent_session_id": outcome["agent_session_id"],
                    "attempt__validator_details_json": json.dumps(outcome["validator_details"]),
                    "checkpoint__provider": outcome["checkpoint_provider"],
                    "checkpoint__status": outcome["checkpoint_status"],
                    "checkpoint__state_ref": outcome["sandbox_state_ref"],
                    "checkpoint__restorable": outcome["checkpoint_restorable"],
                    "checkpoint__created_at_ms": outcome["checkpoint_created_at_ms"],
                    "checkpoint__expires_at_ms": outcome["checkpoint_expires_at_ms"],
                    "finalization__phase": outcome["finalization_phase"],
                    "finalization__idempotency_key": outcome["idempotency_key"],
                    "finalization__manifest_ref": outcome["finalization_manifest_ref"],
                    "finalization__error": outcome["finalization_error"],
                    "evidence__results_json": json.dumps(outcome["results"]),
                    "evidence__trace_ref": outcome["trace_ref"],
                    "evidence__traces_ref": outcome["traces_ref"],
                    "evidence__sandbox_state_ref": outcome["sandbox_state_ref"],
                    "evidence__filesystem_start_ref": outcome["filesystem_start_ref"],
                    "evidence__filesystem_end_ref": outcome["filesystem_end_ref"],
                    "evidence__filesystem_diff_ref": outcome["filesystem_diff_ref"],
                    "evidence__git_status_ref": outcome["git_status_ref"],
                    "evidence__git_patch_ref": outcome["git_patch_ref"],
                    "evidence__git_bundle_ref": outcome["git_bundle_ref"],
                    "evidence__context_ref": outcome["context_ref"],
                    "frictionlog__entries_json": json.dumps(prior_friction),
                }
            )

            required_phase = row["taskgate__required_finalization_phase"]
            actual_rank = self._FINALIZATION_PHASES.get(outcome["finalization_phase"], -1)
            required_rank = self._FINALIZATION_PHASES.get(required_phase)
            if required_rank is None:
                raise ValueError(f"unknown required finalization phase: {required_phase!r}")
            gate_passed = (
                bool(outcome["accepted"])
                and bool(outcome["checkpoint_restorable"])
                and actual_rank >= required_rank
            )

            if gate_passed:
                row["taskgate__passed"] = True
                row["taskgate__status"] = "passed"
                row["commit__sha"] = outcome["sha"]
                row["commit__message"] = outcome["message"]
                row["commit__pushed"] = outcome["pushed"]
                next_index = step_index + 1
                if next_index >= len(plan):
                    row["mission__finished"] = True
                    row["mission__succeeded"] = True
                    row["mission__pr_ready"] = True
                    row["mission__pr_url"] = outcome.get("pr_url") or row.get("mission__pr_url", "")
                    row["taskgate__step_index"] = step_index
                else:
                    nxt = plan[next_index]
                    row["taskgate__step_index"] = next_index
                    row["taskgate__step_name"] = nxt["name"]
                    row["taskgate__prompt"] = nxt["prompt"]
                    row["taskgate__validators_json"] = json.dumps(nxt["validators"])
                    row["taskgate__status"] = "ready"
                    row["taskgate__passed"] = False
                    row["taskgate__attempts"] = 0
            elif attempt_index >= int(row["taskgate__max_attempts"]):
                row["taskgate__status"] = "exhausted"
                row["mission__finished"] = True
                row["mission__succeeded"] = False
                row["mission__failure_reason"] = (
                    f"task {step['name']!r} exhausted {attempt_index} attempts; "
                    f"latest status={outcome['status']} phase={outcome['finalization_phase']}"
                )

            updated.append(row)

        # Rebuild a frame with the same column set the store expects.
        return daft.from_pylist(updated).select(*df.column_names)


# ── Example driver ───────────────────────────────────────────────────────────


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Archetype coding-agent mission")
    parser.add_argument(
        "--codex-login",
        action="store_true",
        help="complete a one-time ChatGPT device login in an Apple Container volume",
    )
    args = parser.parse_args()
    storage = StorageConfig(uri="./archetype_data", namespace="coding_agent_issue_342_v1")
    backend = os.environ.get("CODING_AGENT_BACKEND", "local")
    spec = SandboxSpec(
        backend=backend,
        harness=os.environ.get("CODING_AGENT_HARNESS", "codex"),
        model=os.environ.get("CODING_AGENT_MODEL", ""),
        repo_url=os.environ.get(
            "CODING_AGENT_REPO_URL", "https://github.com/VangelisTech/archetype.git"
        ),
        base_ref=os.environ.get("CODING_AGENT_BASE_REF", "main"),
        branch=os.environ.get("CODING_AGENT_BRANCH", "agent/issue-342-blank-storage-uri"),
        codex_secret_name=os.environ.get("CODEX_MODAL_SECRET", "archetype-codex"),
        claude_secret_name=os.environ.get("CLAUDE_MODAL_SECRET", "archetype-claude-code"),
        github_secret_name=os.environ.get("GITHUB_MODAL_SECRET", ""),
        local_image_name=os.environ.get("CODING_AGENT_LOCAL_IMAGE", ""),
        local_state_dir=os.environ.get(
            "CODING_AGENT_LOCAL_STATE_DIR", ".context/apple-container-snapshots"
        ),
        local_codex_auth_env=os.environ.get("CODING_AGENT_CODEX_AUTH_ENV", ""),
        local_codex_auth_volume=os.environ.get(
            "CODING_AGENT_CODEX_AUTH_VOLUME", "archetype-codex-auth"
        ),
        local_claude_auth_env=os.environ.get("CODING_AGENT_CLAUDE_AUTH_ENV", "ANTHROPIC_API_KEY"),
        push=os.environ.get("CODING_AGENT_PUSH", "").lower() in {"1", "true", "yes"},
    )
    if args.codex_login:
        if spec.backend != "local" or spec.harness != "codex":
            parser.error("--codex-login requires the local backend and codex harness")
        await AppleContainerSandboxClient.login_codex(_local_spec(spec))
        print("Codex OAuth login saved; rerun without --codex-login to start the mission.")
        return
    sandbox = await build_sandbox(spec)

    tick_log: list[str] = []

    async def on_pre_tick(event: PreTick) -> None:
        # Observability only — does not decide whether the gate passes.
        tick_log.append(f"pre_tick t={event.tick} sandbox={sandbox.sandbox_id}")

    async def on_post_tick(event: PostTick) -> None:
        completed = event.tick - 1
        tick_log.append(f"post_tick completed={completed} archetypes={len(event.results)}")

    async def on_destroy(event: OnDestroy) -> None:
        await sandbox.close()
        tick_log.append(f"destroy world={event.world_id} sandbox_closed=True")

    artifact_store = ArtifactStoreConfig.local(
        os.environ.get("CODING_AGENT_ARTIFACT_DIR", ".context/coding-agent-artifacts")
    )
    artifact_resolver = (
        ModalArtifactSourceResolver(
            spec=_modal_spec(spec), sandbox=cast(ModalSandboxClient, sandbox)
        )
        if spec.backend == "modal"
        else None
    )
    async with ArchetypeRuntime(
        artifact_store=artifact_store,
        artifact_source_resolver=artifact_resolver,
    ) as runtime:
        world = runtime.world(
            "coding-mission",
            storage=storage,
            processors=[CodingAgentProcessor()],
            resources=[spec, sandbox],
            hooks=[
                (PreTick, on_pre_tick),
                (PostTick, on_post_tick),
                (OnDestroy, on_destroy),
            ],
        )

        first = PLAN[0]
        await world.spawn(
            Mission(
                name="fix-archetype-issue-342",
                repo=spec.repo_url,
                branch=spec.branch,
                plan_json=json.dumps(PLAN),
            ),
            TaskGate(
                step_index=0,
                step_name=first["name"],
                prompt=first["prompt"],
                validators_json=json.dumps(first["validators"]),
                max_attempts=5,
            ),
            Attempt(),
            Checkpoint(),
            Finalization(),
            Commit(),
            Evidence(),
            FrictionLog(),
        )

        # Episode = mission. Terminal latch = verified, committed PR-ready branch.
        try:
            result = await world.run_episode(
                EpisodeConfig(
                    max_steps=sum(5 for _ in PLAN) + 1,
                    terminal_component=Mission,
                    terminal_field="finished",
                )
            )
        except BaseException:
            # A failed processor aborts the tick before world.destroy() can emit
            # OnDestroy. Close the external VM explicitly on that path.
            await sandbox.close()
            raise

        print("── episode ──")
        print(
            f"world={result.world_id} steps={result.duration_steps} "
            f"terminated={result.terminated} ticks={result.start_tick}→{result.final_tick}"
        )

        history = await world.query(
            Mission,
            TaskGate,
            Attempt,
            Checkpoint,
            Finalization,
            Commit,
            Evidence,
            FrictionLog,
        )
        attempt_rows = (
            history.where(col("attempt__attempt_id") != "").sort("tick").collect().to_pylist()
        )
        assert attempt_rows, "expected at least one persisted mission attempt"
        row = attempt_rows[-1]

        # Publish every recoverable attempt, including validator rejections.
        # A failed checkpoint remains queryable in Archetype history but cannot
        # be promoted into a checkpoint-qualified portable bundle.
        publications = []
        seen_attempts: set[str] = set()
        for attempt_row in attempt_rows:
            attempt_id = str(attempt_row["attempt__attempt_id"])
            if attempt_id in seen_attempts:
                continue
            seen_attempts.add(attempt_id)
            if not attempt_row["checkpoint__restorable"]:
                continue
            publications.append(
                await world.publish_artifacts(_artifact_bundle_from_row(attempt_row))
            )
        indexed_artifacts = (await world.artifacts()).collect().to_pylist()

        print("\n── final mission state ──")
        print(
            f"finished={row['mission__finished']} succeeded={row['mission__succeeded']} "
            f"failure={row['mission__failure_reason']}"
        )
        print(f"pr_ready={row['mission__pr_ready']} url={row['mission__pr_url']}")
        print(
            f"attempt={row['attempt__attempt_index']} status={row['attempt__status']} "
            f"session={row['attempt__agent_session_id']}"
        )
        print(f"last_commit={row['commit__sha']} pushed={row['commit__pushed']}")
        print(f"evidence={row['evidence__results_json']}")
        print(f"trace={row['evidence__trace_ref']}")
        print(f"traces={row['evidence__traces_ref']}")
        print(f"sandbox_state={row['evidence__sandbox_state_ref']}")
        print(f"filesystem_diff={row['evidence__filesystem_diff_ref']}")
        print(f"git_bundle={row['evidence__git_bundle_ref']}")
        print(f"context={row['evidence__context_ref']}")
        if publications:
            publication = publications[-1]
            print(
                f"finalization={publication.status} bundle={publication.bundle_id} "
                f"manifest={publication.manifest_uri}"
            )
        else:
            print("finalization=unpublished (no restorable attempt checkpoint)")
        print(f"published_attempts={len(publications)}/{len(seen_attempts)}")
        print(f"indexed_artifacts={len(indexed_artifacts)}")
        for artifact in sorted(indexed_artifacts, key=lambda value: value["logical_path"]):
            print(
                f"  {artifact['kind']}: {artifact['logical_path']} "
                f"({artifact['size_bytes']} bytes) → {artifact['object_uri']}"
            )

        friction = json.loads(row["frictionlog__entries_json"] or "[]")
        print(f"\n── friction log ({len(friction)} entries) ──")
        for entry in friction:
            print(f"  [{entry['step']}#{entry['attempt']}] {entry['finding']}")
            print(f"    learning: {entry['learning']}")

        # Outer AutoResearch sketch: score = 1.0 iff the verified branch is PR-ready.
        # Real lab loop would fork candidates, run this episode, compare CI+bench.
        score = 1.0 if row["mission__pr_ready"] else 0.0
        print(f"\n── lab-facing evaluation sketch ──\n  frontier_score={score}")

        await world.destroy()

        print("\n── hook timeline ──")
        for line in tick_log:
            print(f"  {line}")


if __name__ == "__main__":
    asyncio.run(main())
