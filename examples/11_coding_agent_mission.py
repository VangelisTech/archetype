# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Run a real, validator-gated coding-agent mission.

This example is the executable composition root for Archetype's agent-mission
families. ``MissionService`` owns deterministic task transitions,
``CodingAgentService`` binds a mission to a live session, and
``SandboxService`` owns provider lifecycle. One world tick records one agent
submission. A rejected attempt is durable data; only an accepted attempt with
independent validator evidence, a commit, and a restorable checkpoint advances
the task.

The default target is a real Archetype bug. Override it without teaching the
agent the solution:

    CODING_AGENT_ISSUE_URL=https://github.com/VangelisTech/archetype/issues/463 \
      uv run python examples/11_coding_agent_mission.py

The prompt remains deliberately naive (``Fix <issue URL>.``). Validators are
independent: they require a material implementation and regression-test diff,
the focused runtime suite, architecture checks, lint, and the normal test
suite. For another issue, provide ``CODING_AGENT_VALIDATORS_JSON`` as a JSON
list of ``ValidatorSpec`` objects containing its issue-specific oracle.

Local execution uses Apple Container, never Docker. Start its service first:

    container system start

First-time local Codex setup uses your ChatGPT subscription OAuth entitlement,
not an OpenAI Platform API key. Complete device login once:

    uv run python examples/11_coding_agent_mission.py --codex-login

The login is stored in the named Apple Container volume
``archetype-codex-auth``. It is not copied from this host session, mounted from
the host workspace, included in snapshots, or exposed to validators. To use an
API key instead, set ``CODEX_API_KEY`` and
``CODING_AGENT_CODEX_AUTH_ENV=CODEX_API_KEY``. Claude Code uses
``ANTHROPIC_API_KEY`` locally; select it with
``CODING_AGENT_HARNESS=claude-code``.

For Modal, API-key Secrets work directly, or Codex/Claude subscription OAuth
can be persisted in a dedicated broker Volume:

    CODING_AGENT_BACKEND=modal CODING_AGENT_HARNESS=codex \
      CODING_AGENT_MODAL_AUTH_MODE=oauth \
      uv run --extra coding-agent python examples/11_coding_agent_mission.py \
      --modal-login

    CODING_AGENT_BACKEND=modal CODING_AGENT_HARNESS=claude-code \
      CODING_AGENT_MODAL_AUTH_MODE=oauth \
      uv run --extra coding-agent python examples/11_coding_agent_mission.py \
      --modal-login

Rerun without ``--modal-login`` to start the mission. The auth Volume is
mounted only into a credential broker. Credentials are staged only while the
CLI runs, refreshed back into the Volume, then removed before validators,
filesystem manifests, checkpoints, and artifact capture.

OpenCode can drive a protected OpenAI-compatible Modal endpoint without an
OpenAI or Anthropic key. Put only ``MODAL_ENDPOINT_TOKEN_ID`` and
``MODAL_ENDPOINT_TOKEN_SECRET`` in the dedicated Modal Secret, then run:

    CODING_AGENT_BACKEND=modal CODING_AGENT_HARNESS=opencode \
      CODING_AGENT_MODEL=Qwen/Qwen3.6-35B-A3B-FP8 \
      CODING_AGENT_OPENCODE_BASE_URL=https://REPLACE-ME/v1 \
      uv run --extra coding-agent python examples/11_coding_agent_mission.py

The default endpoint Secret is ``archetype-modal-endpoint``. Generated
OpenCode configuration contains environment placeholders, never token values.

Modal sessions expose status, events, stdout, stderr, phases, and heartbeats.
The driver prints the ``sb-...`` ID immediately. Attach from another terminal:

    uv run --extra coding-agent python examples/11_coding_agent_mission.py \
      --monitor-sandbox sb-REPLACE-ME

Every attempt captures canonical CLI JSONL, validator results, Git status and
patch, a sanitized Git bundle, ``.context`` when present, full-filesystem
start/end/diff manifests, and a provider-native checkpoint. Apple Container
exports a rehydratable rootfs; Modal snapshots the filesystem as an image.
Declared evidence is then published through ``ArtifactBundleService`` while
the full checkpoint remains a distinct recovery object. Publication uses a
durable ``pending -> uploaded -> indexed`` reconciliation record.

The paid Modal integrations are credential-gated and live in their own
path-triggered workflow. They do not run in normal CI. The resume profile proves
Codex, Claude Code, and OpenCode can continue from a checkpoint. Capacity tests
are likewise manual:

    make test-modal-resume
    make bench-opencode-endpoint CONFIRM_PAID_BENCH=1
    make bench-opencode-agents CONFIRM_PAID_BENCH=1
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from dataclasses import dataclass
from typing import Any, cast

from daft import col

from archetype import (
    ArchetypeRuntime,
    ArtifactBundleRequest,
    ArtifactCandidate,
    ArtifactStoreConfig,
)
from archetype.app.coding_agents import (
    CodingAgentEpisode,
    CodingAgentProcessor,
    CodingAgentService,
)
from archetype.app.missions import (
    Attempt,
    Checkpoint,
    Commit,
    Evidence,
    Finalization,
    FrictionLog,
    Mission,
    MissionService,
    TaskGate,
)
from archetype.app.models import EpisodeConfig
from archetype.app.sandboxes import (
    AgentAuthMode,
    AgentHarness,
    AppleContainerSandboxBackend,
    AppleContainerSandboxClient,
    AppleContainerSandboxSpec,
    ModalArtifactSourceResolver,
    ModalSandboxBackend,
    ModalSandboxClient,
    ModalSandboxSpec,
    OpenCodeWireAPI,
    SandboxService,
)
from archetype.core.config import StorageConfig
from archetype.core.hooks import OnDestroy, PostTick, PreTick

DEFAULT_ISSUE_URL = "https://github.com/VangelisTech/archetype/issues/463"


@dataclass(frozen=True)
class SandboxSpec:
    """Picklable operator configuration; secrets remain provider-side."""

    backend: str = "local"
    harness: str = "codex"
    model: str = ""
    workspace: str = "/workspace/repo"
    repo_url: str = "https://github.com/VangelisTech/archetype.git"
    base_ref: str = "main"
    branch: str = "agent/fix-archetype-bug"
    codex_secret_name: str = "archetype-codex"
    claude_secret_name: str = "archetype-claude-code"
    opencode_secret_name: str = "archetype-modal-endpoint"
    opencode_base_url: str = ""
    opencode_provider_id: str = "archetype-modal"
    opencode_wire_api: str = "chat-completions"
    modal_auth_mode: str = "api-key"
    modal_codex_auth_volume: str = "archetype-codex-auth"
    modal_claude_auth_volume: str = "archetype-claude-code-auth"
    stream_agent_output: bool = True
    github_secret_name: str = ""
    local_image_name: str = ""
    local_state_dir: str = ".context/apple-container-snapshots"
    local_codex_auth_env: str = ""
    local_codex_auth_volume: str = "archetype-codex-auth"
    local_claude_auth_env: str = "ANTHROPIC_API_KEY"
    push: bool = False


def _default_validators() -> list[dict[str, Any]]:
    material_diff = (
        "import subprocess; "
        "tracked=set(subprocess.check_output(['git','diff','--name-only','HEAD'], "
        "text=True).splitlines()); "
        "untracked=set(subprocess.check_output(['git','ls-files','--others',"
        "'--exclude-standard'], text=True).splitlines()); "
        "changed=tracked|untracked; "
        "implementation={p for p in changed if p.startswith('src/archetype/runtime/')}; "
        "tests={p for p in changed if p.startswith('tests/') and 'runtime' in p}; "
        "assert implementation, ('missing runtime implementation', sorted(changed)); "
        "assert tests, ('missing runtime regression test', sorted(changed)); "
        "diff=subprocess.check_output(['git','diff','--no-ext-diff','HEAD'], "
        "text=True).lower(); "
        "assert 'activation' in diff and 'retry' in diff, "
        "('regression must exercise failed activation retry', sorted(tests))"
    )
    return [
        {
            "name": "issue_specific_material_diff",
            "command": ["uv", "run", "python", "-c", material_diff],
        },
        {
            "name": "runtime_contracts",
            "command": [
                "uv",
                "run",
                "pytest",
                "-q",
                "tests/app/test_runtime_contracts.py",
                "tests/app/test_runtime_entrypoint.py",
            ],
            "timeout_seconds": 600,
        },
        {"name": "architecture", "command": ["make", "architecture-audit"]},
        {
            "name": "ruff",
            "command": ["uv", "run", "ruff", "check", "src/archetype/runtime", "tests/app"],
        },
        {"name": "git_diff_check", "command": ["git", "diff", "--check"]},
        {"name": "tests", "command": ["make", "test"], "timeout_seconds": 1800},
    ]


def _mission_plan(issue_url: str) -> list[dict[str, Any]]:
    override = os.environ.get("CODING_AGENT_VALIDATORS_JSON", "").strip()
    validators = json.loads(override) if override else _default_validators()
    if not isinstance(validators, list) or not validators:
        raise ValueError("CODING_AGENT_VALIDATORS_JSON must be a non-empty JSON list")
    return [
        {
            "step": 0,
            "name": "fix_archetype_issue",
            "prompt": f"Fix {issue_url}.",
            "validators": validators,
        }
    ]


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
        opencode_secret_name=spec.opencode_secret_name,
        opencode_base_url=spec.opencode_base_url,
        opencode_provider_id=spec.opencode_provider_id,
        opencode_wire_api=cast(OpenCodeWireAPI, spec.opencode_wire_api),
        auth_mode=cast(AgentAuthMode, spec.modal_auth_mode),
        codex_auth_volume_name=spec.modal_codex_auth_volume,
        claude_auth_volume_name=spec.modal_claude_auth_volume,
        stream_agent_output=spec.stream_agent_output,
        github_secret_name=spec.github_secret_name,
        push=spec.push,
    )


def _provider_spec(spec: SandboxSpec) -> tuple[str, object]:
    if spec.backend == "modal":
        return "modal", _modal_spec(spec)
    if spec.backend == "local":
        return "apple-container", _local_spec(spec)
    raise ValueError(f"unknown sandbox backend: {spec.backend!r}")


def _artifact_bundle_from_row(row: dict[str, Any]) -> ArtifactBundleRequest:
    refs = (
        ("finalization__manifest_ref", "attempt/manifest.json", "attempt_manifest", True),
        ("evidence__trace_ref", "attempt/agent-output.jsonl", "agent_trace", True),
        (
            "evidence__filesystem_start_ref",
            "recovery/filesystem-start.jsonl",
            "filesystem_manifest",
            True,
        ),
        (
            "evidence__filesystem_end_ref",
            "recovery/filesystem-end.jsonl",
            "filesystem_manifest",
            True,
        ),
        (
            "evidence__filesystem_diff_ref",
            "recovery/filesystem-diff.jsonl",
            "filesystem_diff",
            True,
        ),
        ("evidence__git_status_ref", "recovery/git-status.txt", "git_status", True),
        ("evidence__git_patch_ref", "recovery/worktree.patch", "git_patch", True),
        ("evidence__git_bundle_ref", "recovery/repository.bundle", "git_bundle", True),
        ("evidence__live_status_ref", "attempt/live-session.json", "agent_live_status", False),
        ("evidence__live_events_ref", "attempt/live-events.jsonl", "agent_live_events", False),
    )
    candidates = [
        ArtifactCandidate(source_ref=row[key], logical_path=path, kind=kind)
        for key, path, kind, required in refs
        if required or row.get(key)
    ]
    if row.get("evidence__context_ref"):
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


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run an Archetype coding-agent mission")
    parser.add_argument("--codex-login", action="store_true")
    parser.add_argument("--modal-login", action="store_true")
    parser.add_argument("--monitor-sandbox", metavar="SB_ID")
    parser.add_argument("--monitor-disconnect-grace-seconds", type=float, default=180.0)
    args = parser.parse_args()

    issue_url = os.environ.get("CODING_AGENT_ISSUE_URL", DEFAULT_ISSUE_URL)
    issue_slug = issue_url.rstrip("/").rsplit("/", 1)[-1]
    spec = SandboxSpec(
        backend=os.environ.get("CODING_AGENT_BACKEND", "local"),
        harness=os.environ.get("CODING_AGENT_HARNESS", "codex"),
        model=os.environ.get("CODING_AGENT_MODEL", ""),
        workspace=os.environ.get("CODING_AGENT_WORKSPACE", "/workspace/repo"),
        repo_url=os.environ.get(
            "CODING_AGENT_REPO_URL", "https://github.com/VangelisTech/archetype.git"
        ),
        base_ref=os.environ.get("CODING_AGENT_BASE_REF", "main"),
        branch=os.environ.get("CODING_AGENT_BRANCH", f"agent/issue-{issue_slug}"),
        codex_secret_name=os.environ.get("CODEX_MODAL_SECRET", "archetype-codex"),
        claude_secret_name=os.environ.get("CLAUDE_MODAL_SECRET", "archetype-claude-code"),
        opencode_secret_name=os.environ.get(
            "CODING_AGENT_OPENCODE_SECRET", "archetype-modal-endpoint"
        ),
        opencode_base_url=os.environ.get("CODING_AGENT_OPENCODE_BASE_URL", ""),
        opencode_provider_id=os.environ.get("CODING_AGENT_OPENCODE_PROVIDER_ID", "archetype-modal"),
        opencode_wire_api=os.environ.get("CODING_AGENT_OPENCODE_WIRE_API", "chat-completions"),
        modal_auth_mode=os.environ.get("CODING_AGENT_MODAL_AUTH_MODE", "api-key"),
        modal_codex_auth_volume=os.environ.get("CODEX_MODAL_AUTH_VOLUME", "archetype-codex-auth"),
        modal_claude_auth_volume=os.environ.get(
            "CLAUDE_MODAL_AUTH_VOLUME", "archetype-claude-code-auth"
        ),
        stream_agent_output=os.environ.get("CODING_AGENT_STREAM_AGENT_OUTPUT", "1").lower()
        in {"1", "true", "yes"},
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

    if args.monitor_sandbox:
        await ModalSandboxClient.monitor(
            args.monitor_sandbox,
            workspace=spec.workspace,
            disconnect_grace_seconds=args.monitor_disconnect_grace_seconds,
        )
        return
    if args.codex_login:
        if spec.backend != "local" or spec.harness != "codex":
            parser.error("--codex-login requires the local backend and codex harness")
        await AppleContainerSandboxClient.login_codex(_local_spec(spec))
        print("Codex OAuth login saved; rerun without --codex-login.")
        return
    if args.modal_login:
        if spec.backend != "modal" or spec.harness == "opencode":
            parser.error("--modal-login requires Modal with Codex or Claude Code")
        if spec.modal_auth_mode != "oauth":
            parser.error("--modal-login requires CODING_AGENT_MODAL_AUTH_MODE=oauth")
        await ModalSandboxClient.login_oauth(_modal_spec(spec))
        print(f"{spec.harness} OAuth login saved; rerun without --modal-login.")
        return

    plan = _mission_plan(issue_url)
    provider, provider_spec = _provider_spec(spec)
    sandboxes = SandboxService([AppleContainerSandboxBackend(), ModalSandboxBackend()])
    coding_agents = CodingAgentService(MissionService(), sandboxes)
    mission_id = f"archetype-issue-{issue_slug}"
    sandbox_id = await coding_agents.start_episode(mission_id, provider, provider_spec)
    sandbox = sandboxes.session(sandbox_id)
    if sandbox is None:
        raise RuntimeError("sandbox service did not retain the created session")
    print(f"sandbox_session={sandbox_id}", flush=True)
    if spec.backend == "modal":
        print(
            "monitor_command=uv run --extra coding-agent python "
            f"examples/11_coding_agent_mission.py --monitor-sandbox {sandbox_id}",
            flush=True,
        )

    tick_log: list[str] = []

    async def on_pre_tick(event: PreTick) -> None:
        tick_log.append(f"pre_tick t={event.tick} sandbox={sandbox_id}")

    async def on_post_tick(event: PostTick) -> None:
        tick_log.append(f"post_tick completed={event.tick - 1} archetypes={len(event.results)}")

    async def on_destroy(event: OnDestroy) -> None:
        await coding_agents.close_episode(mission_id)
        tick_log.append(f"destroy world={event.world_id} sandbox_closed=True")

    artifact_resolver = (
        ModalArtifactSourceResolver(
            spec=_modal_spec(spec), sandbox=cast(ModalSandboxClient, sandbox)
        )
        if spec.backend == "modal"
        else None
    )
    storage = StorageConfig(uri="./archetype_data", namespace=f"coding_agent_issue_{issue_slug}")
    artifact_store = ArtifactStoreConfig.local(
        os.environ.get("CODING_AGENT_ARTIFACT_DIR", ".context/coding-agent-artifacts")
    )
    try:
        async with ArchetypeRuntime(
            artifact_store=artifact_store,
            artifact_source_resolver=artifact_resolver,
        ) as runtime:
            world = runtime.world(
                "coding-mission",
                storage=storage,
                processors=[CodingAgentProcessor()],
                resources=[spec, coding_agents],
                hooks=[
                    (PreTick, on_pre_tick),
                    (PostTick, on_post_tick),
                    (OnDestroy, on_destroy),
                ],
            )
            first = plan[0]
            await world.spawn(
                Mission(
                    name=mission_id,
                    repo=spec.repo_url,
                    branch=spec.branch,
                    plan_json=json.dumps(plan),
                ),
                TaskGate(
                    step_name=first["name"],
                    prompt=first["prompt"],
                    validators_json=json.dumps(first["validators"]),
                ),
                Attempt(),
                Checkpoint(),
                Finalization(),
                Commit(),
                Evidence(),
                FrictionLog(),
                CodingAgentEpisode(
                    mission_id=mission_id,
                    provider=provider,
                    sandbox_id=sandbox_id,
                    harness=spec.harness,
                ),
            )
            result = await world.run_episode(
                EpisodeConfig(
                    max_steps=sum(5 for _ in plan) + 1,
                    terminal_component=Mission,
                    terminal_field="finished",
                )
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
                CodingAgentEpisode,
            )
            attempt_rows = (
                history.where(col("attempt__attempt_id") != "").sort("tick").collect().to_pylist()
            )
            if not attempt_rows:
                raise RuntimeError("mission completed without a persisted attempt")

            publications = []
            seen: set[str] = set()
            for attempt_row in attempt_rows:
                attempt_id = str(attempt_row["attempt__attempt_id"])
                if attempt_id in seen or not attempt_row["checkpoint__restorable"]:
                    continue
                seen.add(attempt_id)
                publications.append(
                    await world.publish_artifact_bundle(_artifact_bundle_from_row(attempt_row))
                )
            indexed = (await world.artifact_bundles()).collect().to_pylist()
            row = attempt_rows[-1]
            print(
                f"world={result.world_id} steps={result.duration_steps} "
                f"terminated={result.terminated} ticks={result.start_tick}->{result.final_tick}"
            )
            print(
                f"finished={row['mission__finished']} succeeded={row['mission__succeeded']} "
                f"failure={row['mission__failure_reason']}"
            )
            print(
                f"attempt={row['attempt__attempt_index']} status={row['attempt__status']} "
                f"commit={row['commit__sha']} checkpoint={row['checkpoint__state_ref']}"
            )
            print(f"published_attempts={len(publications)} indexed_artifacts={len(indexed)}")
            print(f"trace={row['evidence__trace_ref']}")
            print(f"live_events={row['evidence__live_events_ref']}")
            print(f"filesystem_diff={row['evidence__filesystem_diff_ref']}")
            print(f"git_bundle={row['evidence__git_bundle_ref']}")
            print(f"context={row['evidence__context_ref']}")
            await world.destroy()
    finally:
        await coding_agents.close_episode(mission_id)
        await sandboxes.shutdown()

    for line in tick_log:
        print(line)


if __name__ == "__main__":
    asyncio.run(main())
