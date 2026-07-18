# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Run one coding-agent attempt in a persistent Modal Sandbox.

The sandbox is the mutable filesystem for one coding-agent episode. A call to
``run_attempt`` performs exactly one agent submission, runs authoritative
validators, records a filesystem diff, and checkpoints the sandbox whether the
attempt is accepted or rejected. Mission policy -- including retries and task
advancement -- belongs to the Archetype processor that calls this transport.

Modal and the coding-agent CLIs are optional runtime dependencies. Importing
the sandbox family does not import Modal; :meth:`ModalSandboxClient.create`
loads it only when a real sandbox is requested.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, Literal, Protocol
from urllib.parse import urlsplit

AgentHarness = Literal["codex", "claude-code", "opencode"]
AgentAuthMode = Literal["api-key", "oauth"]
OpenCodeWireAPI = Literal["chat-completions", "responses"]

_OAUTH_MOUNT = "/auth"
_CODEX_AUTH_VOLUME_PATH = f"{_OAUTH_MOUNT}/auth.json"
_CODEX_MISSION_AUTH_PATH = "/root/.codex/auth.json"
_CLAUDE_AUTH_VOLUME_PATH = f"{_OAUTH_MOUNT}/.credentials.json"
_CLAUDE_MISSION_AUTH_PATH = "/root/.claude/.credentials.json"
_OPENCODE_CONFIG_PATH = "/root/.config/archetype/opencode.json"

_AGENT_STREAM_SCRIPT = r"""
set -o pipefail
trace_path=$1
stderr_path=$2
shift 2
mkdir -p "$(dirname "$trace_path")"
: > "$trace_path"
: > "$stderr_path"
"$@" > >(tee -a "$trace_path") 2> >(tee -a "$stderr_path" >&2)
"""

_FILESYSTEM_MANIFEST_SCRIPT = r"""
import hashlib
import json
import os
import stat
import sys

root, output, artifact_dir = map(os.path.abspath, sys.argv[1:4])
excluded = {"/dev", "/proc", "/run", "/sys", artifact_dir}


def is_excluded(path):
    return any(path == prefix or path.startswith(prefix + os.sep) for prefix in excluded)


def kind(mode):
    if stat.S_ISDIR(mode):
        return "directory"
    if stat.S_ISREG(mode):
        return "file"
    if stat.S_ISLNK(mode):
        return "symlink"
    if stat.S_ISSOCK(mode):
        return "socket"
    if stat.S_ISFIFO(mode):
        return "fifo"
    if stat.S_ISCHR(mode):
        return "character_device"
    if stat.S_ISBLK(mode):
        return "block_device"
    return "other"


records = []
stack = [root]
while stack:
    path = stack.pop()
    if is_excluded(path):
        continue
    try:
        info = os.lstat(path)
    except OSError as exc:
        records.append({"path": path, "error": f"{type(exc).__name__}: {exc}"})
        continue

    record = {
        "path": path,
        "type": kind(info.st_mode),
        "mode": stat.S_IMODE(info.st_mode),
        "uid": info.st_uid,
        "gid": info.st_gid,
        "size": info.st_size,
        "mtime_ns": info.st_mtime_ns,
    }
    if stat.S_ISLNK(info.st_mode):
        try:
            record["symlink_target"] = os.readlink(path)
        except OSError as exc:
            record["error"] = f"{type(exc).__name__}: {exc}"
    elif stat.S_ISREG(info.st_mode):
        digest = hashlib.sha256()
        try:
            with open(path, "rb") as file:
                for chunk in iter(lambda: file.read(1024 * 1024), b""):
                    digest.update(chunk)
            record["sha256"] = digest.hexdigest()
        except OSError as exc:
            record["error"] = f"{type(exc).__name__}: {exc}"
    records.append(record)

    if stat.S_ISDIR(info.st_mode):
        try:
            children = sorted((entry.path for entry in os.scandir(path)), reverse=True)
        except OSError as exc:
            record["error"] = f"{type(exc).__name__}: {exc}"
        else:
            stack.extend(children)

records.sort(key=lambda item: item["path"])
os.makedirs(os.path.dirname(output), exist_ok=True)
with open(output, "w", encoding="utf-8") as file:
    for record in records:
        file.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
"""

_FILESYSTEM_DIFF_SCRIPT = r"""
import json
import os
import sys

before_path, after_path, output = sys.argv[1:4]


def load(path):
    with open(path, encoding="utf-8") as file:
        return {record["path"]: record for line in file if (record := json.loads(line))}


before = load(before_path)
after = load(after_path)
os.makedirs(os.path.dirname(output), exist_ok=True)
with open(output, "w", encoding="utf-8") as file:
    for path in sorted(before.keys() | after.keys()):
        old = before.get(path)
        new = after.get(path)
        if old == new:
            continue
        change = "created" if old is None else "deleted" if new is None else "modified"
        file.write(
            json.dumps(
                {"path": path, "change": change, "before": old, "after": new},
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        )
"""


def _default_agent_image(modal: Any, harness: AgentHarness) -> Any:
    """Build the cached default Modal image for one coding-agent harness."""

    image = modal.Image.debian_slim(python_version="3.12").apt_install(
        "ca-certificates", "curl", "git", "openssh-client"
    )
    image = image.run_commands(
        "curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin sh"
    )
    if harness == "codex":
        return image.run_commands(
            "curl -fsSL https://chatgpt.com/codex/install.sh "
            "| env CODEX_NON_INTERACTIVE=1 CODEX_INSTALL_DIR=/usr/local/bin sh"
        )
    image = image.apt_install("nodejs", "npm")
    if harness == "claude-code":
        return image.run_commands("npm install --global @anthropic-ai/claude-code")
    return image.run_commands("npm install --global opencode-ai@1.18.3")


@dataclass(frozen=True)
class ValidatorSpec:
    """One authoritative command that must return the expected exit code."""

    name: str
    command: tuple[str, ...]
    expected_returncode: int = 0
    timeout_seconds: int = 900

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ValidatorSpec:
        return cls(
            name=str(value["name"]),
            command=tuple(str(part) for part in value["command"]),
            expected_returncode=int(value.get("expected_returncode", 0)),
            timeout_seconds=int(value.get("timeout_seconds", 900)),
        )

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["command"] = list(self.command)
        return value


@dataclass(frozen=True)
class ModalSandboxSpec:
    """Picklable configuration for one sandbox-backed coding mission.

    API-key mode injects the selected harness Secret only into the agent
    process. For OpenCode, that Secret contains the Modal endpoint token and a
    generated config stores only environment placeholders. OAuth mode mounts a
    named Volume only into a separate broker Sandbox, stages the credential
    file for the CLI process, persists refreshes atomically, and removes the
    staged file before validators and snapshots.
    Codex's shell environment policy also excludes key/secret/token variables
    from model-generated commands. Use trusted repository content: an agent
    process can still inspect its own process and filesystem while authenticated.

    A named image is recommended for repeated runs.  If ``image_name`` is
    empty, Modal builds a cached Debian image containing git, uv, and the
    selected coding-agent CLI.
    """

    repo_url: str
    branch: str
    base_ref: str = "main"
    app_name: str = "archetype-coding-agents"
    image_name: str = ""
    harness: AgentHarness = "codex"
    auth_mode: AgentAuthMode = "api-key"
    codex_secret_name: str = "archetype-codex"
    claude_secret_name: str = "archetype-claude-code"
    opencode_secret_name: str = "archetype-modal-endpoint"
    codex_auth_volume_name: str = "archetype-codex-auth"
    claude_auth_volume_name: str = "archetype-claude-code-auth"
    github_secret_name: str = ""
    model: str = ""
    opencode_base_url: str = ""
    opencode_provider_id: str = "archetype-modal"
    opencode_wire_api: OpenCodeWireAPI = "chat-completions"
    workspace: str = "/workspace/repo"
    timeout_seconds: int = 4 * 60 * 60
    idle_timeout_seconds: int = 20 * 60
    agent_timeout_seconds: int = 45 * 60
    snapshot_timeout_seconds: int = 120
    snapshot_ttl_seconds: int | None = 30 * 24 * 60 * 60
    snapshot_after_attempt: bool = True
    capture_filesystem_manifests: bool = True
    stream_agent_output: bool = True
    heartbeat_seconds: int = 15
    push: bool = False
    git_author_name: str = "Archetype Coding Agent"
    git_author_email: str = "coding-agent@archetype.local"

    def __post_init__(self) -> None:
        workspace = PurePosixPath(self.workspace)
        if not workspace.is_absolute() or str(workspace) in {"/", "."}:
            raise ValueError("workspace must be a non-root absolute path")
        if not self.repo_url or self.repo_url.startswith("-"):
            raise ValueError("repo_url must be a non-empty git URL")
        if not self.branch or self.branch.startswith("-"):
            raise ValueError("branch must be a non-empty git branch name")
        if not self.base_ref or self.base_ref.startswith("-"):
            raise ValueError("base_ref must be a non-empty git ref")
        if self.push and not self.github_secret_name:
            raise ValueError("push=True requires github_secret_name")
        if self.harness not in {"codex", "claude-code", "opencode"}:
            raise ValueError(f"unsupported coding-agent harness: {self.harness!r}")
        if self.auth_mode not in {"api-key", "oauth"}:
            raise ValueError(f"unsupported coding-agent auth mode: {self.auth_mode!r}")
        if self.harness == "opencode":
            if self.auth_mode != "api-key":
                raise ValueError("OpenCode endpoint auth requires auth_mode='api-key'")
            if not self.model:
                raise ValueError("OpenCode requires an explicit model")
            endpoint = urlsplit(self.opencode_base_url)
            if (
                endpoint.scheme not in {"http", "https"}
                or not endpoint.netloc
                or endpoint.username is not None
                or endpoint.password is not None
                or endpoint.query
                or endpoint.fragment
            ):
                raise ValueError(
                    "opencode_base_url must be an http(s) URL without credentials, query, or "
                    "fragment"
                )
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", self.opencode_provider_id):
                raise ValueError(f"invalid OpenCode provider id: {self.opencode_provider_id!r}")
            if self.opencode_wire_api not in {"chat-completions", "responses"}:
                raise ValueError(f"unsupported OpenCode wire API: {self.opencode_wire_api!r}")
        if self.heartbeat_seconds < 1:
            raise ValueError("heartbeat_seconds must be at least 1")
        for volume_name in (self.codex_auth_volume_name, self.claude_auth_volume_name):
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", volume_name):
                raise ValueError(f"invalid Modal auth volume name: {volume_name!r}")

    @property
    def auth_volume_name(self) -> str:
        """Named Modal Volume holding only the selected harness credential."""

        if self.harness == "codex":
            return self.codex_auth_volume_name
        if self.harness == "claude-code":
            return self.claude_auth_volume_name
        raise ValueError("OpenCode endpoint auth does not use an OAuth volume")

    @property
    def auth_volume_path(self) -> str:
        if self.harness == "codex":
            return _CODEX_AUTH_VOLUME_PATH
        if self.harness == "claude-code":
            return _CLAUDE_AUTH_VOLUME_PATH
        raise ValueError("OpenCode endpoint auth does not use an OAuth credential path")

    @property
    def mission_auth_path(self) -> str:
        if self.harness == "codex":
            return _CODEX_MISSION_AUTH_PATH
        if self.harness == "claude-code":
            return _CLAUDE_MISSION_AUTH_PATH
        raise ValueError("OpenCode endpoint auth does not stage an OAuth credential")


class CodingAgentSandboxSpec(Protocol):
    """Structural fields shared by provider-specific sandbox specifications."""

    repo_url: str
    branch: str
    base_ref: str
    harness: AgentHarness
    model: str
    workspace: str
    agent_timeout_seconds: int
    snapshot_timeout_seconds: int
    snapshot_ttl_seconds: int | None
    snapshot_after_attempt: bool
    capture_filesystem_manifests: bool
    push: bool
    git_author_name: str
    git_author_email: str


@dataclass(frozen=True)
class CommandResult:
    """Captured result from one command inside the sandbox."""

    argv: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str


@dataclass
class CodingAgentSandboxClient[SandboxSpecT: CodingAgentSandboxSpec]:
    """Provider-neutral attempt, validation, evidence, and checkpoint protocol."""

    spec: SandboxSpecT
    _sandbox: Any
    _agent_secret: Any
    _github_secret: Any | None = None
    _closed: bool = False
    _completed: dict[str, dict[str, Any]] = field(default_factory=dict)
    _latest_checkpoint_ref: str = ""
    _active_trace_path: str = ""
    _active_trace_stderr_path: str = ""
    _live_context: dict[str, Any] = field(default_factory=dict)

    @property
    def sandbox_id(self) -> str:
        return str(self._sandbox.object_id)

    async def run_attempt(
        self,
        *,
        prompt: str,
        validators: Sequence[ValidatorSpec | dict[str, Any]],
        step_name: str,
        attempt_index: int,
        idempotency_key: str,
        previous_session_id: str = "",
        previous_validator_details: Sequence[dict[str, Any]] = (),
        correlation: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Run exactly one agent submission, validate it, and checkpoint its state.

        The agent is not trusted to declare success or failure. Validator
        commands run in separate processes without the agent secret, even when
        the agent CLI exits nonzero after producing a valid worktree. Acceptance
        is data returned to the caller; a rejected attempt is not an exception
        and remains resumable through its checkpoint.
        """

        if self._closed:
            raise RuntimeError("sandbox already closed")
        if attempt_index < 1:
            raise ValueError("attempt_index must be at least 1")
        if not idempotency_key:
            raise ValueError("idempotency_key must not be empty")
        correlation_data = dict(correlation or {})
        try:
            json.dumps(correlation_data, sort_keys=True)
        except (TypeError, ValueError) as exc:
            raise ValueError("correlation values must be JSON serializable") from exc

        cached = await self._load_completed(idempotency_key)
        if cached is not None:
            return cached

        normalized = [
            value if isinstance(value, ValidatorSpec) else ValidatorSpec.from_dict(value)
            for value in validators
        ]
        if not normalized:
            raise ValueError("at least one validator is required")
        for validator in normalized:
            if not validator.name or not validator.command:
                raise ValueError("validators require a name and non-empty command")

        attempt_id = hashlib.sha256(idempotency_key.encode()).hexdigest()
        baseline = (await self._git("rev-parse", "HEAD")).stdout.strip()
        trace_dir = f"{self.spec.workspace}/.archetype-agent/traces"
        live_status_path, live_events_path = self._live_artifact_paths()
        directories = [trace_dir]
        if live_status_path:
            directories.append(str(PurePosixPath(live_status_path).parent))
        await self._checked("mkdir", "-p", *directories)
        start_manifest_path = await self._ensure_start_manifest()
        trace_path = (
            f"{trace_dir}/{self._safe_step(step_name)}-{attempt_index}-{attempt_id[:12]}.jsonl"
        )
        self._active_trace_path = trace_path
        self._active_trace_stderr_path = f"{trace_path}.stderr"
        self._live_context = {
            "attempt_id": attempt_id,
            "attempt_index": attempt_index,
            "step_name": step_name,
            "trace_path": trace_path,
            "trace_stderr_path": self._active_trace_stderr_path,
            "correlation": correlation_data,
        }
        await self._emit_live_event("attempt_started", baseline_sha=baseline)

        agent_prompt = (
            self._repair_prompt(prompt, previous_validator_details)
            if previous_validator_details
            else self._initial_prompt(prompt, step_name)
        )
        await self._emit_live_event(
            "agent_started",
            harness=self.spec.harness,
            resumed=bool(previous_session_id),
        )
        try:
            agent = await self._run_agent(agent_prompt, session_id=previous_session_id)
        except BaseException as exc:
            await self._emit_live_event_safely(
                "agent_transport_failed",
                error_type=type(exc).__name__,
                error=self._tail(str(exc), 1000),
            )
            raise
        await self._emit_live_event("agent_finished", returncode=agent.returncode)
        await self._write_text(trace_path, agent.stdout)
        if agent.stderr:
            await self._write_text(f"{trace_path}.stderr", agent.stderr)

        session_id = self._session_id(agent.stdout) or previous_session_id
        agent_execution = {
            "returncode": agent.returncode,
            "completed": agent.returncode == 0,
            "stdout_tail": self._tail(agent.stdout) if agent.returncode else "",
            "stderr_tail": self._tail(agent.stderr) if agent.returncode else "",
        }
        details = await self._run_validators(normalized)

        accepted = all(detail["passed"] for detail in details)
        sha = ""
        message = f"{step_name}: {self._subject(prompt)}"
        pushed = False
        if accepted:
            await self._emit_live_event("commit_started")
            try:
                sha = await self._commit_verified_tree(step_name, prompt, baseline)
            except ValueError as exc:
                details.append(
                    {
                        "name": "git_tree_change",
                        "passed": False,
                        "returncode": 1,
                        "stdout": "",
                        "stderr": str(exc),
                    }
                )
                accepted = False
            else:
                pushed = await self._push_if_configured()
                await self._emit_live_event("commit_finished", sha=sha, pushed=pushed)

        failed = [detail for detail in details if not detail["passed"]]
        friction = []
        if agent.returncode != 0:
            friction.append(
                {
                    "step": step_name,
                    "attempt": attempt_index,
                    "finding": (
                        f"{self.spec.harness} exited with code {agent.returncode}; "
                        "authoritative validators still ran"
                    ),
                    "learning": self._tail(agent.stderr or agent.stdout, 1200),
                }
            )
        if failed:
            friction.append(
                {
                    "step": step_name,
                    "attempt": attempt_index,
                    "finding": "Gate failed: " + ", ".join(str(item["name"]) for item in failed),
                    "learning": self._failure_summary(failed),
                }
            )

        await self._emit_live_event("evidence_capture_started")
        git_recovery = await self._capture_git_recovery(attempt_id, baseline)
        context_path = f"{self.spec.workspace}/.context"
        context_exists = await self._exec("test", "-d", context_path, timeout=30)
        if context_exists.returncode != 0:
            context_path = ""
        end_manifest_path, filesystem_diff_path = await self._capture_attempt_filesystem(
            step_name, attempt_index
        )
        attempt_manifest_path = (
            f"{self.spec.workspace}/.archetype-agent/manifests/{attempt_id}.json"
        )
        attempt_manifest = {
            "schema_version": 1,
            "attempt_id": attempt_id,
            "idempotency_key": idempotency_key,
            "attempt_index": attempt_index,
            "step_name": step_name,
            "correlation": correlation_data,
            "status": "accepted" if accepted else "rejected",
            "sandbox_id": self.sandbox_id,
            "harness": self.spec.harness,
            "agent_session_id": session_id,
            "agent_execution": agent_execution,
            "baseline_sha": baseline,
            "commit_sha": sha,
            "pushed": pushed,
            "validator_details": details,
            "artifacts": {
                "trace": trace_path,
                "trace_stderr": f"{trace_path}.stderr" if agent.stderr else "",
                "live_status": live_status_path,
                "live_events": live_events_path,
                "filesystem_start": start_manifest_path,
                "filesystem_end": end_manifest_path,
                "filesystem_diff": filesystem_diff_path,
                "git_status": git_recovery["status"],
                "git_patch": git_recovery["patch"],
                "git_bundle": git_recovery["bundle"],
                "context_directory": context_path,
            },
        }
        await self._write_text(attempt_manifest_path, json.dumps(attempt_manifest, sort_keys=True))
        await self._emit_live_event(
            "evidence_capture_finished",
            attempt_manifest_path=attempt_manifest_path,
            filesystem_diff_path=filesystem_diff_path,
            git_bundle_path=git_recovery["bundle"],
        )

        checkpoint_created_at_ms = int(time.time() * 1000)
        checkpoint_error = ""
        await self._emit_live_event("checkpoint_started")
        try:
            snapshot_ref = await self._snapshot_if_configured(attempt_id)
        except Exception as exc:
            snapshot_ref = ""
            checkpoint_error = f"{type(exc).__name__}: {self._tail(str(exc), 2000)}"
            friction.append(
                {
                    "step": step_name,
                    "attempt": attempt_index,
                    "finding": "Provider checkpoint failed",
                    "learning": checkpoint_error,
                }
            )
        checkpoint_expires_at_ms = 0
        if snapshot_ref and self.spec.snapshot_ttl_seconds is not None:
            checkpoint_expires_at_ms = (
                checkpoint_created_at_ms + self.spec.snapshot_ttl_seconds * 1000
            )
        checkpoint_ready = bool(snapshot_ref)
        checkpoint_status = (
            "ready" if checkpoint_ready else "failed" if checkpoint_error else "disabled"
        )
        await self._emit_live_event(
            "checkpoint_finished",
            checkpoint_status=checkpoint_status,
            checkpoint_ref=snapshot_ref,
            error=checkpoint_error,
        )
        outcome = {
            "attempt_id": attempt_id,
            "idempotency_key": idempotency_key,
            "attempt_index": attempt_index,
            "attempts": attempt_index,
            "correlation": correlation_data,
            "status": "accepted" if accepted else "rejected",
            "accepted": accepted,
            "sha": sha,
            "baseline_sha": baseline,
            "message": message,
            "pushed": pushed,
            "results": {detail["name"]: detail["passed"] for detail in details},
            "validator_details": details,
            "trace_ref": self._artifact_ref(snapshot_ref, trace_path),
            "traces_ref": self._artifact_ref(snapshot_ref, trace_dir),
            # These point at the live sandbox, rather than the just-created
            # checkpoint, so the terminal checkpoint/attempt events remain
            # available to artifact ingestion before teardown.
            "live_status_ref": self._sandbox_uri(live_status_path) if live_status_path else "",
            "live_events_ref": self._sandbox_uri(live_events_path) if live_events_path else "",
            "sandbox_state_ref": snapshot_ref,
            "checkpoint_status": checkpoint_status,
            "checkpoint_provider": self._checkpoint_provider(),
            "checkpoint_restorable": checkpoint_ready,
            "checkpoint_error": checkpoint_error,
            "checkpoint_created_at_ms": checkpoint_created_at_ms,
            "checkpoint_expires_at_ms": checkpoint_expires_at_ms,
            "finalization_phase": "checkpointed" if checkpoint_ready else "captured",
            "finalization_error": checkpoint_error,
            "finalization_manifest_ref": self._artifact_ref(snapshot_ref, attempt_manifest_path),
            "filesystem_start_ref": self._artifact_ref(snapshot_ref, start_manifest_path),
            "filesystem_end_ref": self._artifact_ref(snapshot_ref, end_manifest_path),
            "filesystem_diff_ref": self._artifact_ref(snapshot_ref, filesystem_diff_path),
            "git_status_ref": self._artifact_ref(snapshot_ref, git_recovery["status"]),
            "git_patch_ref": self._artifact_ref(snapshot_ref, git_recovery["patch"]),
            "git_bundle_ref": self._artifact_ref(snapshot_ref, git_recovery["bundle"]),
            "context_ref": self._artifact_ref(snapshot_ref, context_path),
            "sandbox_id": self.sandbox_id,
            "harness": self.spec.harness,
            "agent_session_id": session_id,
            "agent_returncode": agent.returncode,
            "agent_completed": agent.returncode == 0,
            "codex_thread_id": session_id if self.spec.harness == "codex" else "",
            "claude_session_id": session_id if self.spec.harness == "claude-code" else "",
            "opencode_session_id": session_id if self.spec.harness == "opencode" else "",
            "friction": friction,
            "pr_url": "",
        }
        await self._store_completed(idempotency_key, outcome)
        self._completed[idempotency_key] = outcome
        await self._emit_live_event(
            "attempt_completed",
            status=outcome["status"],
            accepted=accepted,
            checkpoint_status=outcome["checkpoint_status"],
            commit_sha=sha,
        )
        return outcome

    async def close(self) -> None:
        """Terminate the remote compute and release the client connection."""

        if self._closed:
            return
        self._closed = True
        try:
            await self._sandbox.terminate.aio(wait=True)
        finally:
            await self._sandbox.detach.aio()

    async def _prepare_repository(self) -> None:
        parent = str(PurePosixPath(self.spec.workspace).parent)
        await self._checked("mkdir", "-p", parent)
        clone = await self._exec(
            "git",
            *self._git_auth_args(),
            "clone",
            "--branch",
            self.spec.base_ref,
            "--single-branch",
            "--",
            self.spec.repo_url,
            self.spec.workspace,
            timeout=self.spec.agent_timeout_seconds,
            secrets=self._git_secrets(),
        )
        self._raise_for_result(clone, "git clone")
        await self._git("switch", "-C", self.spec.branch)
        await self._git("config", "user.name", self.spec.git_author_name)
        await self._git("config", "user.email", self.spec.git_author_email)
        # Traces and retry receipts belong in snapshots, not commits.
        await self._checked(
            "sh",
            "-c",
            'printf "\\n.archetype-agent/\\n" >> .git/info/exclude',
            workdir=self.spec.workspace,
        )

    async def _run_agent(self, prompt: str, *, session_id: str) -> CommandResult:
        if self.spec.harness == "codex":
            return await self._run_codex(prompt, session_id=session_id)
        if self.spec.harness == "claude-code":
            return await self._run_claude(prompt, session_id=session_id)
        raise ValueError(f"unsupported coding-agent harness: {self.spec.harness!r}")

    async def _run_codex(self, prompt: str, *, session_id: str) -> CommandResult:
        common = [
            "--json",
            "--dangerously-bypass-approvals-and-sandbox",
            "--ignore-user-config",
            "-c",
            'shell_environment_policy.inherit="core"',
            "-c",
            'shell_environment_policy.exclude=["*KEY*","*SECRET*","*TOKEN*"]',
            "-c",
            'cli_auth_credentials_store="file"',
        ]
        if self.spec.model:
            common.extend(["--model", self.spec.model])
        if session_id:
            argv = ["codex", "exec", "resume", *common, session_id, prompt]
        else:
            argv = ["codex", "exec", *common, prompt]
        return await self._exec_agent(
            *argv,
            workdir=self.spec.workspace,
            timeout=self.spec.agent_timeout_seconds,
            secrets=[self._agent_secret] if self._agent_secret is not None else (),
            env={"NO_COLOR": "1", "CODEX_HOME": "/root/.codex"},
        )

    async def _run_claude(self, prompt: str, *, session_id: str) -> CommandResult:
        argv = [
            "claude",
            "--print",
            "--output-format",
            "stream-json",
            "--verbose",
            "--dangerously-skip-permissions",
            "--max-turns",
            "50",
        ]
        if self.spec.model:
            argv.extend(["--model", self.spec.model])
        if session_id:
            argv.extend(["--resume", session_id])
        argv.append(prompt)
        return await self._exec_agent(
            *argv,
            workdir=self.spec.workspace,
            timeout=self.spec.agent_timeout_seconds,
            secrets=[self._agent_secret] if self._agent_secret is not None else (),
            env={
                "NO_COLOR": "1",
                "DISABLE_AUTOUPDATER": "1",
                "CLAUDE_CONFIG_DIR": "/root/.claude",
            },
        )

    async def _run_validators(self, validators: Sequence[ValidatorSpec]) -> list[dict[str, Any]]:
        details: list[dict[str, Any]] = []
        for validator in validators:
            await self._emit_live_event("validator_started", validator=validator.name)
            result = await self._exec(
                *validator.command,
                workdir=self.spec.workspace,
                timeout=validator.timeout_seconds,
            )
            details.append(
                {
                    "name": validator.name,
                    "command": list(validator.command),
                    "expected_returncode": validator.expected_returncode,
                    "returncode": result.returncode,
                    "passed": result.returncode == validator.expected_returncode,
                    "stdout": self._tail(result.stdout),
                    "stderr": self._tail(result.stderr),
                }
            )
            await self._emit_live_event(
                "validator_finished",
                validator=validator.name,
                returncode=result.returncode,
                passed=result.returncode == validator.expected_returncode,
            )
        return details

    async def _exec_agent(
        self,
        *args: str,
        workdir: str | None = None,
        timeout: int | None = None,
        secrets: Sequence[Any] = (),
        env: dict[str, str] | None = None,
    ) -> CommandResult:
        """Provider hook for a live-streamed agent command."""

        return await self._exec(
            *args,
            workdir=workdir,
            timeout=timeout,
            secrets=secrets,
            env=env,
        )

    async def _emit_live_event(self, event_type: str, **details: Any) -> None:
        """Provider hook for live attempt status; unsupported providers are no-ops."""

        del event_type, details

    def _live_artifact_paths(self) -> tuple[str, str]:
        """Return provider-observable status and event paths when supported."""

        return "", ""

    async def _emit_live_event_safely(self, event_type: str, **details: Any) -> None:
        try:
            await self._emit_live_event(event_type, **details)
        except BaseException:
            # Preserve the primary transport/finalization failure. Live status
            # is valuable evidence, but it is not the system of record.
            return

    async def _commit_verified_tree(self, step_name: str, prompt: str, baseline: str) -> str:
        status = (await self._git("status", "--porcelain")).stdout
        if status.strip():
            await self._git("add", "-A")
            await self._git("commit", "-m", f"{step_name}: {self._subject(prompt)}")
        sha = (await self._git("rev-parse", "HEAD")).stdout.strip()
        if sha == baseline:
            raise ValueError(
                f"gate {step_name!r} passed validators but produced no git commit or tree change"
            )
        return sha

    async def _push_if_configured(self) -> bool:
        if not self.spec.push:
            return False
        result = await self._exec(
            "git",
            *self._git_auth_args(),
            "push",
            "--set-upstream",
            "origin",
            f"HEAD:refs/heads/{self.spec.branch}",
            workdir=self.spec.workspace,
            timeout=self.spec.agent_timeout_seconds,
            secrets=self._git_secrets(),
        )
        self._raise_for_result(result, "git push")
        return True

    async def _snapshot_if_configured(self, checkpoint_key: str = "") -> str:
        del checkpoint_key
        if not self.spec.snapshot_after_attempt:
            return ""
        image = await self._sandbox.snapshot_filesystem.aio(
            timeout=self.spec.snapshot_timeout_seconds,
            ttl=self.spec.snapshot_ttl_seconds,
        )
        object_id = str(getattr(image, "object_id", ""))
        if not object_id:
            await image.hydrate.aio()
            object_id = str(image.object_id)
        self._latest_checkpoint_ref = f"modal-image://{object_id}"
        return self._latest_checkpoint_ref

    async def _capture_git_recovery(self, attempt_id: str, baseline: str) -> dict[str, str]:
        directory = f"{self.spec.workspace}/.archetype-agent/recovery"
        status_path = f"{directory}/{attempt_id}-status.txt"
        patch_path = f"{directory}/{attempt_id}.patch"
        bundle_path = f"{directory}/{attempt_id}.bundle"
        await self._checked("mkdir", "-p", directory)
        status = await self._git("status", "--porcelain=v2", "--branch")
        patch = await self._git("diff", "--binary", baseline, "--")
        await self._write_text(status_path, status.stdout)
        await self._write_text(patch_path, patch.stdout)
        await self._git("bundle", "create", bundle_path, "--all")
        return {"status": status_path, "patch": patch_path, "bundle": bundle_path}

    async def _ensure_start_manifest(self) -> str:
        if not self.spec.capture_filesystem_manifests:
            return ""
        path = f"{self.spec.workspace}/.archetype-agent/filesystem/start.jsonl"
        exists = await self._exec("test", "-f", path, timeout=30)
        if exists.returncode != 0:
            await self._capture_filesystem_manifest(path)
        return path

    async def _capture_attempt_filesystem(
        self, step_name: str, attempt_index: int
    ) -> tuple[str, str]:
        if not self.spec.capture_filesystem_manifests:
            return "", ""
        directory = f"{self.spec.workspace}/.archetype-agent/filesystem"
        stem = f"{self._safe_step(step_name)}-{attempt_index}"
        start = f"{directory}/start.jsonl"
        end = f"{directory}/{stem}-end.jsonl"
        diff = f"{directory}/{stem}-diff.jsonl"
        await self._capture_filesystem_manifest(end)
        result = await self._exec(
            "python3",
            "-c",
            _FILESYSTEM_DIFF_SCRIPT,
            start,
            end,
            diff,
            timeout=self.spec.snapshot_timeout_seconds,
        )
        self._raise_for_result(result, "filesystem manifest diff")
        return end, diff

    async def _capture_filesystem_manifest(self, output: str) -> None:
        artifact_dir = f"{self.spec.workspace}/.archetype-agent/filesystem"
        result = await self._exec(
            "python3",
            "-c",
            _FILESYSTEM_MANIFEST_SCRIPT,
            "/",
            output,
            artifact_dir,
            timeout=self.spec.snapshot_timeout_seconds,
        )
        self._raise_for_result(result, "filesystem manifest")

    def _checkpoint_provider(self) -> str:
        return "modal"

    def _sandbox_uri(self, path: str) -> str:
        return f"modal-sandbox://{self.sandbox_id}{path}"

    def _artifact_ref(self, snapshot_ref: str, path: str) -> str:
        if not path:
            return ""
        return f"{snapshot_ref}#{path}" if snapshot_ref else self._sandbox_uri(path)

    async def _load_completed(self, key: str) -> dict[str, Any] | None:
        if key in self._completed:
            return self._completed[key]
        path = self._receipt_path(key)
        result = await self._exec("cat", path, timeout=30)
        if result.returncode != 0:
            return None
        try:
            outcome = json.loads(result.stdout)
        except json.JSONDecodeError:
            return None
        self._completed[key] = outcome
        return outcome

    async def _store_completed(self, key: str, outcome: dict[str, Any]) -> None:
        await self._write_text(self._receipt_path(key), json.dumps(outcome, sort_keys=True))

    def _receipt_path(self, key: str) -> str:
        digest = hashlib.sha256(key.encode()).hexdigest()
        return f"{self.spec.workspace}/.archetype-agent/gates/{digest}.json"

    async def _write_text(self, path: str, value: str) -> None:
        await self._sandbox.filesystem.write_text.aio(value, path)

    async def _git(self, *args: str) -> CommandResult:
        result = await self._exec("git", *args, workdir=self.spec.workspace)
        self._raise_for_result(result, f"git {' '.join(args[:2])}")
        return result

    async def _checked(self, *args: str, **kwargs: Any) -> CommandResult:
        result = await self._exec(*args, **kwargs)
        self._raise_for_result(result, args[0])
        return result

    async def _exec(
        self,
        *args: str,
        workdir: str | None = None,
        timeout: int | None = None,
        secrets: Sequence[Any] = (),
        env: dict[str, str] | None = None,
    ) -> CommandResult:
        process = await self._sandbox.exec.aio(
            *args,
            workdir=workdir,
            timeout=timeout,
            secrets=list(secrets),
            env=env,
        )
        stdout_task = asyncio.create_task(process.stdout.read.aio())
        stderr_task = asyncio.create_task(process.stderr.read.aio())
        returncode, stdout, stderr = await asyncio.gather(
            process.wait.aio(), stdout_task, stderr_task
        )
        return CommandResult(tuple(args), int(returncode), str(stdout), str(stderr))

    def _git_auth_args(self) -> tuple[str, ...]:
        if not self._github_secret:
            return ()
        helper = '!f() { echo "username=x-access-token"; echo "password=$GITHUB_TOKEN"; }; f'
        return ("-c", f"credential.helper={helper}")

    def _git_secrets(self) -> list[Any]:
        return [self._github_secret] if self._github_secret else []

    @staticmethod
    def _raise_for_result(result: CommandResult, label: str) -> None:
        if result.returncode != 0:
            detail = ModalSandboxClient._tail(result.stderr or result.stdout)
            raise RuntimeError(f"{label} failed with exit code {result.returncode}: {detail}")

    def _session_id(self, jsonl: str) -> str:
        for line in jsonl.splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if self.spec.harness == "codex" and event.get("type") == "thread.started":
                return str(event.get("thread_id") or "")
            if self.spec.harness == "claude-code" and event.get("session_id"):
                return str(event["session_id"])
            if self.spec.harness == "opencode":
                session_id = event.get("sessionID") or event.get("session_id")
                if session_id:
                    return str(session_id)
        return ""

    @staticmethod
    def _initial_prompt(prompt: str, step_name: str) -> str:
        return (
            f"Complete task gate {step_name!r}:\n\n{prompt}\n\n"
            "Work directly in the current repository. Inspect its AGENTS.md and follow all "
            "repository instructions. Make the smallest complete change and run useful checks. "
            "Do not commit, push, or open a pull request; the outer gate owns git publication. "
            "When the worktree is ready for authoritative validators, finish your response."
        )

    @staticmethod
    def _repair_prompt(prompt: str, details: Sequence[dict[str, Any]]) -> str:
        failures = json.dumps(list(details), indent=2)
        return (
            f"The authoritative task gate failed after your previous turn. The original task is:\n"
            f"{prompt}\n\nValidator evidence:\n{failures}\n\n"
            "Diagnose the evidence, repair the worktree, rerun relevant checks, and finish only "
            "when it is ready. Do not commit, push, or open a pull request."
        )

    @staticmethod
    def _safe_step(value: str) -> str:
        cleaned = re.sub(r"[^a-zA-Z0-9_.-]+", "-", value).strip("-.")
        return cleaned[:80] or "step"

    @staticmethod
    def _subject(value: str) -> str:
        return " ".join(value.strip().split())[:72] or "complete task gate"

    @staticmethod
    def _tail(value: str, limit: int = 4000) -> str:
        return value[-limit:]

    @staticmethod
    def _failure_summary(details: Sequence[dict[str, Any]]) -> str:
        failed = [item for item in details if not item.get("passed")]
        if not failed:
            return "unknown failure"
        return "; ".join(
            f"{item.get('name', 'command')} exit={item.get('returncode')}: "
            f"{ModalSandboxClient._tail(str(item.get('stderr') or item.get('stdout') or ''), 600)}"
            for item in failed
        )


@dataclass
class ModalSandboxClient(CodingAgentSandboxClient[ModalSandboxSpec]):
    """A live Modal sandbox running the provider-neutral attempt protocol."""

    _auth_sandbox: Any | None = None
    _live_event_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    _live_sequence: int = 0
    _live_phase: str = "idle"
    _live_phase_started_at: float = field(default_factory=time.monotonic)
    _live_session_started_at: float = field(default_factory=time.monotonic)
    _agent_stream_bytes: dict[str, int] = field(default_factory=lambda: {"stdout": 0, "stderr": 0})
    _agent_last_output_at: float | None = None

    @staticmethod
    def live_artifact_paths(workspace: str) -> tuple[str, str]:
        """Stable files that an independent process can poll by sandbox ID."""

        live_dir = f"{workspace}/.archetype-agent/live"
        return f"{live_dir}/session.json", f"{live_dir}/events.jsonl"

    def _live_artifact_paths(self) -> tuple[str, str]:
        return self.live_artifact_paths(self.spec.workspace)

    async def run_attempt(
        self,
        *,
        prompt: str,
        validators: Sequence[ValidatorSpec | dict[str, Any]],
        step_name: str,
        attempt_index: int,
        idempotency_key: str,
        previous_session_id: str = "",
        previous_validator_details: Sequence[dict[str, Any]] = (),
        correlation: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Run an attempt with a heartbeat spanning every finalization phase."""

        self._live_session_started_at = time.monotonic()
        heartbeat = asyncio.create_task(self._heartbeat_session())
        try:
            return await super().run_attempt(
                prompt=prompt,
                validators=validators,
                step_name=step_name,
                attempt_index=attempt_index,
                idempotency_key=idempotency_key,
                previous_session_id=previous_session_id,
                previous_validator_details=previous_validator_details,
                correlation=correlation,
            )
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)

    @classmethod
    async def monitor(
        cls,
        sandbox_id: str,
        *,
        workspace: str = "/workspace/repo",
        follow: bool = True,
        poll_seconds: float = 1.0,
        disconnect_grace_seconds: float = 180.0,
    ) -> dict[str, Any]:
        """Attach to a running sandbox's durable live session files.

        Modal can temporarily reject filesystem reads while snapshotting.  A
        following monitor retains its offsets and retries through that bounded
        provider interruption; ``sandbox_closing`` is the clean terminal event.
        """

        if not sandbox_id.startswith("sb-"):
            raise ValueError("Modal sandbox IDs must start with 'sb-'")
        if poll_seconds <= 0:
            raise ValueError("poll_seconds must be positive")
        if disconnect_grace_seconds <= 0:
            raise ValueError("disconnect_grace_seconds must be positive")
        try:
            import modal
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "Modal support is optional; install it with `uv sync --extra coding-agent`"
            ) from exc

        sandbox = await modal.Sandbox.from_id.aio(sandbox_id)
        status_path, events_path = cls.live_artifact_paths(workspace)
        offsets: dict[str, int] = {}
        status: dict[str, Any] = {}
        disconnected_at: float | None = None
        last_disconnect_notice_at = 0.0

        async def read(path: str) -> str:
            try:
                return str(await sandbox.filesystem.read_text.aio(path))
            except Exception as exc:
                if cls._is_missing_sandbox_path(exc):
                    return ""
                raise

        while True:
            try:
                status_text, events_text = await asyncio.gather(
                    read(status_path), read(events_path)
                )
                if status_text:
                    try:
                        value = json.loads(status_text)
                    except json.JSONDecodeError:
                        value = {}
                    if isinstance(value, dict):
                        status = value

                cls._write_stream_delta(events_path, events_text, offsets, sys.stdout)
                trace_path = str(status.get("trace_path") or "")
                stderr_path = str(status.get("trace_stderr_path") or "")
                if trace_path:
                    cls._write_stream_delta(
                        trace_path,
                        await read(trace_path),
                        offsets,
                        sys.stdout,
                    )
                if stderr_path:
                    cls._write_stream_delta(
                        stderr_path,
                        await read(stderr_path),
                        offsets,
                        sys.stderr,
                    )
            except Exception as exc:
                if not follow:
                    raise
                now = time.monotonic()
                if disconnected_at is None:
                    disconnected_at = now
                disconnected_seconds = now - disconnected_at
                if disconnected_seconds >= disconnect_grace_seconds:
                    print(
                        json.dumps(
                            {
                                "type": "monitor_disconnected",
                                "sandbox_id": sandbox_id,
                                "disconnected_seconds": round(disconnected_seconds, 3),
                                "error": cls._tail(str(exc), 1000),
                            },
                            sort_keys=True,
                        ),
                        flush=True,
                    )
                    return status
                if last_disconnect_notice_at == 0.0 or now - last_disconnect_notice_at >= 15:
                    last_disconnect_notice_at = now
                    print(
                        json.dumps(
                            {
                                "type": "monitor_read_interrupted",
                                "sandbox_id": sandbox_id,
                                "disconnected_seconds": round(disconnected_seconds, 3),
                                "retrying": True,
                                "grace_seconds": disconnect_grace_seconds,
                                "error": cls._tail(str(exc), 1000),
                            },
                            sort_keys=True,
                        ),
                        flush=True,
                    )
                await asyncio.sleep(poll_seconds)
                continue

            if disconnected_at is not None:
                print(
                    json.dumps(
                        {
                            "type": "monitor_reconnected",
                            "sandbox_id": sandbox_id,
                            "disconnected_seconds": round(time.monotonic() - disconnected_at, 3),
                        },
                        sort_keys=True,
                    ),
                    flush=True,
                )
                disconnected_at = None
                last_disconnect_notice_at = 0.0

            if not follow:
                return status
            if status.get("type") == "sandbox_closing":
                return status
            await asyncio.sleep(poll_seconds)

    async def report_live_event(self, event_type: str, **details: Any) -> None:
        """Publish a driver-owned phase into the sandbox's live event stream."""

        if not event_type or event_type == "heartbeat":
            raise ValueError("event_type must name a non-heartbeat phase event")
        await self._emit_live_event_safely(event_type, **details)

    @staticmethod
    def _write_stream_delta(
        path: str,
        value: str,
        offsets: dict[str, int],
        target: Any,
    ) -> None:
        previous = offsets.get(path, 0)
        if previous > len(value):
            previous = 0
        delta = value[previous:]
        offsets[path] = len(value)
        if delta:
            target.write(delta)
            target.flush()

    @classmethod
    async def _modal_base(cls, spec: ModalSandboxSpec) -> tuple[Any, Any]:
        try:
            import modal
        except ImportError as exc:  # pragma: no cover - depends on optional extra
            raise RuntimeError(
                "Modal support is optional; install it with `uv sync --extra coding-agent`"
            ) from exc

        app = await modal.App.lookup.aio(spec.app_name, create_if_missing=True)
        return modal, app

    async def _exec_agent(
        self,
        *args: str,
        workdir: str | None = None,
        timeout: int | None = None,
        secrets: Sequence[Any] = (),
        env: dict[str, str] | None = None,
    ) -> CommandResult:
        """Stream agent output immediately while teeing the canonical trace in-sandbox."""

        if not self._active_trace_path:
            return await super()._exec_agent(
                *args,
                workdir=workdir,
                timeout=timeout,
                secrets=secrets,
                env=env,
            )
        wrapped = (
            "bash",
            "-o",
            "pipefail",
            "-c",
            _AGENT_STREAM_SCRIPT,
            "archetype-agent-stream",
            self._active_trace_path,
            self._active_trace_stderr_path,
            *args,
        )
        process = await self._sandbox.exec.aio(
            *wrapped,
            workdir=workdir,
            timeout=timeout,
            secrets=list(secrets),
            env=env,
        )
        # Modal exposes stdin as an open pipe. Both noninteractive CLIs may
        # consume additional prompt input from stdin and will wait forever if
        # the writer is left open, even when a prompt argument was supplied.
        process.stdin.write_eof()
        await process.stdin.drain.aio()
        self._agent_stream_bytes = {"stdout": 0, "stderr": 0}
        self._agent_last_output_at = None
        stdout_task = asyncio.create_task(
            self._pump_agent_stream(process.stdout, sys.stdout, stream_name="stdout")
        )
        stderr_task = asyncio.create_task(
            self._pump_agent_stream(process.stderr, sys.stderr, stream_name="stderr")
        )
        returncode, stdout, stderr = await asyncio.gather(
            process.wait.aio(), stdout_task, stderr_task
        )
        return CommandResult(tuple(args), int(returncode), stdout, stderr)

    async def _pump_agent_stream(
        self,
        reader: Any,
        target: Any,
        *,
        stream_name: str,
    ) -> str:
        chunks: list[str] = []
        async for chunk in reader:
            value = chunk.decode(errors="replace") if isinstance(chunk, bytes) else str(chunk)
            chunks.append(value)
            self._agent_stream_bytes[stream_name] += len(value.encode())
            self._agent_last_output_at = time.monotonic()
            if self.spec.stream_agent_output:
                target.write(value)
                target.flush()
        return "".join(chunks)

    async def _heartbeat_session(self) -> None:
        while True:
            await asyncio.sleep(self.spec.heartbeat_seconds)
            now = time.monotonic()
            await self._emit_live_event(
                "heartbeat",
                phase=self._live_phase,
                elapsed_seconds=int(now - self._live_session_started_at),
                phase_elapsed_seconds=int(now - self._live_phase_started_at),
                agent_stdout_bytes=self._agent_stream_bytes["stdout"],
                agent_stderr_bytes=self._agent_stream_bytes["stderr"],
                agent_output_bytes=sum(self._agent_stream_bytes.values()),
                seconds_since_agent_output=(
                    int(now - self._agent_last_output_at)
                    if self._agent_last_output_at is not None
                    else None
                ),
            )

    async def _emit_live_event(self, event_type: str, **details: Any) -> None:
        status_path, events_path = self._live_artifact_paths()
        async with self._live_event_lock:
            if event_type != "heartbeat":
                phase = self._phase_for_event(event_type, details)
                if phase != self._live_phase:
                    self._live_phase = phase
                    self._live_phase_started_at = time.monotonic()
                details.setdefault("phase", phase)
            self._live_sequence += 1
            event = {
                "schema_version": 1,
                "sequence": self._live_sequence,
                "timestamp_ms": int(time.time() * 1000),
                "type": event_type,
                "sandbox_id": self.sandbox_id,
                "harness": self.spec.harness,
                **self._live_context,
                **details,
            }
            line = json.dumps(event, sort_keys=True) + "\n"
            try:
                existing = str(await self._sandbox.filesystem.read_text.aio(events_path))
            except Exception as exc:
                if not self._is_missing_sandbox_path(exc):
                    raise
                existing = ""
            await self._sandbox.filesystem.write_text.aio(existing + line, events_path)
            await self._sandbox.filesystem.write_text.aio(line, status_path)
        if self.spec.stream_agent_output:
            sys.stdout.write(line)
            sys.stdout.flush()

    @staticmethod
    def _phase_for_event(event_type: str, details: Mapping[str, Any]) -> str:
        explicit = details.get("phase")
        if explicit:
            return str(explicit)
        if event_type == "agent_started":
            return "agent_running"
        if event_type == "validator_started":
            return f"validator:{details.get('validator', 'unknown')}"
        if event_type == "commit_started":
            return "committing"
        if event_type == "evidence_capture_started":
            return "capturing_evidence"
        if event_type == "checkpoint_started":
            return "checkpointing"
        if event_type == "artifact_publication_started":
            return "publishing_artifacts"
        if event_type == "attempt_completed":
            return "completed"
        return event_type

    @staticmethod
    def _is_missing_sandbox_path(exc: BaseException) -> bool:
        """Normalize Modal's missing-file error without importing Modal eagerly."""

        return isinstance(exc, FileNotFoundError) or type(exc).__name__ == (
            "SandboxFilesystemNotFoundError"
        )

    @classmethod
    async def _modal_dependencies(
        cls, spec: ModalSandboxSpec
    ) -> tuple[Any, Any, Any | None, Any | None, Any | None]:
        modal, app = await cls._modal_base(spec)
        auth_volume = None
        if spec.auth_mode == "oauth":
            auth_volume = modal.Volume.from_name(
                spec.auth_volume_name,
                create_if_missing=False,
                version=2,
            )
            try:
                await auth_volume.hydrate.aio()
            except Exception as exc:
                raise RuntimeError(
                    f"Modal OAuth volume {spec.auth_volume_name!r} is not initialized. Run "
                    "the coding-agent example with --modal-login and complete the subscription "
                    "login, then retry."
                ) from exc
            agent_secret = None
        elif spec.harness == "codex":
            agent_secret = modal.Secret.from_name(
                spec.codex_secret_name, required_keys=["CODEX_API_KEY"]
            )
        elif spec.harness == "claude-code":
            agent_secret = modal.Secret.from_name(
                spec.claude_secret_name, required_keys=["ANTHROPIC_API_KEY"]
            )
        else:
            agent_secret = modal.Secret.from_name(
                spec.opencode_secret_name,
                required_keys=[
                    "MODAL_ENDPOINT_TOKEN_ID",
                    "MODAL_ENDPOINT_TOKEN_SECRET",
                ],
            )
        github_secret = None
        if spec.github_secret_name:
            github_secret = modal.Secret.from_name(
                spec.github_secret_name,
                required_keys=["GITHUB_TOKEN"],
            )
        return modal, app, agent_secret, github_secret, auth_volume

    @classmethod
    async def _start(
        cls,
        spec: ModalSandboxSpec,
        *,
        image: Any,
        app: Any,
        agent_secret: Any | None,
        github_secret: Any | None,
        auth_volume: Any | None,
    ) -> ModalSandboxClient:
        auth_sandbox = None
        if auth_volume is not None:
            auth_sandbox = await cls._create_modal_sandbox(
                spec,
                image=image,
                app=app,
                volumes={_OAUTH_MOUNT: auth_volume},
                workdir=_OAUTH_MOUNT,
                kind="archetype-agent-auth-broker",
            )
        try:
            sandbox = await cls._create_modal_sandbox(spec, image=image, app=app)
        except BaseException:
            if auth_sandbox is not None:
                await cls._terminate(auth_sandbox)
            raise
        return cls(
            spec,
            sandbox,
            agent_secret,
            github_secret,
            _auth_sandbox=auth_sandbox,
        )

    @staticmethod
    async def _create_modal_sandbox(
        spec: ModalSandboxSpec,
        *,
        image: Any,
        app: Any,
        volumes: Mapping[str, Any] | None = None,
        workdir: str | None = None,
        kind: str = "archetype-coding-agent",
    ) -> Any:
        import modal

        return await modal.Sandbox.create.aio(
            app=app,
            image=image,
            timeout=spec.timeout_seconds,
            idle_timeout=spec.idle_timeout_seconds,
            workdir=workdir or str(PurePosixPath(spec.workspace).parent),
            volumes=({key: value for key, value in volumes.items()} if volumes is not None else {}),
            tags={"kind": kind, "branch": spec.branch, "harness": spec.harness},
        )

    @classmethod
    async def create(cls, spec: ModalSandboxSpec) -> ModalSandboxClient:
        """Create a Modal Sandbox, clone the repository, and prepare its branch."""

        modal, app, agent_secret, github_secret, auth_volume = await cls._modal_dependencies(spec)
        image = (
            modal.Image.from_name(spec.image_name)
            if spec.image_name
            else _default_agent_image(modal, spec.harness)
        )
        client = await cls._start(
            spec,
            image=image,
            app=app,
            agent_secret=agent_secret,
            github_secret=github_secret,
            auth_volume=auth_volume,
        )
        if spec.stream_agent_output:
            print(
                json.dumps(
                    {
                        "type": "sandbox_created",
                        "sandbox_id": client.sandbox_id,
                        "harness": spec.harness,
                        "branch": spec.branch,
                        "monitor_command": (
                            "uv run --extra coding-agent python "
                            "examples/11_coding_agent_mission.py "
                            f"--monitor-sandbox {client.sandbox_id}"
                        ),
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
        try:
            if spec.auth_mode == "oauth":
                await client._check_oauth()
            await client._prepare_repository()
            status_path, _events_path = client._live_artifact_paths()
            await client._checked("mkdir", "-p", str(PurePosixPath(status_path).parent))
            await client._emit_live_event("sandbox_ready", phase="ready")
        except BaseException as exc:
            await client._emit_live_event_safely(
                "sandbox_preparation_failed",
                error_type=type(exc).__name__,
                error=client._tail(str(exc), 1000),
            )
            await client.close()
            raise
        return client

    @classmethod
    async def login_oauth(cls, spec: ModalSandboxSpec) -> None:
        """Persist an interactive subscription login in a named Modal Volume."""

        if spec.auth_mode != "oauth":
            raise ValueError("Modal subscription login requires auth_mode='oauth'")
        if spec.harness == "opencode":
            raise ValueError("OpenCode endpoint auth does not support subscription login")
        modal, app = await cls._modal_base(spec)
        volume = modal.Volume.from_name(
            spec.auth_volume_name,
            create_if_missing=True,
            version=2,
        )
        await volume.hydrate.aio()
        image = (
            modal.Image.from_name(spec.image_name)
            if spec.image_name
            else _default_agent_image(modal, spec.harness)
        )
        sandbox = await cls._create_modal_sandbox(
            spec,
            image=image,
            app=app,
            volumes={_OAUTH_MOUNT: volume},
            workdir=_OAUTH_MOUNT,
            kind="archetype-agent-oauth-login",
        )
        try:
            if spec.harness == "codex":
                argv = (
                    "codex",
                    "login",
                    "--device-auth",
                    "-c",
                    'cli_auth_credentials_store="file"',
                )
                env = {"CODEX_HOME": _OAUTH_MOUNT, "NO_COLOR": "1"}
            else:
                argv = ("claude", "auth", "login", "--claudeai")
                env = {
                    "CLAUDE_CONFIG_DIR": _OAUTH_MOUNT,
                    "DISABLE_AUTOUPDATER": "1",
                    "NO_COLOR": "1",
                }
            process = await sandbox.exec.aio(
                *argv,
                workdir=_OAUTH_MOUNT,
                timeout=spec.agent_timeout_seconds,
                env=env,
                pty=True,
            )
            returncode = await cls._passthrough_process(process)
            if returncode != 0:
                raise RuntimeError(
                    f"{spec.harness} subscription login failed with exit code {returncode}"
                )

            status_argv = (
                ("codex", "login", "status")
                if spec.harness == "codex"
                else ("claude", "auth", "status")
            )
            status = await cls._sandbox_exec(
                sandbox,
                *status_argv,
                timeout=60,
                env=env,
            )
            cls._raise_for_result(status, f"{spec.harness} subscription login verification")
        finally:
            cleanup = asyncio.create_task(cls._cleanup_oauth_login(sandbox, spec))
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                # Ctrl-C cancels the caller, but credential-volume cleanup and
                # sandbox termination are part of the durability boundary.
                # Wait for the shielded task before propagating cancellation.
                await cleanup
                raise

    @classmethod
    async def _cleanup_oauth_login(cls, sandbox: Any, spec: ModalSandboxSpec) -> None:
        try:
            credential_name = PurePosixPath(spec.auth_volume_path).name
            cleanup_script = (
                f"find {_OAUTH_MOUNT} -mindepth 1 -maxdepth 1 "
                f"! -name {credential_name} -exec rm -rf -- {{}} +"
            )
            cleaned = await cls._sandbox_exec(
                sandbox,
                "sh",
                "-c",
                cleanup_script,
                timeout=60,
            )
            cls._raise_for_result(cleaned, f"{spec.harness} OAuth volume cleanup")
            synced = await cls._sandbox_exec(sandbox, "sync", _OAUTH_MOUNT, timeout=60)
            cls._raise_for_result(synced, f"{spec.harness} OAuth volume sync")
        finally:
            await cls._terminate(sandbox)

    @staticmethod
    async def _passthrough_process(process: Any) -> int:
        """Bridge a remote login PTY to the caller's terminal without logging secrets."""

        loop = asyncio.get_running_loop()
        write_tasks: set[asyncio.Task[Any]] = set()
        stdin_fd: int | None = None

        async def write_remote(data: bytes) -> None:
            await process.stdin.write.aio(data)
            await process.stdin.drain.aio()

        def stdin_ready() -> None:
            assert stdin_fd is not None
            try:
                data = os.read(stdin_fd, 4096)
            except OSError:
                data = b""
            if not data:
                loop.remove_reader(stdin_fd)
                return
            task = asyncio.create_task(write_remote(data))
            write_tasks.add(task)
            task.add_done_callback(write_tasks.discard)

        async def pump_output(reader: Any, target: Any) -> None:
            async for chunk in reader:
                value = chunk.decode(errors="replace") if isinstance(chunk, bytes) else str(chunk)
                target.write(value)
                target.flush()

        try:
            try:
                stdin_fd = sys.stdin.fileno()
                loop.add_reader(stdin_fd, stdin_ready)
            except (AttributeError, OSError, ValueError, NotImplementedError):
                stdin_fd = None
            output = asyncio.create_task(pump_output(process.stdout, sys.stdout))
            errors = asyncio.create_task(pump_output(process.stderr, sys.stderr))
            returncode = int(await process.wait.aio())
            await asyncio.gather(output, errors)
            return returncode
        finally:
            if stdin_fd is not None:
                try:
                    loop.remove_reader(stdin_fd)
                except (OSError, ValueError):
                    pass
            if write_tasks:
                await asyncio.gather(*write_tasks, return_exceptions=True)

    @staticmethod
    async def _sandbox_exec(
        sandbox: Any,
        *args: str,
        timeout: int,
        env: dict[str, str] | None = None,
    ) -> CommandResult:
        process = await sandbox.exec.aio(*args, timeout=timeout, env=env)
        stdout_task = asyncio.create_task(process.stdout.read.aio())
        stderr_task = asyncio.create_task(process.stderr.read.aio())
        returncode, stdout, stderr = await asyncio.gather(
            process.wait.aio(), stdout_task, stderr_task
        )
        return CommandResult(tuple(args), int(returncode), str(stdout), str(stderr))

    async def _run_codex(self, prompt: str, *, session_id: str) -> CommandResult:
        if self.spec.auth_mode == "api-key":
            return await super()._run_codex(prompt, session_id=session_id)
        await self._stage_oauth()
        try:
            return await super()._run_codex(prompt, session_id=session_id)
        finally:
            await self._persist_and_remove_oauth()

    async def _run_agent(self, prompt: str, *, session_id: str) -> CommandResult:
        if self.spec.harness == "opencode":
            return await self._run_opencode(prompt, session_id=session_id)
        return await super()._run_agent(prompt, session_id=session_id)

    async def _run_opencode(self, prompt: str, *, session_id: str) -> CommandResult:
        provider_package = (
            "@ai-sdk/openai-compatible"
            if self.spec.opencode_wire_api == "chat-completions"
            else "@ai-sdk/openai"
        )
        model_ref = f"{self.spec.opencode_provider_id}/{self.spec.model}"
        config = {
            "$schema": "https://opencode.ai/config.json",
            "model": model_ref,
            "share": "disabled",
            "permission": "allow",
            "provider": {
                self.spec.opencode_provider_id: {
                    "name": "Archetype Modal endpoint",
                    "npm": provider_package,
                    "options": {
                        "baseURL": self.spec.opencode_base_url,
                        "headers": {
                            "Modal-Key": "{env:MODAL_ENDPOINT_TOKEN_ID}",
                            "Modal-Secret": "{env:MODAL_ENDPOINT_TOKEN_SECRET}",
                        },
                    },
                    "models": {self.spec.model: {"name": self.spec.model}},
                }
            },
        }
        await self._checked("mkdir", "-p", str(PurePosixPath(_OPENCODE_CONFIG_PATH).parent))
        await self._write_text(_OPENCODE_CONFIG_PATH, json.dumps(config, sort_keys=True))

        argv = [
            "opencode",
            "run",
            "--pure",
            "--format",
            "json",
            "--model",
            model_ref,
            "--auto",
        ]
        if session_id:
            argv.extend(["--session", session_id])
        argv.append(prompt)
        return await self._exec_agent(
            *argv,
            workdir=self.spec.workspace,
            timeout=self.spec.agent_timeout_seconds,
            secrets=[self._agent_secret] if self._agent_secret is not None else (),
            env={
                "NO_COLOR": "1",
                "OPENCODE_CONFIG": _OPENCODE_CONFIG_PATH,
                "OPENCODE_DISABLE_AUTOUPDATE": "1",
                "OPENCODE_DISABLE_PROJECT_CONFIG": "1",
            },
        )

    async def _run_claude(self, prompt: str, *, session_id: str) -> CommandResult:
        if self.spec.auth_mode == "api-key":
            return await super()._run_claude(prompt, session_id=session_id)
        await self._stage_oauth()
        try:
            return await super()._run_claude(prompt, session_id=session_id)
        finally:
            await self._persist_and_remove_oauth()

    async def _check_oauth(self) -> None:
        await self._stage_oauth()
        try:
            if self.spec.harness == "codex":
                status = await self._exec(
                    "codex",
                    "login",
                    "status",
                    timeout=60,
                    env={"CODEX_HOME": "/root/.codex", "NO_COLOR": "1"},
                )
            else:
                status = await self._exec(
                    "claude",
                    "auth",
                    "status",
                    timeout=60,
                    env={
                        "CLAUDE_CONFIG_DIR": "/root/.claude",
                        "DISABLE_AUTOUPDATER": "1",
                        "NO_COLOR": "1",
                    },
                )
        finally:
            await self._persist_and_remove_oauth()
        if status.returncode != 0:
            raise RuntimeError(
                f"The {self.spec.harness} OAuth volume does not contain a valid subscription "
                "login. Run the coding-agent example with --modal-login and retry."
            )

    async def _stage_oauth(self) -> None:
        if self._auth_sandbox is None:
            raise RuntimeError("Modal OAuth credential broker is not running")
        try:
            payload = await self._auth_sandbox.filesystem.read_text.aio(self.spec.auth_volume_path)
        except Exception as exc:
            raise RuntimeError(
                f"Modal OAuth volume {self.spec.auth_volume_name!r} has no {self.spec.harness} "
                "credential. Run the coding-agent example with --modal-login and retry."
            ) from exc
        self._validate_oauth_payload(payload)
        parent = str(PurePosixPath(self.spec.mission_auth_path).parent)
        await self._checked("mkdir", "-p", parent)
        await self._sandbox.filesystem.write_text.aio(payload, self.spec.mission_auth_path)
        secured = await self._exec("chmod", "600", self.spec.mission_auth_path, timeout=30)
        self._raise_for_result(secured, f"stage {self.spec.harness} OAuth credential")

    async def _persist_and_remove_oauth(self) -> None:
        persistence_error: BaseException | None = None
        try:
            payload = await self._sandbox.filesystem.read_text.aio(self.spec.mission_auth_path)
            self._validate_oauth_payload(payload)
            assert self._auth_sandbox is not None
            temporary_path = f"{self.spec.auth_volume_path}.next"
            await self._auth_sandbox.filesystem.write_text.aio(payload, temporary_path)
            secured = await self._auth_exec("chmod", "600", temporary_path, timeout=30)
            self._raise_for_result(
                secured, f"secure refreshed {self.spec.harness} OAuth credential"
            )
            promoted = await self._auth_exec(
                "mv",
                "-f",
                temporary_path,
                self.spec.auth_volume_path,
                timeout=30,
            )
            self._raise_for_result(
                promoted, f"persist refreshed {self.spec.harness} OAuth credential"
            )
            synced = await self._auth_exec("sync", _OAUTH_MOUNT, timeout=60)
            self._raise_for_result(synced, f"sync {self.spec.harness} OAuth volume")
        except BaseException as exc:
            persistence_error = exc
        finally:
            removed = await self._exec("rm", "-f", self.spec.mission_auth_path, timeout=30)
            self._raise_for_result(removed, f"remove staged {self.spec.harness} OAuth credential")
        if persistence_error is not None:
            raise persistence_error

    async def _auth_exec(
        self,
        *args: str,
        timeout: int,
        env: dict[str, str] | None = None,
    ) -> CommandResult:
        if self._auth_sandbox is None:
            raise RuntimeError("Modal OAuth credential broker is not running")
        return await self._sandbox_exec(
            self._auth_sandbox,
            *args,
            timeout=timeout,
            env=env,
        )

    @staticmethod
    def _validate_oauth_payload(payload: str) -> None:
        try:
            value = json.loads(payload)
        except (TypeError, json.JSONDecodeError) as exc:
            raise RuntimeError("OAuth credential payload is not valid JSON") from exc
        if not isinstance(value, dict) or not value:
            raise RuntimeError("OAuth credential payload must be a non-empty JSON object")

    async def close(self) -> None:
        """Terminate the mission and its credential broker without deleting the auth volume."""

        if self._closed:
            return
        if self._live_sequence:
            await self._emit_live_event_safely("sandbox_closing", phase="teardown")
        self._closed = True
        failures: list[BaseException] = []
        for sandbox in (self._sandbox, self._auth_sandbox):
            if sandbox is None:
                continue
            try:
                await self._terminate(sandbox)
            except BaseException as exc:
                failures.append(exc)
        if failures:
            raise failures[0]

    @staticmethod
    async def _terminate(sandbox: Any) -> None:
        try:
            await sandbox.terminate.aio(wait=True)
        finally:
            await sandbox.detach.aio()

    @classmethod
    async def restore(cls, spec: ModalSandboxSpec, checkpoint_ref: str) -> ModalSandboxClient:
        """Create a credential-free sandbox for artifact recovery.

        Use :meth:`resume` when a coding agent must make another model call.
        Recovery sandboxes deliberately receive no model, GitHub, or OAuth
        credential, even when ``spec`` names them.
        """

        image_id = cls._checkpoint_image_id(checkpoint_ref)
        modal, app = await cls._modal_base(spec)
        image = modal.Image.from_id(image_id)
        client = await cls._start(
            spec,
            image=image,
            app=app,
            agent_secret=None,
            github_secret=None,
            auth_volume=None,
        )
        try:
            await client._git("rev-parse", "--is-inside-work-tree")
        except BaseException:
            await client.close()
            raise
        client._latest_checkpoint_ref = checkpoint_ref
        return client

    @classmethod
    async def resume(cls, spec: ModalSandboxSpec, checkpoint_ref: str) -> ModalSandboxClient:
        """Resume authenticated agent execution from a filesystem checkpoint.

        This is the secure continuation path. Provider credentials are
        resolved again from the names in ``spec`` and retain the same
        process-only or broker-only isolation used by :meth:`create`.
        """

        image_id = cls._checkpoint_image_id(checkpoint_ref)
        modal, app, agent_secret, github_secret, auth_volume = await cls._modal_dependencies(spec)
        image = modal.Image.from_id(image_id)
        client = await cls._start(
            spec,
            image=image,
            app=app,
            agent_secret=agent_secret,
            github_secret=github_secret,
            auth_volume=auth_volume,
        )
        try:
            if spec.auth_mode == "oauth":
                await client._check_oauth()
            await client._git("rev-parse", "--is-inside-work-tree")
            branch = (await client._git("branch", "--show-current")).stdout.strip()
            if branch != spec.branch:
                raise RuntimeError(
                    f"Modal checkpoint branch mismatch: expected {spec.branch!r}, got {branch!r}"
                )
            await client._rehydrate_live_events()
            await client._emit_live_event(
                "sandbox_resumed",
                phase="ready",
                checkpoint_ref=checkpoint_ref,
            )
        except BaseException:
            await client.close()
            raise
        client._latest_checkpoint_ref = checkpoint_ref
        if spec.stream_agent_output:
            print(
                json.dumps(
                    {
                        "type": "sandbox_resumed",
                        "sandbox_id": client.sandbox_id,
                        "harness": spec.harness,
                        "branch": spec.branch,
                        "checkpoint_ref": checkpoint_ref,
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
        return client

    async def _rehydrate_live_events(self) -> None:
        """Continue the checkpoint's append-only event sequence."""

        _status_path, events_path = self._live_artifact_paths()
        try:
            value = str(await self._sandbox.filesystem.read_text.aio(events_path))
        except Exception as exc:
            if self._is_missing_sandbox_path(exc):
                return
            raise
        sequences = []
        for line in value.splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            sequence = event.get("sequence")
            if isinstance(sequence, int) and sequence >= 0:
                sequences.append(sequence)
        self._live_sequence = max(sequences, default=0)

    @staticmethod
    def _checkpoint_image_id(checkpoint_ref: str) -> str:
        prefix = "modal-image://"
        image_id = checkpoint_ref.removeprefix(prefix)
        if not checkpoint_ref.startswith(prefix) or not image_id or "#" in image_id:
            raise ValueError("Modal checkpoint must be a non-empty modal-image:// reference")
        return image_id


@dataclass(frozen=True)
class _MaterializedModalArtifact:
    """Structural resolver result without importing the app-layer contract."""

    path: Path
    source_ref: str
    logical_path: str
    kind: str


@dataclass
class ModalArtifactSourceResolver:
    """Materialize refs from a live or restorable Modal Sandbox.

    Supplying ``sandbox`` avoids another cold start during normal episode
    finalization. A later reconciler can supply only ``spec`` and restore a
    ``modal-image://`` checkpoint on demand.
    """

    spec: ModalSandboxSpec
    sandbox: ModalSandboxClient | None = None

    async def materialize(
        self,
        candidates: Sequence[Any],
        destination: Path,
    ) -> list[_MaterializedModalArtifact]:
        modal_candidates = []
        for candidate in candidates:
            if candidate.source_ref.startswith(("modal-image://", "modal-sandbox://")):
                modal_candidates.append(candidate)
            else:
                raise ValueError(
                    "ModalArtifactSourceResolver accepts only modal-image:// or "
                    "modal-sandbox:// references"
                )

        resolved: list[_MaterializedModalArtifact] = []

        grouped: dict[str, list] = {}
        for candidate in modal_candidates:
            checkpoint, remote_path = self._split_ref(candidate.source_ref)
            grouped.setdefault(checkpoint, []).append((candidate, remote_path))

        for group_index, (checkpoint, requested) in enumerate(grouped.items()):
            client = None
            owns_client = False
            if checkpoint.startswith("modal-image://"):
                if self.sandbox is not None and self.sandbox._latest_checkpoint_ref == checkpoint:
                    client = self.sandbox
                else:
                    client = await ModalSandboxClient.restore(self.spec, checkpoint)
                    owns_client = True
            elif (
                self.sandbox is not None
                and checkpoint == f"modal-sandbox://{self.sandbox.sandbox_id}"
            ):
                client = self.sandbox
            if client is None:
                raise RuntimeError(
                    "Modal sandbox artifact references require their matching live "
                    "ModalSandboxClient"
                )
            try:
                group_destination = destination / f"modal-{group_index:04d}"
                group_destination.mkdir(parents=True, exist_ok=True)
                for candidate, remote_path in requested:
                    try:
                        if candidate.recursive:
                            files = await self._walk_files(client, remote_path)
                            if not files and candidate.required:
                                raise FileNotFoundError(
                                    f"required Modal artifact directory is empty: {remote_path}"
                                )
                            for file_index, file_path in enumerate(files):
                                relative = PurePosixPath(file_path).relative_to(remote_path)
                                output = group_destination / (
                                    f"{file_index:08d}-{PurePosixPath(file_path).name}"
                                )
                                await client._sandbox.filesystem.copy_to_local.aio(
                                    file_path, output
                                )
                                resolved.append(
                                    _MaterializedModalArtifact(
                                        path=output,
                                        source_ref=f"{checkpoint}#{file_path}",
                                        logical_path=(
                                            PurePosixPath(candidate.logical_path) / relative
                                        ).as_posix(),
                                        kind=candidate.kind,
                                    )
                                )
                        else:
                            output = group_destination / (
                                f"{len(resolved):08d}-{PurePosixPath(remote_path).name}"
                            )
                            await client._sandbox.filesystem.copy_to_local.aio(remote_path, output)
                            resolved.append(
                                _MaterializedModalArtifact(
                                    path=output,
                                    source_ref=f"{checkpoint}#{remote_path}",
                                    logical_path=candidate.logical_path,
                                    kind=candidate.kind,
                                )
                            )
                    except Exception:
                        if candidate.required:
                            raise
            finally:
                if owns_client:
                    await client.close()
        return resolved

    @staticmethod
    def _split_ref(source_ref: str) -> tuple[str, str]:
        if source_ref.startswith("modal-image://"):
            checkpoint, marker, path = source_ref.rpartition("#")
            if not marker or not checkpoint.removeprefix("modal-image://") or not path:
                raise ValueError("Modal image artifact refs require modal-image://<id>#<path>")
            return checkpoint, path
        value = source_ref.removeprefix("modal-sandbox://")
        sandbox_id, marker, path = value.partition("/")
        if not sandbox_id or not marker:
            raise ValueError("Modal sandbox artifact refs require modal-sandbox://<id>/<path>")
        return f"modal-sandbox://{sandbox_id}", f"/{path}"

    @classmethod
    async def _walk_files(cls, client: ModalSandboxClient, root: str) -> list[str]:
        entries = await client._sandbox.filesystem.list_files.aio(root)
        files: list[str] = []
        for entry in entries:
            if entry.is_file():
                files.append(entry.path)
            elif entry.is_dir():
                files.extend(await cls._walk_files(client, entry.path))
        return sorted(files)


class ModalSandboxBackend:
    """Provider adapter consumed by :class:`SandboxService`."""

    name = "modal"

    async def create(self, spec: ModalSandboxSpec) -> ModalSandboxClient:
        return await ModalSandboxClient.create(spec)

    async def restore(self, spec: ModalSandboxSpec, checkpoint_ref: str) -> ModalSandboxClient:
        return await ModalSandboxClient.restore(spec, checkpoint_ref)

    async def resume(self, spec: ModalSandboxSpec, checkpoint_ref: str) -> ModalSandboxClient:
        return await ModalSandboxClient.resume(spec, checkpoint_ref)

    async def authenticate(self, spec: ModalSandboxSpec) -> None:
        if spec.auth_mode == "oauth":
            await ModalSandboxClient.login_oauth(spec)


__all__ = [
    "AgentAuthMode",
    "AgentHarness",
    "CodingAgentSandboxClient",
    "CodingAgentSandboxSpec",
    "CommandResult",
    "ModalArtifactSourceResolver",
    "ModalSandboxBackend",
    "ModalSandboxClient",
    "ModalSandboxSpec",
    "OpenCodeWireAPI",
    "ValidatorSpec",
]
