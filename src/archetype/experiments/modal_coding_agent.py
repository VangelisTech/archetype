# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Run one coding-agent attempt in a persistent Modal Sandbox.

The sandbox is the mutable filesystem for one coding-agent episode. A call to
``run_attempt`` performs exactly one agent submission, runs authoritative
validators, records a filesystem diff, and checkpoints the sandbox whether the
attempt is accepted or rejected. Mission policy -- including retries and task
advancement -- belongs to the Archetype processor that calls this transport.

Modal and the coding-agent CLIs are optional runtime dependencies. Importing
``archetype.experiments`` does not import Modal; :meth:`ModalSandboxClient.create`
loads it only when a real sandbox is requested.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import PurePosixPath
from typing import Any, Literal, Protocol

AgentHarness = Literal["codex", "claude-code"]

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
    return image.apt_install("nodejs", "npm").run_commands(
        "npm install --global @anthropic-ai/claude-code"
    )


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

    The selected harness secret is injected only into the agent process, not
    sandbox setup or validators. Codex's shell environment policy also excludes
    key/secret/token variables from model-generated commands. Claude Code does
    not currently document an equivalent subprocess environment filter, so use
    a dedicated key and trusted repository content for that harness.

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
    codex_secret_name: str = "archetype-codex"
    claude_secret_name: str = "archetype-claude-code"
    github_secret_name: str = ""
    model: str = ""
    workspace: str = "/workspace/repo"
    timeout_seconds: int = 4 * 60 * 60
    idle_timeout_seconds: int = 20 * 60
    agent_timeout_seconds: int = 45 * 60
    snapshot_timeout_seconds: int = 120
    snapshot_ttl_seconds: int | None = 30 * 24 * 60 * 60
    snapshot_after_attempt: bool = True
    capture_filesystem_manifests: bool = True
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
        if self.harness not in {"codex", "claude-code"}:
            raise ValueError(f"unsupported coding-agent harness: {self.harness!r}")


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


class GateFailedError(RuntimeError):
    """The candidate cannot satisfy an invariant required for acceptance."""


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

        The agent is not trusted to declare success.  Validator commands run in
        separate processes without the agent secret. Acceptance is data returned
        to the caller; a rejected attempt is not an exception and remains
        resumable through its checkpoint.
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
        await self._checked("mkdir", "-p", trace_dir)
        start_manifest_path = await self._ensure_start_manifest()

        agent_prompt = (
            self._repair_prompt(prompt, previous_validator_details)
            if previous_validator_details
            else self._initial_prompt(prompt, step_name)
        )
        agent = await self._run_agent(agent_prompt, session_id=previous_session_id)
        trace_path = (
            f"{trace_dir}/{self._safe_step(step_name)}-{attempt_index}-{attempt_id[:12]}.jsonl"
        )
        await self._write_text(trace_path, agent.stdout)
        if agent.stderr:
            await self._write_text(f"{trace_path}.stderr", agent.stderr)

        session_id = self._session_id(agent.stdout) or previous_session_id
        if agent.returncode != 0:
            details = [
                {
                    "name": "agent_exec",
                    "passed": False,
                    "returncode": agent.returncode,
                    "stdout": self._tail(agent.stdout),
                    "stderr": self._tail(agent.stderr),
                }
            ]
        else:
            details = await self._run_validators(normalized)

        accepted = all(detail["passed"] for detail in details)
        sha = ""
        message = f"{step_name}: {self._subject(prompt)}"
        pushed = False
        if accepted:
            try:
                sha = await self._commit_verified_tree(step_name, prompt, baseline)
            except GateFailedError as exc:
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

        failed = [detail for detail in details if not detail["passed"]]
        friction = []
        if failed:
            friction.append(
                {
                    "step": step_name,
                    "attempt": attempt_index,
                    "finding": "Gate failed: " + ", ".join(str(item["name"]) for item in failed),
                    "learning": self._failure_summary(failed),
                }
            )

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
            "baseline_sha": baseline,
            "commit_sha": sha,
            "pushed": pushed,
            "validator_details": details,
            "artifacts": {
                "trace": trace_path,
                "trace_stderr": f"{trace_path}.stderr" if agent.stderr else "",
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

        checkpoint_created_at_ms = int(time.time() * 1000)
        checkpoint_error = ""
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
            "sandbox_state_ref": snapshot_ref,
            "checkpoint_status": (
                "ready" if checkpoint_ready else "failed" if checkpoint_error else "disabled"
            ),
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
            "codex_thread_id": session_id if self.spec.harness == "codex" else "",
            "claude_session_id": session_id if self.spec.harness == "claude-code" else "",
            "friction": friction,
            "pr_url": "",
        }
        await self._store_completed(idempotency_key, outcome)
        self._completed[idempotency_key] = outcome
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
        return await self._run_claude(prompt, session_id=session_id)

    async def _run_codex(self, prompt: str, *, session_id: str) -> CommandResult:
        common = [
            "--json",
            "--dangerously-bypass-approvals-and-sandbox",
            "--ignore-user-config",
            "-c",
            'shell_environment_policy.inherit="core"',
            "-c",
            'shell_environment_policy.exclude=["*KEY*","*SECRET*","*TOKEN*"]',
        ]
        if self.spec.model:
            common.extend(["--model", self.spec.model])
        if session_id:
            argv = ["codex", "exec", "resume", *common, session_id, prompt]
        else:
            argv = ["codex", "exec", *common, prompt]
        return await self._exec(
            *argv,
            workdir=self.spec.workspace,
            timeout=self.spec.agent_timeout_seconds,
            secrets=[self._agent_secret],
            env={"NO_COLOR": "1"},
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
        return await self._exec(
            *argv,
            workdir=self.spec.workspace,
            timeout=self.spec.agent_timeout_seconds,
            secrets=[self._agent_secret],
            env={"NO_COLOR": "1", "DISABLE_AUTOUPDATER": "1"},
        )

    async def _run_validators(self, validators: Sequence[ValidatorSpec]) -> list[dict[str, Any]]:
        details: list[dict[str, Any]] = []
        for validator in validators:
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
        return details

    async def _commit_verified_tree(self, step_name: str, prompt: str, baseline: str) -> str:
        status = (await self._git("status", "--porcelain")).stdout
        if status.strip():
            await self._git("add", "-A")
            await self._git("commit", "-m", f"{step_name}: {self._subject(prompt)}")
        sha = (await self._git("rev-parse", "HEAD")).stdout.strip()
        if sha == baseline:
            raise GateFailedError(
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


class ModalSandboxClient(CodingAgentSandboxClient[ModalSandboxSpec]):
    """A live Modal sandbox running the provider-neutral attempt protocol."""

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

    @classmethod
    async def _modal_dependencies(cls, spec: ModalSandboxSpec) -> tuple[Any, Any, Any, Any | None]:
        modal, app = await cls._modal_base(spec)
        if spec.harness == "codex":
            agent_secret = modal.Secret.from_name(
                spec.codex_secret_name,
                required_keys=["CODEX_API_KEY"],
            )
        else:
            agent_secret = modal.Secret.from_name(
                spec.claude_secret_name,
                required_keys=["ANTHROPIC_API_KEY"],
            )
        github_secret = None
        if spec.github_secret_name:
            github_secret = modal.Secret.from_name(
                spec.github_secret_name,
                required_keys=["GITHUB_TOKEN"],
            )
        return modal, app, agent_secret, github_secret

    @classmethod
    async def _start(
        cls,
        spec: ModalSandboxSpec,
        *,
        image: Any,
        app: Any,
        agent_secret: Any,
        github_secret: Any | None,
    ) -> ModalSandboxClient:
        sandbox = await cls._create_modal_sandbox(spec, image=image, app=app)
        return cls(spec, sandbox, agent_secret, github_secret)

    @staticmethod
    async def _create_modal_sandbox(spec: ModalSandboxSpec, *, image: Any, app: Any) -> Any:
        import modal

        return await modal.Sandbox.create.aio(
            app=app,
            image=image,
            timeout=spec.timeout_seconds,
            idle_timeout=spec.idle_timeout_seconds,
            workdir=str(PurePosixPath(spec.workspace).parent),
            tags={"kind": "archetype-coding-agent", "branch": spec.branch},
        )

    @classmethod
    async def create(cls, spec: ModalSandboxSpec) -> ModalSandboxClient:
        """Create a Modal Sandbox, clone the repository, and prepare its branch."""

        modal, app, agent_secret, github_secret = await cls._modal_dependencies(spec)
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
        )
        try:
            await client._prepare_repository()
        except BaseException:
            await client.close()
            raise
        return client

    @classmethod
    async def restore(cls, spec: ModalSandboxSpec, checkpoint_ref: str) -> ModalSandboxClient:
        """Create a new sandbox from a previously recorded filesystem snapshot."""

        prefix = "modal-image://"
        image_id = checkpoint_ref.removeprefix(prefix)
        if not checkpoint_ref.startswith(prefix) or not image_id or "#" in image_id:
            raise ValueError("Modal checkpoint must be a non-empty modal-image:// reference")
        modal, app = await cls._modal_base(spec)
        image = modal.Image.from_id(image_id)
        client = await cls._start(
            spec,
            image=image,
            app=app,
            # Artifact recovery needs only the provider credential. Model and
            # GitHub secrets are deliberately absent from restored sandboxes.
            agent_secret=None,
            github_secret=None,
        )
        try:
            await client._git("rev-parse", "--is-inside-work-tree")
        except BaseException:
            await client.close()
            raise
        client._latest_checkpoint_ref = checkpoint_ref
        return client


@dataclass
class ModalArtifactSourceResolver:
    """Materialize refs from a live or restorable Modal Sandbox.

    Supplying ``sandbox`` avoids another cold start during normal episode
    finalization. A later reconciler can supply only ``spec`` and restore a
    ``modal-image://`` checkpoint on demand.
    """

    spec: ModalSandboxSpec
    sandbox: ModalSandboxClient | None = None

    async def materialize(self, candidates, destination):
        from archetype.app.artifact_service import CheckpointArtifactSourceResolver
        from archetype.app.artifacts import MaterializedArtifact

        modal_candidates = []
        fallback = []
        for candidate in candidates:
            if candidate.source_ref.startswith(("modal-image://", "modal-sandbox://")):
                modal_candidates.append(candidate)
            else:
                fallback.append(candidate)

        resolved = []
        if fallback:
            resolved.extend(
                await CheckpointArtifactSourceResolver().materialize(
                    tuple(fallback), destination / "fallback"
                )
            )

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
                                    MaterializedArtifact(
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
                                MaterializedArtifact(
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


__all__ = [
    "AgentHarness",
    "CodingAgentSandboxClient",
    "CodingAgentSandboxSpec",
    "CommandResult",
    "GateFailedError",
    "ModalArtifactSourceResolver",
    "ModalSandboxClient",
    "ModalSandboxSpec",
    "ValidatorSpec",
]
