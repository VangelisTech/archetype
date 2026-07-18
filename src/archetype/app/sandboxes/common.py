# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Provider-neutral coding-agent attempt kernel.

Provider adapters implement command execution, text writes, checkpointing,
resource teardown, and provider URI construction. This module owns the common
execution -> validation -> repository finalization -> evidence -> checkpoint
-> artifact-handoff protocol and has no dependency on a sandbox SDK.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Any

from archetype.app.sandboxes.models import (
    AgentExecution,
    ArtifactHandoff,
    AttemptPhase,
    CheckpointCapture,
    CodingAgentSandboxSpec,
    CommandResult,
    EvidenceCapture,
    PreparedAttempt,
    RepositoryFinalization,
    RepositoryPhaseReceipt,
    ValidationEvidence,
    ValidatorSpec,
)

_OPENCODE_CONFIG_PATH = "/root/.config/archetype/opencode.json"
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


@dataclass
class CodingAgentSandboxClient[SandboxSpecT: CodingAgentSandboxSpec](ABC):
    """Provider-neutral attempt, validation, evidence, and checkpoint protocol.

    The sandbox-local receipt suppresses duplicate work while one checkpoint is
    available. It is deliberately not a durable pre-execution claim and makes
    no exactly-once submission promise.
    """

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
    @abstractmethod
    def sandbox_id(self) -> str:
        """Return the provider-owned live sandbox identity."""

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
        """Run one submission through six explicit, provider-neutral phases."""

        if self._closed:
            raise RuntimeError("sandbox already closed")
        if attempt_index < 1:
            raise ValueError("attempt_index must be at least 1")
        if not idempotency_key:
            raise ValueError("idempotency_key must not be empty")
        if not prompt.strip():
            raise ValueError("prompt must not be empty")
        if not step_name.strip():
            raise ValueError("step_name must not be empty")

        correlation_data = dict(correlation or {})
        try:
            json.dumps(correlation_data, sort_keys=True)
        except (TypeError, ValueError) as exc:
            raise ValueError("correlation values must be JSON serializable") from exc

        normalized = tuple(
            value if isinstance(value, ValidatorSpec) else ValidatorSpec.from_dict(value)
            for value in validators
        )
        if not normalized:
            raise ValueError("at least one validator is required")
        request_fingerprint = self._request_fingerprint(
            prompt=prompt,
            validators=normalized,
            step_name=step_name,
            attempt_index=attempt_index,
            previous_session_id=previous_session_id,
            previous_validator_details=previous_validator_details,
            correlation=correlation_data,
        )

        cached = await self._load_completed(idempotency_key)
        if cached is not None:
            if cached.get("request_fingerprint") != request_fingerprint:
                raise ValueError("idempotency_key was reused with a different attempt request")
            return cached

        recovered = await self._load_repository_receipt(idempotency_key)
        if recovered is not None and recovered.request_fingerprint != request_fingerprint:
            raise ValueError("idempotency_key was reused with a different attempt request")

        prepared = await self._prepare_attempt(
            prompt=prompt,
            step_name=step_name,
            attempt_index=attempt_index,
            idempotency_key=idempotency_key,
            previous_validator_details=previous_validator_details,
            correlation=correlation_data,
            request_fingerprint=request_fingerprint,
            recovered=recovered,
        )
        execution = await self._execution_phase(prepared, previous_session_id, recovered)
        validation = await self._validation_phase(normalized, recovered)
        repository = await self._repository_finalization_phase(
            prepared, execution, validation, recovered
        )
        evidence = await self._evidence_phase(prepared, execution, repository)
        checkpoint = await self._checkpoint_phase(prepared)
        outcome = await self._artifact_handoff_phase(
            prepared,
            execution,
            repository,
            evidence,
            checkpoint,
        )
        await self._emit_live_event(
            "attempt_completed",
            status=outcome["status"],
            accepted=outcome["accepted"],
            checkpoint_status=outcome["checkpoint_status"],
            commit_sha=outcome["sha"],
        )
        return outcome

    async def _prepare_attempt(
        self,
        *,
        prompt: str,
        step_name: str,
        attempt_index: int,
        idempotency_key: str,
        previous_validator_details: Sequence[dict[str, Any]],
        correlation: dict[str, Any],
        request_fingerprint: str,
        recovered: RepositoryPhaseReceipt | None = None,
    ) -> PreparedAttempt:
        attempt_id = hashlib.sha256(idempotency_key.encode()).hexdigest()
        trace_dir = f"{self.spec.workspace}/.archetype-agent/traces"
        gate_dir = f"{self.spec.workspace}/.archetype-agent/gates"
        manifest_dir = f"{self.spec.workspace}/.archetype-agent/manifests"
        live_status_path, live_events_path = self._live_artifact_paths()
        directories = [trace_dir, gate_dir, manifest_dir]
        if live_status_path:
            directories.append(str(PurePosixPath(live_status_path).parent))
        await self._checked("mkdir", "-p", *directories)
        if recovered is not None:
            prepared = recovered.prepared
            if (
                prepared.attempt_id != attempt_id
                or prepared.idempotency_key != idempotency_key
                or prepared.request_fingerprint != request_fingerprint
            ):
                raise ValueError("repository phase receipt does not match the attempt identity")
            self._activate_attempt(prepared)
            return prepared

        baseline = (await self._git("rev-parse", "HEAD")).stdout.strip()
        start_manifest_path = await self._ensure_start_manifest()
        trace_path = (
            f"{trace_dir}/{self._safe_step(step_name)}-{attempt_index}-{attempt_id[:12]}.jsonl"
        )
        trace_stderr_path = f"{trace_path}.stderr"
        agent_prompt = (
            self._repair_prompt(prompt, previous_validator_details)
            if previous_validator_details
            else self._initial_prompt(prompt, step_name)
        )
        prepared = PreparedAttempt(
            attempt_id=attempt_id,
            request_fingerprint=request_fingerprint,
            idempotency_key=idempotency_key,
            attempt_index=attempt_index,
            step_name=step_name,
            prompt=prompt,
            agent_prompt=agent_prompt,
            correlation=correlation,
            baseline_sha=baseline,
            trace_dir=trace_dir,
            trace_path=trace_path,
            trace_stderr_path=trace_stderr_path,
            live_status_path=live_status_path,
            live_events_path=live_events_path,
            filesystem_start_path=start_manifest_path,
        )
        self._activate_attempt(prepared)
        return prepared

    def _activate_attempt(self, prepared: PreparedAttempt) -> None:
        self._active_trace_path = prepared.trace_path
        self._active_trace_stderr_path = prepared.trace_stderr_path
        self._live_context = {
            "attempt_id": prepared.attempt_id,
            "attempt_index": prepared.attempt_index,
            "step_name": prepared.step_name,
            "trace_path": prepared.trace_path,
            "trace_stderr_path": prepared.trace_stderr_path,
            "correlation": prepared.correlation,
        }

    async def _execution_phase(
        self,
        prepared: PreparedAttempt,
        previous_session_id: str,
        recovered: RepositoryPhaseReceipt | None = None,
    ) -> AgentExecution:
        if recovered is not None:
            await self._emit_live_event(
                "attempt_resumed",
                resumed_from=AttemptPhase.REPOSITORY_FINALIZATION.value,
                baseline_sha=prepared.baseline_sha,
            )
            return recovered.execution
        await self._emit_live_event("attempt_started", baseline_sha=prepared.baseline_sha)
        await self._emit_live_event(
            "agent_started",
            harness=self.spec.harness,
            resumed=bool(previous_session_id),
        )
        try:
            result = await self._run_agent(prepared.agent_prompt, session_id=previous_session_id)
        except BaseException as exc:
            await self._emit_live_event_safely(
                "agent_transport_failed",
                error_type=type(exc).__name__,
                error=self._tail(str(exc), 1000),
            )
            raise
        await self._emit_live_event("agent_finished", returncode=result.returncode)
        await self._write_text(prepared.trace_path, result.stdout)
        if result.stderr:
            await self._write_text(prepared.trace_stderr_path, result.stderr)

        session_id = self._session_id(result.stdout) or previous_session_id
        metadata = {
            "returncode": result.returncode,
            "completed": result.returncode == 0,
            "stdout_tail": self._tail(result.stdout) if result.returncode else "",
            "stderr_tail": self._tail(result.stderr) if result.returncode else "",
        }
        friction: tuple[dict[str, Any], ...] = ()
        if result.returncode != 0:
            friction = (
                {
                    "step": prepared.step_name,
                    "attempt": prepared.attempt_index,
                    "finding": (
                        f"{self.spec.harness} exited with code {result.returncode}; "
                        "authoritative validators still ran"
                    ),
                    "learning": self._tail(result.stderr or result.stdout, 1200),
                },
            )
        return AgentExecution(result, session_id, metadata, friction)

    async def _validation_phase(
        self,
        validators: Sequence[ValidatorSpec],
        recovered: RepositoryPhaseReceipt | None = None,
    ) -> ValidationEvidence:
        if recovered is not None:
            return ValidationEvidence(recovered.repository.details)
        return ValidationEvidence(tuple(await self._run_validators(validators)))

    async def _repository_finalization_phase(
        self,
        prepared: PreparedAttempt,
        execution: AgentExecution,
        validation: ValidationEvidence,
        recovered: RepositoryPhaseReceipt | None = None,
    ) -> RepositoryFinalization:
        if recovered is not None:
            return recovered.repository
        details = list(validation.details)
        accepted = validation.accepted
        sha = ""
        pushed = False
        message = f"{prepared.step_name}: {self._subject(prepared.prompt)}"
        if accepted:
            await self._emit_live_event("commit_started")
            try:
                sha = await self._commit_verified_tree(
                    prepared.step_name, prepared.prompt, prepared.baseline_sha
                )
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
        friction: tuple[dict[str, Any], ...] = ()
        if failed:
            friction = (
                {
                    "step": prepared.step_name,
                    "attempt": prepared.attempt_index,
                    "finding": "Gate failed: " + ", ".join(str(item["name"]) for item in failed),
                    "learning": self._failure_summary(failed),
                },
            )
        repository = RepositoryFinalization(
            accepted=accepted,
            details=tuple(details),
            commit_sha=sha,
            message=message,
            pushed=pushed,
            friction=friction,
        )
        await self._store_repository_receipt(
            RepositoryPhaseReceipt(
                request_fingerprint=prepared.request_fingerprint,
                prepared=prepared,
                execution=execution,
                repository=repository,
            )
        )
        return repository

    async def _evidence_phase(
        self,
        prepared: PreparedAttempt,
        execution: AgentExecution,
        repository: RepositoryFinalization,
    ) -> EvidenceCapture:
        await self._emit_live_event("evidence_capture_started")
        git_recovery = await self._capture_git_recovery(prepared.attempt_id, prepared.baseline_sha)
        context_path = f"{self.spec.workspace}/.context"
        context_exists = await self._exec("test", "-d", context_path, timeout=30)
        if context_exists.returncode != 0:
            context_path = ""
        filesystem_end_path, filesystem_diff_path = await self._capture_attempt_filesystem(
            prepared.step_name, prepared.attempt_index
        )
        attempt_manifest_path = (
            f"{self.spec.workspace}/.archetype-agent/manifests/{prepared.attempt_id}.json"
        )
        await self._checked("mkdir", "-p", str(PurePosixPath(attempt_manifest_path).parent))
        evidence = EvidenceCapture(
            attempt_manifest_path=attempt_manifest_path,
            trace_path=prepared.trace_path,
            trace_stderr_path=prepared.trace_stderr_path if execution.result.stderr else "",
            live_status_path=prepared.live_status_path,
            live_events_path=prepared.live_events_path,
            filesystem_start_path=prepared.filesystem_start_path,
            filesystem_end_path=filesystem_end_path,
            filesystem_diff_path=filesystem_diff_path,
            git_status_path=git_recovery["status"],
            git_patch_path=git_recovery["patch"],
            git_bundle_path=git_recovery["bundle"],
            context_path=context_path,
        )
        attempt_manifest = {
            "schema_version": 1,
            "attempt_id": prepared.attempt_id,
            "request_fingerprint": prepared.request_fingerprint,
            "idempotency_key": prepared.idempotency_key,
            "attempt_index": prepared.attempt_index,
            "step_name": prepared.step_name,
            "correlation": prepared.correlation,
            "status": "accepted" if repository.accepted else "rejected",
            "sandbox_id": self.sandbox_id,
            "harness": self.spec.harness,
            "agent_session_id": execution.session_id,
            "agent_execution": execution.metadata,
            "baseline_sha": prepared.baseline_sha,
            "commit_sha": repository.commit_sha,
            "pushed": repository.pushed,
            "validator_details": list(repository.details),
            "artifacts": {
                "trace": evidence.trace_path,
                "trace_stderr": evidence.trace_stderr_path,
                "live_status": evidence.live_status_path,
                "live_events": evidence.live_events_path,
                "filesystem_start": evidence.filesystem_start_path,
                "filesystem_end": evidence.filesystem_end_path,
                "filesystem_diff": evidence.filesystem_diff_path,
                "git_status": evidence.git_status_path,
                "git_patch": evidence.git_patch_path,
                "git_bundle": evidence.git_bundle_path,
                "context_directory": evidence.context_path,
            },
        }
        await self._write_text(attempt_manifest_path, json.dumps(attempt_manifest, sort_keys=True))
        await self._emit_live_event(
            "evidence_capture_finished",
            attempt_manifest_path=attempt_manifest_path,
            filesystem_diff_path=filesystem_diff_path,
            git_bundle_path=evidence.git_bundle_path,
        )
        return evidence

    async def _checkpoint_phase(self, prepared: PreparedAttempt) -> CheckpointCapture:
        created_at_ms = int(time.time() * 1000)
        error = ""
        await self._emit_live_event("checkpoint_started")
        try:
            ref = await self._snapshot_if_configured(prepared.attempt_id)
        except Exception as exc:
            ref = ""
            error = f"{type(exc).__name__}: {self._tail(str(exc), 2000)}"
        if ref:
            self._latest_checkpoint_ref = ref
        expires_at_ms = 0
        if ref and self.spec.snapshot_ttl_seconds is not None:
            expires_at_ms = created_at_ms + self.spec.snapshot_ttl_seconds * 1000
        restorable = bool(ref)
        status = "ready" if restorable else "failed" if error else "disabled"
        await self._emit_live_event(
            "checkpoint_finished",
            checkpoint_status=status,
            checkpoint_ref=ref,
            error=error,
        )
        friction: tuple[dict[str, Any], ...] = ()
        if error:
            friction = (
                {
                    "step": prepared.step_name,
                    "attempt": prepared.attempt_index,
                    "finding": "Provider checkpoint failed",
                    "learning": error,
                },
            )
        return CheckpointCapture(
            ref=ref,
            status=status,
            provider=self._checkpoint_provider(),
            restorable=restorable,
            error=error,
            created_at_ms=created_at_ms,
            expires_at_ms=expires_at_ms,
            friction=friction,
        )

    async def _artifact_handoff_phase(
        self,
        prepared: PreparedAttempt,
        execution: AgentExecution,
        repository: RepositoryFinalization,
        evidence: EvidenceCapture,
        checkpoint: CheckpointCapture,
    ) -> dict[str, Any]:
        """Declare source refs; authoritative indexing belongs to mission finalization."""

        await self._emit_live_event("artifact_handoff_started")
        handoff = self._declare_artifact_handoff(prepared, evidence, checkpoint)
        friction = (
            *execution.friction,
            *repository.friction,
            *checkpoint.friction,
        )
        outcome = {
            "attempt_id": prepared.attempt_id,
            "request_fingerprint": prepared.request_fingerprint,
            "idempotency_key": prepared.idempotency_key,
            "attempt_index": prepared.attempt_index,
            "attempts": prepared.attempt_index,
            "correlation": prepared.correlation,
            "status": "accepted" if repository.accepted else "rejected",
            "accepted": repository.accepted,
            "sha": repository.commit_sha,
            "baseline_sha": prepared.baseline_sha,
            "message": repository.message,
            "pushed": repository.pushed,
            "results": {detail["name"]: detail["passed"] for detail in repository.details},
            "validator_details": list(repository.details),
            **handoff.refs,
            "sandbox_state_ref": checkpoint.ref,
            "checkpoint_status": checkpoint.status,
            "checkpoint_provider": checkpoint.provider,
            "checkpoint_restorable": checkpoint.restorable,
            "checkpoint_error": checkpoint.error,
            "checkpoint_created_at_ms": checkpoint.created_at_ms,
            "checkpoint_expires_at_ms": checkpoint.expires_at_ms,
            "finalization_phase": handoff.finalization_phase,
            "finalization_error": handoff.finalization_error,
            "sandbox_id": self.sandbox_id,
            "harness": self.spec.harness,
            "agent_session_id": execution.session_id,
            "agent_returncode": execution.result.returncode,
            "agent_completed": execution.result.returncode == 0,
            "codex_thread_id": execution.session_id if self.spec.harness == "codex" else "",
            "claude_session_id": (
                execution.session_id if self.spec.harness == "claude-code" else ""
            ),
            "opencode_session_id": (
                execution.session_id if self.spec.harness == "opencode" else ""
            ),
            "friction": list(friction),
            "pr_url": "",
        }
        await self._store_completed(prepared.idempotency_key, outcome)
        self._completed[prepared.idempotency_key] = outcome
        await self._emit_live_event(
            "artifact_handoff_finished",
            finalization_phase=handoff.finalization_phase,
            declared_ref_count=sum(bool(value) for value in handoff.refs.values()),
        )
        return outcome

    def _declare_artifact_handoff(
        self,
        prepared: PreparedAttempt,
        evidence: EvidenceCapture,
        checkpoint: CheckpointCapture,
    ) -> ArtifactHandoff:
        refs = {
            "trace_ref": self._artifact_ref(checkpoint.ref, evidence.trace_path),
            "traces_ref": self._artifact_ref(checkpoint.ref, prepared.trace_dir),
            # Live refs intentionally include terminal events written after the checkpoint.
            "live_status_ref": (
                self._sandbox_uri(evidence.live_status_path) if evidence.live_status_path else ""
            ),
            "live_events_ref": (
                self._sandbox_uri(evidence.live_events_path) if evidence.live_events_path else ""
            ),
            "finalization_manifest_ref": self._artifact_ref(
                checkpoint.ref, evidence.attempt_manifest_path
            ),
            "filesystem_start_ref": self._artifact_ref(
                checkpoint.ref, evidence.filesystem_start_path
            ),
            "filesystem_end_ref": self._artifact_ref(checkpoint.ref, evidence.filesystem_end_path),
            "filesystem_diff_ref": self._artifact_ref(
                checkpoint.ref, evidence.filesystem_diff_path
            ),
            "git_status_ref": self._artifact_ref(checkpoint.ref, evidence.git_status_path),
            "git_patch_ref": self._artifact_ref(checkpoint.ref, evidence.git_patch_path),
            "git_bundle_ref": self._artifact_ref(checkpoint.ref, evidence.git_bundle_path),
            "context_ref": self._artifact_ref(checkpoint.ref, evidence.context_path),
        }
        return ArtifactHandoff(
            refs=refs,
            finalization_phase="checkpointed" if checkpoint.restorable else "captured",
            finalization_error=checkpoint.error,
        )

    @abstractmethod
    async def close(self) -> None:
        """Release the provider resource; repeated calls must be harmless."""

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
        if self.spec.harness == "opencode":
            return await self._run_opencode(prompt, session_id=session_id)
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
        argv = (
            ["codex", "exec", "resume", *common, session_id, prompt]
            if session_id
            else ["codex", "exec", *common, prompt]
        )
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

    async def _run_opencode(self, prompt: str, *, session_id: str) -> CommandResult:
        if not self.spec.model:
            raise ValueError("OpenCode requires an explicit model")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", self.spec.opencode_provider_id):
            raise ValueError(f"invalid OpenCode provider id: {self.spec.opencode_provider_id!r}")
        packages = {
            "chat-completions": "@ai-sdk/openai-compatible",
            "responses": "@ai-sdk/openai",
        }
        try:
            provider_package = packages[self.spec.opencode_wire_api]
        except KeyError as exc:
            raise ValueError(
                f"unsupported OpenCode wire API: {self.spec.opencode_wire_api!r}"
            ) from exc

        headers: dict[str, str] = {}
        for header, env_name in sorted(self.spec.opencode_header_env.items()):
            if not header.strip() or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", env_name):
                raise ValueError("OpenCode header environment bindings must be valid names")
            headers[header] = f"{{env:{env_name}}}"

        model_ref = f"{self.spec.opencode_provider_id}/{self.spec.model}"
        config = {
            "$schema": "https://opencode.ai/config.json",
            "model": model_ref,
            "share": "disabled",
            "permission": "allow",
            "provider": {
                self.spec.opencode_provider_id: {
                    "name": "Archetype OpenCode endpoint",
                    "npm": provider_package,
                    "options": {
                        "baseURL": self.spec.opencode_base_url,
                        "headers": headers,
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
        return await self._exec(
            *args,
            workdir=workdir,
            timeout=timeout,
            secrets=secrets,
            env=env,
        )

    async def _emit_live_event(self, event_type: str, **details: Any) -> None:
        del event_type, details

    def _live_artifact_paths(self) -> tuple[str, str]:
        return "", ""

    async def _emit_live_event_safely(self, event_type: str, **details: Any) -> None:
        try:
            await self._emit_live_event(event_type, **details)
        except BaseException:
            return

    async def _commit_verified_tree(self, step_name: str, prompt: str, baseline: str) -> str:
        current_head = (await self._git("rev-parse", "HEAD")).stdout.strip()
        if current_head != baseline:
            await self._git("reset", "--mixed", baseline)
            raise ValueError(
                "repository HEAD changed during agent execution; "
                "agent-authored commits are not accepted and were returned to the worktree"
            )
        status = (await self._git("status", "--porcelain")).stdout
        if status.strip():
            await self._git("add", "-A")
            await self._git(
                "-c",
                f"user.name={self.spec.git_author_name}",
                "-c",
                f"user.email={self.spec.git_author_email}",
                "commit",
                "-m",
                f"{step_name}: {self._subject(prompt)}",
            )
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

    def _artifact_ref(self, snapshot_ref: str, path: str) -> str:
        if not path:
            return ""
        return f"{snapshot_ref}#{path}" if snapshot_ref else self._sandbox_uri(path)

    async def _load_completed(self, key: str) -> dict[str, Any] | None:
        if key in self._completed:
            return self._completed[key]
        result = await self._exec("cat", self._receipt_path(key), timeout=30)
        if result.returncode != 0:
            return None
        try:
            outcome = json.loads(result.stdout)
        except json.JSONDecodeError:
            return None
        self._completed[key] = outcome
        return outcome

    async def _store_completed(self, key: str, outcome: dict[str, Any]) -> None:
        path = self._receipt_path(key)
        await self._checked("mkdir", "-p", str(PurePosixPath(path).parent))
        await self._write_text(path, json.dumps(outcome, sort_keys=True))

    async def _load_repository_receipt(self, key: str) -> RepositoryPhaseReceipt | None:
        result = await self._exec("cat", self._repository_receipt_path(key), timeout=30)
        if result.returncode != 0:
            return None
        try:
            value = json.loads(result.stdout)
            return self._parse_repository_receipt(value)
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            raise ValueError("repository phase receipt is invalid; refusing replay") from exc

    async def _store_repository_receipt(self, receipt: RepositoryPhaseReceipt) -> None:
        path = self._repository_receipt_path(receipt.prepared.idempotency_key)
        await self._checked("mkdir", "-p", str(PurePosixPath(path).parent))
        await self._write_text(
            path,
            json.dumps(self._repository_receipt_payload(receipt), sort_keys=True),
        )

    def _receipt_path(self, key: str) -> str:
        digest = hashlib.sha256(key.encode()).hexdigest()
        return f"{self.spec.workspace}/.archetype-agent/gates/{digest}.json"

    def _repository_receipt_path(self, key: str) -> str:
        return self._receipt_path(key).removesuffix(".json") + ".repository.json"

    @staticmethod
    def _repository_receipt_payload(receipt: RepositoryPhaseReceipt) -> dict[str, Any]:
        prepared = receipt.prepared
        execution = receipt.execution
        repository = receipt.repository
        return {
            "schema_version": 1,
            "phase": AttemptPhase.REPOSITORY_FINALIZATION.value,
            "request_fingerprint": receipt.request_fingerprint,
            "prepared": {
                "attempt_id": prepared.attempt_id,
                "request_fingerprint": prepared.request_fingerprint,
                "idempotency_key": prepared.idempotency_key,
                "attempt_index": prepared.attempt_index,
                "step_name": prepared.step_name,
                "prompt": prepared.prompt,
                "agent_prompt": prepared.agent_prompt,
                "correlation": prepared.correlation,
                "baseline_sha": prepared.baseline_sha,
                "trace_dir": prepared.trace_dir,
                "trace_path": prepared.trace_path,
                "trace_stderr_path": prepared.trace_stderr_path,
                "live_status_path": prepared.live_status_path,
                "live_events_path": prepared.live_events_path,
                "filesystem_start_path": prepared.filesystem_start_path,
            },
            "execution": {
                "argv": list(execution.result.argv),
                "returncode": execution.result.returncode,
                "stderr_present": bool(execution.result.stderr),
                "session_id": execution.session_id,
                "metadata": execution.metadata,
                "friction": list(execution.friction),
            },
            "repository": {
                "accepted": repository.accepted,
                "details": list(repository.details),
                "commit_sha": repository.commit_sha,
                "message": repository.message,
                "pushed": repository.pushed,
                "friction": list(repository.friction),
            },
        }

    @staticmethod
    def _parse_repository_receipt(value: Any) -> RepositoryPhaseReceipt:
        if not isinstance(value, dict):
            raise TypeError("repository receipt must be a JSON object")
        if value["schema_version"] != 1:
            raise ValueError("unsupported repository receipt schema")
        if value["phase"] != AttemptPhase.REPOSITORY_FINALIZATION.value:
            raise ValueError("repository receipt has the wrong phase")

        prepared_value = value["prepared"]
        execution_value = value["execution"]
        repository_value = value["repository"]
        if not all(
            isinstance(item, dict) for item in (prepared_value, execution_value, repository_value)
        ):
            raise TypeError("repository receipt sections must be JSON objects")

        correlation = prepared_value["correlation"]
        metadata = execution_value["metadata"]
        details = repository_value["details"]
        execution_friction = execution_value["friction"]
        repository_friction = repository_value["friction"]
        if not isinstance(correlation, dict) or not isinstance(metadata, dict):
            raise TypeError("repository receipt mappings are invalid")
        if not all(
            isinstance(items, list) and all(isinstance(item, dict) for item in items)
            for items in (details, execution_friction, repository_friction)
        ):
            raise TypeError("repository receipt evidence must be lists of objects")

        prepared = PreparedAttempt(
            attempt_id=str(prepared_value["attempt_id"]),
            request_fingerprint=str(prepared_value["request_fingerprint"]),
            idempotency_key=str(prepared_value["idempotency_key"]),
            attempt_index=int(prepared_value["attempt_index"]),
            step_name=str(prepared_value["step_name"]),
            prompt=str(prepared_value["prompt"]),
            agent_prompt=str(prepared_value["agent_prompt"]),
            correlation=dict(correlation),
            baseline_sha=str(prepared_value["baseline_sha"]),
            trace_dir=str(prepared_value["trace_dir"]),
            trace_path=str(prepared_value["trace_path"]),
            trace_stderr_path=str(prepared_value["trace_stderr_path"]),
            live_status_path=str(prepared_value["live_status_path"]),
            live_events_path=str(prepared_value["live_events_path"]),
            filesystem_start_path=str(prepared_value["filesystem_start_path"]),
        )
        execution = AgentExecution(
            result=CommandResult(
                argv=tuple(str(item) for item in execution_value["argv"]),
                returncode=int(execution_value["returncode"]),
                stdout="",
                stderr="captured" if bool(execution_value["stderr_present"]) else "",
            ),
            session_id=str(execution_value["session_id"]),
            metadata=dict(metadata),
            friction=tuple(dict(item) for item in execution_friction),
        )
        repository = RepositoryFinalization(
            accepted=bool(repository_value["accepted"]),
            details=tuple(dict(item) for item in details),
            commit_sha=str(repository_value["commit_sha"]),
            message=str(repository_value["message"]),
            pushed=bool(repository_value["pushed"]),
            friction=tuple(dict(item) for item in repository_friction),
        )
        fingerprint = str(value["request_fingerprint"])
        if fingerprint != prepared.request_fingerprint:
            raise ValueError("repository receipt fingerprint is inconsistent")
        return RepositoryPhaseReceipt(fingerprint, prepared, execution, repository)

    @staticmethod
    def _request_fingerprint(
        *,
        prompt: str,
        validators: Sequence[ValidatorSpec],
        step_name: str,
        attempt_index: int,
        previous_session_id: str,
        previous_validator_details: Sequence[dict[str, Any]],
        correlation: Mapping[str, Any],
    ) -> str:
        payload = {
            "prompt": prompt,
            "validators": [validator.to_dict() for validator in validators],
            "step_name": step_name,
            "attempt_index": attempt_index,
            "previous_session_id": previous_session_id,
            "previous_validator_details": list(previous_validator_details),
            "correlation": dict(correlation),
        }
        try:
            encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        except (TypeError, ValueError) as exc:
            raise ValueError("attempt request values must be JSON serializable") from exc
        return hashlib.sha256(encoded.encode()).hexdigest()

    async def _git(self, *args: str) -> CommandResult:
        result = await self._exec("git", *args, workdir=self.spec.workspace)
        self._raise_for_result(result, f"git {' '.join(args[:2])}")
        return result

    async def _checked(self, *args: str, **kwargs: Any) -> CommandResult:
        result = await self._exec(*args, **kwargs)
        self._raise_for_result(result, args[0])
        return result

    @abstractmethod
    async def _exec(
        self,
        *args: str,
        workdir: str | None = None,
        timeout: int | None = None,
        secrets: Sequence[Any] = (),
        env: dict[str, str] | None = None,
    ) -> CommandResult:
        """Execute a command through the provider adapter."""

    @abstractmethod
    async def _write_text(self, path: str, value: str) -> None:
        """Write UTF-8 text inside the sandbox."""

    @abstractmethod
    async def _snapshot_if_configured(self, checkpoint_key: str = "") -> str:
        """Return a provider checkpoint reference or an empty disabled reference."""

    @abstractmethod
    def _checkpoint_provider(self) -> str:
        """Return the stable provider identifier for checkpoint references."""

    @abstractmethod
    def _sandbox_uri(self, path: str) -> str:
        """Return a live provider URI for a path."""

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
            detail = (result.stderr or result.stdout)[-4000:]
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
            "The authoritative task gate failed after your previous turn. The original task is:\n"
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

    @classmethod
    def _failure_summary(cls, details: Sequence[dict[str, Any]]) -> str:
        failed = [item for item in details if not item.get("passed")]
        if not failed:
            return "unknown failure"
        return "; ".join(
            f"{item.get('name', 'command')} exit={item.get('returncode')}: "
            f"{cls._tail(str(item.get('stderr') or item.get('stdout') or ''), 600)}"
            for item in failed
        )
