#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Run manifest-selected operational scenarios and write structured evidence."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import platform
import re
import shutil
import signal
import site
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, NoReturn, cast

from packaging.utils import canonicalize_name, parse_wheel_filename

if __package__:
    from scripts.process_lease_guardian import (
        ACK_DIR_ENV as PROCESS_LEASE_ACK_DIR_ENV,
    )
    from scripts.process_lease_guardian import (
        CLOSED_ENV as PROCESS_LEASE_CLOSED_ENV,
    )
    from scripts.process_lease_guardian import (
        LEASE_ENV as PROCESS_LEASE_ENV,
    )
    from scripts.process_lease_guardian import (
        READY_ENV as PROCESS_LEASE_READY_ENV,
    )
    from scripts.process_lease_guardian import (
        RESULT_SCHEMA as PROCESS_LEASE_RESULT_SCHEMA,
    )
    from scripts.process_lease_guardian import (
        WRAPPER_ENV as PROCESS_LEASE_WRAPPER_ENV,
    )
    from scripts.process_lease_guardian import (
        guard as reap_process_leases,
    )
    from scripts.run_example_receipt import CAPTURED_RECEIPT_ENV, RECEIPT_PREFIX
    from scripts.validate_operational_scenarios import (
        REGISTRY,
        ROOT,
        load_scenarios,
        validate_operational_scenarios,
    )
else:
    # ``python scripts/run_operational_scenarios.py`` places only ``scripts/``
    # on sys.path. Keep the CLI executable without adding the repository root
    # (and therefore its source tree) to installed-wheel scenario imports.
    from process_lease_guardian import (
        ACK_DIR_ENV as PROCESS_LEASE_ACK_DIR_ENV,
    )
    from process_lease_guardian import (
        CLOSED_ENV as PROCESS_LEASE_CLOSED_ENV,
    )
    from process_lease_guardian import (
        LEASE_ENV as PROCESS_LEASE_ENV,
    )
    from process_lease_guardian import (
        READY_ENV as PROCESS_LEASE_READY_ENV,
    )
    from process_lease_guardian import (
        RESULT_SCHEMA as PROCESS_LEASE_RESULT_SCHEMA,
    )
    from process_lease_guardian import (
        WRAPPER_ENV as PROCESS_LEASE_WRAPPER_ENV,
    )
    from process_lease_guardian import (
        guard as reap_process_leases,
    )
    from run_example_receipt import CAPTURED_RECEIPT_ENV, RECEIPT_PREFIX
    from validate_operational_scenarios import (
        REGISTRY,
        ROOT,
        load_scenarios,
        validate_operational_scenarios,
    )

RESULT_SCHEMA = "archetype.operational-results/v1"
WORKSPACE_DISTRIBUTIONS: tuple[tuple[str, str, str], ...] = (
    ("archetype-ecs", "packages/archetype-ecs/src", "archetype"),
    ("archetype-missions", "packages/archetype-missions/src", "archetype.missions"),
    (
        "archetype-physical-ai",
        "packages/archetype-physical-ai/src",
        "archetype.physical_ai",
    ),
    ("archetype-research", "packages/archetype-research/src", "archetype.research"),
)
_PROCESS_TERM_GRACE_SECONDS = 2.0
_PROCESS_KILL_GRACE_SECONDS = 2.0
_PROCESS_LEASE_GUARDIAN_ENV = "ARCHETYPE_OPERATIONAL_PROCESS_LEASE_GUARDIAN"
_PROCESS_LEASE_GUARDIAN_TIMEOUT_SECONDS = 20.0
_PROCESS_LEASE_READY_TIMEOUT_SECONDS = 5.0


@dataclass(frozen=True)
class WheelArtifact:
    """One exact first-party wheel selected for an installed scenario."""

    distribution: str
    path: Path
    version: str


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _sha256_bytes(value: bytes) -> str:
    return f"sha256:{hashlib.sha256(value).hexdigest()}"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return f"sha256:{digest.hexdigest()}"


def _strict_json_object(encoded: str, *, label: str) -> dict[str, Any]:
    """Decode a non-empty JSON object without accepting NaN or Infinity."""

    def reject_constant(value: str) -> None:
        raise ValueError(f"non-standard JSON constant {value}")

    try:
        payload = json.loads(encoded, parse_constant=reject_constant)
    except (json.JSONDecodeError, ValueError) as exc:
        raise ValueError(f"{label} is not strict JSON: {exc}") from exc
    if not isinstance(payload, dict) or not payload:
        raise TypeError(f"{label} must be a non-empty JSON object")
    return payload


def _portable_error(exc: Exception) -> str:
    rendered = f"{type(exc).__name__}: {exc}"
    temporary_roots = {
        tempfile.gettempdir(),
        str(Path(tempfile.gettempdir()).resolve()),
    }
    for temporary_root in sorted(temporary_roots, key=len, reverse=True):
        rendered = re.sub(
            rf"{re.escape(temporary_root)}/archetype-operational-[^\s'\"\]]+",
            "<isolated-run>",
            rendered,
        )
    return rendered


def _remove_run_root(run_root: Path) -> None:
    """Remove runner-owned isolation state through one injectable cleanup seam."""

    shutil.rmtree(run_root)


def _git_state(root: Path) -> tuple[str, bool]:
    revision = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    dirty = bool(
        subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=all"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    )
    return revision, dirty


def _tested_subject_record(
    *,
    root: Path,
    tested_checkout: Path,
    revision: str | None,
    dirty: bool | None,
) -> dict[str, object]:
    resolved_root = root.resolve()
    resolved_tested = tested_checkout.resolve()
    return {
        "commit": revision,
        "dirty": dirty,
        "checkout": str(resolved_tested),
        "checkout_relationship": (
            "same_as_harness" if resolved_tested == resolved_root else "distinct_from_harness"
        ),
    }


def _package_subject_record(tested_subject: dict[str, object]) -> dict[str, object]:
    return {field: tested_subject[field] for field in ("commit", "dirty", "checkout_relationship")}


def _validate_distinct_wheel_location(
    *,
    wheel: Path,
    root: Path,
    tested_checkout: Path,
) -> None:
    resolved_root = root.resolve()
    resolved_tested = tested_checkout.resolve()
    if resolved_tested == resolved_root:
        return
    if not wheel.resolve().is_relative_to(resolved_tested):
        raise ValueError(
            "a distinct tested-checkout wheel must be located under that checkout: "
            f"{wheel.resolve()} is outside {resolved_tested}"
        )


def _workspace_source_roots(checkout: Path) -> tuple[Path, ...]:
    """Return all world-stack source roots in deterministic distribution order."""

    return tuple((checkout / relative).resolve() for _, relative, _ in WORKSPACE_DISTRIBUTIONS)


def _resolve_wheel_artifacts(
    *,
    wheel: Path,
    wheel_dir: Path | None,
) -> tuple[WheelArtifact, ...]:
    """Resolve one same-version local wheel for each world-stack distribution."""

    anchor = wheel.resolve()
    if not anchor.is_file():
        raise ValueError("--wheel must name the built archetype-ecs wheel in wheel mode")
    try:
        anchor_name, anchor_version, _anchor_build, _anchor_tags = parse_wheel_filename(anchor.name)
    except ValueError as exc:
        raise ValueError(f"--wheel is not a valid wheel filename: {anchor.name}") from exc
    if canonicalize_name(anchor_name) != "archetype-ecs":
        raise ValueError("--wheel must name the archetype-ecs wheel anchor")

    artifact_root = (wheel_dir or anchor.parent).resolve()
    if not artifact_root.is_dir():
        raise ValueError(f"--wheel-dir must name a directory: {artifact_root}")

    expected = tuple(distribution for distribution, _source, _module in WORKSPACE_DISTRIBUTIONS)
    candidates: dict[str, list[tuple[Path, str]]] = {name: [] for name in expected}
    for candidate in sorted(artifact_root.glob("*.whl")):
        try:
            distribution, version, _build, _tags = parse_wheel_filename(candidate.name)
        except ValueError:
            continue
        canonical = canonicalize_name(distribution)
        if canonical in candidates:
            candidates[canonical].append((candidate.resolve(), str(version)))

    artifacts: list[WheelArtifact] = []
    for distribution in expected:
        matches = candidates[distribution]
        if len(matches) != 1:
            raise ValueError(
                "wheel mode requires exactly one local wheel for "
                f"{distribution} under {artifact_root}; found {len(matches)}"
            )
        path, version = matches[0]
        if version != str(anchor_version):
            raise ValueError(
                "wheel mode requires one same-version first-party artifact set; "
                f"{distribution} is {version}, archetype-ecs is {anchor_version}"
            )
        artifacts.append(WheelArtifact(distribution, path, version))

    framework = artifacts[0]
    if framework.path != anchor:
        raise ValueError(
            "--wheel must be the exact archetype-ecs artifact selected from --wheel-dir"
        )
    return tuple(artifacts)


def _wheel_artifact_records(
    artifacts: tuple[WheelArtifact, ...],
) -> list[dict[str, str]]:
    return [
        {
            "distribution": artifact.distribution,
            "filename": artifact.path.name,
            "digest": _sha256_file(artifact.path),
        }
        for artifact in artifacts
    ]


def _has_run_demo(path: Path) -> bool:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (OSError, SyntaxError):
        return False
    return any(
        isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "run_demo"
        for node in tree.body
    )


def _service_available(name: str) -> bool:
    commands = {
        "docker": ["docker", "info"],
        "apple-container": ["container", "system", "status"],
    }
    command = commands.get(name)
    if command is None or shutil.which(command[0]) is None:
        return False
    try:
        return (
            subprocess.run(
                command,
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=10,
            ).returncode
            == 0
        )
    except (OSError, subprocess.TimeoutExpired):
        return False


def missing_prerequisites(row: dict[str, Any]) -> list[str]:
    """Return typed prerequisites not satisfied by this process host."""

    missing: list[str] = []
    for encoded in row["prerequisites"]:
        kind, name = encoded.split(":", 1)
        available = False
        if kind in {"credential", "infrastructure"}:
            available = bool(os.environ.get(name))
        elif kind == "binary":
            available = shutil.which(name) is not None
        elif kind == "platform":
            available = sys.platform == name or platform.system().lower() == name.lower()
        elif kind == "service":
            available = _service_available(name)
        if not available:
            missing.append(encoded)
    return missing


def _scenario_environment(
    base: dict[str, str],
    row: dict[str, Any],
) -> dict[str, str]:
    """Build the scenario environment, including explicit external-lane gates."""

    env = base.copy()
    prerequisites = set(row["prerequisites"])
    if "service:docker" in prerequisites:
        env["ARCHETYPE_DOCKER_SANDBOX_PARITY"] = "1"
    if "service:apple-container" in prerequisites:
        env["ARCHETYPE_APPLE_CONTAINER_SANDBOX_PARITY"] = "1"
    if row.get("id") == "dogfood.agent_mission.modal_live":
        env["ARCHETYPE_MODAL_AGENT_MISSION_LIVE"] = "1"
    if row.get("id") == "example.14_biome_agent":
        env[_PROCESS_LEASE_GUARDIAN_ENV] = "1"
    return env


def _process_group_alive(process_group: int) -> bool:
    try:
        os.killpg(process_group, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _signal_process_group(process_group: int, requested_signal: signal.Signals) -> None:
    try:
        os.killpg(process_group, requested_signal)
    except (PermissionError, ProcessLookupError):
        # Darwin rejects a group-wide signal when any remaining member is not
        # signalable. The bounded liveness poll still has to prove ESRCH; a
        # persistent permission denial is therefore reported as cleanup debt.
        pass


def _wait_for_process_group_exit(process_group: int, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _process_group_alive(process_group):
            return True
        time.sleep(0.05)
    return not _process_group_alive(process_group)


def _terminate_process_group(process_group: int) -> bool:
    """Terminate an already-reaped leader's remaining group; report closure."""

    if not _process_group_alive(process_group):
        return True
    _signal_process_group(process_group, signal.SIGTERM)
    if _wait_for_process_group_exit(process_group, _PROCESS_TERM_GRACE_SECONDS):
        return True
    _signal_process_group(process_group, signal.SIGKILL)
    return _wait_for_process_group_exit(process_group, _PROCESS_KILL_GRACE_SECONDS)


def _stop_timed_out_process(process: subprocess.Popen[bytes]) -> tuple[int | None, bool]:
    """Stop and reap a timed-out leader, then close its whole process group."""

    _signal_process_group(process.pid, signal.SIGTERM)
    try:
        returncode: int | None = process.wait(timeout=_PROCESS_TERM_GRACE_SECONDS)
    except subprocess.TimeoutExpired:
        _signal_process_group(process.pid, signal.SIGKILL)
        try:
            process.kill()
        except ProcessLookupError:
            pass
        try:
            returncode = process.wait(timeout=_PROCESS_KILL_GRACE_SECONDS)
        except subprocess.TimeoutExpired:
            returncode = process.poll()
    group_closed = _terminate_process_group(process.pid)
    return returncode, group_closed


def _wait_for_guardian_ready(
    guardian: subprocess.Popen[bytes],
    ready_file: Path,
) -> bool:
    deadline = time.monotonic() + _PROCESS_LEASE_READY_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if ready_file.is_file():
            return guardian.poll() is None
        if guardian.poll() is not None:
            return False
        time.sleep(0.02)
    return ready_file.is_file() and guardian.poll() is None


def _wait_for_guarded_process(
    process: subprocess.Popen[bytes],
    *,
    timeout_seconds: int,
    guardian: subprocess.Popen[bytes] | None,
) -> int:
    """Wait while proving an enabled nested-process guardian remains live."""

    deadline = time.monotonic() + timeout_seconds
    while True:
        if guardian is not None and guardian.poll() is not None:
            raise RuntimeError("process lease guardian exited while the scenario was running")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise subprocess.TimeoutExpired(process.args, timeout_seconds)
        try:
            return process.wait(timeout=min(remaining, 0.25))
        except subprocess.TimeoutExpired:
            pass


def _process_group_snapshot(process_group: int) -> str:
    """One line per still-running group member: pid, age, command.

    Best-effort diagnostics only — a snapshot failure must never mask the
    timeout it is trying to explain.
    """
    try:
        listing = subprocess.run(
            ["ps", "-eo", "pid=,pgid=,etime=,command="],
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        ).stdout
    except (OSError, subprocess.SubprocessError) as exc:
        return f"(snapshot unavailable: {type(exc).__name__}: {exc})"
    members = []
    for line in listing.splitlines():
        fields = line.split(maxsplit=3)
        if len(fields) == 4 and fields[1] == str(process_group):
            pid, _, etime, command = fields
            members.append(f"  pid={pid} age={etime} cmd={command}")
    return "\n".join(members) if members else "(no group members visible)"


_FAILURE_TAIL_BYTES = 16_384
_SECRET_ENV_NAME = re.compile(r"KEY|TOKEN|SECRET|PASSWORD|CREDENTIAL")
_SECRET_MIN_LENGTH = 8


def _secret_environment_values(env: dict[str, str]) -> list[tuple[str, str]]:
    """Env values that must never appear in retained output, longest first.

    Longest-first so a value that contains another (a token embedding a key
    id) is masked before its substring turns the remainder unrecognizable.
    Short values are excluded: masking every 3-character string would
    destroy the log, and no real credential is that short.
    """
    values = [
        (name, value)
        for name, value in env.items()
        if _SECRET_ENV_NAME.search(name) and len(value) >= _SECRET_MIN_LENGTH
    ]
    return sorted(values, key=lambda item: len(item[1]), reverse=True)


def _write_captured_log(
    *,
    raw: bytes,
    destination: Path,
    redacted: bool,
    failed: bool = False,
    secret_values: list[tuple[str, str]] | None = None,
) -> dict[str, object]:
    destination.parent.mkdir(parents=True, exist_ok=True)
    failure_tail_retained = False
    if redacted and failed and raw:
        # Redact secrets, not the assertion: a redacted receipt on a FAILED
        # run previously discarded the entire output, which made the two R2
        # incidents on #675/#678 undiagnosable ("exact assertion is
        # unrecoverable"). Retain a bounded tail with credential values
        # masked; digest and byte count still describe the full raw output.
        tail = raw[-_FAILURE_TAIL_BYTES:]
        text = tail.decode("utf-8", errors="replace")
        for name, value in secret_values or ():
            text = text.replace(value, f"***{name}***")
        destination.write_text(
            "Redacted receipt: run FAILED, so the output tail is retained "
            "with credential values masked.\n"
            f"(last {len(tail)} of {len(raw)} bytes)\n---\n" + text,
            encoding="utf-8",
        )
        failure_tail_retained = True
    elif redacted:
        destination.write_text(
            "Output omitted by redacted_receipt policy.\n",
            encoding="utf-8",
        )
    else:
        destination.write_bytes(raw)
    return {
        "path": str(destination),
        "bytes": len(raw),
        "digest": _sha256_bytes(raw),
        "redacted": redacted,
        "failure_tail_retained": failure_tail_retained,
    }


def _run_process(
    command: list[str],
    *,
    cwd: Path,
    env: dict[str, str],
    timeout_seconds: int,
    log_prefix: Path,
    redacted: bool,
) -> dict[str, object]:
    started = time.perf_counter()
    with tempfile.TemporaryDirectory(prefix="archetype-operational-capture-") as capture:
        capture_root = Path(capture)
        stdout_path = capture_root / "stdout"
        stderr_path = capture_root / "stderr"
        lease_file = capture_root / "process-leases.jsonl"
        lease_ack_dir = capture_root / "process-lease-acks"
        lease_ready_file = capture_root / "process-lease-ready.json"
        lease_closed_file = capture_root / "process-lease-closed.json"
        lease_result_file = capture_root / "process-lease-cleanup.json"
        launch_error: str | None = None
        process: subprocess.Popen[bytes] | None = None
        guardian: subprocess.Popen[bytes] | None = None
        process_lease_cleanup: dict[str, object] | None = None
        timed_out = False
        process_group_leaked = False
        process_env = env.copy()
        lease_guardian_required = process_env.get(_PROCESS_LEASE_GUARDIAN_ENV) == "1"
        with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
            if lease_guardian_required:
                try:
                    guardian = subprocess.Popen(
                        [
                            sys.executable,
                            str(Path(__file__).with_name("process_lease_guardian.py").resolve()),
                            "serve",
                            "--lease-file",
                            str(lease_file),
                            "--ack-dir",
                            str(lease_ack_dir),
                            "--ready-file",
                            str(lease_ready_file),
                            "--closed-file",
                            str(lease_closed_file),
                            "--result-file",
                            str(lease_result_file),
                        ],
                        stdin=subprocess.PIPE,
                        stdout=subprocess.DEVNULL,
                        stderr=stderr,
                        start_new_session=True,
                    )
                except OSError as exc:
                    launch_error = f"process lease guardian failed to launch: {exc}"
                else:
                    if not _wait_for_guardian_ready(guardian, lease_ready_file):
                        launch_error = "process lease guardian did not become ready"
                    else:
                        process_env[PROCESS_LEASE_ENV] = str(lease_file)
                        process_env[PROCESS_LEASE_ACK_DIR_ENV] = str(lease_ack_dir)
                        process_env[PROCESS_LEASE_READY_ENV] = str(lease_ready_file)
                        process_env[PROCESS_LEASE_CLOSED_ENV] = str(lease_closed_file)
                        process_env[PROCESS_LEASE_WRAPPER_ENV] = str(
                            Path(__file__).with_name("process_lease_guardian.py").resolve()
                        )

            returncode: int | None = None
            try:
                if launch_error is None:
                    try:
                        process = subprocess.Popen(
                            command,
                            cwd=cwd,
                            env=process_env,
                            stdout=stdout,
                            stderr=stderr,
                            start_new_session=True,
                        )
                    except OSError as exc:
                        launch_error = f"{type(exc).__name__}: {exc}"
                    else:
                        try:
                            returncode = _wait_for_guarded_process(
                                process,
                                timeout_seconds=timeout_seconds,
                                guardian=guardian,
                            )
                        except subprocess.TimeoutExpired:
                            timed_out = True
                            # Photograph the group BEFORE killing it: a scenario that
                            # hangs silently for its whole budget (#678's 600s R2
                            # stall) otherwise leaves a receipt naming nothing. The
                            # snapshot names the processes that were still running.
                            timeout_snapshot = _process_group_snapshot(process.pid)
                            returncode, group_closed = _stop_timed_out_process(process)
                            process_group_leaked = not group_closed
                        except RuntimeError as exc:
                            launch_error = str(exc)
                            returncode, group_closed = _stop_timed_out_process(process)
                            process_group_leaked = not group_closed
            except BaseException:
                if process is not None and process.poll() is None:
                    _stop_timed_out_process(process)
                raise
            finally:
                if guardian is not None:
                    assert guardian.stdin is not None
                    try:
                        guardian.stdin.close()
                    except BrokenPipeError:
                        pass
                    try:
                        guardian_returncode = guardian.wait(
                            timeout=_PROCESS_LEASE_GUARDIAN_TIMEOUT_SECONDS
                        )
                    except subprocess.TimeoutExpired:
                        _stop_timed_out_process(guardian)
                        guardian_returncode = guardian.poll()
                        launch_error = "process lease guardian timed out during cleanup"
                        process_group_leaked = True
                    if not lease_result_file.is_file():
                        try:
                            fallback_closed = reap_process_leases(lease_file, lease_result_file)
                        except Exception as exc:
                            launch_error = (
                                "process lease fallback cleanup failed: "
                                f"{type(exc).__name__}: {exc}"
                            )
                            process_group_leaked = True
                        else:
                            guardian_returncode = 0 if fallback_closed else 1
                    if lease_result_file.is_file():
                        try:
                            process_lease_cleanup = _strict_json_object(
                                lease_result_file.read_text(encoding="utf-8"),
                                label="process lease cleanup result",
                            )
                        except (OSError, TypeError, ValueError) as exc:
                            launch_error = (
                                f"invalid process lease cleanup result: {type(exc).__name__}: {exc}"
                            )
                            process_group_leaked = True
                    else:
                        launch_error = "process lease guardian emitted no cleanup result"
                        process_group_leaked = True
                    if process_lease_cleanup is not None:
                        lease_status = process_lease_cleanup.get("status")
                        lease_schema = process_lease_cleanup.get("schema")
                        active_count = process_lease_cleanup.get("active_lease_count")
                        if (
                            guardian_returncode != 0
                            or lease_schema != PROCESS_LEASE_RESULT_SCHEMA
                            or lease_status != "closed"
                            or not isinstance(active_count, int)
                            or isinstance(active_count, bool)
                            or active_count < 0
                        ):
                            launch_error = "process lease guardian could not prove cleanup"
                            process_group_leaked = True
                        elif active_count and not timed_out:
                            # A normal process exit must release its own leases.
                            # The guardian recovered the host, but the scenario
                            # still violated its cleanup contract.
                            process_group_leaked = True

        stdout_raw = stdout_path.read_bytes()
        stderr_raw = stderr_path.read_bytes()
        if launch_error is not None:
            stderr_raw += f"\noperational launch error: {launch_error}\n".encode()
        if timed_out:
            stderr_raw += (
                f"\noperational timeout: no exit within {timeout_seconds}s; "
                f"process-group snapshot at SIGTERM:\n{timeout_snapshot}\n"
            ).encode()
        if process is not None and not timed_out:
            # Give well-behaved children a short exit window. Anything retaining
            # the scenario's process group afterward is owned cleanup debt even
            # when the runner succeeds in killing it.
            group_closed_naturally = _wait_for_process_group_exit(
                process.pid,
                _PROCESS_TERM_GRACE_SECONDS,
            )
            if not group_closed_naturally:
                cleanup_succeeded = _terminate_process_group(process.pid)
                process_group_leaked = True
                if not cleanup_succeeded:
                    launch_error = "process group survived SIGTERM and SIGKILL"

    failed = timed_out or launch_error is not None or returncode != 0
    secret_values = _secret_environment_values(process_env)
    result: dict[str, object] = {
        "command": command,
        "returncode": returncode,
        "timed_out": timed_out,
        "launch_error": launch_error,
        "duration_seconds": round(time.perf_counter() - started, 6),
        "stdout": _write_captured_log(
            raw=stdout_raw,
            destination=log_prefix.with_suffix(".stdout.log"),
            redacted=redacted,
            failed=failed,
            secret_values=secret_values,
        ),
        "stderr": _write_captured_log(
            raw=stderr_raw,
            destination=log_prefix.with_suffix(".stderr.log"),
            redacted=redacted,
            failed=failed,
            secret_values=secret_values,
        ),
        "stdout_text": stdout_raw.decode("utf-8", errors="replace"),
        "process_group_leaked": process_group_leaked,
    }
    if process_lease_cleanup is not None:
        result["process_leases"] = process_lease_cleanup
    return result


def _base_environment(
    root: Path,
    *,
    mode: str,
    tested_checkout: Path | None = None,
) -> dict[str, str]:
    env = os.environ.copy()
    env.pop("PYTHONHOME", None)
    env.pop("PYTHONPATH", None)
    env.pop("PYTHONSTARTUP", None)
    env.pop("PYTHONINSPECT", None)
    env.pop("VIRTUAL_ENV", None)
    env["PYTHONNOUSERSITE"] = "1"
    if mode == "source":
        subject_root = (tested_checkout or root).resolve()
        env["PYTHONPATH"] = os.pathsep.join(
            str(path) for path in (*_workspace_source_roots(subject_root), root.resolve())
        )
    return env


def _prepare_wheel_python(
    wheels: tuple[Path, ...],
    destination: Path,
) -> tuple[Path, dict[str, str]]:
    if len(wheels) != len(WORKSPACE_DISTRIBUTIONS):
        raise ValueError("installed-wheel scenarios require all four world-stack wheels")
    uv = shutil.which("uv")
    if uv is None:
        raise RuntimeError("installed-wheel scenarios require uv")
    environment = destination / "wheel-venv"
    subprocess.run(
        [uv, "venv", "--python", sys.executable, str(environment)],
        cwd=destination,
        check=True,
        capture_output=True,
        text=True,
    )
    python = environment / "bin" / "python"
    subprocess.run(
        [
            uv,
            "pip",
            "install",
            "--python",
            str(python),
            "--no-deps",
            *(str(wheel.resolve()) for wheel in wheels),
        ],
        cwd=destination,
        check=True,
        capture_output=True,
        text=True,
    )
    child_site = subprocess.run(
        [str(python), "-c", "import site; print(site.getsitepackages()[0])"],
        cwd=destination,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    dependency_sites = [path for path in site.getsitepackages() if Path(path).is_dir()]
    Path(child_site, "_archetype_operational_dependencies.pth").write_text(
        "".join(f"{path}\n" for path in dependency_sites),
        encoding="utf-8",
    )
    env = _base_environment(destination, mode="wheel")
    env["VIRTUAL_ENV"] = str(environment)
    env["PATH"] = os.pathsep.join((str(environment / "bin"), env.get("PATH", "")))
    return python, env


def _package_probe(
    python: Path,
    *,
    cwd: Path,
    env: dict[str, str],
    root: Path,
    tested_checkout: Path,
    mode: str,
) -> dict[str, object]:
    probe = (
        "import importlib.util, json, pathlib, sys, archetype;"
        "modules=('archetype.missions','archetype.physical_ai','archetype.research');"
        "specs={name: importlib.util.find_spec(name) for name in modules};"
        "print(json.dumps({'version': archetype.__version__,"
        "'path': str(pathlib.Path(archetype.__file__).resolve()),"
        "'library_paths': {name: (str(pathlib.Path(spec.origin).resolve()) "
        "if spec is not None and spec.origin is not None else None) "
        "for name, spec in specs.items()},'sys_path': sys.path}))"
    )
    completed = subprocess.run(
        [str(python), "-c", probe],
        cwd=cwd,
        env=env,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    payload = json.loads(completed.stdout.strip().splitlines()[-1])
    package_path = Path(payload["path"]).resolve()
    harness_source_roots = _workspace_source_roots(root)
    tested_source_roots = _workspace_source_roots(tested_checkout)
    tested_source_package = (tested_source_roots[0] / "archetype").resolve()
    from_tested_source = package_path.is_relative_to(tested_source_package)
    if mode == "source" and not from_tested_source:
        raise RuntimeError(
            f"source scenario imported package outside tested checkout: {package_path}"
        )

    library_paths = payload.get("library_paths")
    if not isinstance(library_paths, dict):
        raise RuntimeError("package probe did not return world-library paths")
    resolved_libraries: dict[str, str] = {}
    for index, (_distribution, _relative, module) in enumerate(
        WORKSPACE_DISTRIBUTIONS[1:],
        start=1,
    ):
        encoded = library_paths.get(module)
        if not isinstance(encoded, str) or not encoded:
            raise RuntimeError(f"package probe could not resolve installed world library {module}")
        library_path = Path(encoded).resolve()
        if mode == "source" and not library_path.is_relative_to(tested_source_roots[index]):
            raise RuntimeError(
                f"source scenario imported {module} outside tested checkout: {library_path}"
            )
        resolved_libraries[module] = str(library_path)

    if mode == "wheel":
        wheel_environment = python.parent.parent.resolve()
        source_roots = set(harness_source_roots) | set(tested_source_roots)
        from_checkout_source = any(
            package_path.is_relative_to(source_root / "archetype") for source_root in source_roots
        )
        if from_checkout_source or not package_path.is_relative_to(wheel_environment):
            raise RuntimeError(
                f"wheel scenario did not import from its isolated environment: {package_path}"
            )
        for module, encoded in resolved_libraries.items():
            library_path = Path(encoded)
            if any(library_path.is_relative_to(source_root) for source_root in source_roots):
                raise RuntimeError(
                    f"wheel scenario imported {module} from checkout source: {library_path}"
                )
            if not library_path.is_relative_to(wheel_environment):
                raise RuntimeError(
                    f"wheel scenario did not import {module} from its isolated environment: "
                    f"{library_path}"
                )
        resolved_sys_path = {
            Path(value).resolve()
            for value in payload.get("sys_path", [])
            if isinstance(value, str) and value
        }
        leaked_source_roots = sorted(
            str(path)
            for path in resolved_sys_path
            if any(
                path == source_root or path.is_relative_to(source_root)
                for source_root in source_roots
            )
        )
        if leaked_source_roots:
            raise RuntimeError(
                "wheel scenario leaked checkout source paths through sys.path: "
                f"{leaked_source_roots}"
            )
    return {
        "version": payload["version"],
        "path": str(package_path),
        "world_libraries": resolved_libraries,
        "origin": mode,
    }


def _adapt_command(
    command: list[str],
    *,
    python: Path,
    root: Path,
) -> list[str]:
    adapted: list[str] = []
    for index, value in enumerate(command):
        if index == 0 and value == "python":
            adapted.append(str(python))
            continue
        if index == 0 and value == "pytest":
            # The repository pytest configuration adds ``src`` to sys.path.
            # Every installed-wheel pytest command, including a multi-file
            # source command that cannot reuse one oracle node, must disable
            # that setting. Source mode receives its selected checkout through
            # the explicit PYTHONPATH built by ``_base_environment``.
            adapted.extend((str(python), "-m", "pytest", "-o", "pythonpath="))
            continue
        candidate = root / value
        adapted.append(str(candidate.resolve()) if candidate.exists() else value)
    return adapted


def _adapt_example_command(
    command: list[str],
    *,
    python: Path,
    root: Path,
    source_path: Path,
    execution_source_path: Path,
) -> list[str]:
    """Adapt an example command and relocate its source in isolated wheel runs."""

    adapted = _adapt_command(command, python=python, root=root)
    declared_source = str(source_path.resolve())
    execution_source = str(execution_source_path.resolve())
    if declared_source == execution_source:
        return adapted
    source_indexes = [index for index, value in enumerate(adapted) if value == declared_source]
    if len(source_indexes) != 1:
        raise ValueError(
            "a relocated example source_command must contain source_path exactly once: "
            f"{source_path.relative_to(root)}"
        )
    adapted[source_indexes[0]] = execution_source
    return adapted


def _pytest_command(
    python: Path,
    reference: str,
    *,
    mode: str,
    junit_path: Path,
) -> list[str]:
    if mode not in {"source", "wheel"}:
        raise ValueError(f"unsupported pytest oracle mode: {mode}")
    command = [str(python), "-m", "pytest", "-q", "-o", "pythonpath="]
    # The repository's pytest configuration intentionally adds the harness
    # ``src`` tree. Disable it in both modes: source runs receive the selected
    # tested checkout through PYTHONPATH, while wheel runs must receive neither
    # checkout's source tree.
    command.extend((f"--junitxml={junit_path}", reference))
    return command


def _copy_example_source(source_path: Path, scenario_root: Path) -> Path:
    """Copy an example tree so wheel execution cannot import it in place."""

    copied_examples = scenario_root / "example-source"
    shutil.copytree(
        source_path.parent,
        copied_examples,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "archetype_data"),
    )
    return copied_examples / source_path.name


def _semantic_receipt(stdout: str) -> dict[str, object] | None:
    encoded = [
        line.removeprefix(RECEIPT_PREFIX)
        for line in stdout.splitlines()
        if line.startswith(RECEIPT_PREFIX)
    ]
    if not encoded:
        return None
    if len(encoded) != 1:
        raise ValueError("source emitted more than one structured receipt")
    return _strict_json_object(encoded[0], label="example receipt")


def _absolute_nodeid(root: Path, nodeid: str) -> str:
    path, separator, suffix = nodeid.partition("::")
    resolved = str((root / path).resolve())
    return f"{resolved}::{suffix}" if separator else resolved


def _eval_task_id(reference: str) -> str:
    _path, separator, symbol = reference.partition("::")
    if not separator or not symbol.startswith("task_") or symbol == "task_":
        raise ValueError(f"eval reference must end in ::task_<id>: {reference}")
    return symbol.removeprefix("task_")


def _parse_eval_receipt(
    path: Path,
    *,
    reference: str,
    expected_revision: str,
    expected_dirty: bool,
    root: Path,
) -> tuple[bool, dict[str, object]]:
    payload = _strict_json_object(path.read_text(encoding="utf-8"), label="eval receipt")
    if payload.get("schema_version") != 1 or payload.get("kind") != "eval":
        raise ValueError("eval receipt has an unsupported schema or kind")

    revision = payload.get("revision")
    if not isinstance(revision, dict):
        raise TypeError("eval receipt revision must be an object")
    if revision.get("commit") != expected_revision:
        raise ValueError(
            "eval receipt revision does not match the operational run: "
            f"{revision.get('commit')!r} != {expected_revision!r}"
        )
    if revision.get("dirty") is not expected_dirty:
        raise ValueError(
            "eval receipt checkout state does not match the operational run: "
            f"{revision.get('dirty')!r} != {expected_dirty!r}"
        )

    invocation = payload.get("invocation")
    if (
        not isinstance(invocation, list)
        or not invocation
        or any(not isinstance(item, str) for item in invocation)
    ):
        raise TypeError("eval receipt invocation must be a non-empty list of strings")
    invocation_path = Path(invocation[0])
    if not invocation_path.is_absolute():
        invocation_path = root / invocation_path
    expected_invocation = (root / "evals" / "run.py").resolve()
    if invocation_path.resolve() != expected_invocation:
        raise ValueError(
            "eval receipt invocation is not the active checkout runner: "
            f"{invocation_path.resolve()} != {expected_invocation}"
        )

    results = payload.get("results")
    if not isinstance(results, list):
        raise TypeError("eval receipt results must be a list")
    task_id = _eval_task_id(reference)
    matches = [
        item for item in results if isinstance(item, dict) and item.get("task_id") == task_id
    ]
    if len(matches) != 1:
        raise ValueError(f"eval receipt must contain exactly one result for {task_id!r}")
    match = matches[0]
    trials = match.get("trials")
    if not isinstance(trials, list) or not trials:
        raise ValueError(f"eval result {task_id!r} must contain trial evidence")

    grader_names: set[str] = set()
    grader_evidence: list[dict[str, object]] = []
    evidence_passed = True
    for index, trial in enumerate(trials):
        if not isinstance(trial, dict):
            raise TypeError(f"eval result {task_id!r} trial {index} must be an object")
        graders = trial.get("graders")
        if not isinstance(graders, list) or not graders:
            raise ValueError(f"eval result {task_id!r} trial {index} has no grader evidence")
        evidence_passed = evidence_passed and trial.get("passed") is True
        for grader_index, grader in enumerate(graders):
            if not isinstance(grader, dict):
                raise TypeError(
                    f"eval result {task_id!r} trial {index} grader {grader_index} must be an object"
                )
            grader_record = cast(dict[str, Any], grader)
            name = grader_record.get("name")
            if not isinstance(name, str) or not name:
                raise ValueError(
                    f"eval result {task_id!r} trial {index} grader {grader_index} "
                    "has no stable name"
                )
            grader_names.add(name)
            evidence_passed = evidence_passed and grader_record.get("passed") is True
            if "evidence" in grader_record:
                structured = grader_record["evidence"]
                if not isinstance(structured, (dict, list)):
                    raise TypeError(
                        f"eval result {task_id!r} trial {index} grader {name!r} "
                        "evidence must be an object or list"
                    )
                try:
                    json.dumps(structured, allow_nan=False, sort_keys=True)
                except (TypeError, ValueError) as exc:
                    raise TypeError(
                        f"eval result {task_id!r} trial {index} grader {name!r} "
                        "evidence is not portable JSON"
                    ) from exc
                grader_evidence.append(
                    {
                        "trial": index,
                        "grader": name,
                        "evidence": structured,
                    }
                )

    contract_ids = match.get("contract_ids", [])
    if not isinstance(contract_ids, list) or any(
        not isinstance(contract_id, str) for contract_id in contract_ids
    ):
        raise TypeError(f"eval result {task_id!r} contract_ids must be strings")
    all_passed = match.get("all_passed") is True and evidence_passed
    semantic: dict[str, object] = {
        "task_id": task_id,
        "all_passed": all_passed,
        "trial_count": len(trials),
        "grader_names": sorted(grader_names),
        "contract_ids": sorted(contract_ids),
    }
    if grader_evidence:
        semantic["grader_evidence"] = grader_evidence
    return all_passed, semantic


def _parse_pytest_junit(path: Path) -> tuple[bool, dict[str, object]]:
    try:
        root = ET.parse(path).getroot()
    except (OSError, ET.ParseError) as exc:
        raise ValueError(f"pytest did not produce valid JUnit evidence: {exc}") from exc
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    if not suites:
        raise ValueError("pytest JUnit evidence contains no test suites")

    def count(suite: ET.Element, field: str) -> int:
        try:
            return int(suite.attrib.get(field, "0"))
        except ValueError as exc:
            raise ValueError(f"pytest JUnit {field} count is not an integer") from exc

    counts = {
        field: sum(count(suite, field) for suite in suites)
        for field in ("tests", "failures", "errors", "skipped")
    }
    if counts["tests"] <= 0:
        raise ValueError("pytest oracle collected no tests")
    passed = counts["failures"] == 0 and counts["errors"] == 0 and counts["skipped"] == 0
    return passed, {"pytest": counts}


def _run_oracle(
    row: dict[str, Any],
    *,
    python: Path,
    root: Path,
    scenario_root: Path,
    env: dict[str, str],
    logs: Path,
    source_semantic: dict[str, object] | None,
    source_eval_output: Path | None,
    source_pytest_output: Path | None,
    mode: str,
    expected_revision: str,
    expected_dirty: bool,
) -> tuple[dict[str, object], dict[str, object] | None]:
    oracle = row["semantic_oracle"]
    kind = oracle["kind"]
    reference = oracle["ref"]
    if kind not in {"eval", "pytest"}:
        return (
            {
                "kind": kind,
                "ref": reference,
                "passed": False,
                "error": f"unsupported semantic oracle kind: {kind}",
            },
            None,
        )

    if kind == "eval":
        eval_output = source_eval_output or scenario_root / "eval-results.json"
        try:
            passed, semantic = _parse_eval_receipt(
                eval_output,
                reference=reference,
                expected_revision=expected_revision,
                expected_dirty=expected_dirty,
                root=root,
            )
        except (OSError, TypeError, ValueError) as exc:
            return (
                {
                    "kind": kind,
                    "ref": reference,
                    "execution": "source",
                    "passed": False,
                    "error": f"{type(exc).__name__}: {exc}",
                },
                None,
            )
        return (
            {
                "kind": kind,
                "ref": reference,
                "execution": "source",
                "receipt_digest": _sha256_file(eval_output),
                "passed": passed,
            },
            semantic,
        )

    pytest_output = source_pytest_output or scenario_root / "oracle-junit.xml"
    result: dict[str, object]
    if source_pytest_output is None:
        oracle_env = env
        if source_semantic is not None:
            captured_receipt = scenario_root / "captured-example-receipt.json"
            captured_receipt.write_text(
                json.dumps(source_semantic, allow_nan=False, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            oracle_env = env.copy()
            oracle_env[CAPTURED_RECEIPT_ENV] = str(captured_receipt)
        command = _pytest_command(
            python,
            _absolute_nodeid(root, reference),
            mode=mode,
            junit_path=pytest_output,
        )
        result = _run_process(
            command,
            cwd=root,
            env=oracle_env,
            timeout_seconds=row["timeout_seconds"],
            log_prefix=logs / "oracle",
            redacted=row["artifact_policy"] == "redacted_receipt",
        )
        process_passed = (
            result["returncode"] == 0
            and not result["timed_out"]
            and not result["process_group_leaked"]
        )
        result.pop("stdout_text", None)
        if source_semantic is not None:
            result["semantic_input"] = "captured_source_receipt"
    else:
        result = {"execution": "source"}
        process_passed = True
    try:
        semantic_passed, semantic = _parse_pytest_junit(pytest_output)
    except ValueError as exc:
        semantic_passed = False
        semantic = cast(dict[str, object], {"pytest_nodeid": reference})
        result["error"] = f"{type(exc).__name__}: {exc}"
    semantic["pytest_nodeid"] = reference
    result.update(
        {
            "kind": kind,
            "ref": reference,
            "passed": process_passed and semantic_passed,
        }
    )
    return result, semantic


def _force_cli_option(command: list[str], option: str, value: str) -> list[str]:
    forced = list(command)
    for index, argument in enumerate(forced):
        if argument == option:
            if index + 1 >= len(forced):
                raise ValueError(f"{option} requires a value")
            forced[index + 1] = value
            return forced
        if argument.startswith(f"{option}="):
            forced[index] = f"{option}={value}"
            return forced
    forced.extend((option, value))
    return forced


def _source_is_pytest_oracle(row: dict[str, Any]) -> bool:
    command = row["source_command"]
    return (
        row["semantic_oracle"]["kind"] == "pytest"
        and bool(command)
        and command[0] == "pytest"
        and row["semantic_oracle"]["ref"] in command
    )


def _run_one(
    row: dict[str, Any],
    *,
    python: Path,
    root: Path,
    mode: str,
    cadence: str,
    run_root: Path,
    logs_root: Path,
    package: dict[str, object],
    base_env: dict[str, str],
    expected_revision: str,
    expected_dirty: bool,
    tested_subject: dict[str, object],
) -> dict[str, object]:
    started_at = _utc_now()
    started = time.perf_counter()
    scenario_id = row["id"]
    missing = missing_prerequisites(row)
    result: dict[str, object] = {
        "scenario": scenario_id,
        "owner": row["owner"],
        "tier": row["tier"],
        "mode": mode,
        "started_at": started_at,
        "status": "failed",
        "package": package,
        "tested_subject": tested_subject,
        "backend_provider": [
            value.split(":", 1)[1]
            for value in row["prerequisites"]
            if value.startswith(("service:", "infrastructure:"))
        ]
        or ["local"],
        "contracts": row["contracts"],
        "artifact_schema": row["artifact_schema"],
    }
    if missing:
        # A release-required scenario must execute. Preserve ``not_run`` for
        # lower cadences where the registry explicitly permits it, but turn a
        # missing release prerequisite into a concrete failed result.
        release_forced = cadence == "release"
        missing_status = (
            "failed" if release_forced or row["missing_prerequisite"] != "not_run" else "not_run"
        )
        reason = f"missing prerequisites: {', '.join(missing)}"
        if release_forced and row["missing_prerequisite"] == "not_run":
            reason = f"release cadence requires execution; {reason}"
        result.update(
            {
                "status": missing_status,
                "reason": reason,
                "missing_prerequisites": missing,
                "duration_seconds": round(time.perf_counter() - started, 6),
                "cleanup": {"policy": row["cleanup_policy"], "status": "not_acquired"},
            }
        )
        return result

    scenario_root = run_root / scenario_id.replace("/", "_")
    scenario_root.mkdir(parents=True)
    logs = logs_root / scenario_id.replace("/", "_")
    env = _scenario_environment(base_env, row)
    env["TMPDIR"] = str(scenario_root / "tmp")
    env["ARCHETYPE_OPERATIONAL_STORAGE_URI"] = str(scenario_root / "storage")
    Path(env["TMPDIR"]).mkdir()

    source_path = root / row["source_path"]
    source_eval_output: Path | None = None
    source_pytest_output: Path | None = None
    captures_example_receipt = row["kind"] == "example" and _has_run_demo(source_path)
    source_cwd = scenario_root if row["kind"] == "example" else root
    receipt_root: Path | None = None
    receipt_storage: Path | None = None
    receipt_env: dict[str, str] | None = None
    if captures_example_receipt:
        execution_source_path = (
            _copy_example_source(source_path, scenario_root) if mode == "wheel" else source_path
        )
        source_cwd = scenario_root / "entrypoint"
        source_cwd.mkdir()
        env["TMPDIR"] = str(source_cwd / "tmp")
        env["ARCHETYPE_OPERATIONAL_STORAGE_URI"] = str(source_cwd / "storage")
        Path(env["TMPDIR"]).mkdir()
        receipt_root = scenario_root / "receipt"
        receipt_root.mkdir()
        receipt_storage = receipt_root / "storage"
        receipt_env = env.copy()
        receipt_env["TMPDIR"] = str(receipt_root / "tmp")
        receipt_env["ARCHETYPE_OPERATIONAL_STORAGE_URI"] = str(receipt_storage)
        Path(receipt_env["TMPDIR"]).mkdir()
        command = _adapt_example_command(
            row["source_command"],
            python=python,
            root=root,
            source_path=source_path,
            execution_source_path=execution_source_path,
        )
    elif _source_is_pytest_oracle(row):
        source_pytest_output = scenario_root / "source-junit.xml"
        command = _pytest_command(
            python,
            _absolute_nodeid(root, row["semantic_oracle"]["ref"]),
            mode=mode,
            junit_path=source_pytest_output,
        )
    else:
        command = _adapt_command(row["source_command"], python=python, root=root)
        if row["semantic_oracle"]["kind"] == "eval":
            source_eval_output = scenario_root / "eval-results.json"
            command = _force_cli_option(command, "--out", str(source_eval_output))
    source_result = _run_process(
        command,
        cwd=source_cwd,
        env=env,
        timeout_seconds=row["timeout_seconds"],
        log_prefix=logs / "source",
        redacted=row["artifact_policy"] == "redacted_receipt",
    )
    source_passed = (
        source_result["returncode"] == 0
        and not source_result["timed_out"]
        and not source_result["process_group_leaked"]
    )
    semantic: dict[str, object] | None = None
    receipt_result: dict[str, object] | None = None
    receipt_passed = not captures_example_receipt
    if captures_example_receipt:
        if source_passed:
            assert receipt_root is not None
            assert receipt_storage is not None
            assert receipt_env is not None
            receipt_command = [
                str(python),
                str((root / "scripts" / "run_example_receipt.py").resolve()),
                "--source",
                str(execution_source_path.resolve()),
                "--storage-uri",
                str(receipt_storage),
            ]
            receipt_result = _run_process(
                receipt_command,
                cwd=receipt_root,
                env=receipt_env,
                timeout_seconds=row["timeout_seconds"],
                log_prefix=logs / "receipt",
                redacted=row["artifact_policy"] == "redacted_receipt",
            )
            receipt_process_passed = (
                receipt_result["returncode"] == 0
                and not receipt_result["timed_out"]
                and not receipt_result["process_group_leaked"]
            )
            if receipt_process_passed:
                try:
                    semantic = _semantic_receipt(str(receipt_result["stdout_text"]))
                except (TypeError, ValueError) as exc:
                    receipt_result["error"] = f"{type(exc).__name__}: {exc}"
                if semantic is None and "error" not in receipt_result:
                    receipt_result["error"] = "receipt capture emitted no structured receipt"
            receipt_passed = receipt_process_passed and semantic is not None
            receipt_result.pop("stdout_text", None)
        else:
            receipt_result = {
                "status": "not_run",
                "reason": "declared source command failed",
                "process_group_leaked": False,
            }
    oracle_result: dict[str, object]
    oracle_semantic: dict[str, object] | None
    reusable_oracle_evidence = (
        source_eval_output is not None and source_eval_output.is_file()
    ) or (source_pytest_output is not None and source_pytest_output.is_file())
    oracle_ready = (
        source_passed and receipt_passed
        if captures_example_receipt
        else source_passed or reusable_oracle_evidence
    )
    if oracle_ready:
        oracle_result, oracle_semantic = _run_oracle(
            row,
            python=python,
            root=root,
            scenario_root=scenario_root,
            env=env,
            logs=logs,
            source_semantic=semantic,
            source_eval_output=source_eval_output,
            source_pytest_output=source_pytest_output,
            mode=mode,
            expected_revision=expected_revision,
            expected_dirty=expected_dirty,
        )
    else:
        oracle_result = {
            "kind": row["semantic_oracle"]["kind"],
            "ref": row["semantic_oracle"]["ref"],
            "passed": False,
            "error": (
                "declared source command failed"
                if not source_passed
                else "structured receipt capture failed"
            ),
        }
        oracle_semantic = None
    source_result.pop("stdout_text", None)
    receipt_leaked = bool(
        receipt_result is not None and receipt_result.get("process_group_leaked", False)
    )
    cleanup_ok = (
        not source_result["process_group_leaked"]
        and not receipt_leaked
        and not oracle_result.get("process_group_leaked", False)
    )
    passed = source_passed and receipt_passed and bool(oracle_result["passed"]) and cleanup_ok
    result.update(
        {
            "status": "passed" if passed else "failed",
            "reason": (
                "" if passed else "source, receipt capture, oracle, or cleanup contract failed"
            ),
            "source": source_result,
            "oracle": oracle_result,
            "semantic": semantic or oracle_semantic or {},
            "cleanup": {
                "policy": row["cleanup_policy"],
                "status": "closed" if cleanup_ok else "leaked",
                "process_group_leaked": not cleanup_ok,
            },
            "duration_seconds": round(time.perf_counter() - started, 6),
        }
    )
    if receipt_result is not None:
        result["receipt_capture"] = receipt_result
    return result


def _select_scenarios(
    rows: list[dict[str, Any]],
    *,
    mode: str,
    cadence: str,
    scenario_ids: set[str],
    kind: str | None,
    min_tier: int,
    max_tier: int,
) -> list[dict[str, Any]]:
    known = {row["id"] for row in rows}
    unknown = scenario_ids - known
    if unknown:
        raise ValueError(f"unknown scenario ids: {sorted(unknown)}")
    selected = [
        row
        for row in rows
        if mode in row["applicability"]
        and cadence in row["required_cadence"]
        and min_tier <= row["tier"] <= max_tier
        and (not scenario_ids or row["id"] in scenario_ids)
        and (kind is None or row["kind"] == kind)
    ]
    selected_ids = {row["id"] for row in selected}
    excluded = scenario_ids - selected_ids
    if excluded:
        raise ValueError(
            "requested scenario ids do not match mode/cadence/tier/kind selection: "
            f"{sorted(excluded)}"
        )
    if not selected:
        raise ValueError("no operational scenarios matched the selection")
    return sorted(selected, key=lambda row: row["id"])


def _rewrite_paths(value: Any, replacements: list[tuple[str, str]]) -> Any:
    if isinstance(value, str):
        for original, replacement in replacements:
            value = value.replace(original, replacement)
        return value
    if isinstance(value, list):
        return [_rewrite_paths(item, replacements) for item in value]
    if isinstance(value, dict):
        return {key: _rewrite_paths(item, replacements) for key, item in value.items()}
    return value


def _publish_logs(captured: Path, destination: Path) -> None:
    if destination.exists():
        shutil.rmtree(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(captured, destination)


def _write_envelope(path: Path, envelope: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(envelope, allow_nan=False, indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        delete=False,
    ) as stream:
        temporary = Path(stream.name)
        stream.write(encoded)
    temporary.replace(path)


def run_scenarios(
    *,
    root: Path,
    registry: Path,
    output: Path,
    mode: str,
    wheel: Path | None,
    cadence: str,
    scenario_ids: set[str],
    kind: str | None,
    min_tier: int,
    max_tier: int,
    expected_revision: str | None,
    require_clean: bool,
    require_run: bool,
    tested_checkout: Path | None = None,
    expected_tested_revision: str | None = None,
    require_tested_clean: bool = False,
    wheel_dir: Path | None = None,
) -> tuple[dict[str, object], bool]:
    errors = validate_operational_scenarios(root=root, registry_path=registry)
    if errors:
        raise ValueError("invalid operational registry:\n" + "\n".join(errors))
    revision, dirty = _git_state(root)
    if expected_revision is not None and revision != expected_revision:
        raise RuntimeError(f"expected revision {expected_revision}, active checkout is {revision}")
    if require_clean and dirty:
        raise RuntimeError("clean operational evidence requested from a dirty checkout")
    tested_root = (tested_checkout or root).resolve()
    if tested_root == root.resolve():
        tested_revision, tested_dirty = revision, dirty
    else:
        tested_revision, tested_dirty = _git_state(tested_root)
    if expected_tested_revision is not None and tested_revision != expected_tested_revision:
        raise RuntimeError(
            "expected tested-subject revision "
            f"{expected_tested_revision}, tested checkout is {tested_revision}"
        )
    if require_tested_clean and tested_dirty:
        raise RuntimeError("clean tested-subject evidence requested from a dirty tested checkout")
    tested_subject = _tested_subject_record(
        root=root,
        tested_checkout=tested_root,
        revision=tested_revision,
        dirty=tested_dirty,
    )

    rows = _select_scenarios(
        load_scenarios(registry),
        mode=mode,
        cadence=cadence,
        scenario_ids=scenario_ids,
        kind=kind,
        min_tier=min_tier,
        max_tier=max_tier,
    )

    output = output.resolve()
    logs_root = output.parent / f"{output.stem}.d"
    run_root = Path(tempfile.mkdtemp(prefix="archetype-operational-"))
    captured_logs_root = run_root / "captured-logs"
    captured_logs_root.mkdir()
    started_at = _utc_now()
    started = time.perf_counter()
    wheel_digest: str | None = None
    installed_artifacts: list[dict[str, str]] = []
    isolated_cleanup: dict[str, object] = {
        "status": "closed",
        "path": "<isolated-run>",
    }
    try:
        if mode == "wheel":
            if wheel is None:
                raise ValueError("--wheel must name the built archetype-ecs wheel in wheel mode")
            wheel_artifacts = _resolve_wheel_artifacts(wheel=wheel, wheel_dir=wheel_dir)
            for artifact in wheel_artifacts:
                _validate_distinct_wheel_location(
                    wheel=artifact.path,
                    root=root,
                    tested_checkout=tested_root,
                )
            python, wheel_env = _prepare_wheel_python(
                tuple(artifact.path for artifact in wheel_artifacts),
                run_root,
            )
            installed_artifacts = _wheel_artifact_records(wheel_artifacts)
            wheel_digest = installed_artifacts[0]["digest"]
            package = _package_probe(
                python,
                cwd=run_root,
                env=wheel_env,
                root=root,
                tested_checkout=tested_root,
                mode=mode,
            )
            package["artifact_digest"] = wheel_digest
            package["artifacts"] = installed_artifacts
            base_env = wheel_env
        else:
            if wheel is not None or wheel_dir is not None:
                raise ValueError("--wheel and --wheel-dir are valid only in wheel mode")
            python = Path(sys.executable)
            base_env = _base_environment(
                root,
                mode=mode,
                tested_checkout=tested_root,
            )
            package = _package_probe(
                python,
                cwd=run_root,
                env=base_env,
                root=root,
                tested_checkout=tested_root,
                mode=mode,
            )
        package["tested_subject"] = _package_subject_record(tested_subject)

        results = []
        for row in rows:
            try:
                result = _run_one(
                    row,
                    python=python,
                    root=root,
                    mode=mode,
                    cadence=cadence,
                    run_root=run_root,
                    logs_root=captured_logs_root,
                    package=package,
                    base_env=base_env,
                    expected_revision=revision,
                    expected_dirty=dirty,
                    tested_subject=tested_subject,
                )
            except Exception as exc:
                result = {
                    "scenario": row["id"],
                    "owner": row["owner"],
                    "tier": row["tier"],
                    "mode": mode,
                    "started_at": _utc_now(),
                    "status": "failed",
                    "reason": f"runner error: {_portable_error(exc)}",
                    "package": package,
                    "tested_subject": tested_subject,
                    "contracts": row["contracts"],
                    "artifact_schema": row["artifact_schema"],
                    "semantic": {},
                    "cleanup": {
                        "policy": row["cleanup_policy"],
                        "status": "unknown",
                    },
                    "duration_seconds": 0.0,
                }
            results.append(result)
        _publish_logs(captured_logs_root, logs_root)
        retained_logs = logs_root.relative_to(output.parent)
        results = _rewrite_paths(
            results,
            [
                (str(captured_logs_root.resolve()), str(retained_logs)),
                (str(captured_logs_root), str(retained_logs)),
                (str(run_root.resolve()), "<isolated-run>"),
                (str(run_root), "<isolated-run>"),
            ],
        )
    finally:
        try:
            _remove_run_root(run_root)
        except OSError as exc:
            isolated_cleanup = {
                "status": "leaked",
                "path": "<isolated-run>",
                "error": _portable_error(exc),
            }

    if isolated_cleanup["status"] != "closed":
        cleanup_error = str(isolated_cleanup["error"])
        for result in results:
            cleanup = result.get("cleanup")
            if isinstance(cleanup, dict) and cleanup.get("status") == "closed":
                cleanup.update(
                    {
                        "status": "leaked",
                        "isolated_filesystem_leaked": True,
                        "filesystem_error": cleanup_error,
                    }
                )
            if result["status"] == "passed":
                result["status"] = "failed"
                result["reason"] = "isolated filesystem cleanup failed"

    failed = any(row["status"] == "failed" for row in results)
    not_run = any(row["status"] == "not_run" for row in results)
    passed = not failed and not (require_run and not_run) and isolated_cleanup["status"] == "closed"
    envelope: dict[str, object] = {
        "schema": RESULT_SCHEMA,
        "profile": f"{cadence}:{mode}:tier-{min_tier}-{max_tier}",
        "revision": revision,
        "clean_checkout": not dirty,
        "invocation": str(Path(__file__).resolve()),
        "repository": str(root.resolve()),
        "tested_subject": tested_subject,
        "mode": mode,
        "wheel": (
            {
                "path": str(wheel.resolve()),
                "filename": wheel.name,
                "digest": wheel_digest,
                "artifacts": installed_artifacts,
            }
            if wheel is not None
            else None
        ),
        "python": {
            "version": platform.python_version(),
            "implementation": platform.python_implementation(),
        },
        "started_at": started_at,
        "duration_seconds": round(time.perf_counter() - started, 6),
        "outcome": "passed" if passed else "failed",
        "cleanup": isolated_cleanup,
        "status_counts": {
            status: sum(row["status"] == status for row in results)
            for status in ("passed", "failed", "not_run")
        },
        "results": results,
    }
    _write_envelope(output, envelope)
    return envelope, passed


class _ReceiptArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise ValueError(f"argument error: {message}")


def _argument_value(
    arguments: list[str],
    option: str,
    *,
    default: str,
) -> str:
    for index, argument in enumerate(arguments):
        if argument.startswith(f"{option}="):
            return argument.partition("=")[2]
        if (
            argument == option
            and index + 1 < len(arguments)
            and not arguments[index + 1].startswith("-")
        ):
            return arguments[index + 1]
    return default


def main(argv: list[str] | None = None) -> int:
    started_at = _utc_now()
    started = time.perf_counter()
    arguments = list(sys.argv[1:] if argv is None else argv)
    root = Path(_argument_value(arguments, "--repo-root", default=str(ROOT))).resolve()
    tested_root = Path(_argument_value(arguments, "--tested-checkout", default=str(root))).resolve()
    output = Path(_argument_value(arguments, "--out", default="operational-results.json")).resolve()
    mode = _argument_value(arguments, "--mode", default="source")
    cadence = _argument_value(arguments, "--cadence", default="pr")
    parser = _ReceiptArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--tested-checkout", type=Path)
    parser.add_argument("--registry", type=Path, default=REGISTRY)
    parser.add_argument("--out", type=Path, default=Path("operational-results.json"))
    parser.add_argument("--mode", choices=("source", "wheel"), default="source")
    parser.add_argument("--wheel", type=Path)
    parser.add_argument("--wheel-dir", type=Path)
    parser.add_argument("--cadence", choices=("pr", "main", "release"), default="pr")
    parser.add_argument("--scenario", action="append", default=[])
    parser.add_argument("--kind", choices=("example", "dogfood"))
    parser.add_argument("--min-tier", type=int, default=0)
    parser.add_argument("--max-tier", type=int, default=1)
    parser.add_argument("--expected-revision")
    parser.add_argument("--expected-tested-revision")
    parser.add_argument("--require-clean", action="store_true")
    parser.add_argument("--require-tested-clean", action="store_true")
    parser.add_argument("--require-run", action="store_true")
    try:
        args = parser.parse_args(arguments)
        root = args.repo_root.resolve()
        tested_root = (args.tested_checkout or root).resolve()
        output = args.out.resolve()
        mode = args.mode
        cadence = args.cadence
        if not 0 <= args.min_tier <= args.max_tier <= 6:
            raise ValueError("tiers must satisfy 0 <= min-tier <= max-tier <= 6")
        envelope, passed = run_scenarios(
            root=root,
            registry=args.registry.resolve(),
            output=output,
            mode=args.mode,
            wheel=args.wheel.resolve() if args.wheel else None,
            cadence=args.cadence,
            scenario_ids=set(args.scenario),
            kind=args.kind,
            min_tier=args.min_tier,
            max_tier=args.max_tier,
            expected_revision=args.expected_revision,
            require_clean=args.require_clean,
            require_run=args.require_run,
            tested_checkout=tested_root,
            expected_tested_revision=args.expected_tested_revision,
            require_tested_clean=args.require_tested_clean,
            wheel_dir=args.wheel_dir.resolve() if args.wheel_dir else None,
        )
    except Exception as exc:
        try:
            revision, dirty = _git_state(root)
        except (OSError, subprocess.SubprocessError):
            revision, dirty = None, None
        if tested_root == root and revision is not None:
            tested_revision, tested_dirty = revision, dirty
        else:
            try:
                tested_revision, tested_dirty = _git_state(tested_root)
            except (OSError, subprocess.SubprocessError):
                tested_revision, tested_dirty = None, None
        error_envelope = {
            "schema": RESULT_SCHEMA,
            "profile": f"{cadence}:{mode}:setup",
            "revision": revision,
            "clean_checkout": None if dirty is None else not dirty,
            "invocation": str(Path(__file__).resolve()),
            "repository": str(root),
            "tested_subject": _tested_subject_record(
                root=root,
                tested_checkout=tested_root,
                revision=tested_revision,
                dirty=tested_dirty,
            ),
            "mode": mode,
            "started_at": started_at,
            "duration_seconds": round(time.perf_counter() - started, 6),
            "outcome": "failed",
            "status_counts": {"passed": 0, "failed": 1, "not_run": 0},
            "results": [],
            "error": _portable_error(exc),
        }
        _write_envelope(output, error_envelope)
        print(f"operational runner error: {exc}", file=sys.stderr)
        return 2
    counts = cast(dict[str, int], envelope["status_counts"])
    print(
        "Operational scenarios "
        f"{envelope['outcome']}: {counts['passed']} passed, "
        f"{counts['failed']} failed, {counts['not_run']} not run; "
        f"receipt={output}"
    )
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
