#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Exercise the shipped API and CLI over an isolated loopback server."""

from __future__ import annotations

import json
import os
import re
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

RECEIPT_SCHEMA = "archetype.runtime-loopback/v1"
RECEIPT_FILENAME = "runtime-loopback-receipt.json"
RECEIPT_PREFIX = "ARCHETYPE_OPERATIONAL_RECEIPT="
_UUID_PATTERN = re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b")


def receipt_path(workspace: Path) -> Path:
    """Return the stable receipt path retained for the semantic oracle."""

    return workspace / RECEIPT_FILENAME


def _cli_entrypoint() -> str:
    entrypoint = Path(sys.executable).with_name("archetype")
    if not entrypoint.is_file():
        raise RuntimeError("installed Archetype CLI entrypoint is unavailable")
    return str(entrypoint)


def _port_is_closed(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.settimeout(0.2)
        return probe.connect_ex(("127.0.0.1", port)) != 0


def _wait_for_server(
    process: subprocess.Popen[str],
    *log_paths: Path,
) -> tuple[str, int]:
    deadline = time.monotonic() + 30
    port: int | None = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("shipped API server exited before readiness")
        if port is None:
            output = "\n".join(
                path.read_text(encoding="utf-8", errors="replace") for path in log_paths
            )
            match = re.search(r"http://127\.0\.0\.1:(\d+)", output)
            if match is not None:
                port = int(match.group(1))
        if port is not None:
            base_url = f"http://127.0.0.1:{port}"
            try:
                with urllib.request.urlopen(f"{base_url}/healthz", timeout=0.5) as response:
                    payload = json.loads(response.read())
                    if response.status == 200 and payload == {"status": "ok"}:
                        return base_url, port
            except (OSError, TimeoutError, urllib.error.URLError, json.JSONDecodeError):
                pass
        time.sleep(0.1)
    raise RuntimeError("shipped API server did not become ready")


def _run_cli(
    *arguments: str,
    env: dict[str, str],
    cwd: Path,
    tracked_pids: list[int],
) -> str:
    process = subprocess.Popen(
        [_cli_entrypoint(), *arguments],
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    tracked_pids.append(process.pid)
    try:
        stdout, _stderr = process.communicate(timeout=120)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate()
        raise RuntimeError("shipped CLI command timed out") from None
    if process.returncode != 0:
        raise RuntimeError("shipped CLI command failed")
    return stdout.strip()


def _run_cli_failure(
    *arguments: str,
    env: dict[str, str],
    cwd: Path,
    tracked_pids: list[int],
) -> str:
    process = subprocess.Popen(
        [_cli_entrypoint(), *arguments],
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    tracked_pids.append(process.pid)
    try:
        stdout, stderr = process.communicate(timeout=120)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate()
        raise RuntimeError("shipped CLI negative probe timed out") from None
    if process.returncode == 0:
        raise RuntimeError("shipped CLI negative probe unexpectedly succeeded")
    return f"{stdout}\n{stderr}".strip()


def _pid_is_reaped(pid: int) -> bool:
    try:
        os.waitpid(pid, os.WNOHANG)
    except ChildProcessError:
        return True
    return False


def _uuid_from(output: str, *, label: str) -> str:
    match = _UUID_PATTERN.search(output.lower())
    if match is None:
        raise RuntimeError(f"shipped CLI {label} omitted its world identity")
    return match.group(0)


def _entity_id_from(output: str) -> int:
    match = re.search(r"Spawned entity:\s*(\d+)", output)
    if match is None:
        raise RuntimeError("shipped CLI spawn omitted its reserved entity identity")
    return int(match.group(1))


def _json_list(output: str, *, label: str) -> list[dict[str, Any]]:
    try:
        value = json.loads(output)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"shipped CLI {label} did not emit JSON") from exc
    if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
        raise RuntimeError(f"shipped CLI {label} emitted the wrong JSON shape")
    return value


def run_runtime_loopback(workspace: Path) -> dict[str, object]:
    """Run one real API-server/CLI lifecycle and return bounded evidence."""

    workspace = workspace.resolve()
    workspace.mkdir(parents=True, exist_ok=True)
    catalog = workspace / "catalog"
    storage = workspace / "world-store"
    server_logs = workspace / "server-logs"
    server_logs.mkdir(exist_ok=True)

    env = os.environ.copy()
    env.update(
        {
            "ARCHETYPE_CATALOG_DIR": str(catalog),
            "DO_NOT_TRACK": "1",
            "LOGFIRE_SEND_TO_LOGFIRE": "0",
            "NO_COLOR": "1",
            "PYTHONUNBUFFERED": "1",
            "TMPDIR": str(workspace / "tmp"),
        }
    )
    Path(env["TMPDIR"]).mkdir(parents=True, exist_ok=True)
    for name in (
        "ARCHETYPE_CONTROL_CATALOG_URL",
        "ARCHETYPE_CONTROL_CATALOG_TOKEN",
        "LOGFIRE_TOKEN",
        "LOGFIRE_API_KEY",
    ):
        env.pop(name, None)

    server: subprocess.Popen[str] | None = None
    server_reaped = False
    server_graceful = False
    port_closed = False
    port: int | None = None
    route_evidence: dict[str, object] = {}
    tracked_pids: list[int] = []

    def cli(*arguments: str) -> str:
        return _run_cli(
            *arguments,
            env=env,
            cwd=workspace,
            tracked_pids=tracked_pids,
        )

    def rejected_cli(*arguments: str) -> str:
        return _run_cli_failure(
            *arguments,
            env=env,
            cwd=workspace,
            tracked_pids=tracked_pids,
        )

    stdout_path = server_logs / "stdout.log"
    stderr_path = server_logs / "stderr.log"
    with stdout_path.open("w", encoding="utf-8") as stdout:
        with stderr_path.open("w", encoding="utf-8") as stderr:
            try:
                server = subprocess.Popen(
                    [
                        _cli_entrypoint(),
                        "serve",
                        "--host",
                        "127.0.0.1",
                        "--port",
                        "0",
                    ],
                    cwd=workspace,
                    env=env,
                    stdout=stdout,
                    stderr=stderr,
                    text=True,
                )
                tracked_pids.append(server.pid)
                base_url, port = _wait_for_server(server, stdout_path, stderr_path)

                created = cli(
                    "world",
                    "create",
                    "runtime-loopback",
                    "--storage",
                    str(storage),
                    "--namespace",
                    "runtime_loopback",
                    "--url",
                    base_url,
                    "--role",
                    "admin",
                )
                world_id = _uuid_from(created, label="create")

                component_payload = json.dumps(
                    [
                        {
                            "type": "Mission",
                            "name": "runtime-loopback",
                            "repository": "local",
                            "branch": "main",
                            "base_ref": "main",
                        }
                    ],
                    sort_keys=True,
                )
                spawned = cli(
                    "entity",
                    "spawn",
                    world_id,
                    "--components",
                    component_payload,
                    "--url",
                    base_url,
                    "--role",
                    "player",
                )
                entity_id = _entity_id_from(spawned)

                stepped = cli(
                    "step",
                    world_id,
                    "--url",
                    base_url,
                    "--role",
                    "operator",
                )
                ran = cli(
                    "run",
                    world_id,
                    "--steps",
                    "1",
                    "--url",
                    base_url,
                    "--role",
                    "operator",
                )
                queried = _json_list(
                    cli(
                        "query",
                        world_id,
                        "Mission",
                        "--json",
                        "--url",
                        base_url,
                        "--role",
                        "viewer",
                    ),
                    label="query",
                )

                forked = cli(
                    "world",
                    "fork",
                    world_id,
                    "--name",
                    "runtime-loopback-fork",
                    "--url",
                    base_url,
                    "--role",
                    "operator",
                )
                fork_id = _uuid_from(forked, label="fork")
                if fork_id == world_id:
                    raise RuntimeError("shipped CLI fork reused its source identity")

                fork_rows = _json_list(
                    cli(
                        "query",
                        fork_id,
                        "Mission",
                        "--json",
                        "--url",
                        base_url,
                        "--role",
                        "viewer",
                    ),
                    label="fork query",
                )
                if not any(row.get("entity_id") == entity_id for row in fork_rows):
                    raise RuntimeError("shipped CLI fork query lost the source entity")

                history = _json_list(
                    cli(
                        "history",
                        world_id,
                        "--json",
                        "--url",
                        base_url,
                        "--role",
                        "viewer",
                    ),
                    label="history",
                )
                required_history = {
                    "spawn",
                    "step",
                    "run",
                    "query_components",
                    "fork_world",
                }
                observed_history = {
                    str(row.get("command_type") or row.get("type", ""))
                    for row in history
                    if row.get("status") == "succeeded"
                }
                if not required_history.issubset(observed_history):
                    raise RuntimeError("shipped CLI history omitted required successful operations")

                for selected_id in (fork_id, world_id):
                    destroyed = cli(
                        "world",
                        "destroy",
                        selected_id,
                        "--url",
                        base_url,
                        "--role",
                        "operator",
                    )
                    if selected_id not in destroyed:
                        raise RuntimeError("shipped CLI destroy omitted its world identity")

                rejected = rejected_cli(
                    "world",
                    "inspect",
                    world_id,
                    "--json",
                    "--url",
                    base_url,
                    "--role",
                    "viewer",
                ).lower()
                if "404" not in rejected and "not found" not in rejected:
                    raise RuntimeError("destroyed world did not produce not-found evidence")

                remaining = _json_list(
                    cli(
                        "world",
                        "list",
                        "--json",
                        "--url",
                        base_url,
                        "--role",
                        "admin",
                    ),
                    label="world list",
                )
                if remaining:
                    raise RuntimeError("shipped API retained destroyed loopback worlds")
                if not queried or not any(row.get("entity_id") == entity_id for row in queried):
                    raise RuntimeError("shipped CLI query omitted the spawned entity")
                if not history:
                    raise RuntimeError("shipped CLI history emitted no actor-aware evidence")
                if "Step complete:" not in stepped or "Run complete:" not in ran:
                    raise RuntimeError("shipped CLI simulation responses lost their stable shape")

                route_evidence = {
                    "create": True,
                    "reserved_spawn": True,
                    "step": True,
                    "run": True,
                    "query": True,
                    "history": True,
                    "fork": True,
                    "fork_readable": True,
                    "destroy": True,
                    "post_destroy_not_found": True,
                    "history_semantic": True,
                    "spawned_entity_id": entity_id,
                    "query_rows": len(queried),
                    "history_rows": len(history),
                }
            finally:
                if server is not None:
                    if server.poll() is None:
                        server.send_signal(signal.SIGINT)
                        try:
                            server.wait(timeout=20)
                        except subprocess.TimeoutExpired:
                            server.kill()
                            server.wait(timeout=10)
                    server_reaped = server.poll() is not None
                    server_graceful = server.returncode == 0
                if port is not None:
                    deadline = time.monotonic() + 5
                    while time.monotonic() < deadline:
                        if _port_is_closed(port):
                            port_closed = True
                            break
                        time.sleep(0.05)

    unreaped_children = sum(not _pid_is_reaped(pid) for pid in tracked_pids)
    if not server_reaped or not server_graceful or not port_closed or unreaped_children:
        raise RuntimeError(
            "shipped API loopback cleanup invariant failed "
            f"(reaped={server_reaped}, graceful={server_graceful}, "
            f"socket_closed={port_closed}, child_count={unreaped_children})"
        )

    receipt: dict[str, object] = {
        "schema": RECEIPT_SCHEMA,
        "transport": {
            "host": "127.0.0.1",
            "dynamic_port": True,
            "shipped_server": True,
            "shipped_cli_http": True,
        },
        "routes": route_evidence,
        "cleanup": {
            "server_process": "closed",
            "server_exit": "graceful",
            "listening_socket": "closed",
            "provider_children": unreaped_children,
            "workspace": "runner_owned",
        },
    }
    encoded = json.dumps(receipt, allow_nan=False, separators=(",", ":"), sort_keys=True)
    if len(encoded.encode("utf-8")) > 4096:
        raise RuntimeError("runtime loopback receipt exceeded its byte budget")
    receipt_path(workspace).write_text(encoded + "\n", encoding="utf-8")
    return receipt


def main() -> int:
    storage_uri = os.environ.get("ARCHETYPE_OPERATIONAL_STORAGE_URI", "").strip()
    workspace = Path(storage_uri).resolve().parent if storage_uri else Path.cwd() / ".loopback"
    try:
        receipt = run_runtime_loopback(workspace)
    except Exception:
        print("runtime loopback scenario failed", file=sys.stderr)
        return 1
    print(
        RECEIPT_PREFIX + json.dumps(receipt, allow_nan=False, separators=(",", ":"), sort_keys=True)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
