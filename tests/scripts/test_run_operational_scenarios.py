# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Execution contracts for structured operational scenario receipts."""

from __future__ import annotations

import asyncio
import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, cast

import pytest

import scripts.run_operational_scenarios as operational_runner
from scripts.process_lease_guardian import (
    ACK_DIR_ENV as PROCESS_LEASE_ACK_DIR_ENV,
)
from scripts.process_lease_guardian import (
    CLOSED_ENV as PROCESS_LEASE_CLOSED_ENV,
)
from scripts.process_lease_guardian import (
    LEASE_ENV as PROCESS_LEASE_ENV,
)
from scripts.process_lease_guardian import LEASE_SCHEMA as PROCESS_LEASE_SCHEMA
from scripts.process_lease_guardian import (
    READY_ENV as PROCESS_LEASE_READY_ENV,
)
from scripts.process_lease_guardian import (
    RESULT_SCHEMA as PROCESS_LEASE_RESULT_SCHEMA,
)
from scripts.process_lease_guardian import (
    TARGET_STATUS_DIR_ENV as PROCESS_TARGET_STATUS_DIR_ENV,
)
from scripts.process_lease_guardian import (
    WRAPPER_ENV as PROCESS_LEASE_WRAPPER_ENV,
)
from scripts.process_lease_guardian import _process_birth_identity, guard
from scripts.run_example_receipt import (
    CAPTURED_RECEIPT_ENV,
    MAX_RECEIPT_BYTES,
    MAX_RECEIPT_DEPTH,
    _json_receipt,
    captured_receipt_or_run,
    run_example,
)
from scripts.run_operational_scenarios import (
    RESULT_SCHEMA,
    _adapt_command,
    _adapt_example_command,
    _base_environment,
    _package_probe,
    _parse_eval_receipt,
    _parse_pytest_junit,
    _pytest_command,
    _resolve_wheel_artifacts,
    _run_process,
    _scenario_environment,
    _select_scenarios,
    _semantic_receipt,
    _tested_subject_record,
    _validate_distinct_wheel_location,
    _workspace_source_roots,
    missing_prerequisites,
    run_scenarios,
)
from scripts.validate_operational_scenarios import REGISTRY, ROOT, load_scenarios

RUNNER = ROOT / "scripts" / "run_operational_scenarios.py"


def test_source_quickstart_records_semantics_provenance_and_cleanup(tmp_path: Path) -> None:
    output = tmp_path / "operational.json"

    envelope, passed = run_scenarios(
        root=ROOT,
        registry=REGISTRY,
        output=output,
        mode="source",
        wheel=None,
        cadence="pr",
        scenario_ids={"example.00_quickstart"},
        kind="example",
        min_tier=1,
        max_tier=1,
        expected_revision=None,
        require_clean=False,
        require_run=True,
    )

    assert passed
    assert envelope["schema"] == RESULT_SCHEMA
    assert (
        envelope["revision"]
        == subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    )
    assert envelope["tested_subject"] == {
        "commit": envelope["revision"],
        "dirty": not envelope["clean_checkout"],
        "checkout": str(ROOT),
        "checkout_relationship": "same_as_harness",
    }
    (result,) = cast(list[dict[str, Any]], envelope["results"])
    assert result["status"] == "passed"
    assert result["semantic"]["value"] == 3
    assert result["oracle"]["semantic_input"] == "captured_source_receipt"
    assert result["cleanup"] == {
        "policy": "isolated",
        "status": "closed",
        "process_group_leaked": False,
    }
    assert Path(result["package"]["path"]).is_relative_to(
        ROOT / "packages" / "archetype-ecs" / "src"
    )
    assert set(result["package"]["world_libraries"]) == {
        "archetype.missions",
        "archetype.physical_ai",
        "archetype.research",
    }
    assert result["package"]["tested_subject"] == {
        "commit": envelope["revision"],
        "dirty": not envelope["clean_checkout"],
        "checkout_relationship": "same_as_harness",
    }
    assert result["tested_subject"] == envelope["tested_subject"]
    assert result["source"]["command"] == [
        sys.executable,
        str((ROOT / "examples" / "00_quickstart.py").resolve()),
    ]
    assert result["source"]["returncode"] == 0
    assert result["receipt_capture"]["returncode"] == 0
    stdout_log = Path(result["source"]["stdout"]["path"])
    assert not stdout_log.is_absolute()
    assert stdout_log.parts[0] == f"{output.stem}.d"
    assert (output.parent / stdout_log).is_file()
    assert "archetype-operational-" not in json.dumps(envelope)
    assert "<isolated-run>" in json.dumps(result["receipt_capture"]["command"])
    assert output.is_file()


def test_example_declared_source_command_failure_cannot_be_hidden_by_valid_run_demo(
    tmp_path: Path,
) -> None:
    registry_text = REGISTRY.read_text(encoding="utf-8")
    declared_command = 'source_command = ["python", "examples/00_quickstart.py"]'
    assert registry_text.count(declared_command) == 1
    registry = tmp_path / "operational_scenarios.toml"
    registry.write_text(
        registry_text.replace(
            declared_command,
            'source_command = ["python", "-c", "raise SystemExit(17)"]',
        ),
        encoding="utf-8",
    )
    output = tmp_path / "entrypoint-failed.json"

    envelope, passed = run_scenarios(
        root=ROOT,
        registry=registry,
        output=output,
        mode="source",
        wheel=None,
        cadence="pr",
        scenario_ids={"example.00_quickstart"},
        kind="example",
        min_tier=1,
        max_tier=1,
        expected_revision=None,
        require_clean=False,
        require_run=True,
    )

    assert passed is False
    assert envelope["outcome"] == "failed"
    (result,) = cast(list[dict[str, Any]], envelope["results"])
    assert result["status"] == "failed"
    assert result["source"]["returncode"] == 17
    assert result["receipt_capture"] == {
        "status": "not_run",
        "reason": "declared source command failed",
        "process_group_leaked": False,
    }


def test_isolated_filesystem_cleanup_failure_fails_the_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "cleanup-failed.json"
    remove_tree = operational_runner.shutil.rmtree

    def remove_then_fail(path: Path) -> None:
        remove_tree(path)
        raise OSError("injected isolated cleanup failure")

    monkeypatch.setattr(operational_runner, "_remove_run_root", remove_then_fail)

    envelope, passed = run_scenarios(
        root=ROOT,
        registry=REGISTRY,
        output=output,
        mode="source",
        wheel=None,
        cadence="pr",
        scenario_ids={"example.00_quickstart"},
        kind="example",
        min_tier=1,
        max_tier=1,
        expected_revision=None,
        require_clean=False,
        require_run=True,
    )

    assert passed is False
    assert envelope["outcome"] == "failed"
    assert envelope["cleanup"] == {
        "status": "leaked",
        "path": "<isolated-run>",
        "error": "OSError: injected isolated cleanup failure",
    }
    (result,) = cast(list[dict[str, Any]], envelope["results"])
    assert result["status"] == "failed"
    assert result["reason"] == "isolated filesystem cleanup failed"
    assert result["cleanup"] == {
        "policy": "isolated",
        "status": "leaked",
        "process_group_leaked": False,
        "isolated_filesystem_leaked": True,
        "filesystem_error": "OSError: injected isolated cleanup failure",
    }


@pytest.mark.asyncio
async def test_semantic_oracle_consumes_captured_receipt_without_rerunning_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt = tmp_path / "captured.json"
    receipt.write_text('{"value": 17}\n', encoding="utf-8")
    monkeypatch.setenv(CAPTURED_RECEIPT_ENV, str(receipt))

    async def source_must_not_run(*, storage_uri: str) -> dict[str, object]:
        raise AssertionError(f"unexpected second source execution at {storage_uri}")

    result = await captured_receipt_or_run(
        source_must_not_run,
        str(tmp_path / "storage"),
    )

    assert result == {"value": 17}


def test_example_oracle_rejects_wrong_captured_semantics(tmp_path: Path) -> None:
    receipt = tmp_path / "wrong.json"
    receipt.write_text('{"value": 17}\n', encoding="utf-8")
    environment = os.environ.copy()
    environment[CAPTURED_RECEIPT_ENV] = str(receipt)
    environment["PYTHONPATH"] = os.pathsep.join(
        str(path) for path in (*_workspace_source_roots(ROOT), ROOT)
    )

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "tests/integration/test_core_example_receipts.py"
            "::test_quickstart_receipt_proves_three_processor_ticks",
        ],
        cwd=ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 1
    assert "{'value': 17}" in completed.stdout


def test_example_receipt_budget_rejects_oversized_and_deep_values() -> None:
    with pytest.raises(ValueError, match="byte receipt budget"):
        _json_receipt({"blob": "x" * MAX_RECEIPT_BYTES}, label="oversized")

    deeply_nested: dict[str, object] = {}
    cursor = deeply_nested
    for _ in range(MAX_RECEIPT_DEPTH + 1):
        child: dict[str, object] = {}
        cursor["child"] = child
        cursor = child
    with pytest.raises(ValueError, match="maximum JSON depth"):
        _json_receipt(deeply_nested, label="deep")


def test_missing_credential_is_explicitly_not_available(monkeypatch) -> None:
    row = next(
        scenario for scenario in load_scenarios() if scenario["id"] == "example.05_llm_agents"
    )
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    assert missing_prerequisites(row) == ["credential:OPENAI_API_KEY"]
    assert row["missing_prerequisite"] == "not_run"


def test_timeout_is_failed_and_process_group_is_cleaned(tmp_path: Path) -> None:
    result = _run_process(
        [
            sys.executable,
            "-c",
            (
                "import subprocess, sys, time; "
                "subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)']); "
                "time.sleep(30)"
            ),
        ],
        cwd=tmp_path,
        env=os.environ.copy(),
        timeout_seconds=1,
        log_prefix=tmp_path / "timeout",
        redacted=False,
    )

    assert result["timed_out"] is True
    assert result["returncode"] != 0
    assert result["process_group_leaked"] is False


def test_timeout_reaps_registered_nested_process_group_and_listener(tmp_path: Path) -> None:
    marker = tmp_path / "nested.json"
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    nested_source = """
import json
import os
import signal
import socket
import sys
import time
from pathlib import Path

signal.signal(signal.SIGTERM, signal.SIG_IGN)
listener = socket.socket()
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("127.0.0.1", int(sys.argv[3])))
listener.listen()
Path(sys.argv[1]).write_text(json.dumps({
    "pid": os.getpid(),
    "pgid": os.getpgrp(),
    "parent_pgid": int(sys.argv[2]),
    "port": int(sys.argv[3]),
}))
while True:
    time.sleep(1)
"""
    outer_source = f"""
import os
import subprocess
import sys
import time
from pathlib import Path

marker = Path(sys.argv[1])
nested = subprocess.Popen(
    [
        sys.executable,
        os.environ[{PROCESS_LEASE_WRAPPER_ENV!r}],
        "exec",
        "--lease-prefix",
        "nested",
        "--host",
        "127.0.0.1",
        "--port",
        sys.argv[3],
        "--",
        sys.executable,
        "-c",
        sys.argv[2],
        str(marker),
        str(os.getpgrp()),
        sys.argv[3],
    ],
    start_new_session=True,
)
while not marker.is_file():
    if nested.poll() is not None:
        raise SystemExit("nested listener exited before registration")
    time.sleep(0.01)
time.sleep(60)
"""
    env = os.environ.copy()
    env[operational_runner._PROCESS_LEASE_GUARDIAN_ENV] = "1"
    nested_group: int | None = None
    try:
        result = _run_process(
            [sys.executable, "-c", outer_source, str(marker), nested_source, str(port)],
            cwd=tmp_path,
            env=env,
            timeout_seconds=5,
            log_prefix=tmp_path / "nested-timeout",
            redacted=False,
        )
        state = json.loads(marker.read_text(encoding="utf-8"))
        nested_group = state["pgid"]

        assert state["pid"] != nested_group
        assert nested_group != state["parent_pgid"]
        assert result["timed_out"] is True
        assert result["returncode"] != 0
        assert result["process_group_leaked"] is False
        cleanup = cast(dict[str, Any], result["process_leases"])
        assert cleanup["schema"] == PROCESS_LEASE_RESULT_SCHEMA
        assert cleanup["status"] == "closed"
        assert cleanup["active_lease_count"] == 1
        assert cleanup["errors"] == []
        (lease,) = cast(list[dict[str, Any]], cleanup["leases"])
        assert lease["lease_id"] == f"nested:{nested_group}"
        assert lease["pid"] == nested_group
        assert lease["process_group"] == nested_group
        assert lease["host"] == "127.0.0.1"
        assert lease["port"] == state["port"]
        assert isinstance(lease["birth_identity"], str)
        assert lease["birth_identity"]
        assert lease["ownership_matches"] is True
        assert lease["released"] is False
        assert lease["release_was_truthful"] is None
        assert lease["release_group_was_alive"] is None
        assert lease["release_port_was_open"] is None
        assert lease["group_was_alive"] is True
        assert lease["port_was_open"] is True
        assert lease["group_closed"] is True
        assert lease["port_closed"] is True
        with pytest.raises(ProcessLookupError):
            os.killpg(nested_group, 0)
        with socket.socket() as probe:
            probe.settimeout(0.2)
            assert probe.connect_ex(("127.0.0.1", state["port"])) != 0
    finally:
        if nested_group is not None:
            try:
                os.killpg(nested_group, signal.SIGKILL)
            except ProcessLookupError:
                pass


def test_guarded_wrapper_reports_target_exit_without_abandoning_group(tmp_path: Path) -> None:
    observed = tmp_path / "target-exit.json"
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    outer_source = f"""
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

wrapper = subprocess.Popen(
    [
        sys.executable,
        os.environ[{PROCESS_LEASE_WRAPPER_ENV!r}],
        "exec",
        "--lease-prefix",
        "status",
        "--host",
        "127.0.0.1",
        "--port",
        sys.argv[2],
        "--",
        sys.executable,
        "-c",
        "raise SystemExit(7)",
    ],
    start_new_session=True,
)
lease_id = f"status:{{wrapper.pid}}"
digest = hashlib.sha256(lease_id.encode()).hexdigest()
status = Path(os.environ[{PROCESS_TARGET_STATUS_DIR_ENV!r}]) / f"{{digest}}.target.json"
deadline = time.monotonic() + 5
while time.monotonic() < deadline and not status.is_file():
    if wrapper.poll() is not None:
        raise SystemExit("supervisor abandoned its process group")
    time.sleep(0.01)
if not status.is_file():
    raise SystemExit("supervisor emitted no target status")
payload = json.loads(status.read_text())
payload["wrapper_poll"] = wrapper.poll()
Path(sys.argv[1]).write_text(json.dumps(payload))
time.sleep(60)
"""
    env = os.environ.copy()
    env[operational_runner._PROCESS_LEASE_GUARDIAN_ENV] = "1"

    result = _run_process(
        [sys.executable, "-c", outer_source, str(observed), str(port)],
        cwd=tmp_path,
        env=env,
        timeout_seconds=3,
        log_prefix=tmp_path / "target-exit",
        redacted=False,
    )

    payload = json.loads(observed.read_text())
    assert payload["schema"] == PROCESS_LEASE_SCHEMA
    assert payload["lease_id"] == f"status:{payload['pid']}"
    assert payload["pid"] == payload["process_group"]
    assert payload["target_pid"] != payload["pid"]
    assert payload["returncode"] == 7
    assert payload["status"] == "target_exited"
    assert payload["wrapper_poll"] is None
    assert result["timed_out"] is True
    assert result["process_group_leaked"] is False
    cleanup = cast(dict[str, Any], result["process_leases"])
    assert cleanup["status"] == "closed"


def test_guarded_normal_release_is_verified_before_cleanup_closes(tmp_path: Path) -> None:
    marker = tmp_path / "normal-release.json"
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    target_source = """
import json
import os
import socket
import sys
import time
from pathlib import Path

listener = socket.socket()
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("127.0.0.1", int(sys.argv[2])))
listener.listen()
Path(sys.argv[1]).write_text(json.dumps({"pid": os.getpid(), "pgid": os.getpgrp()}))
time.sleep(60)
"""
    outer_source = f"""
import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, {str(ROOT / "examples")!r})
from biome_agent.bootstrap import terminate

marker = Path(sys.argv[1])
process = subprocess.Popen(
    [
        sys.executable,
        os.environ[{PROCESS_LEASE_WRAPPER_ENV!r}],
        "exec",
        "--lease-prefix",
        "biome",
        "--host",
        "127.0.0.1",
        "--port",
        sys.argv[3],
        "--",
        sys.executable,
        "-c",
        sys.argv[2],
        str(marker),
        sys.argv[3],
    ],
    start_new_session=True,
)
while not marker.is_file():
    if process.poll() is not None:
        raise SystemExit("guarded target exited before readiness")
    time.sleep(0.01)
terminate(
    process,
    host="127.0.0.1",
    port=int(sys.argv[3]),
    term_timeout=1,
    kill_timeout=2,
    port_timeout=2,
)
"""
    env = os.environ.copy()
    env[operational_runner._PROCESS_LEASE_GUARDIAN_ENV] = "1"

    result = _run_process(
        [sys.executable, "-c", outer_source, str(marker), target_source, str(port)],
        cwd=tmp_path,
        env=env,
        timeout_seconds=10,
        log_prefix=tmp_path / "normal-release",
        redacted=False,
    )

    assert result["returncode"] == 0
    assert result["timed_out"] is False
    assert result["launch_error"] is None
    assert result["process_group_leaked"] is False
    cleanup = cast(dict[str, Any], result["process_leases"])
    assert cleanup["status"] == "closed"
    assert cleanup["active_lease_count"] == 0
    assert cleanup["errors"] == []
    (lease,) = cast(list[dict[str, Any]], cleanup["leases"])
    assert lease["released"] is True
    assert lease["release_was_truthful"] is True
    assert lease["release_group_was_alive"] is False
    assert lease["release_port_was_open"] is False
    assert lease["ownership_matches"] is True
    assert lease["group_was_alive"] is False
    assert lease["port_was_open"] is False
    assert lease["group_closed"] is True
    assert lease["port_closed"] is True


def test_guardian_rejects_release_while_group_and_port_are_live(tmp_path: Path) -> None:
    marker = tmp_path / "dishonest-release-ready"
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    target_source = """
import socket
import sys
import time
from pathlib import Path

listener = socket.socket()
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("127.0.0.1", int(sys.argv[2])))
listener.listen()
Path(sys.argv[1]).write_text("ready")
time.sleep(60)
"""
    process = subprocess.Popen(
        [sys.executable, "-c", target_source, str(marker), str(port)],
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not marker.is_file():
            assert process.poll() is None
            time.sleep(0.01)
        assert marker.is_file()
        birth_identity = _process_birth_identity(process.pid)
        assert birth_identity is not None
        lease_id = f"dishonest:{process.pid}"
        acquire = {
            "schema": PROCESS_LEASE_SCHEMA,
            "operation": "acquire",
            "lease_id": lease_id,
            "pid": process.pid,
            "process_group": process.pid,
            "host": "127.0.0.1",
            "port": port,
            "birth_identity": birth_identity,
        }
        release = {
            "schema": PROCESS_LEASE_SCHEMA,
            "operation": "release",
            "lease_id": lease_id,
            "group_was_alive": True,
            "port_was_open": True,
        }
        lease_file = tmp_path / "dishonest-leases.jsonl"
        lease_file.write_text(
            json.dumps(acquire, sort_keys=True) + "\n" + json.dumps(release, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        result_file = tmp_path / "dishonest-result.json"
        os.killpg(process.pid, signal.SIGTERM)
        process.wait(timeout=2)
        with socket.socket() as probe:
            probe.settimeout(0.2)
            assert probe.connect_ex(("127.0.0.1", port)) != 0

        assert guard(lease_file, result_file) is False

        result = json.loads(result_file.read_text(encoding="utf-8"))
        assert result["status"] == "leaked"
        assert result["active_lease_count"] == 0
        (lease,) = result["leases"]
        assert lease["released"] is True
        assert lease["release_was_truthful"] is False
        assert lease["release_group_was_alive"] is True
        assert lease["release_port_was_open"] is True
        assert lease["group_was_alive"] is False
        assert lease["port_was_open"] is False
        assert lease["group_closed"] is True
        assert lease["port_closed"] is True
        with pytest.raises(ProcessLookupError):
            os.killpg(process.pid, 0)
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=2)


def test_guardian_refuses_stale_group_after_original_leader_exits(tmp_path: Path) -> None:
    child_ready = tmp_path / "descendant-ready"
    leader_ready = tmp_path / "leader.json"
    release_leader = tmp_path / "release-leader"
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    child_source = """
import socket
import sys
import time
from pathlib import Path

listener = socket.socket()
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("127.0.0.1", int(sys.argv[2])))
listener.listen()
Path(sys.argv[1]).write_text("ready")
time.sleep(60)
"""
    leader_source = """
import json
import os
import subprocess
import sys
import time
from pathlib import Path

child = subprocess.Popen([
    sys.executable,
    "-c",
    sys.argv[1],
    sys.argv[2],
    sys.argv[5],
])
while not Path(sys.argv[2]).is_file():
    if child.poll() is not None:
        raise SystemExit("descendant exited before readiness")
    time.sleep(0.01)
Path(sys.argv[3]).write_text(json.dumps({
    "pid": os.getpid(),
    "pgid": os.getpgrp(),
    "child_pid": child.pid,
}))
while not Path(sys.argv[4]).is_file():
    time.sleep(0.01)
"""
    leader = subprocess.Popen(
        [
            sys.executable,
            "-c",
            leader_source,
            child_source,
            str(child_ready),
            str(leader_ready),
            str(release_leader),
            str(port),
        ],
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    process_group: int | None = None
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not leader_ready.is_file():
            assert leader.poll() is None
            time.sleep(0.01)
        state = json.loads(leader_ready.read_text())
        process_group = state["pgid"]
        assert state["pid"] == leader.pid == process_group
        birth_identity = _process_birth_identity(leader.pid)
        assert birth_identity is not None
        lease_id = f"leader-exit:{leader.pid}"
        lease_file = tmp_path / "leader-exit-leases.jsonl"
        lease_file.write_text(
            json.dumps(
                {
                    "schema": PROCESS_LEASE_SCHEMA,
                    "operation": "acquire",
                    "lease_id": lease_id,
                    "pid": leader.pid,
                    "process_group": process_group,
                    "host": "127.0.0.1",
                    "port": port,
                    "birth_identity": birth_identity,
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        release_leader.write_text("exit")
        leader.wait(timeout=2)
        with socket.socket() as probe:
            probe.settimeout(0.2)
            assert probe.connect_ex(("127.0.0.1", port)) == 0

        result_file = tmp_path / "leader-exit-result.json"
        assert guard(lease_file, result_file) is False

        result = json.loads(result_file.read_text())
        assert result["status"] == "leaked"
        (lease,) = result["leases"]
        assert lease["ownership_matches"] is False
        assert lease["group_was_alive"] is True
        assert lease["group_closed"] is False
        assert lease["port_closed"] is False
        os.killpg(process_group, 0)
        with socket.socket() as probe:
            probe.settimeout(0.2)
            assert probe.connect_ex(("127.0.0.1", port)) == 0
    finally:
        if process_group is not None:
            try:
                os.killpg(process_group, signal.SIGKILL)
            except ProcessLookupError:
                pass
        if leader.poll() is None:
            leader.kill()
            leader.wait(timeout=2)


def test_shutdown_during_ready_publication_still_closes_guardian(tmp_path: Path) -> None:
    lease_file = tmp_path / "ready-race-leases.jsonl"
    ack_dir = tmp_path / "ready-race-acks"
    ready_file = tmp_path / "ready-race-ready.json"
    closed_file = tmp_path / "ready-race-closed.json"
    result_file = tmp_path / "ready-race-result.json"
    source = """
import os
import signal
import sys
from pathlib import Path

sys.path.insert(0, sys.argv[1])
import scripts.process_lease_guardian as guardian

ready_file = Path(sys.argv[4])
write_marker = guardian._write_marker

def write_then_cancel(path, payload):
    write_marker(path, payload)
    if path == ready_file:
        os.kill(os.getpid(), signal.SIGTERM)

guardian._write_marker = write_then_cancel
closed = guardian.serve(
    lease_file=Path(sys.argv[2]),
    ack_dir=Path(sys.argv[3]),
    ready_file=ready_file,
    closed_file=Path(sys.argv[5]),
    result_file=Path(sys.argv[6]),
)
raise SystemExit(0 if closed else 1)
"""
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            source,
            str(ROOT),
            str(lease_file),
            str(ack_dir),
            str(ready_file),
            str(closed_file),
            str(result_file),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        assert process.wait(timeout=5) == 0
        assert ready_file.is_file()
        assert closed_file.is_file()
        result = json.loads(result_file.read_text())
        assert result["status"] == "closed"
        assert result["active_lease_count"] == 0
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=2)


def test_parent_death_and_repeated_signals_still_close_guarded_listener(
    tmp_path: Path,
) -> None:
    lease_file = tmp_path / "abrupt-leases.jsonl"
    ack_dir = tmp_path / "abrupt-acks"
    ready_file = tmp_path / "abrupt-ready.json"
    closed_file = tmp_path / "abrupt-closed.json"
    result_file = tmp_path / "abrupt-result.json"
    guardian_pid_file = tmp_path / "guardian.pid"
    listener_marker = tmp_path / "listener.json"
    runner_ready = tmp_path / "runner-ready"
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    listener_source = """
import json
import os
import signal
import socket
import sys
import time
from pathlib import Path

signal.signal(signal.SIGTERM, signal.SIG_IGN)
listener = socket.socket()
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("127.0.0.1", int(sys.argv[2])))
listener.listen()
Path(sys.argv[1]).write_text(json.dumps({
    "pid": os.getpid(),
    "pgid": os.getpgrp(),
    "port": int(sys.argv[2]),
}))
time.sleep(60)
"""
    runner_source = """
import os
import subprocess
import sys
import time
from pathlib import Path

guardian = subprocess.Popen(
    [
        sys.executable,
        sys.argv[1],
        "serve",
        "--lease-file",
        sys.argv[2],
        "--ack-dir",
        sys.argv[3],
        "--ready-file",
        sys.argv[4],
        "--closed-file",
        sys.argv[5],
        "--result-file",
        sys.argv[6],
    ],
    stdin=subprocess.PIPE,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
    start_new_session=True,
)
while not Path(sys.argv[4]).is_file():
    if guardian.poll() is not None:
        raise SystemExit("guardian exited before readiness")
    time.sleep(0.01)
env = os.environ.copy()
env.update({
    "ARCHETYPE_OPERATIONAL_PROCESS_LEASE_FILE": sys.argv[2],
    "ARCHETYPE_OPERATIONAL_PROCESS_LEASE_ACK_DIR": sys.argv[3],
    "ARCHETYPE_OPERATIONAL_PROCESS_LEASE_READY_FILE": sys.argv[4],
    "ARCHETYPE_OPERATIONAL_PROCESS_LEASE_CLOSED_FILE": sys.argv[5],
    "ARCHETYPE_OPERATIONAL_PROCESS_LEASE_WRAPPER": sys.argv[1],
    "ARCHETYPE_OPERATIONAL_PROCESS_TARGET_STATUS_DIR": str(
        Path(sys.argv[3]).with_name("abrupt-target-status")
    ),
})
listener = subprocess.Popen(
    [
        sys.executable,
        sys.argv[1],
        "exec",
        "--lease-prefix",
        "abrupt",
        "--host",
        "127.0.0.1",
        "--port",
        sys.argv[10],
        "--",
        sys.executable,
        "-c",
        sys.argv[11],
        sys.argv[8],
        sys.argv[10],
    ],
    env=env,
    start_new_session=True,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
)
while not Path(sys.argv[8]).is_file():
    if listener.poll() is not None:
        raise SystemExit("listener exited before readiness")
    time.sleep(0.01)
Path(sys.argv[7]).write_text(str(guardian.pid))
Path(sys.argv[9]).write_text(str(listener.pid))
time.sleep(60)
"""
    guardian_script = ROOT / "scripts" / "process_lease_guardian.py"
    runner = subprocess.Popen(
        [
            sys.executable,
            "-c",
            runner_source,
            str(guardian_script),
            str(lease_file),
            str(ack_dir),
            str(ready_file),
            str(closed_file),
            str(result_file),
            str(guardian_pid_file),
            str(listener_marker),
            str(runner_ready),
            str(port),
            listener_source,
        ],
        cwd=tmp_path,
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    guardian_group: int | None = None
    listener_group: int | None = None
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and not runner_ready.is_file():
            assert runner.poll() is None
            time.sleep(0.02)
        assert runner_ready.is_file()
        guardian_group = int(guardian_pid_file.read_text())
        listener_state = json.loads(listener_marker.read_text())
        listener_group = listener_state["pgid"]
        assert listener_state["pid"] != listener_group
        with socket.socket() as probe:
            probe.settimeout(0.2)
            assert probe.connect_ex(("127.0.0.1", port)) == 0

        os.kill(runner.pid, signal.SIGKILL)
        runner.wait(timeout=2)

        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not closed_file.is_file():
            time.sleep(0.02)
        assert closed_file.is_file(), "guardian did not observe parent-pipe EOF"
        os.kill(guardian_group, signal.SIGTERM)
        os.kill(guardian_group, signal.SIGTERM)

        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and not result_file.is_file():
            time.sleep(0.05)
        assert result_file.is_file(), "guardian emitted no result after repeated cancellation"
        result = json.loads(result_file.read_text())
        assert result["status"] == "closed"
        assert result["active_lease_count"] == 1
        (lease,) = result["leases"]
        assert lease["ownership_matches"] is True
        assert lease["released"] is False
        assert lease["group_closed"] is True
        assert lease["port_closed"] is True
        with pytest.raises(ProcessLookupError):
            os.killpg(listener_group, 0)
        with socket.socket() as probe:
            probe.settimeout(0.2)
            assert probe.connect_ex(("127.0.0.1", port)) != 0
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                os.killpg(guardian_group, 0)
            except ProcessLookupError:
                break
            time.sleep(0.02)
        else:
            pytest.fail("guardian wrote its result but did not exit")
    finally:
        for process_group in (listener_group, guardian_group, runner.pid):
            if process_group is None:
                continue
            try:
                os.killpg(process_group, signal.SIGKILL)
            except ProcessLookupError:
                pass
        if runner.poll() is None:
            runner.wait(timeout=2)


def test_guarded_exec_refuses_target_when_guardian_closes_before_ack(tmp_path: Path) -> None:
    lease_file = tmp_path / "leases.jsonl"
    ack_dir = tmp_path / "acks"
    ready_file = tmp_path / "ready.json"
    closed_file = tmp_path / "closed.json"
    target_marker = tmp_path / "target-ran"
    ready_file.write_text('{"status":"ready"}\n', encoding="utf-8")
    env = os.environ.copy()
    env.update(
        {
            PROCESS_LEASE_ENV: str(lease_file),
            PROCESS_LEASE_ACK_DIR_ENV: str(ack_dir),
            PROCESS_LEASE_READY_ENV: str(ready_file),
            PROCESS_LEASE_CLOSED_ENV: str(closed_file),
            PROCESS_TARGET_STATUS_DIR_ENV: str(tmp_path / "target-status"),
        }
    )
    wrapper = ROOT / "scripts" / "process_lease_guardian.py"
    process = subprocess.Popen(
        [
            sys.executable,
            str(wrapper),
            "exec",
            "--lease-prefix",
            "preack",
            "--host",
            "127.0.0.1",
            "--port",
            "27750",
            "--",
            sys.executable,
            "-c",
            f"from pathlib import Path; Path({str(target_marker)!r}).write_text('ran')",
        ],
        cwd=tmp_path,
        env=env,
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        record: dict[str, Any] | None = None
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and record is None:
            assert process.poll() is None
            if lease_file.is_file():
                try:
                    records = [json.loads(line) for line in lease_file.read_text().splitlines()]
                except json.JSONDecodeError:
                    records = []
                if len(records) == 1 and records[0].get("birth_identity"):
                    record = records[0]
            time.sleep(0.01)
        assert record is not None, "wrapper never published a complete pre-exec lease"
        assert record["pid"] == process.pid
        assert record["process_group"] == process.pid
        time.sleep(0.1)
        assert process.poll() is None
        assert not target_marker.exists(), "target ran without a guardian acknowledgement"

        closed_file.write_text('{"status":"closed"}\n', encoding="utf-8")
        assert process.wait(timeout=2) != 0
        assert not target_marker.exists()
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=2)


def test_transient_group_signal_denial_still_requires_exit_proof(monkeypatch) -> None:
    requested_signals: list[int] = []

    def signal_then_close(_process_group: int, requested_signal: int) -> None:
        requested_signals.append(requested_signal)
        if requested_signal == signal.SIGTERM:
            raise PermissionError("transient group-wide denial")
        if requested_signals.count(0) > 1:
            raise ProcessLookupError

    monkeypatch.setattr(os, "killpg", signal_then_close)

    assert operational_runner._terminate_process_group(1234) is True
    assert requested_signals == [0, signal.SIGTERM, 0]


def test_persistent_group_signal_denial_reports_cleanup_debt(monkeypatch) -> None:
    requested_signals: list[int] = []

    def deny_signal(_process_group: int, requested_signal: int) -> None:
        requested_signals.append(requested_signal)
        raise PermissionError("persistent group-wide denial")

    monkeypatch.setattr(os, "killpg", deny_signal)
    monkeypatch.setattr(operational_runner, "_PROCESS_TERM_GRACE_SECONDS", 0.0)
    monkeypatch.setattr(operational_runner, "_PROCESS_KILL_GRACE_SECONDS", 0.0)

    assert operational_runner._terminate_process_group(1234) is False
    assert requested_signals == [0, signal.SIGTERM, 0, signal.SIGKILL, 0]


def test_redacted_log_retains_only_digest_and_size(tmp_path: Path) -> None:
    secret = "provider-secret-output"
    result = _run_process(
        [sys.executable, "-c", f"print({secret!r})"],
        cwd=tmp_path,
        env=os.environ.copy(),
        timeout_seconds=10,
        log_prefix=tmp_path / "redacted",
        redacted=True,
    )

    stdout = cast(dict[str, Any], result["stdout"])
    retained = Path(stdout["path"]).read_text(encoding="utf-8")
    assert result["returncode"] == 0
    assert stdout["redacted"] is True
    assert stdout["bytes"] == len(f"{secret}\n".encode())
    assert stdout["digest"].startswith("sha256:")
    assert secret not in retained


def test_setup_failure_still_writes_a_failed_receipt(tmp_path: Path) -> None:
    output = tmp_path / "failed.json"
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        str(path) for path in (*_workspace_source_roots(ROOT), ROOT)
    )

    completed = subprocess.run(
        [
            sys.executable,
            str(RUNNER),
            "--registry",
            str(tmp_path / "missing.toml"),
            "--out",
            str(output),
        ],
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 2
    receipt = json.loads(output.read_text(encoding="utf-8"))
    assert receipt["schema"] == RESULT_SCHEMA
    assert receipt["outcome"] == "failed"
    assert receipt["status_counts"]["failed"] == 1
    assert "missing.toml" in receipt["error"]


def test_explicit_scenario_must_match_every_selection_filter() -> None:
    with pytest.raises(ValueError, match="do not match mode/cadence/tier/kind"):
        _select_scenarios(
            load_scenarios(),
            mode="wheel",
            cadence="pr",
            scenario_ids={"example.05_llm_agents"},
            kind="example",
            min_tier=6,
            max_tier=6,
        )


def test_selection_order_is_stable_independent_of_requested_set_order() -> None:
    selected = _select_scenarios(
        load_scenarios(),
        mode="source",
        cadence="pr",
        scenario_ids={"example.01_world_mutations", "example.00_quickstart"},
        kind="example",
        min_tier=1,
        max_tier=1,
    )

    assert [row["id"] for row in selected] == [
        "example.00_quickstart",
        "example.01_world_mutations",
    ]


@pytest.mark.parametrize("mode", ["source", "wheel"])
def test_pytest_oracle_disables_repository_pythonpath_in_both_modes(
    tmp_path: Path,
    mode: str,
) -> None:
    command = _pytest_command(
        Path("/isolated/python"),
        "/repo/tests/test_contract.py::test_contract",
        mode=mode,
        junit_path=tmp_path / "junit.xml",
    )

    assert command[:4] == ["/isolated/python", "-m", "pytest", "-q"]
    assert command[4:6] == ["-o", "pythonpath="]


def test_adapted_multifile_pytest_cannot_import_checkout_source(tmp_path: Path) -> None:
    checkout_package = tmp_path / "src" / "operational_isolation_probe.py"
    checkout_package.parent.mkdir()
    checkout_package.write_text("ORIGIN = 'checkout'\n", encoding="utf-8")
    wheel_site = tmp_path / "wheel-site"
    wheel_site.mkdir()
    (wheel_site / "operational_isolation_probe.py").write_text(
        "ORIGIN = 'wheel'\n",
        encoding="utf-8",
    )
    (tmp_path / "pyproject.toml").write_text(
        "[tool.pytest.ini_options]\npythonpath = ['src', '.']\n",
        encoding="utf-8",
    )
    tests = tmp_path / "tests"
    tests.mkdir()
    for name in ("test_first.py", "test_second.py"):
        (tests / name).write_text(
            "from operational_isolation_probe import ORIGIN\n\n"
            "def test_import_origin():\n"
            "    assert ORIGIN == 'wheel'\n",
            encoding="utf-8",
        )
    declared = ["pytest", "-q", "tests/test_first.py", "tests/test_second.py"]
    adapted = _adapt_command(declared, python=Path(sys.executable), root=tmp_path)
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(wheel_site)
    environment["PYTHONNOUSERSITE"] = "1"
    environment.pop("PYTHONHOME", None)

    leaking = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", *adapted[-2:]],
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    isolated = subprocess.run(
        adapted,
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert leaking.returncode == 1
    assert adapted[:6] == [
        sys.executable,
        "-m",
        "pytest",
        "-o",
        "pythonpath=",
        "-q",
    ]
    assert isolated.returncode == 0, isolated.stdout + isolated.stderr


def test_distinct_source_environment_and_probe_bind_to_tested_checkout(
    tmp_path: Path,
) -> None:
    tested_checkout = tmp_path / "tested"
    package_dir = tested_checkout / "packages" / "archetype-ecs" / "src" / "archetype"
    package_dir.mkdir(parents=True)
    (package_dir / "__init__.py").write_text(
        "from pkgutil import extend_path\n"
        "__path__ = extend_path(__path__, __name__)\n"
        "__version__ = 'baseline-test'\n",
        encoding="utf-8",
    )
    for package, module in (
        ("archetype-missions", "missions"),
        ("archetype-physical-ai", "physical_ai"),
        ("archetype-research", "research"),
    ):
        library = tested_checkout / "packages" / package / "src" / "archetype" / module
        library.mkdir(parents=True)
        (library / "__init__.py").write_text("value = 1\n", encoding="utf-8")
    environment = _base_environment(
        ROOT,
        mode="source",
        tested_checkout=tested_checkout,
    )

    package = _package_probe(
        Path(sys.executable),
        cwd=tmp_path,
        env=environment,
        root=ROOT,
        tested_checkout=tested_checkout,
        mode="source",
    )

    assert environment["PYTHONPATH"].split(os.pathsep) == [
        *(str(path) for path in _workspace_source_roots(tested_checkout)),
        str(ROOT),
    ]
    assert package["version"] == "baseline-test"
    assert Path(cast(str, package["path"])).is_relative_to(package_dir)


def test_tested_subject_relationship_and_distinct_wheel_location_are_explicit(
    tmp_path: Path,
) -> None:
    tested_checkout = tmp_path / "tested"
    tested_checkout.mkdir()
    inside_wheel = tested_checkout / "dist" / "subject.whl"
    inside_wheel.parent.mkdir()
    inside_wheel.touch()
    outside_wheel = tmp_path / "outside.whl"
    outside_wheel.touch()

    subject = _tested_subject_record(
        root=ROOT,
        tested_checkout=tested_checkout,
        revision="8d3700eb",
        dirty=False,
    )

    assert subject["checkout_relationship"] == "distinct_from_harness"
    _validate_distinct_wheel_location(
        wheel=inside_wheel,
        root=ROOT,
        tested_checkout=tested_checkout,
    )
    with pytest.raises(ValueError, match="must be located under that checkout"):
        _validate_distinct_wheel_location(
            wheel=outside_wheel,
            root=ROOT,
            tested_checkout=tested_checkout,
        )
    # The default same-checkout flow remains compatible with external build
    # directories used by existing callers.
    _validate_distinct_wheel_location(
        wheel=outside_wheel,
        root=ROOT,
        tested_checkout=ROOT,
    )


def _write_first_party_wheel_set(directory: Path) -> dict[str, Path]:
    wheels = {
        "archetype-ecs": directory / "archetype_ecs-0.6.0-py3-none-any.whl",
        "archetype-missions": directory / "archetype_missions-0.6.0-py3-none-any.whl",
        "archetype-physical-ai": directory / "archetype_physical_ai-0.6.0-py3-none-any.whl",
        "archetype-research": directory / "archetype_research-0.6.0-py3-none-any.whl",
    }
    directory.mkdir(parents=True, exist_ok=True)
    for distribution, path in wheels.items():
        path.write_bytes(f"exact artifact for {distribution}\n".encode())
    return wheels


def test_wheel_mode_installs_and_records_the_exact_four_artifact_set(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wheels = _write_first_party_wheel_set(tmp_path / "dist")
    anchor = wheels["archetype-ecs"]
    installed: list[Path] = []

    def prepare(selected: tuple[Path, ...], destination: Path):
        del destination
        installed.extend(selected)
        return Path(sys.executable), _base_environment(tmp_path, mode="wheel")

    monkeypatch.setattr(operational_runner, "_prepare_wheel_python", prepare)
    monkeypatch.setattr(
        operational_runner,
        "_package_probe",
        lambda *args, **kwargs: {
            "version": "0.6.0",
            "path": "/isolated/archetype/__init__.py",
            "world_libraries": {},
            "origin": "wheel",
        },
    )
    monkeypatch.setattr(
        operational_runner,
        "_run_one",
        lambda *args, **kwargs: {
            "status": "passed",
            "cleanup": {"status": "closed"},
        },
    )

    envelope, passed = run_scenarios(
        root=ROOT,
        registry=REGISTRY,
        output=tmp_path / "operational.json",
        mode="wheel",
        wheel=anchor,
        wheel_dir=tmp_path / "dist",
        cadence="pr",
        scenario_ids={"example.00_quickstart"},
        kind="example",
        min_tier=1,
        max_tier=1,
        expected_revision=None,
        require_clean=False,
        require_run=True,
    )

    assert passed
    assert installed == list(wheels.values())
    wheel_record = cast(dict[str, Any], envelope["wheel"])
    assert wheel_record["filename"] == anchor.name
    assert wheel_record["digest"].startswith("sha256:")
    assert [artifact["distribution"] for artifact in wheel_record["artifacts"]] == list(wheels)
    assert [artifact["filename"] for artifact in wheel_record["artifacts"]] == [
        path.name for path in wheels.values()
    ]
    assert all(artifact["digest"].startswith("sha256:") for artifact in wheel_record["artifacts"])


def test_wheel_set_resolution_rejects_missing_duplicate_and_mixed_versions(
    tmp_path: Path,
) -> None:
    wheels = _write_first_party_wheel_set(tmp_path)
    anchor = wheels["archetype-ecs"]

    assert [
        artifact.distribution
        for artifact in _resolve_wheel_artifacts(
            wheel=anchor,
            wheel_dir=tmp_path,
        )
    ] == list(wheels)

    wheels["archetype-research"].unlink()
    with pytest.raises(ValueError, match="exactly one local wheel for archetype-research"):
        _resolve_wheel_artifacts(wheel=anchor, wheel_dir=tmp_path)

    mixed = tmp_path / "archetype_research-0.6.3-py3-none-any.whl"
    mixed.write_bytes(b"mixed version")
    with pytest.raises(ValueError, match="same-version first-party artifact set"):
        _resolve_wheel_artifacts(wheel=anchor, wheel_dir=tmp_path)

    expected = tmp_path / "archetype_research-0.6.0-py3-none-any.whl"
    expected.write_bytes(b"expected version")
    with pytest.raises(ValueError, match="exactly one local wheel for archetype-research"):
        _resolve_wheel_artifacts(wheel=anchor, wheel_dir=tmp_path)


def test_wheel_probe_rejects_leakage_from_any_world_library_source_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment = tmp_path / "wheel-venv"
    python = environment / "bin" / "python"
    research_source = (
        ROOT / "packages" / "archetype-research" / "src" / "archetype" / "research" / "__init__.py"
    )
    payload = {
        "version": "0.6.0",
        "path": str(environment / "lib" / "archetype" / "__init__.py"),
        "library_paths": {
            "archetype.missions": str(
                environment / "lib" / "archetype" / "missions" / "__init__.py"
            ),
            "archetype.physical_ai": str(
                environment / "lib" / "archetype" / "physical_ai" / "__init__.py"
            ),
            "archetype.research": str(research_source),
        },
        "sys_path": [],
    }
    monkeypatch.setattr(
        operational_runner.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args=args[0],
            returncode=0,
            stdout=json.dumps(payload) + "\n",
            stderr="",
        ),
    )

    with pytest.raises(RuntimeError, match="imported archetype.research from checkout source"):
        _package_probe(
            python,
            cwd=tmp_path,
            env={},
            root=ROOT,
            tested_checkout=ROOT,
            mode="wheel",
        )


def test_external_service_prerequisites_enable_dedicated_test_lanes() -> None:
    docker = _scenario_environment(
        {},
        {"prerequisites": ["service:docker"]},
    )
    apple = _scenario_environment(
        {},
        {"prerequisites": ["service:apple-container"]},
    )
    modal = _scenario_environment(
        {},
        {
            "id": "dogfood.agent_mission.modal_live",
            "prerequisites": ["credential:MODAL_TOKEN_ID"],
        },
    )
    biome = _scenario_environment(
        {},
        {
            "id": "example.14_biome_agent",
            "prerequisites": ["infrastructure:ARCHETYPE_BIOME_LIVE"],
        },
    )

    assert docker["ARCHETYPE_DOCKER_SANDBOX_PARITY"] == "1"
    assert apple["ARCHETYPE_APPLE_CONTAINER_SANDBOX_PARITY"] == "1"
    assert modal["ARCHETYPE_MODAL_AGENT_MISSION_LIVE"] == "1"
    assert biome[operational_runner._PROCESS_LEASE_GUARDIAN_ENV] == "1"


def test_skipped_only_pytest_evidence_cannot_pass(tmp_path: Path) -> None:
    junit = tmp_path / "junit.xml"
    junit.write_text(
        '<testsuites><testsuite tests="1" failures="0" errors="0" skipped="1">'
        '<testcase name="external"><skipped /></testcase>'
        "</testsuite></testsuites>",
        encoding="utf-8",
    )

    passed, semantic = _parse_pytest_junit(junit)

    assert passed is False
    pytest_counts = cast(dict[str, int], semantic["pytest"])
    assert pytest_counts["skipped"] == 1


def test_eval_receipt_is_exact_revision_bound_and_preserves_structured_evidence(
    tmp_path: Path,
) -> None:
    receipt = tmp_path / "eval.json"
    receipt.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "kind": "eval",
                "revision": {"commit": "abc123", "dirty": False},
                "invocation": [str(ROOT / "evals" / "run.py"), "--profile", "capability"],
                "results": [
                    {
                        "task_id": "agent_mission_transition_authority",
                        "all_passed": True,
                        "contract_ids": ["missions.agent_v1.exact_head_critic"],
                        "trials": [
                            {
                                "passed": True,
                                "graders": [
                                    {
                                        "name": "exact_head",
                                        "passed": True,
                                        "evidence": {
                                            "candidate_digest": "sha256:abc",
                                            "cleanup": "closed",
                                        },
                                    }
                                ],
                            }
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    passed, semantic = _parse_eval_receipt(
        receipt,
        reference=(
            "evals/suites/capability/agent_missions.py::task_agent_mission_transition_authority"
        ),
        expected_revision="abc123",
        expected_dirty=False,
        root=ROOT,
    )

    assert passed
    assert semantic["grader_names"] == ["exact_head"]
    assert semantic["grader_evidence"] == [
        {
            "trial": 0,
            "grader": "exact_head",
            "evidence": {
                "candidate_digest": "sha256:abc",
                "cleanup": "closed",
            },
        }
    ]
    with pytest.raises(ValueError, match="revision does not match"):
        _parse_eval_receipt(
            receipt,
            reference=(
                "evals/suites/capability/agent_missions.py::task_agent_mission_transition_authority"
            ),
            expected_revision="different",
            expected_dirty=False,
            root=ROOT,
        )


def test_semantic_receipt_rejects_duplicates_and_non_standard_json() -> None:
    with pytest.raises(ValueError, match="more than one"):
        _semantic_receipt(
            'ARCHETYPE_OPERATIONAL_RECEIPT={"ok": true}\n'
            'ARCHETYPE_OPERATIONAL_RECEIPT={"ok": true}\n'
        )
    with pytest.raises(ValueError, match="strict JSON"):
        _semantic_receipt('ARCHETYPE_OPERATIONAL_RECEIPT={"value": NaN}\n')


def test_relocated_example_command_uses_copied_source_and_preserves_flags(
    tmp_path: Path,
) -> None:
    source_path = ROOT / "examples" / "09_cloud_storage.py"
    execution_source_path = tmp_path / source_path.name
    execution_source_path.write_text("# isolated copy\n", encoding="utf-8")

    command = _adapt_example_command(
        ["python", "examples/09_cloud_storage.py", "--smoke-local"],
        python=Path(sys.executable),
        root=ROOT,
        source_path=source_path,
        execution_source_path=execution_source_path,
    )

    assert command == [
        sys.executable,
        str(execution_source_path.resolve()),
        "--smoke-local",
    ]
    assert str(source_path.resolve()) not in command


def test_relocated_example_command_fails_closed_without_declared_source(
    tmp_path: Path,
) -> None:
    execution_source_path = tmp_path / "00_quickstart.py"
    execution_source_path.write_text("# isolated copy\n", encoding="utf-8")

    with pytest.raises(ValueError, match="must contain source_path exactly once"):
        _adapt_example_command(
            ["python", "-c", "raise SystemExit(0)"],
            python=Path(sys.executable),
            root=ROOT,
            source_path=ROOT / "examples" / "00_quickstart.py",
            execution_source_path=execution_source_path,
        )


def test_example_receipt_cannot_leak_isolated_storage_path(tmp_path: Path) -> None:
    source = tmp_path / "leaky_example.py"
    source.write_text(
        "async def run_demo(storage_uri: str):\n    return {'storage': storage_uri}\n",
        encoding="utf-8",
    )
    storage = str(tmp_path / "private-storage")

    with pytest.raises(ValueError, match="leaked its isolated storage location"):
        asyncio.run(run_example(source, storage))


def test_invalid_tier_selection_still_writes_a_failed_receipt(tmp_path: Path) -> None:
    output = tmp_path / "invalid-tier.json"
    completed = subprocess.run(
        [
            sys.executable,
            str(RUNNER),
            "--out",
            str(output),
            "--min-tier",
            "3",
            "--max-tier",
            "1",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 2
    receipt = json.loads(output.read_text(encoding="utf-8"))
    assert receipt["outcome"] == "failed"
    assert "tiers must satisfy" in receipt["error"]


def test_argument_parse_failure_still_writes_a_failed_receipt(tmp_path: Path) -> None:
    output = tmp_path / "invalid-argument.json"
    completed = subprocess.run(
        [
            sys.executable,
            str(RUNNER),
            "--out",
            str(output),
            "--unknown-runner-option",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 2
    receipt = json.loads(output.read_text(encoding="utf-8"))
    assert receipt["outcome"] == "failed"
    assert "unrecognized arguments" in receipt["error"]


def test_expected_tested_revision_failure_records_both_provenance_planes(
    tmp_path: Path,
) -> None:
    output = tmp_path / "tested-revision-mismatch.json"
    completed = subprocess.run(
        [
            sys.executable,
            str(RUNNER),
            "--out",
            str(output),
            "--tested-checkout",
            str(ROOT),
            "--expected-tested-revision",
            "not-the-active-revision",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 2
    receipt = json.loads(output.read_text(encoding="utf-8"))
    assert receipt["outcome"] == "failed"
    assert receipt["tested_subject"] == {
        "commit": receipt["revision"],
        "dirty": not receipt["clean_checkout"],
        "checkout": str(ROOT),
        "checkout_relationship": "same_as_harness",
    }
    assert "expected tested-subject revision" in receipt["error"]
