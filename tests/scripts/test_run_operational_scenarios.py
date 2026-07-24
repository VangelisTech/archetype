# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Execution contracts for structured operational scenario receipts."""

from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
from pathlib import Path
from typing import Any, cast

import pytest

import scripts.run_operational_scenarios as operational_runner
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
    _adapt_example_command,
    _base_environment,
    _package_probe,
    _parse_eval_receipt,
    _parse_pytest_junit,
    _pytest_command,
    _run_process,
    _scenario_environment,
    _select_scenarios,
    _semantic_receipt,
    _tested_subject_record,
    _validate_distinct_wheel_location,
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
    assert Path(result["package"]["path"]).is_relative_to(ROOT / "src")
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
    environment["PYTHONPATH"] = os.pathsep.join((str(ROOT / "src"), str(ROOT)))

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
    env["PYTHONPATH"] = os.pathsep.join((str(ROOT / "src"), str(ROOT)))

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
            scenario_ids={"example.04_messaging"},
            kind="example",
            min_tier=1,
            max_tier=1,
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


def test_distinct_source_environment_and_probe_bind_to_tested_checkout(
    tmp_path: Path,
) -> None:
    tested_checkout = tmp_path / "tested"
    package_dir = tested_checkout / "src" / "archetype"
    package_dir.mkdir(parents=True)
    (package_dir / "__init__.py").write_text(
        "__version__ = 'baseline-test'\n",
        encoding="utf-8",
    )
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
        str(tested_checkout.resolve() / "src"),
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


def test_external_service_prerequisites_enable_dedicated_test_lanes() -> None:
    docker = _scenario_environment(
        {},
        {"prerequisites": ["service:docker"]},
    )
    apple = _scenario_environment(
        {},
        {"prerequisites": ["service:apple-container"]},
    )

    assert docker["ARCHETYPE_DOCKER_SANDBOX_PARITY"] == "1"
    assert apple["ARCHETYPE_APPLE_CONTAINER_SANDBOX_PARITY"] == "1"


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
