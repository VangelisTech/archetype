# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the Tier-0 operational-scenario inventory."""

from __future__ import annotations

import copy
import json
import re
import tomllib
from pathlib import Path
from typing import Any

import pytest

from scripts.validate_operational_scenarios import (
    REGISTRY,
    ROOT,
    load_scenarios,
    validate_operational_scenarios,
)

_VALID_REGISTRY = """\
version = 1

[[scenario]]
id = "example.00_quickstart"
kind = "example"
owner = "runtime"
owner_paths = ["packages/archetype-ecs/src/archetype/runtime"]
source_path = "examples/00_quickstart.py"
source_command = ["python", "examples/00_quickstart.py"]
required_extras = []
tier = 1
applicability = ["source", "wheel"]
timeout_seconds = 60
prerequisites = []
missing_prerequisite = "fail"
semantic_oracle = { kind = "pytest", ref = "tests/test_example.py::test_demo" }
contracts = ["runtime.lifecycle"]
cleanup_policy = "isolated"
artifact_policy = "receipt"
artifact_schema = "archetype.operational-results/v1"
required_cadence = ["pr", "main", "release"]
workflow = { path = ".github/workflows/external.yml", trigger_paths = ["packages/archetype-ecs/src/archetype/runtime/**"] }
"""

_WORKFLOW = """\
name: Fixture external evidence
on:
  workflow_dispatch:
  pull_request:
    paths:
      - "packages/archetype-ecs/src/archetype/runtime/**"
jobs:
  evidence:
    runs-on: ubuntu-latest
"""

_VALID_CONTRACTS = """\
version = 1

[[contract]]
id = "runtime.lifecycle"
"""

_REVISION = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
_TRACKED_RECEIPT = f"""\

[[tracked_receipt]]
id = "agent-mission-baseline"
path = "eval-capability-results.json"
scenario_id = "example.00_quickstart"
expected_revision = "{_REVISION}"
require_clean = true
expected_kind = "eval"
expected_profile = "capability"
expected_outcome = "passed"
invocation_entrypoint = "evals/run.py"
required_task_id = "agent_mission_transition_authority"
required_graders = [
  "mission_processors_own_transitions",
  "mission_retry_uses_repository_evidence",
]
"""

_VALID_RECEIPT: dict[str, Any] = {
    "schema_version": 1,
    "kind": "eval",
    "profile": "capability",
    "outcome": "passed",
    "revision": {"commit": _REVISION, "dirty": False},
    "invocation": ["evals/run.py", "--profile", "capability"],
    "results": [
        {
            "task_id": "agent_mission_transition_authority",
            "all_passed": True,
            "trials": [
                {
                    "trial": 0,
                    "passed": True,
                    "graders": [
                        {
                            "name": "mission_processors_own_transitions",
                            "passed": True,
                        },
                        {
                            "name": "mission_retry_uses_repository_evidence",
                            "passed": True,
                        },
                    ],
                }
            ],
        }
    ],
}


def _write_fixture(
    root: Path,
    *,
    registry: str = _VALID_REGISTRY,
    workflow: str = _WORKFLOW,
    receipt: dict[str, Any] | str | None = None,
) -> Path:
    (root / "quality").mkdir()
    (root / "examples").mkdir()
    (root / "packages" / "archetype-ecs" / "src" / "archetype" / "runtime").mkdir(parents=True)
    (root / "tests").mkdir()
    (root / "evals").mkdir()
    (root / ".github" / "workflows").mkdir(parents=True)
    (root / "examples" / "00_quickstart.py").write_text("print(3)\n", encoding="utf-8")
    (root / "evals" / "run.py").write_text("raise SystemExit(0)\n", encoding="utf-8")
    (root / "tests" / "test_example.py").write_text(
        "def test_demo():\n    assert True\n",
        encoding="utf-8",
    )
    (root / "quality" / "contracts.toml").write_text(
        _VALID_CONTRACTS,
        encoding="utf-8",
    )
    (root / ".github" / "workflows" / "external.yml").write_text(
        workflow,
        encoding="utf-8",
    )
    registry_path = root / "quality" / "operational_scenarios.toml"
    registry_path.write_text(registry, encoding="utf-8")
    if receipt is not None:
        encoded = receipt if isinstance(receipt, str) else json.dumps(receipt)
        (root / "eval-capability-results.json").write_text(encoded, encoding="utf-8")
    return registry_path


def _audit(root: Path, registry: str = _VALID_REGISTRY, workflow: str = _WORKFLOW) -> list[str]:
    path = _write_fixture(root, registry=registry, workflow=workflow)
    return validate_operational_scenarios(root=root, registry_path=path)


def _audit_receipt(
    root: Path,
    *,
    receipt: dict[str, Any] | str = _VALID_RECEIPT,
    tracked_receipt: str = _TRACKED_RECEIPT,
) -> list[str]:
    path = _write_fixture(
        root,
        registry=f"{_VALID_REGISTRY}{tracked_receipt}",
        receipt=receipt,
    )
    return validate_operational_scenarios(root=root, registry_path=path)


def test_repository_operational_scenario_registry_is_valid() -> None:
    assert REGISTRY.is_file()
    assert validate_operational_scenarios(root=ROOT, registry_path=REGISTRY) == []


def test_repository_baselines_are_not_generated_verification_outputs() -> None:
    with REGISTRY.open("rb") as stream:
        registry = tomllib.load(stream)
    baseline_paths = {row["path"] for row in registry.get("tracked_receipt", [])}
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    generated_paths = {
        path.strip("\"'") for path in re.findall(r"--out\s+(?P<path>[^ \\\n]+)", makefile)
    }

    assert all(path.startswith("quality/baselines/") for path in baseline_paths)
    assert baseline_paths.isdisjoint(generated_paths)
    ignore_rules = (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
    assert "/eval-*-results.json" in ignore_rules
    assert "/operational*-results.json" in ignore_rules
    assert "/operational*-results.d/" in ignore_rules


def test_external_rows_claim_only_the_cadences_and_contracts_their_jobs_enforce() -> None:
    rows = {row["id"]: row for row in load_scenarios()}
    generic_schema = "archetype.operational-results/v1"

    docker = rows["dogfood.sandbox.docker"]
    r2 = rows["dogfood.storage.r2"]
    apple = rows["dogfood.sandbox.apple_container"]

    assert docker["required_cadence"] == ["release"]
    assert r2["required_cadence"] == ["pr", "main", "release"]
    assert apple["required_cadence"] == ["demand"]
    assert {docker["artifact_schema"], r2["artifact_schema"], apple["artifact_schema"]} == {
        generic_schema
    }
    assert docker["contracts"] == ["missions.sandbox.checkpoint_restore"]
    assert apple["contracts"] == ["missions.sandbox.checkpoint_restore"]
    assert r2["contracts"] == [
        "runtime.trust.actor_free",
        "world.fork.lineage",
        "world.run_identity.cold_resume",
        "ingestion.catalog.cold_roundtrip",
        "artifacts.ingestion.occurrence_identity",
        "artifacts.ingestion.common_visibility",
    ]


def test_example_rows_claim_only_the_behavior_their_receipts_exercise() -> None:
    rows = {row["id"]: row for row in load_scenarios()}

    llm = rows["example.05_llm_agents"]
    assert llm["semantic_oracle"] == {
        "kind": "pytest",
        "ref": (
            "tests/integration/test_llm_example_receipt.py::"
            "test_llm_agent_receipt_proves_per_tick_thought_coverage"
        ),
    }
    assert rows["example.01_world_mutations"]["contracts"] == [
        "world.mutations.compose_deterministically"
    ]
    assert rows["example.00_quickstart"]["contracts"] == [
        "core.processors.execution",
        "runtime.trust.actor_free",
    ]
    assert rows["example.02_fork_counterfactual"]["contracts"] == ["world.fork.lineage"]
    assert rows["example.03_time_travel"]["contracts"] == [
        "world.run_identity.cold_resume",
        "query.cold_reads.correct",
    ]
    assert rows["example.06_trajectory_analysis"]["contracts"] == [
        "missions.trajectory.runtime_service"
    ]
    assert rows["example.09_cloud_storage"]["contracts"] == ["runtime.trust.actor_free"]
    assert rows["example.11_coding_agent_mission.dry_run"]["contracts"] == [
        "missions.agent_v1.public_authoring"
    ]
    for row in rows.values():
        assert row["artifact_schema"] == "archetype.operational-results/v1"


def test_runtime_loopback_is_required_source_and_wheel_dogfood() -> None:
    rows = {row["id"]: row for row in load_scenarios()}
    loopback = rows["dogfood.runtime.loopback"]

    assert loopback["kind"] == "dogfood"
    assert loopback["owner"] == "runtime"
    assert loopback["owner_paths"] == [
        "packages/archetype-ecs/src/archetype/runtime",
        "packages/archetype-ecs/src/archetype/runtime_resources.py",
        "packages/archetype-ecs/src/archetype/wiring.py",
        "packages/archetype-ecs/src/archetype/api",
        "packages/archetype-ecs/src/archetype/commands",
    ]
    assert loopback["tier"] == 1
    assert loopback["applicability"] == ["source", "wheel"]
    assert loopback["prerequisites"] == []
    assert loopback["missing_prerequisite"] == "fail"
    assert loopback["contracts"] == [
        "runtime.trust.actor_free",
        "runtime.lifecycle.single_flight_and_drain",
        "runtime.lifecycle.retryable_teardown",
        "gateway.authorization.rbac",
        "world.fork.lineage",
    ]
    assert loopback["cleanup_policy"] == "isolated"
    assert loopback["artifact_policy"] == "receipt"
    assert loopback["artifact_schema"] == "archetype.operational-results/v1"
    assert loopback["required_cadence"] == ["pr", "main", "release"]


def test_operational_audit_runs_in_the_pr_static_profile() -> None:
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    lint = re.search(r"^lint:(?P<dependencies>[^\n]*)$", makefile, re.MULTILINE)

    assert lint is not None
    assert "operational-audit" in lint.group("dependencies").split()
    assert re.search(
        r"^operational-audit:\n"
        r"\t@PYTHONPATH=\$\(PYTHONPATH\):\. uv run python "
        r"scripts/validate_operational_scenarios\.py$",
        makefile,
        re.MULTILINE,
    )


def test_minimal_registry_is_valid_and_accepts_explicit_workflow_owners(
    tmp_path: Path,
) -> None:
    assert _audit(tmp_path) == []


def test_optional_tracked_receipt_validates_exact_capability_evidence(
    tmp_path: Path,
) -> None:
    assert _audit_receipt(tmp_path) == []


def test_tracked_receipt_rejects_stale_revision(tmp_path: Path) -> None:
    receipt = copy.deepcopy(_VALID_RECEIPT)
    receipt["revision"]["commit"] = "b" * 40

    errors = _audit_receipt(tmp_path, receipt=receipt)

    assert any("does not match expected_revision" in error for error in errors)


def test_tracked_receipt_rejects_dirty_clean_evidence(tmp_path: Path) -> None:
    receipt = copy.deepcopy(_VALID_RECEIPT)
    receipt["revision"]["dirty"] = True

    errors = _audit_receipt(tmp_path, receipt=receipt)

    assert any("produced from a dirty checkout" in error for error in errors)


def test_tracked_receipt_rejects_invocation_outside_active_checkout(
    tmp_path: Path,
) -> None:
    receipt = copy.deepcopy(_VALID_RECEIPT)
    receipt["invocation"][0] = "../another-checkout/evals/run.py"

    errors = _audit_receipt(tmp_path, receipt=receipt)

    assert any("invocation is outside the active checkout" in error for error in errors)


def test_tracked_receipt_requires_portable_relative_invocation(tmp_path: Path) -> None:
    receipt = copy.deepcopy(_VALID_RECEIPT)
    receipt["invocation"][0] = str((tmp_path / "evals" / "run.py").resolve())

    errors = _audit_receipt(tmp_path, receipt=receipt)

    assert any("invocation entrypoint must be repository-relative" in error for error in errors)


def test_tracked_receipt_requires_named_task_exactly_once(tmp_path: Path) -> None:
    receipt = copy.deepcopy(_VALID_RECEIPT)
    receipt["results"][0]["task_id"] = "another_task"

    errors = _audit_receipt(tmp_path, receipt=receipt)

    assert any("exactly one result for task" in error for error in errors)


def test_tracked_receipt_requires_every_named_grader_in_each_trial(
    tmp_path: Path,
) -> None:
    receipt = copy.deepcopy(_VALID_RECEIPT)
    receipt["results"][0]["trials"][0]["graders"].pop()

    errors = _audit_receipt(tmp_path, receipt=receipt)

    assert any("missing required graders" in error for error in errors)


def test_tracked_receipt_requires_nonempty_trials(tmp_path: Path) -> None:
    receipt = copy.deepcopy(_VALID_RECEIPT)
    receipt["results"][0]["trials"] = []

    errors = _audit_receipt(tmp_path, receipt=receipt)

    assert any("non-empty trial evidence" in error for error in errors)


def test_tracked_receipt_scenario_must_resolve(tmp_path: Path) -> None:
    tracked_receipt = _TRACKED_RECEIPT.replace(
        'scenario_id = "example.00_quickstart"',
        'scenario_id = "dogfood.unknown"',
    )

    errors = _audit_receipt(tmp_path, tracked_receipt=tracked_receipt)

    assert any("references unknown scenario 'dogfood.unknown'" in error for error in errors)


def test_tracked_receipt_rejects_malformed_receipt_schema(tmp_path: Path) -> None:
    receipt = copy.deepcopy(_VALID_RECEIPT)
    receipt["schema_version"] = 2
    receipt["revision"] = []

    errors = _audit_receipt(tmp_path, receipt=receipt)

    assert any("schema_version must be 1" in error for error in errors)
    assert any("revision must be an object" in error for error in errors)


def test_tracked_receipt_manifest_has_strict_known_fields(tmp_path: Path) -> None:
    tracked_receipt = f'{_TRACKED_RECEIPT}unexpected = "drift"\n'

    errors = _audit_receipt(tmp_path, tracked_receipt=tracked_receipt)

    assert any("unknown fields ['unexpected']" in error for error in errors)


def test_every_numbered_example_requires_a_scenario(tmp_path: Path) -> None:
    registry_path = _write_fixture(tmp_path)
    (tmp_path / "examples" / "14_unregistered.py").write_text(
        "print('unregistered')\n",
        encoding="utf-8",
    )

    errors = validate_operational_scenarios(root=tmp_path, registry_path=registry_path)

    assert any("14_unregistered.py" in error for error in errors)


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        (
            'owner_paths = ["packages/archetype-ecs/src/archetype/runtime"]',
            'owner_paths = ["packages/archetype-ecs/src/archetype/missing"]',
            "owner_paths names a missing path",
        ),
        (
            'source_path = "examples/00_quickstart.py"',
            'source_path = "examples/missing.py"',
            "source_path names a missing file",
        ),
        ("tier = 1", "tier = true", "tier must be an integer"),
        (
            'applicability = ["source", "wheel"]',
            'applicability = ["editable"]',
            "unknown applicability",
        ),
        ("timeout_seconds = 60", "timeout_seconds = 0", "must be a positive integer"),
        (
            "prerequisites = []",
            'prerequisites = ["OPENAI_API_KEY"]',
            "invalid prerequisite",
        ),
        (
            'semantic_oracle = { kind = "pytest", ref = "tests/test_example.py::test_demo" }',
            'semantic_oracle = { kind = "exit", ref = "zero" }',
            "semantic_oracle.kind",
        ),
        (
            'cleanup_policy = "isolated"',
            'cleanup_policy = "best_effort"',
            "cleanup_policy",
        ),
        (
            'artifact_policy = "receipt"',
            'artifact_policy = "stdout"',
            "artifact_policy",
        ),
        (
            'required_cadence = ["pr", "main", "release"]',
            'required_cadence = ["nightly"]',
            "unknown required_cadence",
        ),
    ],
)
def test_execution_and_ownership_fields_fail_closed(
    tmp_path: Path,
    old: str,
    new: str,
    message: str,
) -> None:
    assert _VALID_REGISTRY.count(old) == 1
    errors = _audit(tmp_path, _VALID_REGISTRY.replace(old, new))

    assert any(message in error for error in errors), errors


def test_required_scenario_needs_a_semantic_oracle(tmp_path: Path) -> None:
    registry = _VALID_REGISTRY.replace(
        'semantic_oracle = { kind = "pytest", ref = "tests/test_example.py::test_demo" }\n',
        "",
    )

    errors = _audit(tmp_path, registry)

    assert any("semantic_oracle must be a table" in error for error in errors)


def test_receipt_presence_is_not_an_executable_semantic_oracle(tmp_path: Path) -> None:
    registry = _VALID_REGISTRY.replace(
        'semantic_oracle = { kind = "pytest", ref = "tests/test_example.py::test_demo" }',
        'semantic_oracle = { kind = "receipt", ref = "archetype.example/v1" }',
    )

    errors = _audit(tmp_path, registry)

    assert any("semantic_oracle.kind" in error for error in errors)


def test_scenario_contracts_must_reference_registered_contract_ids(tmp_path: Path) -> None:
    registry = _VALID_REGISTRY.replace(
        'contracts = ["runtime.lifecycle"]',
        'contracts = ["runtime.lifecycle", "runtime.invented"]',
    )

    errors = _audit(tmp_path, registry)

    assert any("references unknown contract IDs ['runtime.invented']" in error for error in errors)


def test_release_scenario_needs_a_versioned_artifact_schema(tmp_path: Path) -> None:
    registry = _VALID_REGISTRY.replace(
        'artifact_schema = "archetype.operational-results/v1"',
        'artifact_schema = ""',
    )

    errors = _audit(tmp_path, registry)

    assert any("artifact_schema must be a versioned stable identifier" in error for error in errors)


def test_credential_skip_cannot_silently_report_pass(tmp_path: Path) -> None:
    registry = _VALID_REGISTRY.replace(
        'prerequisites = []\nmissing_prerequisite = "fail"',
        'prerequisites = ["credential:OPENAI_API_KEY"]\nmissing_prerequisite = "pass"',
    )

    errors = _audit(tmp_path, registry)

    assert any("cannot silently pass" in error for error in errors)


def test_workflow_path_trigger_must_cover_every_declared_owner(tmp_path: Path) -> None:
    registry = _VALID_REGISTRY.replace(
        'trigger_paths = ["packages/archetype-ecs/src/archetype/runtime/**"]',
        'trigger_paths = ["examples/**"]',
    )
    workflow = _WORKFLOW.replace(
        '"packages/archetype-ecs/src/archetype/runtime/**"',
        '"examples/**"',
    )

    errors = _audit(tmp_path, registry, workflow)

    assert any(
        "do not cover owner path packages/archetype-ecs/src/archetype/runtime" in error
        for error in errors
    )


def test_advisory_pr_workflow_still_audits_declared_owner_paths(tmp_path: Path) -> None:
    registry = _VALID_REGISTRY.replace(
        'required_cadence = ["pr", "main", "release"]',
        'required_cadence = ["release"]',
    ).replace(
        'trigger_paths = ["packages/archetype-ecs/src/archetype/runtime/**"]',
        'trigger_paths = ["examples/**"]',
    )
    workflow = _WORKFLOW.replace(
        '"packages/archetype-ecs/src/archetype/runtime/**"',
        '"examples/**"',
    )

    errors = _audit(tmp_path, registry, workflow)

    assert any(
        "do not cover owner path packages/archetype-ecs/src/archetype/runtime" in error
        for error in errors
    )


def test_workflow_must_contain_every_declared_trigger(tmp_path: Path) -> None:
    registry = _VALID_REGISTRY.replace(
        'trigger_paths = ["packages/archetype-ecs/src/archetype/runtime/**"]',
        'trigger_paths = ["packages/archetype-ecs/src/archetype/runtime/**", "quality/operational_scenarios.toml"]',
    )

    errors = _audit(tmp_path, registry)

    assert any("omit declared triggers" in error for error in errors)


def test_unfiltered_pull_request_workflow_covers_all_owner_paths(tmp_path: Path) -> None:
    workflow = _WORKFLOW.replace(
        '  pull_request:\n    paths:\n      - "packages/archetype-ecs/src/archetype/runtime/**"\n',
        "  pull_request:\n",
    )
    registry = _VALID_REGISTRY.replace(
        'trigger_paths = ["packages/archetype-ecs/src/archetype/runtime/**"]',
        "trigger_paths = []",
    )

    assert _audit(tmp_path, registry, workflow) == []
