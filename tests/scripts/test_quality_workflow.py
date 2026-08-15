# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the deliberately small pull-request verification profile."""

from __future__ import annotations

import ast
import json
import re
import subprocess
import tomllib
from pathlib import Path
from typing import cast

import pytest

from scripts.release_artifact import DISTRIBUTIONS, PUBLISHER_WORKFLOWS
from scripts.run_operational_scenarios import _select_scenarios, run_scenarios
from scripts.validate_operational_scenarios import load_scenarios

ROOT = Path(__file__).resolve().parents[2]
QUALITY_WORKFLOW = ROOT / ".github" / "workflows" / "python-tests.yml"
RELEASE_WORKFLOW = ROOT / ".github" / "workflows" / "release.yml"
MAKEFILE = ROOT / "Makefile"
QUARANTINE = ROOT / "quality" / "quarantine" / "review-gate"
OPERATIONAL_SCENARIOS = ROOT / "quality" / "operational_scenarios.toml"


def _job(workflow: str, job_id: str) -> str:
    match = re.search(
        rf"^  {re.escape(job_id)}:\n(?P<body>.*?)(?=^  [a-z][a-z0-9-]*:\n|\Z)",
        workflow,
        re.MULTILINE | re.DOTALL,
    )
    assert match is not None, f"workflow lost the {job_id!r} job"
    return match.group("body")


def test_pull_request_workflow_has_only_two_jobs() -> None:
    workflow = QUALITY_WORKFLOW.read_text(encoding="utf-8")
    _, _, jobs = workflow.partition("\njobs:\n")

    assert re.findall(r"^  ([a-z][a-z0-9-]*):$", jobs, re.MULTILINE) == [
        "static",
        "tests",
    ]
    assert "merge_group:" not in workflow
    assert "make static" in _job(workflow, "static")
    assert "make test" in _job(workflow, "tests")
    assert "make package-smoke" in _job(workflow, "tests")
    assert "Distribution matrix" not in workflow
    assert "uv build --all-packages" in MAKEFILE.read_text(encoding="utf-8")
    assert "make test-cov" not in workflow
    assert "R2_" not in workflow
    assert "codecov" not in workflow.lower()


def test_local_pr_profile_matches_the_two_ci_jobs() -> None:
    makefile = MAKEFILE.read_text(encoding="utf-8")
    verify_pr = re.search(r"^verify-pr:(?P<dependencies>[^\n]*)$", makefile, re.MULTILINE)
    verify_full_source = re.search(
        r"^verify-full-source:(?P<dependencies>[^\n]*)$", makefile, re.MULTILINE
    )
    verify_full = re.search(r"^verify-full:(?P<dependencies>[^\n]*)$", makefile, re.MULTILINE)

    assert verify_pr is not None
    assert verify_pr.group("dependencies").split() == ["static", "test", "package-smoke"]
    assert verify_full_source is not None
    assert verify_full is not None
    source = verify_full_source.group("dependencies").split()
    for target in (
        "test-cov",
        "eval-conformance",
        "eval-reliability",
        "eval-capability",
        "examples-smoke",
        "operational-runtime",
        "operational-commands",
        "docs",
        "test-process",
    ):
        assert target in source
    assert verify_full.group("dependencies").split() == [
        "verify-full-source",
        "package-smoke",
        "operational-wheel-existing",
    ]


def test_review_gate_and_merge_queue_are_not_executable_workflows() -> None:
    active = ROOT / ".github" / "workflows"
    for name in (
        "deterministic-review.yml",
        "automerge.yml",
        "queue-reevaluator.yml",
        "merge-group-recheck.yml",
    ):
        assert not (active / name).exists()
        assert (QUARANTINE / "workflows" / name).is_file()


def test_release_publish_requires_credentialed_r2_evidence() -> None:
    workflow = RELEASE_WORKFLOW.read_text(encoding="utf-8")
    external = _job(workflow, "external-evidence")
    gate = _job(workflow, "release-evidence-gate")
    publish = _job(workflow, "publish")

    for variable in (
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY",
        "R2_API_ENDPOINT",
        "R2_BUCKET",
    ):
        assert variable in external
    assert "target: operational-release-r2" in external
    assert "tests/infrastructure/test_r2_idempotency.py" in external
    assert "operational-release-r2-results.json" in gate
    assert "needs: [release-evidence-gate, python-compatibility]" in _job(
        workflow, "testpypi-preflight"
    )
    assert "needs: pypi-preflight" in publish

    selected = _select_scenarios(
        load_scenarios(),
        mode="wheel",
        cadence="release",
        scenario_ids={"dogfood.storage.r2"},
        kind=None,
        min_tier=0,
        max_tier=6,
    )
    assert [row["id"] for row in selected] == ["dogfood.storage.r2"]


def test_example_smoke_keeps_mission_authoring_credential_free() -> None:
    makefile = MAKEFILE.read_text(encoding="utf-8")
    assert re.search(r"^examples-smoke: examples-local$", makefile, re.MULTILINE)
    assert "--mode source --cadence pr --kind example --max-tier 1" in makefile

    with OPERATIONAL_SCENARIOS.open("rb") as stream:
        scenarios = tomllib.load(stream)["scenario"]
    mission = next(
        row for row in scenarios if row["id"] == "example.11_coding_agent_mission.dry_run"
    )

    assert mission["source_command"][-3:] == ["--dry-run", "--backend", "modal"]
    assert mission["prerequisites"] == []
    assert mission["missing_prerequisite"] == "fail"
    assert mission["tier"] == 1
    assert "pr" in mission["required_cadence"]


def test_operational_receipts_cover_commands_and_runtime_from_source_and_wheel() -> None:
    makefile = MAKEFILE.read_text(encoding="utf-8")
    with OPERATIONAL_SCENARIOS.open("rb") as stream:
        scenarios = tomllib.load(stream)["scenario"]
    commands = next(row for row in scenarios if row["id"] == "dogfood.commands.local")

    assert commands["owner"] == "commands"
    assert commands["applicability"] == ["source", "wheel"]
    assert commands["prerequisites"] == []
    assert commands["missing_prerequisite"] == "fail"

    source_commands = re.search(
        r"^operational-commands:\n(?P<body>(?:\t.*\n)+)", makefile, re.MULTILINE
    )
    source_runtime = re.search(
        r"^operational-runtime:\n(?P<body>(?:\t.*\n)+)", makefile, re.MULTILINE
    )
    wheel = re.search(r"^operational-wheel:\n(?P<body>(?:\t.*\n)+)", makefile, re.MULTILINE)
    verify_full_source = re.search(
        r"^verify-full-source:(?P<dependencies>[^\n]*)$", makefile, re.MULTILINE
    )
    assert source_commands is not None
    assert source_runtime is not None
    assert wheel is not None
    assert verify_full_source is not None
    assert source_commands.group("body").count("--scenario dogfood.commands.local") == 1
    assert wheel.group("body").count("--scenario dogfood.commands.local") == 1
    assert source_runtime.group("body").count("--scenario dogfood.runtime.loopback") == 1
    assert wheel.group("body").count("--scenario dogfood.runtime.loopback") == 1
    assert '--wheel-dir "$(OPERATIONAL_DIST_DIR)"' in wheel.group("body")
    dependencies = verify_full_source.group("dependencies").split()
    assert "operational-commands" in dependencies
    assert "operational-runtime" in dependencies


def test_required_release_execution_cannot_accept_not_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    envelope, passed = run_scenarios(
        root=ROOT,
        registry=OPERATIONAL_SCENARIOS,
        output=tmp_path / "required-not-run.json",
        mode="source",
        wheel=None,
        cadence="release",
        scenario_ids={"example.05_llm_agents"},
        kind="example",
        min_tier=6,
        max_tier=6,
        expected_revision=None,
        require_clean=False,
        require_run=True,
    )

    assert passed is False
    assert envelope["outcome"] == "failed"
    assert envelope["status_counts"] == {"passed": 0, "failed": 1, "not_run": 0}
    (result,) = cast(list[dict[str, str]], envelope["results"])
    assert result["status"] == "failed"
    assert "release cadence requires execution" in result["reason"]


def test_release_profile_builds_and_tests_one_exact_artifact_matrix() -> None:
    makefile = MAKEFILE.read_text(encoding="utf-8")
    verify_release = re.search(r"^verify-release:(?P<dependencies>[^\n]*)$", makefile, re.MULTILINE)
    release_artifact = re.search(
        r"^release-artifact:\n(?P<body>(?:\t.*\n)+)", makefile, re.MULTILINE
    )
    operational_release = re.search(
        r"^operational-release:(?P<dependencies>[^\n]*)\n(?P<body>(?:\t.*\n)+)",
        makefile,
        re.MULTILINE,
    )
    assert verify_release is not None
    assert verify_release.group("dependencies").split() == [
        "verify-full-source",
        "operational-release",
    ]
    assert release_artifact is not None
    assert operational_release is not None
    artifact_body = release_artifact.group("body")
    build = re.search(r"^build:[^\n]*\n(?P<body>(?:\t.*\n)+)", makefile, re.MULTILINE)
    assert build is not None
    assert "uv build --all-packages --no-sources --clear --out-dir dist" in build.group("body")
    assert artifact_body.count("scripts/package_smoke.py") == 1
    assert artifact_body.count("scripts/release_artifact.py record") == 1
    assert operational_release.group("dependencies").split() == ["release-artifact"]
    assert "--min-tier 0 --max-tier 4" in operational_release.group("body")
    assert makefile.count("$(MAKE) --no-print-directory build") == 2
    assert (
        "operational-wheel-existing:\n"
        "\t@$(MAKE) --no-print-directory operational-wheel OPERATIONAL_BUILD_COMMAND=true"
    ) in makefile
    assert ".NOTPARALLEL: verify-full verify-release" in makefile
    release_runner = re.search(
        r"^define RUN_RELEASE_SCENARIOS\n(?P<body>.*?)^endef$",
        makefile,
        re.MULTILINE | re.DOTALL,
    )
    assert release_runner is not None
    assert '--wheel-dir "$(OPERATIONAL_DIST_DIR)"' in release_runner.group("body")


def test_hosted_oidc_is_the_only_publish_authority() -> None:
    makefile = MAKEFILE.read_text(encoding="utf-8")
    release_artifact = (ROOT / "scripts" / "release_artifact.py").read_text(encoding="utf-8")
    assert re.search(r"^publish(?:-test)?:", makefile, re.MULTILINE) is None
    assert "uv publish" not in makefile
    assert 'choices=("record", "verify")' in release_artifact
    assert "uv publish" not in release_artifact
    assert "--publish-url" not in release_artifact


def test_manual_registry_verification_matches_the_hosted_release_oracles() -> None:
    makefile = MAKEFILE.read_text(encoding="utf-8")
    for target in ("verify-test-index", "verify-published"):
        match = re.search(
            rf"^{target}:[^\n]*\n(?P<body>(?:\t.*\n)+)",
            makefile,
            re.MULTILINE,
        )
        assert match is not None
        body = match.group("body")
        assert "scripts/verify_release_index.py" in body
        assert "scripts/registry_smoke.py" in body
        assert '--manifest "$(RELEASE_ARTIFACT_MANIFEST)"' in body
        assert "--integrity-template" in body
        assert "--publisher-repository VangelisTech/archetype" in body
        assert "--publisher-workflow" not in body
        assert "--registry-artifact-host" in body
    test_index = re.search(
        r"^verify-test-index:[^\n]*\n(?P<body>(?:\t.*\n)+)",
        makefile,
        re.MULTILINE,
    )
    assert test_index is not None
    assert "https://test.pypi.org/simple" in test_index.group("body")
    assert "https://pypi.org/simple" in test_index.group("body")
    assert "--registry-artifact-host test-files.pythonhosted.org" in test_index.group("body")
    assert "--attestation-staging" not in test_index.group("body")
    published = re.search(
        r"^verify-published:[^\n]*\n(?P<body>(?:\t.*\n)+)",
        makefile,
        re.MULTILINE,
    )
    assert published is not None
    assert "--registry-artifact-host files.pythonhosted.org" in published.group("body")


def test_new_distribution_publishers_are_direct_isolated_workflows() -> None:
    assert tuple(PUBLISHER_WORKFLOWS) == DISTRIBUTIONS
    assert PUBLISHER_WORKFLOWS["archetype-ecs"] == "release.yml"
    release = RELEASE_WORKFLOW.read_text(encoding="utf-8")
    profile = _job(release, "release-profile")

    for distribution, workflow_name in PUBLISHER_WORKFLOWS.items():
        prefix = distribution.replace("-", "_")
        assert f"name: dist-{distribution}\n" in profile
        assert f"path: dist/{prefix}-*" in profile
        if distribution == "archetype-ecs":
            continue

        path = ROOT / ".github" / "workflows" / workflow_name
        workflow = path.read_text(encoding="utf-8")
        authorize = _job(workflow, "authorize")
        testpypi = _job(workflow, "publish-testpypi")
        pypi = _job(workflow, "publish-pypi")

        assert "workflow_dispatch:" in workflow
        assert "workflow_call:" not in workflow
        assert "push:" not in workflow
        assert "group: archetype-release" not in workflow
        references = re.findall(r"^\s*- uses:\s+([^\s#]+)", workflow, re.MULTILINE)
        assert references
        assert all(
            re.fullmatch(r"[^@\s]+@[0-9a-f]{40}", reference) is not None for reference in references
        )
        assert "actions: read" in authorize
        assert "contents: read" in authorize
        assert "id-token: write" not in authorize
        assert "scripts/verify_publisher_dispatch.py" in authorize
        assert f"--expected-workflow {workflow_name}" in authorize
        assert f"--distribution {distribution}" in authorize
        assert "publisher-dispatch-${{ inputs.registry }}-${{ inputs.parent_run_attempt }}" in (
            authorize
        )

        for registry, job in (("testpypi", testpypi), ("pypi", pypi)):
            environment = f"release-{registry}"
            assert f"if: inputs.registry == '{registry}'" in job
            assert f"environment: {environment}" in job
            assert "actions: read" in job
            assert "id-token: write" in job
            assert f"name: dist-{distribution}" in job
            assert "run-id: ${{ inputs.parent_run_id }}" in job
            assert "github-token: ${{ github.token }}" in job
            assert "pypa/gh-action-pypi-publish@" in job
            assert "skip-existing: true" in job
            assert "attestations: true" in job
            assert "actions/checkout@" not in job
            assert "run:" not in job
            assert job.count("- uses:") == 2
        assert "repository-url: https://test.pypi.org/legacy/" in testpypi
        assert "repository-url:" not in pypi


def test_every_release_scenario_is_installed_wheel_applicable() -> None:
    with OPERATIONAL_SCENARIOS.open("rb") as stream:
        scenarios = tomllib.load(stream)["scenario"]
    required = [row for row in scenarios if "release" in row["required_cadence"]]
    assert required
    assert all("wheel" in row["applicability"] for row in required)
    ids = {row["id"] for row in required}
    assert {
        "dogfood.runtime.shutdown",
        "dogfood.agent_mission.modal_activity_contracts",
        "dogfood.physical_ai.hosted_episode",
        "dogfood.storage.r2",
    } <= ids
    core_ids = {row["id"] for row in required if int(row["tier"]) <= 4}
    assert ids - core_ids == {
        "example.05_llm_agents",
        "example.14_biome_agent",
        "dogfood.agent_mission.modal_live",
        "dogfood.physical_ai.modal_r2_live",
        "dogfood.sandbox.docker",
        "dogfood.storage.r2",
        "dogfood.sandbox.apple_container",
    }


def test_live_modal_release_scenario_is_credentialed_and_opted_in() -> None:
    with OPERATIONAL_SCENARIOS.open("rb") as stream:
        scenarios = tomllib.load(stream)["scenario"]
    modal = next(row for row in scenarios if row["id"] == "dogfood.agent_mission.modal_live")

    assert modal["source_path"] == "tests/infrastructure/test_modal_agent_mission_live.py"
    assert modal["semantic_oracle"]["ref"] in modal["source_command"]
    assert modal["tier"] == 6
    assert modal["applicability"] == ["source", "wheel"]
    assert modal["required_extras"] == ["coding-agent"]
    assert modal["cleanup_policy"] == "provider"
    assert modal["artifact_policy"] == "redacted_receipt"
    assert set(modal["prerequisites"]) == {
        "credential:MODAL_TOKEN_ID",
        "credential:MODAL_TOKEN_SECRET",
        "infrastructure:CODING_AGENT_MODAL_WORKSPACE",
        "infrastructure:CODING_AGENT_MODAL_ENVIRONMENT",
        "infrastructure:CODEX_AUTH_VOLUME",
        "infrastructure:CODING_AGENT_GITHUB_SECRET",
    }
    assert "ARCHETYPE_MODAL_AGENT_MISSION_LIVE=1" in MAKEFILE.read_text(encoding="utf-8")


def test_live_physical_ai_release_scenario_binds_modal_compute_to_r2() -> None:
    with OPERATIONAL_SCENARIOS.open("rb") as stream:
        scenarios = tomllib.load(stream)["scenario"]
    physical = next(row for row in scenarios if row["id"] == "dogfood.physical_ai.modal_r2_live")

    assert physical["source_path"] == "tests/infrastructure/test_modal_physical_r2_live.py"
    assert physical["semantic_oracle"]["ref"] in physical["source_command"]
    assert physical["tier"] == 6
    assert physical["applicability"] == ["source", "wheel"]
    assert physical["required_extras"] == ["coding-agent"]
    assert physical["cleanup_policy"] == "provider"
    assert physical["artifact_policy"] == "redacted_receipt"
    assert set(physical["prerequisites"]) == {
        "credential:MODAL_TOKEN_ID",
        "credential:MODAL_TOKEN_SECRET",
        "credential:R2_ACCESS_KEY_ID",
        "credential:R2_SECRET_ACCESS_KEY",
        "infrastructure:CODING_AGENT_MODAL_WORKSPACE",
        "infrastructure:CODING_AGENT_MODAL_ENVIRONMENT",
        "infrastructure:R2_API_ENDPOINT",
        "infrastructure:R2_BUCKET",
    }
    assert "ARCHETYPE_MODAL_PHYSICAL_R2_LIVE=1" in MAKEFILE.read_text(encoding="utf-8")


def test_release_workflow_aggregates_platform_evidence_before_publish() -> None:
    workflow = RELEASE_WORKFLOW.read_text(encoding="utf-8")
    profile = _job(workflow, "release-profile")
    external = _job(workflow, "external-evidence")
    apple = _job(workflow, "apple-evidence")
    compatibility = _job(workflow, "python-compatibility")
    gate = _job(workflow, "release-evidence-gate")
    test_preflight = _job(workflow, "testpypi-preflight")
    publish_test = _job(workflow, "publish-testpypi")
    publish_test_libraries = _job(workflow, "publish-testpypi-libraries")
    test_smoke = _job(workflow, "testpypi-smoke")
    pypi_preflight = _job(workflow, "pypi-preflight")
    publish = _job(workflow, "publish")
    publish_libraries = _job(workflow, "publish-libraries")
    registry_smoke = _job(workflow, "registry-smoke")
    github_release = _job(workflow, "github-release")

    assert "make verify-release" in profile
    assert "uv sync --all-packages --all-extras --group dev --group docs" in profile
    assert "uv sync --all-packages --all-extras --group dev" in external
    assert "release-artifact.json" in profile
    assert "operational-release-results.json" in profile
    for target in (
        "operational-release-openai",
        "operational-release-docker",
        "operational-release-r2",
        "operational-release-modal",
        "operational-release-physical-modal-r2",
    ):
        assert f"target: {target}" in external
        make_target = re.search(
            rf"^{target}:[^\n]*\n(?P<body>(?:\t.*\n)+)",
            MAKEFILE.read_text(encoding="utf-8"),
            re.MULTILINE,
        )
        assert make_target is not None
        assert "--min-tier 0 --max-tier 6" in make_target.group("body")
    assert "tests/infrastructure/test_r2_idempotency.py" in external
    assert "scripts/verify_release_evidence.py" in gate
    assert "operational-release-apple-results.json" in gate
    assert "operational-release-modal-results.json" in gate
    assert "operational-release-physical-modal-r2-results.json" in gate
    assert "group: archetype-release" in workflow
    assert "cancel-in-progress: false" in workflow
    assert "runs-on: ${{ fromJSON(matrix.runner) }}" in external
    assert "operational-release-apple" not in external
    assert "group: archetype-release-macos" in apple
    assert "archetype-apple-container-macos-26" in apple
    assert "environment: release-apple-macos" in apple
    assert "- uses: actions/setup-python@" not in apple
    assert 'UV_PYTHON: "3.12"' in apple
    assert 'uv sync --python "3.12" --all-packages --all-extras --group dev' in apple
    assert "sys.version_info[:2] == (3, 12)" in apple
    assert 'test "$(uname -m)" = "arm64"' in apple
    assert "container system status" in apple
    assert "make operational-release-apple" in apple
    assert "needs: [release-profile, external-evidence, apple-evidence]" in gate
    assert 'UV_PYTHON: "3.13"' in compatibility
    assert 'uv sync --python "3.13" --all-packages --all-extras --group dev' in compatibility
    assert "sys.version_info[:2] == (3, 13)" in compatibility
    assert "needs: [release-evidence-gate, python-compatibility]" in test_preflight
    assert "scripts/verify_release_index.py" in test_preflight
    assert "scripts/verify_release_ref.py" in test_preflight
    assert "--publisher-environment release-testpypi" in test_preflight
    assert "pypi-attestations==0.0.30" in test_preflight
    assert "--registry-artifact-host test-files.pythonhosted.org" in test_preflight
    assert "--attestation-staging" not in test_preflight
    assert '--expected-commit "$GITHUB_SHA"' in test_preflight
    assert "needs: testpypi-preflight" in publish_test
    assert "environment: release-testpypi" in publish_test
    assert "repository-url: https://test.pypi.org/legacy/" in publish_test
    assert "needs: [publish-testpypi, publish-testpypi-libraries]" in test_smoke
    assert "scripts/verify_release_index.py" in test_smoke
    assert "scripts/registry_smoke.py" in test_smoke
    assert "--manifest evidence/release-artifact.json" in test_smoke
    assert "testpypi-install-evidence.json" in test_smoke
    assert "--publisher-environment release-testpypi" in test_smoke
    assert "pypi-attestations==0.0.30" in test_smoke
    assert "--registry-artifact-host test-files.pythonhosted.org" in test_smoke
    assert "--attestation-staging" not in test_smoke
    assert '--expected-commit "$GITHUB_SHA"' in test_smoke
    assert "needs: testpypi-smoke" in pypi_preflight
    assert "scripts/verify_release_index.py" in pypi_preflight
    assert "scripts/verify_release_ref.py" in pypi_preflight
    assert "--publisher-environment release-pypi" in pypi_preflight
    assert "pypi-attestations==0.0.30" in pypi_preflight
    assert "--registry-artifact-host files.pythonhosted.org" in pypi_preflight
    assert '--expected-commit "$GITHUB_SHA"' in pypi_preflight
    assert "needs: pypi-preflight" in publish
    assert "needs: [publish, publish-libraries]" in registry_smoke
    assert "scripts/verify_release_index.py" in registry_smoke
    assert "scripts/registry_smoke.py" in registry_smoke
    assert "--manifest evidence/release-artifact.json" in registry_smoke
    assert "pypi-install-evidence.json" in registry_smoke
    assert "--publisher-environment release-pypi" in registry_smoke
    assert "pypi-attestations==0.0.30" in registry_smoke
    assert "--registry-artifact-host files.pythonhosted.org" in registry_smoke
    assert '--expected-commit "$GITHUB_SHA"' in registry_smoke
    assert "needs: registry-smoke" in github_release

    for publishing_job in (publish_test, publish):
        assert "name: dist-archetype-ecs" in publishing_job
        assert "pypa/gh-action-pypi-publish@" in publishing_job
        assert "skip-existing: true" in publishing_job
        assert "attestations: true" in publishing_job
        assert "id-token: write" in publishing_job
        assert "actions/checkout@" not in publishing_job
        assert "run:" not in publishing_job
        assert "uv build" not in publishing_job
        assert "make build" not in publishing_job
        assert "github.triggering_actor == 'everettVT'" in publishing_job

    for registry, coordinator in (
        ("testpypi", publish_test_libraries),
        ("pypi", publish_libraries),
    ):
        assert f"--registry {registry}" in coordinator
        assert "actions: write" in coordinator
        assert "contents: read" in coordinator
        assert "id-token: write" not in coordinator
        assert "scripts/dispatch_release_publishers.py dispatch" in coordinator
        assert "scripts/dispatch_release_publishers.py await" in coordinator
        assert '--parent-run-id "$GITHUB_RUN_ID"' in coordinator
        assert '--parent-run-attempt "$GITHUB_RUN_ATTEMPT"' in coordinator
        assert '--tag "$GITHUB_REF_NAME"' in coordinator
        assert '--expected-commit "$GITHUB_SHA"' in coordinator
        assert f"publisher-dispatch-{registry}.json" in coordinator
        assert f"name: publisher-dispatch-{registry}-${{{{ github.run_attempt }}}}" in coordinator
        assert (
            coordinator.index("dispatch_release_publishers.py dispatch")
            < coordinator.index(f"name: publisher-dispatch-{registry}-")
            < coordinator.index("dispatch_release_publishers.py await")
        )
        assert "github.triggering_actor == 'everettVT'" in coordinator

    assert "github.triggering_actor == 'everettVT'" in github_release
    assert 'version: "latest"' not in workflow
    assert workflow.count('version: "0.9.28"') == 8
    assert workflow.count("persist-credentials: false") == 12


def test_release_workflow_pins_every_external_action_to_a_full_commit() -> None:
    workflow = RELEASE_WORKFLOW.read_text(encoding="utf-8")
    references = re.findall(r"^\s*- uses:\s+([^\s#]+)", workflow, re.MULTILINE)

    assert references
    assert {
        reference
        for reference in references
        if re.fullmatch(r"[^@\s]+@[0-9a-f]{40}", reference) is None
    } == set()


def test_release_workflow_is_operator_dispatched_from_an_immutable_tag() -> None:
    workflow = RELEASE_WORKFLOW.read_text(encoding="utf-8")
    authorize = _job(workflow, "authorize-release")
    profile = _job(workflow, "release-profile")
    compatibility = _job(workflow, "python-compatibility")
    github_release = _job(workflow, "github-release")

    assert "workflow_dispatch:" in workflow
    assert "push:\n    tags:" not in workflow
    assert "RELEASE_ACTOR: ${{ github.actor }}" in authorize
    assert "RELEASE_TRIGGERING_ACTOR: ${{ github.triggering_actor }}" in authorize
    assert 'test "$RELEASE_ACTOR" = "everettVT"' in authorize
    assert 'test "$RELEASE_TRIGGERING_ACTOR" = "everettVT"' in authorize
    assert 'test "$RELEASE_REF_TYPE" = "tag"' in authorize
    assert 'test "$RELEASE_INPUT_TAG" = "$RELEASE_REF_NAME"' in authorize
    assert "needs: authorize-release" in profile
    assert "needs: authorize-release" in compatibility
    assert 'git merge-base --is-ancestor "${GITHUB_REF_NAME}^{commit}" origin/main' in profile
    assert "scripts/verify_release_ref.py" in _job(workflow, "testpypi-preflight")
    assert "scripts/verify_release_ref.py" in _job(workflow, "pypi-preflight")
    assert "tag_name: ${{ github.ref_name }}" in github_release


def test_commands_operational_oracle_does_not_import_pytest_modules() -> None:
    path = ROOT / "tests" / "integration" / "test_commands_operational.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imported_modules = [
        module
        for node in ast.walk(tree)
        for module in (
            [alias.name for alias in node.names]
            if isinstance(node, ast.Import)
            else [node.module or ""]
            if isinstance(node, ast.ImportFrom)
            else []
        )
    ]
    assert not [
        module
        for module in imported_modules
        if any(part.startswith("test_") for part in module.split("."))
    ]


def test_operational_wheel_failures_still_emit_a_receipt(tmp_path: Path) -> None:
    makefile = MAKEFILE.read_text(encoding="utf-8")
    target = re.search(
        r"^operational-wheel:(?P<dependencies>[^\n]*)\n(?P<body>(?:\t.*\n)+)",
        makefile,
        re.MULTILINE,
    )
    assert target is not None
    assert target.group("dependencies").strip() == ""
    body = target.group("body")
    assert "$(OPERATIONAL_BUILD_COMMAND) || build_status=$$?" in body
    assert 'wheel="$(OPERATIONAL_DIST_DIR)/.missing-operational-wheel.whl"' in body

    for label, build_command in (("build-failed", "false"), ("wheel-missing", "true")):
        output = tmp_path / f"{label}.json"
        completed = subprocess.run(
            [
                "make",
                "--no-print-directory",
                "operational-wheel",
                f"OPERATIONAL_BUILD_COMMAND={build_command}",
                f"OPERATIONAL_DIST_DIR={tmp_path / label / 'dist'}",
                f"OPERATIONAL_WHEEL_RESULTS={output}",
            ],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        assert completed.returncode != 0
        receipt = json.loads(output.read_text(encoding="utf-8"))
        assert receipt["schema"] == "archetype.operational-results/v1"
        assert receipt["outcome"] == "failed"
        assert "--wheel must name the built archetype-ecs wheel" in receipt["error"]
