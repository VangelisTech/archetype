# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Contracts for current native CI and exact installed release verification."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
QUALITY_WORKFLOW = ROOT / ".github/workflows/python-tests.yml"
RELEASE_WORKFLOW = ROOT / ".github/workflows/release.yml"
MAKEFILE = ROOT / "Makefile"
CONTRIBUTING = ROOT / "CONTRIBUTING.md"
QUARANTINE = ROOT / "quality/quarantine/review-gate"


def _job(workflow, job_id):
    match = re.search(
        rf"^  {re.escape(job_id)}:\n(?P<body>.*?)(?=^  [a-z][a-z0-9-]*:\n|\Z)",
        workflow,
        re.MULTILINE | re.DOTALL,
    )
    assert match is not None, f"workflow lost {job_id!r}"
    return match.group("body")


def _dependencies(target):
    match = re.search(
        rf"^{re.escape(target)}:(?P<dependencies>[^\n]*)$", MAKEFILE.read_text(), re.MULTILINE
    )
    assert match is not None
    return match.group("dependencies").split()


def test_pull_request_workflow_has_python_ddlog_and_installed_checks():
    workflow = QUALITY_WORKFLOW.read_text()
    assert re.findall(
        r"^  ([a-z][a-z0-9-]*):$", workflow.partition("\njobs:\n")[2], re.MULTILINE
    ) == ["static", "tests", "ddlog-storage", "installed-native"]
    assert "merge_group:" not in workflow
    assert "make static" in _job(workflow, "static")
    tests = _job(workflow, "tests")
    assert "make test" in tests and "make package-smoke" in tests
    assert "needs: ddlog-storage" in tests and "native-cabi" in tests
    storage = _job(workflow, "ddlog-storage")
    for package in ("archetype-ddlog", "archetype-ddlog-python"):
        assert f"clippy -p {package} --all-targets --locked -- -D warnings" in storage
        assert f"test -p {package} --locked" in storage
    installed = _job(workflow, "installed-native")
    assert "scripts/run_native_acceptance.py" in installed
    assert "--candidate-dir dist --candidate-manifest release-artifact.json" in installed
    assert "if: always()" in installed and "if-no-files-found: error" in installed
    assert "R2_" not in workflow


def test_local_pr_profile_matches_ci_jobs():
    assert _dependencies("verify-pr") == [
        "static",
        "test",
        "package-smoke",
        "ddlog-check",
        "ddlog-python-check",
    ]
    assert _dependencies("verify-full") == ["verify-full-source", "installed-native-acceptance"]
    assert _dependencies("verify-full-source") == [
        "static",
        "test",
        "docs",
        "package-smoke",
        "ddlog-check",
        "ddlog-python-check",
        "current-reliability",
        "current-coverage",
    ]


def test_contributing_ci_profile_and_review_authority_match_harness():
    guide = CONTRIBUTING.read_text()
    for command in ("make ci", "make verify-full", "make verify-release", "make docs"):
        assert f"`{command}`" in guide
    for value in (
        "Static",
        "Tests (3.12)",
        "eligible independent approval",
        "merge normally",
        "one rerun",
        "explicitly authorized target and prefix",
    ):
        assert value in guide


def test_release_profile_tests_one_sealed_candidate_without_rebuilding():
    makefile = MAKEFILE.read_text()
    assert _dependencies("verify-release") == ["verify-full-source"]
    body = re.search(
        r"^verify-release:[^\n]*\n(?P<body>(?:\t.*\n)+)", makefile, re.MULTILINE
    ).group("body")
    assert body.count("scripts/release_artifact.py record") == 1
    assert body.count("scripts/run_native_acceptance.py") == 1
    assert "--candidate-dir dist --candidate-manifest release-artifact.json" in body
    assert 'verify --dist "$(ACCEPTANCE_STAGE)/wheels"' in body
    assert "build" not in body
    assert ".NOTPARALLEL: verify-full verify-release" in makefile
    workflow = RELEASE_WORKFLOW.read_text()
    profile = _job(workflow, "release-profile")
    assert "needs: authorize-release" in profile
    assert "make verify-release" in profile
    assert "if: always()" in profile and "if-no-files-found: error" in profile


def test_local_tools_and_current_release_verifier_have_no_publish_authority():
    makefile = MAKEFILE.read_text()
    artifact = (ROOT / "scripts/release_artifact.py").read_text()
    assert re.search(r"^publish(?:-test)?:", makefile, re.MULTILINE) is None
    assert "uv publish" not in makefile and "uv publish" not in artifact
    assert 'choices=("record", "verify")' in artifact
    workflow = RELEASE_WORKFLOW.read_text()
    assert "id-token: write" not in workflow
    assert "pypa/gh-action-pypi-publish@" not in workflow
    assert "R2_" not in workflow


def test_release_workflow_is_operator_dispatched_from_exact_immutable_tag():
    workflow = RELEASE_WORKFLOW.read_text()
    assert "workflow_dispatch:" in workflow and "push:" not in workflow
    authorize = _job(workflow, "authorize-release")
    for value in (
        '"$RELEASE_ACTOR" == "everettVT"',
        '"$RELEASE_TRIGGERING_ACTOR" == "everettVT"',
        '"$RELEASE_REF_TYPE" == "tag"',
        '"$RELEASE_INPUT_TAG" == "$RELEASE_REF_NAME"',
        "git ls-remote --exit-code",
        'resolved_sha="${peeled_sha:-$direct_sha}"',
        '"$resolved_sha" == "$RELEASE_COMMIT"',
    ):
        assert value in authorize
    assert "cancel-in-progress: false" in workflow


def test_release_check_emits_the_immutable_annotated_tag_recipe() -> None:
    makefile = MAKEFILE.read_text(encoding="utf-8")
    target = re.search(
        r"^release-check:[^\n]*\n(?P<body>(?:\t.*\n)+)",
        makefile,
        re.MULTILINE,
    )

    assert target is not None
    body = target.group("body")
    assert "git fetch origin main" in body
    assert 'git tag -a v$(VERSION) origin/main -m \\"Release v$(VERSION)\\"' in body
    assert "git push origin refs/tags/v$(VERSION):refs/tags/v$(VERSION)" in body
    assert 'git tag v$(VERSION)"' not in body


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


def test_release_workflow_pins_every_external_action_to_a_full_commit() -> None:
    workflow = RELEASE_WORKFLOW.read_text(encoding="utf-8")
    references = re.findall(r"^\s*- uses:\s+([^\s#]+)", workflow, re.MULTILINE)

    assert references
    assert {
        reference
        for reference in references
        if re.fullmatch(r"[^@\s]+@[0-9a-f]{40}", reference) is None
    } == set()
