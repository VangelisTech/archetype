# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the scheduled, bounded daily security audit."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
WORKFLOW = ROOT / ".github" / "workflows" / "daily-security-audit.yml"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import daily_security_audit as audit  # noqa: E402


def _result() -> dict:
    return {
        "summary": "The current ingress boundary uses developer-mode roles.",
        "standing_audit_delta": "Authentication moved under archetype.api.",
        "dependency_assessment": "The dependency audit completed without a resolver failure.",
        "findings": [
            {
                "severity": "medium",
                "title": "Example finding",
                "evidence": [
                    "src/archetype/commands/dispatch.py: authorization precedes delegation"
                ],
                "recommendation": "Keep the executable API contract current.",
            }
        ],
        "positive_design_decisions": ["The actor-free application port is separated from ingress."],
        "next_actions": ["Re-run the focused authorization contract."],
    }


def _artifacts(tmp_path: Path) -> Path:
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    (artifacts / "report-date.txt").write_text("2026-07-18\n", encoding="utf-8")
    (artifacts / "commit.txt").write_text(f"{'a' * 40}\n", encoding="utf-8")
    (artifacts / "dependency-audit-status.txt").write_text(
        "uv export succeeded\npip-audit succeeded\n", encoding="utf-8"
    )
    return artifacts


def test_schema_and_runtime_validator_reject_unknown_fields() -> None:
    assert audit.AUDIT_SCHEMA["additionalProperties"] is False
    value = _result()
    value["untrusted_extension"] = "must not silently publish"

    with pytest.raises(audit.AuditError, match="keys disagree"):
        audit.validate_result(value)


def test_materialize_renders_validated_report_and_neutralizes_mentions(tmp_path: Path) -> None:
    artifacts = _artifacts(tmp_path)
    result = _result()
    result["summary"] = "Notify @security only through the owning workflow."
    result_path = artifacts / "model-result.json"
    result_path.write_text(json.dumps(result), encoding="utf-8")
    report_path = artifacts / "daily-security-audit.md"
    github_output = tmp_path / "github-output.txt"

    generated = audit.materialize(
        result_path=result_path,
        artifacts_dir=artifacts,
        report_path=report_path,
        generator_outcome="success",
        run_url="https://github.com/VangelisTech/archetype/actions/runs/1",
        github_output=github_output,
    )

    report = report_path.read_text(encoding="utf-8")
    assert generated is True
    assert "# Daily Security Audit — 2026-07-18" in report
    assert "[MEDIUM] Example finding" in report
    assert "@\u200bsecurity" in report
    assert "generated=true" in github_output.read_text(encoding="utf-8")
    assert json.loads((artifacts / "generation-status.json").read_text())["generated"] is True


def test_materialize_publishes_failure_evidence_before_the_workflow_fails(tmp_path: Path) -> None:
    artifacts = _artifacts(tmp_path)
    result_path = artifacts / "model-result.json"
    result_path.write_text("", encoding="utf-8")
    report_path = artifacts / "daily-security-audit.md"
    github_output = tmp_path / "github-output.txt"

    generated = audit.materialize(
        result_path=result_path,
        artifacts_dir=artifacts,
        report_path=report_path,
        generator_outcome="failure",
        run_url="https://github.com/VangelisTech/archetype/actions/runs/2",
        github_output=github_output,
    )

    report = report_path.read_text(encoding="utf-8")
    assert generated is False
    assert "Daily Security Audit Generation Failure" in report
    assert "do not treat this fallback as a completed security review" in report
    assert "generated=false" in github_output.read_text(encoding="utf-8")


def test_schema_limits_keep_the_rendered_issue_below_githubs_body_limit() -> None:
    value = {
        "summary": "s" * audit.MAX_TEXT_LENGTH,
        "standing_audit_delta": "d" * audit.MAX_TEXT_LENGTH,
        "dependency_assessment": "a" * audit.MAX_TEXT_LENGTH,
        "findings": [
            {
                "severity": "high",
                "title": "t" * 240,
                "evidence": ["e" * audit.MAX_ITEM_LENGTH] * audit.MAX_EVIDENCE_ITEMS,
                "recommendation": "r" * audit.MAX_RECOMMENDATION_LENGTH,
            }
            for _ in range(audit.MAX_FINDINGS)
        ],
        "positive_design_decisions": ["p" * audit.MAX_ITEM_LENGTH] * audit.MAX_LIST_ITEMS,
        "next_actions": ["n" * audit.MAX_ITEM_LENGTH] * audit.MAX_LIST_ITEMS,
    }

    report = audit.render_report(
        audit.validate_result(value),
        report_date="2026-07-18",
        commit="a" * 40,
        run_url="https://github.com/VangelisTech/archetype/actions/runs/1",
        dependency_status="pip-audit succeeded",
    )

    assert len(report.encode()) < 65_536


def test_workflow_uses_current_read_only_scope_and_fail_loud_publication() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "SECURITY_AUDIT_2026-03-28.md" not in workflow
    assert "src/archetype/app/auth/" not in workflow
    assert "src/archetype/app/gateway/auth/" not in workflow
    assert "src/archetype/app/storage/" not in workflow
    assert "src/archetype/app/artifacts/" not in workflow
    assert "src/archetype/api/deps.py" in workflow
    assert "src/archetype/commands/policy.py" in workflow
    assert "src/archetype/storage/" in workflow
    assert "src/archetype/missions/sandboxes/versions.toml" in workflow
    assert '--allowedTools "Read,Grep,Glob"' in workflow
    assert "--max-turns 32" in workflow
    assert "do not search for those removed legacy paths" in workflow
    assert "before exhausting the turn budget" in workflow
    assert "--json-schema '${{ steps.schema.outputs.schema }}'" in workflow
    assert "id: security_review" in workflow
    assert "continue-on-error: true" in workflow
    assert "python3 scripts/daily_security_audit.py materialize" in workflow
    assert workflow.count("if: always()") >= 4
    assert "steps.report.outputs.generated" in workflow
    assert 'if [[ "${GENERATED}" != "true" ]]' in workflow
