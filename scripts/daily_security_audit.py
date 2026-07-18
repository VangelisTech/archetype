# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Validate and render the scheduled security audit's model output.

The model is deliberately read-only. This module owns the durable CI side
effects: schema validation, bounded Markdown rendering, failure evidence, and
the machine-readable outcome consumed by the workflow's final fail-loud step.
"""

from __future__ import annotations

import argparse
import json
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

SEVERITIES = ("critical", "high", "medium", "low", "info")
MAX_FINDINGS = 10
MAX_LIST_ITEMS = 8
MAX_EVIDENCE_ITEMS = 4
MAX_ITEM_LENGTH = 500
MAX_RECOMMENDATION_LENGTH = 1_000
MAX_TEXT_LENGTH = 2_000

AUDIT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "summary",
        "standing_audit_delta",
        "dependency_assessment",
        "findings",
        "positive_design_decisions",
        "next_actions",
    ],
    "properties": {
        "summary": {"type": "string", "minLength": 1, "maxLength": MAX_TEXT_LENGTH},
        "standing_audit_delta": {
            "type": "string",
            "minLength": 1,
            "maxLength": MAX_TEXT_LENGTH,
        },
        "dependency_assessment": {
            "type": "string",
            "minLength": 1,
            "maxLength": MAX_TEXT_LENGTH,
        },
        "findings": {
            "type": "array",
            "maxItems": MAX_FINDINGS,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["severity", "title", "evidence", "recommendation"],
                "properties": {
                    "severity": {"type": "string", "enum": list(SEVERITIES)},
                    "title": {"type": "string", "minLength": 1, "maxLength": 240},
                    "evidence": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": MAX_EVIDENCE_ITEMS,
                        "items": {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": MAX_ITEM_LENGTH,
                        },
                    },
                    "recommendation": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": MAX_RECOMMENDATION_LENGTH,
                    },
                },
            },
        },
        "positive_design_decisions": {
            "type": "array",
            "maxItems": MAX_LIST_ITEMS,
            "items": {"type": "string", "minLength": 1, "maxLength": MAX_ITEM_LENGTH},
        },
        "next_actions": {
            "type": "array",
            "minItems": 1,
            "maxItems": MAX_LIST_ITEMS,
            "items": {"type": "string", "minLength": 1, "maxLength": MAX_ITEM_LENGTH},
        },
    },
}

_EXPECTED_KEYS = frozenset(AUDIT_SCHEMA["required"])
_FINDING_KEYS = frozenset(("severity", "title", "evidence", "recommendation"))
_WHITESPACE_RE = re.compile(r"\s+")


class AuditError(ValueError):
    """Raised when model output cannot be published as a trusted audit."""


def _text(value: object, field: str, *, max_length: int = MAX_TEXT_LENGTH) -> str:
    if not isinstance(value, str):
        raise AuditError(f"{field} must be a string")
    normalized = value.strip()
    if not normalized:
        raise AuditError(f"{field} must not be empty")
    if len(normalized) > max_length:
        raise AuditError(f"{field} exceeds {max_length} characters")
    return normalized


def _string_list(
    value: object,
    field: str,
    *,
    min_items: int = 0,
    max_items: int = MAX_LIST_ITEMS,
    max_length: int = MAX_ITEM_LENGTH,
) -> list[str]:
    if not isinstance(value, list):
        raise AuditError(f"{field} must be an array")
    if not min_items <= len(value) <= max_items:
        raise AuditError(f"{field} must contain between {min_items} and {max_items} items")
    return [
        _text(item, f"{field}[{index}]", max_length=max_length) for index, item in enumerate(value)
    ]


def validate_result(value: object) -> dict[str, Any]:
    """Return normalized structured audit evidence or raise ``AuditError``."""
    if not isinstance(value, Mapping):
        raise AuditError("audit result must be a JSON object")
    keys = frozenset(value)
    if keys != _EXPECTED_KEYS:
        missing = sorted(_EXPECTED_KEYS - keys)
        extra = sorted(keys - _EXPECTED_KEYS)
        raise AuditError(
            f"audit result keys disagree with schema; missing={missing}, extra={extra}"
        )

    raw_findings = value["findings"]
    if not isinstance(raw_findings, list):
        raise AuditError("findings must be an array")
    if len(raw_findings) > MAX_FINDINGS:
        raise AuditError(f"findings must contain at most {MAX_FINDINGS} items")

    findings: list[dict[str, Any]] = []
    for index, raw_finding in enumerate(raw_findings):
        if not isinstance(raw_finding, Mapping):
            raise AuditError(f"findings[{index}] must be an object")
        keys = frozenset(raw_finding)
        if keys != _FINDING_KEYS:
            raise AuditError(f"findings[{index}] keys disagree with schema")
        severity = _text(raw_finding["severity"], f"findings[{index}].severity", max_length=20)
        if severity not in SEVERITIES:
            raise AuditError(f"findings[{index}].severity is not recognized")
        findings.append(
            {
                "severity": severity,
                "title": _text(raw_finding["title"], f"findings[{index}].title", max_length=240),
                "evidence": _string_list(
                    raw_finding["evidence"],
                    f"findings[{index}].evidence",
                    min_items=1,
                    max_items=MAX_EVIDENCE_ITEMS,
                ),
                "recommendation": _text(
                    raw_finding["recommendation"],
                    f"findings[{index}].recommendation",
                    max_length=MAX_RECOMMENDATION_LENGTH,
                ),
            }
        )

    return {
        "summary": _text(value["summary"], "summary"),
        "standing_audit_delta": _text(value["standing_audit_delta"], "standing_audit_delta"),
        "dependency_assessment": _text(value["dependency_assessment"], "dependency_assessment"),
        "findings": findings,
        "positive_design_decisions": _string_list(
            value["positive_design_decisions"], "positive_design_decisions"
        ),
        "next_actions": _string_list(value["next_actions"], "next_actions", min_items=1),
    }


def _markdown(value: str) -> str:
    # Collapse model-supplied formatting and neutralize accidental mentions in
    # the issue body. GitHub sanitizes HTML, but keeping model output as plain
    # prose makes the publication boundary explicit and predictable.
    return _WHITESPACE_RE.sub(" ", value).replace("@", "@\u200b").strip()


def _standing_audit_url(run_url: str) -> str:
    repository_url, separator, _ = run_url.partition("/actions/runs/")
    if separator and repository_url.startswith("https://github.com/"):
        return f"{repository_url}/blob/main/docs/reports/2026-03-28-security-program-review.md"
    return "https://github.com/VangelisTech/archetype/blob/main/docs/reports/2026-03-28-security-program-review.md"


def render_report(
    result: Mapping[str, Any],
    *,
    report_date: str,
    commit: str,
    run_url: str,
    dependency_status: str,
) -> str:
    """Render validated audit evidence as bounded GitHub Markdown."""
    standing_audit_url = _standing_audit_url(run_url)
    lines = [
        f"# Daily Security Audit — {_markdown(report_date)}",
        "",
        f"- Commit: `{_markdown(commit)}`",
        f"- Workflow run: [evidence]({_markdown(run_url)})",
        f"- Standing audit: [2026-03-28 security program review]({standing_audit_url})",
        f"- Dependency tooling: {_markdown(dependency_status)}",
        "",
        "## Summary",
        "",
        _markdown(str(result["summary"])),
        "",
        "## Standing-audit delta",
        "",
        _markdown(str(result["standing_audit_delta"])),
        "",
        "## Dependency assessment",
        "",
        _markdown(str(result["dependency_assessment"])),
        "",
        "## Findings",
        "",
    ]

    findings = result["findings"]
    if not findings:
        lines.append("No current findings were reported by the bounded review.")
    else:
        for finding in findings:
            lines.extend(
                [
                    f"### [{str(finding['severity']).upper()}] {_markdown(str(finding['title']))}",
                    "",
                ]
            )
            lines.extend(f"- Evidence: {_markdown(item)}" for item in finding["evidence"])
            lines.extend(
                [
                    f"- Recommendation: {_markdown(str(finding['recommendation']))}",
                    "",
                ]
            )

    lines.extend(["", "## Positive design decisions", ""])
    positive = result["positive_design_decisions"]
    if positive:
        lines.extend(f"- {_markdown(item)}" for item in positive)
    else:
        lines.append("- None called out in this run.")

    lines.extend(["", "## Highest-priority next actions", ""])
    lines.extend(
        f"{index}. {_markdown(item)}" for index, item in enumerate(result["next_actions"], 1)
    )
    return "\n".join(lines).rstrip() + "\n"


def render_failure_report(
    *,
    report_date: str,
    commit: str,
    run_url: str,
    dependency_status: str,
    generator_outcome: str,
    reason: str,
) -> str:
    """Render durable, actionable evidence when model generation fails."""
    standing_audit_url = _standing_audit_url(run_url)
    return "\n".join(
        [
            f"# Daily Security Audit Generation Failure — {_markdown(report_date)}",
            "",
            "> The bounded security review did not produce schema-valid evidence. "
            "The workflow publishes this report and then fails loudly.",
            "",
            f"- Commit: `{_markdown(commit)}`",
            f"- Workflow run: [diagnostics]({_markdown(run_url)})",
            f"- Generator outcome: `{_markdown(generator_outcome)}`",
            f"- Validation failure: {_markdown(reason)}",
            f"- Dependency tooling: {_markdown(dependency_status)}",
            f"- Standing audit: [2026-03-28 security program review]({standing_audit_url})",
            "",
            "## Required follow-up",
            "",
            "1. Inspect the retained dependency and model-output artifacts.",
            "2. Repair the bounded generator or its current source manifest.",
            "3. Re-run this workflow; do not treat this fallback as a completed security review.",
            "",
        ]
    )


def _read(path: Path, *, fallback: str) -> str:
    try:
        value = path.read_text(encoding="utf-8").strip()
    except OSError:
        return fallback
    return (value or fallback)[:MAX_TEXT_LENGTH]


def _append_github_output(path: Path, *, generated: bool) -> None:
    with path.open("a", encoding="utf-8") as stream:
        stream.write(f"generated={'true' if generated else 'false'}\n")


def materialize(
    *,
    result_path: Path,
    artifacts_dir: Path,
    report_path: Path,
    generator_outcome: str,
    run_url: str,
    github_output: Path,
) -> bool:
    """Publish a validated report or explicit failure evidence.

    Returns whether the report came from schema-valid model output. The
    function itself succeeds for a generator failure so later workflow steps
    can publish the fallback issue and artifact before the final gate fails.
    """
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    report_date = _read(artifacts_dir / "report-date.txt", fallback="unknown date")
    commit = _read(artifacts_dir / "commit.txt", fallback="unknown commit")
    dependency_status = _read(
        artifacts_dir / "dependency-audit-status.txt",
        fallback="dependency audit status unavailable",
    )

    generated = False
    failure_reason = "generator did not complete successfully"
    try:
        if generator_outcome != "success":
            raise AuditError(f"generator outcome was {generator_outcome!r}")
        raw_result = json.loads(result_path.read_text(encoding="utf-8"))
        result = validate_result(raw_result)
        report = render_report(
            result,
            report_date=report_date,
            commit=commit,
            run_url=run_url,
            dependency_status=dependency_status,
        )
        generated = True
        failure_reason = ""
    except (AuditError, json.JSONDecodeError, OSError) as exc:
        failure_reason = str(exc)[:1_000] or type(exc).__name__
        report = render_failure_report(
            report_date=report_date,
            commit=commit,
            run_url=run_url,
            dependency_status=dependency_status,
            generator_outcome=generator_outcome,
            reason=failure_reason,
        )

    report_path.write_text(report, encoding="utf-8")
    (artifacts_dir / "generation-status.json").write_text(
        json.dumps(
            {
                "generated": generated,
                "generator_outcome": generator_outcome,
                "failure_reason": failure_reason or None,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    _append_github_output(github_output, generated=generated)
    return generated


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("schema", help="print the compact model output schema")

    materialize_parser = subparsers.add_parser(
        "materialize", help="render validated model output or failure evidence"
    )
    materialize_parser.add_argument("--result", type=Path, required=True)
    materialize_parser.add_argument("--artifacts-dir", type=Path, required=True)
    materialize_parser.add_argument("--report", type=Path, required=True)
    materialize_parser.add_argument("--generator-outcome", required=True)
    materialize_parser.add_argument("--run-url", required=True)
    materialize_parser.add_argument("--github-output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "schema":
        print(f"schema={json.dumps(AUDIT_SCHEMA, separators=(',', ':'))}")
        return 0
    materialize(
        result_path=args.result,
        artifacts_dir=args.artifacts_dir,
        report_path=args.report,
        generator_outcome=args.generator_outcome,
        run_url=args.run_url,
        github_output=args.github_output,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
