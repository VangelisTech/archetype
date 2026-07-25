# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Deterministic validation and publication helpers for footgun review CI.

The model supplies analysis. This module owns the merge-critical mechanics:
binding that analysis to an exact commit and diff, validating review coverage,
rendering GitHub review payloads, and verifying the posted evidence.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import re
import subprocess
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
BOT_LOGIN = "github-actions[bot]"
REQUIRED_CATEGORIES = (
    "row-dropping",
    "unguarded-llm-calls",
    "api-signature-mismatch",
    "missing-type-key",
    "private-api-coupling",
    "monotonic-state",
    "fork-ownership-mismatch",
    "store-vs-live-reads",
    "governance-bypass",
    "dead-code-contracts",
    "substring-matching-on-structured-data",
    "fail-open-failure-paths",
    "identity-keying-disagreement",
    "error-path-unwind",
    "off-lifecycle-states",
    "wrong-return-values",
    "dag-breaking-collects",
    "non-serializable-closures",
    "with-column-vs-with-columns",
    "deprecated-daft-apis",
    "arrow-serialization-violations",
    "tick-boundary-violations",
    "observability-boundary-and-authority",
    "telemetry-safety-and-cardinality",
)
# Merge-gate severity tier. The skill rulebook defines the semantics and tests
# derive this tuple from it: `blocking` findings fail `review-complete`;
# `advisory` findings post as resolvable threads that gate queue-ready until
# resolved, where a written disposition is a sanctioned resolution.
SEVERITIES = (
    "blocking",
    "advisory",
)

# Prose ceilings, enforced by the validator and mirrored into the constrained
# schema. `review_context` must cover every changed file and every lens covers
# the whole diff, so unbounded prose grows the published evidence as
# files x lenses: a 71-file diff produced a 51KB comment whose content was
# mostly per-lens narration of the categories that did not apply. Findings
# carry their own detail and post as threads, so bounding narration here costs
# no finding fidelity.
SUMMARY_MAX = 600
ASSESSMENT_MAX = 300

# Retention for the uploaded validated artifact. Every elision in the
# published comment points at that artifact as the complete record, so this
# must outlive the review it explains; at the previous 1 day, a degraded
# comment became a permanent record pointing at a dead link the next morning.
# `retention-days` in the review workflow mirrors this value and a drift test
# holds the two together.
ARTIFACT_RETENTION_DAYS = 90

# The parallel review matrix runs one detector job per lens. Every lens
# reviews the full diff against its category subset; `merge` reassembles the
# lens results into one full-coverage review. The partition invariant —
# every required category assigned to exactly one lens — is enforced by
# merge_lens_results, so a category added to REQUIRED_CATEGORIES without a
# lens assignment fails the gate closed rather than silently going
# unreviewed.
LENSES: dict[str, tuple[str, ...]] = {
    "daft-shape": (
        "row-dropping",
        "unguarded-llm-calls",
        "monotonic-state",
        "dag-breaking-collects",
        "non-serializable-closures",
        "with-column-vs-with-columns",
        "deprecated-daft-apis",
    ),
    "state-lifecycle": (
        "fork-ownership-mismatch",
        "store-vs-live-reads",
        "tick-boundary-violations",
        "error-path-unwind",
        "off-lifecycle-states",
    ),
    "contracts": (
        "api-signature-mismatch",
        "missing-type-key",
        "private-api-coupling",
        "wrong-return-values",
        "dead-code-contracts",
        "arrow-serialization-violations",
    ),
    "authority": (
        "governance-bypass",
        "fail-open-failure-paths",
        "identity-keying-disagreement",
        "substring-matching-on-structured-data",
    ),
    "observability": (
        "observability-boundary-and-authority",
        "telemetry-safety-and-cardinality",
    ),
}

_HUNK_RE = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@(?: .*)?$")
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")


class GateError(ValueError):
    """Raised when review evidence cannot safely satisfy the merge gate."""


def _run_git(*args: str) -> bytes:
    completed = subprocess.run(
        ("git", *args),
        check=True,
        capture_output=True,
    )
    return completed.stdout


def _validate_shas(base_sha: str, head_sha: str) -> None:
    if not _SHA_RE.fullmatch(base_sha) or not _SHA_RE.fullmatch(head_sha):
        raise GateError("base and head must be full lowercase Git SHAs")


def _scope_payload(base_sha: str, head_sha: str, files: Sequence[str]) -> dict[str, Any]:
    _validate_shas(base_sha, head_sha)
    if not files:
        raise GateError("the pull request has no changed files to review")
    if len(files) != len(set(files)):
        raise GateError("the pull request file manifest contains duplicates")
    return {
        "schema_version": SCHEMA_VERSION,
        "base_sha": base_sha,
        "head_sha": head_sha,
        "files": sorted(files),
        "categories": list(REQUIRED_CATEGORIES),
    }


def build_scope(base_sha: str, head_sha: str) -> tuple[dict[str, Any], str]:
    """Return the exact file/category manifest and rename-aware unified diff."""
    _validate_shas(base_sha, head_sha)
    comparison = f"{base_sha}...{head_sha}"
    names = _run_git(
        "-c",
        "core.quotePath=false",
        "diff",
        "--name-only",
        "--find-renames",
        "-z",
        comparison,
    )
    files = sorted(path.decode("utf-8") for path in names.rstrip(b"\0").split(b"\0") if path)
    diff = _run_git(
        "-c",
        "core.quotePath=false",
        "diff",
        "--no-ext-diff",
        "--no-color",
        "--find-renames",
        "--unified=3",
        comparison,
    ).decode("utf-8")
    return _scope_payload(base_sha, head_sha, files), diff


def build_github_scope(
    *,
    repository: str,
    pr_number: int,
    base_sha: str,
    head_sha: str,
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    file_pages: Any,
    diff: str,
) -> dict[str, Any]:
    """Validate an API-fetched PR snapshot and return its deterministic scope."""

    def validate_metadata(metadata: Mapping[str, Any], label: str) -> int:
        base = _expect_mapping(metadata.get("base"), f"{label}.base")
        base_repo = _expect_mapping(base.get("repo"), f"{label}.base.repo")
        head = _expect_mapping(metadata.get("head"), f"{label}.head")
        if metadata.get("number") != pr_number:
            raise GateError(f"{label} pull request number does not match the event")
        if metadata.get("state") != "open":
            raise GateError(f"{label} pull request is not open")
        if str(base_repo.get("full_name", "")).casefold() != repository.casefold():
            raise GateError(f"{label} base repository does not match the event")
        if base.get("sha") != base_sha or head.get("sha") != head_sha:
            raise GateError(f"{label} base/head does not match the event")
        changed_files = metadata.get("changed_files")
        if not isinstance(changed_files, int) or isinstance(changed_files, bool):
            raise GateError(f"{label}.changed_files must be an integer")
        return changed_files

    before_count = validate_metadata(before, "before")
    after_count = validate_metadata(after, "after")
    if before_count != after_count:
        raise GateError("pull request changed while the review scope was fetched")

    file_items = _flatten_pages(file_pages, "files")
    files: list[str] = []
    for index, item in enumerate(file_items):
        filename = item.get("filename")
        if not isinstance(filename, str) or not filename:
            raise GateError(f"files[{index}].filename must be a non-empty string")
        files.append(filename)
    if len(files) != before_count:
        raise GateError(
            "the fetched file manifest does not match the pull request changed-file count"
        )
    if not diff.strip():
        raise GateError("the fetched pull request diff is empty")
    return _scope_payload(base_sha, head_sha, files)


def _diff_path(value: str) -> str | None:
    if value == "/dev/null":
        return None
    if value.startswith('"'):
        try:
            value = ast.literal_eval(value)
        except (SyntaxError, ValueError) as error:
            raise GateError(f"invalid quoted path in unified diff: {value!r}") from error
    if value.startswith(("a/", "b/")):
        return value[2:]
    return value


def changed_line_anchors(diff: str) -> set[tuple[str, str, int]]:
    """Return commentable changed lines as ``(path, side, line)`` tuples."""
    anchors: set[tuple[str, str, int]] = set()
    old_path: str | None = None
    new_path: str | None = None
    old_line = new_line = 0
    old_remaining = new_remaining = 0
    in_hunk = False

    for raw_line in diff.splitlines():
        if not in_hunk and raw_line.startswith("--- "):
            old_path = _diff_path(raw_line[4:])
            continue
        if not in_hunk and raw_line.startswith("+++ "):
            new_path = _diff_path(raw_line[4:])
            continue

        match = _HUNK_RE.match(raw_line)
        if match:
            old_line = int(match.group(1))
            old_remaining = int(match.group(2) or "1")
            new_line = int(match.group(3))
            new_remaining = int(match.group(4) or "1")
            in_hunk = old_remaining > 0 or new_remaining > 0
            continue

        if not in_hunk or raw_line.startswith("\\ No newline at end of file"):
            continue

        prefix = raw_line[:1]
        comment_path = new_path or old_path
        if prefix == "-":
            if comment_path is not None:
                anchors.add((comment_path, "LEFT", old_line))
            old_line += 1
            old_remaining -= 1
        elif prefix == "+":
            if comment_path is not None:
                anchors.add((comment_path, "RIGHT", new_line))
            new_line += 1
            new_remaining -= 1
        elif prefix == " ":
            old_line += 1
            new_line += 1
            old_remaining -= 1
            new_remaining -= 1
        else:
            raise GateError(f"unexpected unified diff line inside hunk: {raw_line!r}")

        if old_remaining < 0 or new_remaining < 0:
            raise GateError("unified diff hunk exceeded its declared line count")
        in_hunk = old_remaining > 0 or new_remaining > 0

    return anchors


def _expect_mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise GateError(f"{label} must be an object")
    return value


def _expect_list(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise GateError(f"{label} must be an array")
    return value


def _text(value: Any, label: str, *, minimum: int, maximum: int | None = None) -> str:
    if not isinstance(value, str) or len(value.strip()) < minimum:
        raise GateError(f"{label} must contain at least {minimum} non-whitespace characters")
    stripped = value.strip()
    if maximum is not None and len(stripped) > maximum:
        raise GateError(
            f"{label} must contain at most {maximum} characters; got {len(stripped)}. "
            "State the concrete observation, not the categories that did not apply."
        )
    return stripped


def _exact_unique_strings(value: Any, expected: Sequence[str], label: str) -> list[str]:
    items = _expect_list(value, label)
    if any(not isinstance(item, str) for item in items):
        raise GateError(f"{label} must contain only strings")
    if len(items) != len(set(items)):
        raise GateError(f"{label} must not contain duplicates")
    if set(items) != set(expected):
        missing = sorted(set(expected) - set(items))
        extra = sorted(set(items) - set(expected))
        raise GateError(f"{label} does not match scope; missing={missing}, extra={extra}")
    return sorted(items)


def lens_categories(lens: str) -> tuple[str, ...]:
    """Return the category subset one review lens is responsible for."""
    if lens not in LENSES:
        raise GateError(f"unknown review lens {lens!r}; expected one of {sorted(LENSES)}")
    return LENSES[lens]


def validate_result(
    raw_result: Mapping[str, Any],
    scope: Mapping[str, Any],
    diff: str,
    *,
    categories: Sequence[str] = REQUIRED_CATEGORIES,
    summary_maximum: int = SUMMARY_MAX,
) -> dict[str, Any]:
    """Validate and normalize model output against the exact reviewed diff.

    ``categories`` narrows the required coverage to one lens's subset; the
    default demands the full detector category list. ``summary_maximum``
    scales for the merged review, which concatenates one bounded summary per
    lens.
    """
    head_sha = scope.get("head_sha")
    if raw_result.get("head_sha") != head_sha:
        raise GateError("review head_sha does not match the pull request head")

    scoped_files = _expect_list(scope.get("files"), "scope.files")
    if any(not isinstance(item, str) for item in scoped_files):
        raise GateError("scope.files must contain only strings")
    files = _exact_unique_strings(raw_result.get("reviewed_files"), scoped_files, "reviewed_files")
    reviewed_categories = _exact_unique_strings(
        raw_result.get("reviewed_categories"),
        categories,
        "reviewed_categories",
    )
    summary = _text(raw_result.get("summary"), "summary", minimum=80, maximum=summary_maximum)

    context_entries = _expect_list(raw_result.get("review_context"), "review_context")
    if not context_entries:
        raise GateError("review_context must describe at least one changed area")
    contextualized_files: list[str] = []
    normalized_context: list[dict[str, Any]] = []
    for index, raw_entry in enumerate(context_entries):
        entry = _expect_mapping(raw_entry, f"review_context[{index}]")
        entry_files = _expect_list(entry.get("files"), f"review_context[{index}].files")
        if not entry_files or any(not isinstance(item, str) for item in entry_files):
            raise GateError(f"review_context[{index}].files must contain file paths")
        unknown = sorted(set(entry_files) - set(scoped_files))
        if unknown:
            raise GateError(f"review_context[{index}] references files outside the diff: {unknown}")
        contextualized_files.extend(entry_files)
        normalized_context.append(
            {
                "area": _text(entry.get("area"), f"review_context[{index}].area", minimum=3),
                "files": sorted(set(entry_files)),
                "assessment": _text(
                    entry.get("assessment"),
                    f"review_context[{index}].assessment",
                    minimum=30,
                    maximum=ASSESSMENT_MAX,
                ),
            }
        )
    # Coverage — not partition — is the invariant: a file may appear in more
    # than one area when it genuinely spans concerns.
    if set(contextualized_files) != set(scoped_files):
        missing = sorted(set(scoped_files) - set(contextualized_files))
        raise GateError(f"review_context does not cover every changed file: {missing}")

    anchors = changed_line_anchors(diff)
    findings = _expect_list(raw_result.get("findings"), "findings")
    normalized_findings: list[dict[str, Any]] = []
    for index, raw_finding in enumerate(findings):
        finding = _expect_mapping(raw_finding, f"findings[{index}]")
        category = finding.get("category")
        if category not in categories:
            raise GateError(f"findings[{index}].category is not a reviewed category")
        severity = finding.get("severity")
        if severity not in SEVERITIES:
            raise GateError(f"findings[{index}].severity must be one of {', '.join(SEVERITIES)}")
        path = finding.get("path")
        if path not in scoped_files:
            raise GateError(f"findings[{index}].path is outside the reviewed diff")
        side = finding.get("side")
        if side not in {"LEFT", "RIGHT"}:
            raise GateError(f"findings[{index}].side must be LEFT or RIGHT")
        line = finding.get("line")
        if not isinstance(line, int) or isinstance(line, bool) or line < 1:
            raise GateError(f"findings[{index}].line must be a positive integer")
        if (path, side, line) not in anchors:
            raise GateError(
                f"findings[{index}] is not anchored to a changed diff line: {path}:{line} {side}"
            )
        normalized_findings.append(
            {
                "category": category,
                "severity": severity,
                "title": _text(finding.get("title"), f"findings[{index}].title", minimum=5),
                "path": path,
                "side": side,
                "line": line,
                "what_it_does": _text(
                    finding.get("what_it_does"),
                    f"findings[{index}].what_it_does",
                    minimum=20,
                ),
                "what_goes_wrong": _text(
                    finding.get("what_goes_wrong"),
                    f"findings[{index}].what_goes_wrong",
                    minimum=20,
                ),
                "fix": _text(finding.get("fix"), f"findings[{index}].fix", minimum=20),
            }
        )

    return {
        "schema_version": SCHEMA_VERSION,
        "head_sha": head_sha,
        "summary": summary,
        "reviewed_files": files,
        "reviewed_categories": reviewed_categories,
        "review_context": normalized_context,
        "findings": normalized_findings,
    }


def merged_summary_maximum() -> int:
    """Return the merged-summary ceiling derived from the lens partition.

    The merge joins one ``[lens] <summary>`` segment per lens with single
    spaces, so the merged ceiling follows from the per-lens ceiling rather
    than being a second independently-tuned number.
    """
    prefixes = sum(len(f"[{lens}] ") for lens in LENSES)
    separators = max(0, len(LENSES) - 1)
    return prefixes + separators + SUMMARY_MAX * len(LENSES)


def merge_lens_results(
    lens_results: Mapping[str, Mapping[str, Any]], scope: Mapping[str, Any], diff: str
) -> dict[str, Any]:
    """Reassemble per-lens reviews into one full-coverage validated review.

    Fails closed unless every lens is present, every lens result validates
    against its own category subset, and the lens partition covers
    REQUIRED_CATEGORIES exactly — so a category without a lens assignment,
    or a missing lens artifact, blocks the merge rather than shrinking the
    reviewed surface.
    """
    assigned = [category for categories in LENSES.values() for category in categories]
    if len(assigned) != len(set(assigned)):
        raise GateError("the lens partition assigns a category to more than one lens")
    if set(assigned) != set(REQUIRED_CATEGORIES):
        missing = sorted(set(REQUIRED_CATEGORIES) - set(assigned))
        extra = sorted(set(assigned) - set(REQUIRED_CATEGORIES))
        raise GateError(
            f"the lens partition does not cover the required categories; "
            f"missing={missing}, extra={extra}"
        )
    if set(lens_results) != set(LENSES):
        missing = sorted(set(LENSES) - set(lens_results))
        extra = sorted(set(lens_results) - set(LENSES))
        raise GateError(
            f"lens results do not match the lens partition; missing={missing}, extra={extra}"
        )

    merged_context: list[dict[str, Any]] = []
    summaries: list[str] = []
    # Dedupe on the anchoring identity; when duplicates disagree on severity,
    # the blocking one wins — a dedupe must never soften the gate.
    findings_by_key: dict[tuple[str, str, str, int], dict[str, Any]] = {}
    for lens in LENSES:
        validated = validate_result(lens_results[lens], scope, diff, categories=LENSES[lens])
        summaries.append(f"[{lens}] {validated['summary']}")
        for entry in validated["review_context"]:
            merged_context.append({**entry, "area": f"{lens}: {entry['area']}"})
        for finding in validated["findings"]:
            key = (finding["category"], finding["path"], finding["side"], finding["line"])
            kept = findings_by_key.get(key)
            if kept is None or (
                kept["severity"] == "advisory" and finding["severity"] == "blocking"
            ):
                findings_by_key[key] = finding
    merged_findings = list(findings_by_key.values())

    merged = {
        "head_sha": scope.get("head_sha"),
        "summary": " ".join(summaries),
        "reviewed_files": list(scope.get("files") or []),
        "reviewed_categories": list(REQUIRED_CATEGORIES),
        "review_context": merged_context,
        "findings": merged_findings,
    }
    return validate_result(merged, scope, diff, summary_maximum=merged_summary_maximum())


def extract_structured_json(raw: str) -> dict[str, Any] | None:
    """Best-effort extraction of one JSON object from free-form CLI output.

    Non-constrained backends print the structured result inside a normal
    model response (prose, code fences). Return the last standalone JSON
    object — preferring one carrying ``head_sha`` — or None when the text
    contains no parseable object; the validator supplies the bounded-retry
    feedback either way.
    """
    decoder = json.JSONDecoder()
    last_object: dict[str, Any] | None = None
    last_bound: dict[str, Any] | None = None
    index = 0
    while True:
        start = raw.find("{", index)
        if start == -1:
            break
        try:
            value, end = decoder.raw_decode(raw, start)
        except ValueError:
            index = start + 1
            continue
        index = end
        if isinstance(value, dict):
            last_object = value
            if "head_sha" in value:
                last_bound = value
    return last_bound or last_object


def retry_feedback(error: GateError) -> str:
    """Turn one validation failure into bounded correction guidance."""
    return (
        "The first detector response did not pass Archetype's exact-scope validator:\n\n"
        f"{error}\n\n"
        "Return one corrected, complete structured result for the same exact head. "
        "`reviewed_files` and every `review_context.files` array contain changed paths only. "
        "Mention protected-base implementation or evidence paths in assessment prose, not in "
        "those arrays. Use the authoritative scope values; do not return schema examples or "
        "placeholder values. Preserve substantive analysis from the first response when it is "
        "still valid.\n"
    )


def artifact_digest(result: Mapping[str, Any]) -> str:
    canonical = json.dumps(result, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def result_schema(categories: Sequence[str] = REQUIRED_CATEGORIES) -> dict[str, Any]:
    """Return the constrained-output schema used by Claude Code."""
    text = {"type": "string", "minLength": 1}
    return {
        "type": "object",
        "properties": {
            "head_sha": {"type": "string", "pattern": "^[0-9a-f]{40}$"},
            "summary": {"type": "string", "minLength": 80, "maxLength": SUMMARY_MAX},
            "reviewed_files": {"type": "array", "items": text, "minItems": 1},
            "reviewed_categories": {"type": "array", "items": text, "minItems": 1},
            "review_context": {
                "type": "array",
                "minItems": 1,
                "items": {
                    "type": "object",
                    "properties": {
                        "area": {"type": "string", "minLength": 3},
                        "files": {"type": "array", "items": text, "minItems": 1},
                        "assessment": {
                            "type": "string",
                            "minLength": 30,
                            "maxLength": ASSESSMENT_MAX,
                        },
                    },
                    "required": ["area", "files", "assessment"],
                    "additionalProperties": False,
                },
            },
            "findings": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "category": {"type": "string", "enum": list(categories)},
                        "severity": {"type": "string", "enum": list(SEVERITIES)},
                        "title": {"type": "string", "minLength": 5},
                        "path": text,
                        "side": {"type": "string", "enum": ["LEFT", "RIGHT"]},
                        "line": {"type": "integer", "minimum": 1},
                        "what_it_does": {"type": "string", "minLength": 20},
                        "what_goes_wrong": {"type": "string", "minLength": 20},
                        "fix": {"type": "string", "minLength": 20},
                    },
                    "required": [
                        "category",
                        "severity",
                        "title",
                        "path",
                        "side",
                        "line",
                        "what_it_does",
                        "what_goes_wrong",
                        "fix",
                    ],
                    "additionalProperties": False,
                },
            },
        },
        "required": [
            "head_sha",
            "summary",
            "reviewed_files",
            "reviewed_categories",
            "review_context",
            "findings",
        ],
        "additionalProperties": False,
    }


def evidence_marker(head_sha: str, finding_count: int, digest: str) -> str:
    return (
        "<!-- archetype-footgun-review "
        f"schema={SCHEMA_VERSION} head={head_sha} findings={finding_count} digest={digest} -->"
    )


def _markdown_code(value: str) -> str:
    return value.replace("`", "\\`")


_PUBLISHED_BODY_LIMIT = 60000


def _artifact_section(
    result: Mapping[str, Any],
    run_url: str | None,
    artifact_name: str | None,
    *,
    inline: bool,
) -> list[str]:
    lines = [
        "<details>",
        "<summary>Validated review artifact</summary>",
        "",
    ]
    if inline:
        payload = json.dumps(result, indent=2, ensure_ascii=False)
        # Findings may quote code fences; the outer fence must be longer than
        # any backtick run inside the payload.
        longest_run = max((len(match.group(0)) for match in re.finditer(r"`+", payload)), default=0)
        fence = "`" * max(3, longest_run + 1)
        lines.extend([f"{fence}json", payload, fence, ""])
    else:
        if not run_url or not artifact_name:
            raise GateError("oversized review evidence requires a named validated artifact")
        lines.extend(
            [
                "The validated structured output exceeds the inline comment budget; "
                "download the named validated artifact instead.",
                "",
            ]
        )
    if run_url and artifact_name:
        lines.extend(
            [
                f"Validated artifact: [{artifact_name}]({run_url}#artifacts) "
                f"({ARTIFACT_RETENTION_DAYS}-day retention).",
                "",
            ]
        )
    lines.append("</details>")
    return lines


def render_evidence(
    result: Mapping[str, Any],
    digest: str,
    run_url: str | None = None,
    artifact_name: str | None = None,
) -> str:
    findings = _expect_list(result.get("findings"), "findings")
    files = _expect_list(result.get("reviewed_files"), "reviewed_files")
    categories = _expect_list(result.get("reviewed_categories"), "reviewed_categories")
    context = _expect_list(result.get("review_context"), "review_context")
    head_sha = str(result["head_sha"])
    finding_count = len(findings)
    blocking_count = sum(
        1
        for finding in findings
        if _expect_mapping(finding, "findings entry").get("severity") == "blocking"
    )
    outcome = (
        "no findings"
        if finding_count == 0
        else (
            f"{finding_count} finding(s) — "
            f"{blocking_count} blocking, {finding_count - blocking_count} advisory"
        )
    )
    marker = evidence_marker(head_sha, finding_count, digest)

    def prose(*, context_detail: bool) -> list[str]:
        rendered_lines = [
            f"## Footgun review — {outcome}",
            "",
            f"**Exact head:** `{head_sha}`  ",
            f"**Validated scope:** {len(files)} changed file(s), "
            f"{len(categories)} detector categories assigned across {len(LENSES)} lenses",
            "",
            str(result["summary"]),
            "",
            "<details>",
            "<summary>Context reviewed</summary>",
            "",
        ]
        if context_detail:
            for raw_entry in context:
                entry = _expect_mapping(raw_entry, "review_context entry")
                entry_files = ", ".join(
                    f"`{_markdown_code(str(path))}`"
                    for path in _expect_list(entry["files"], "files")
                )
                rendered_lines.extend(
                    [f"- **{entry['area']}** ({entry_files}): {entry['assessment']}", ""]
                )
        else:
            rendered_lines.extend(
                [
                    f"{len(context)} reviewed area(s) covering {len(files)} changed file(s). "
                    "The per-area assessments exceed the published comment budget; read them "
                    "in the validated artifact below.",
                    "",
                ]
            )
        rendered_lines.extend(
            [
                "</details>",
                "",
                "<details>",
                "<summary>Detector categories assigned</summary>",
                "",
                *[f"- `{category}`" for category in categories],
                "",
                "</details>",
                "",
            ]
        )
        return rendered_lines

    def body(*, inline: bool, context_detail: bool) -> str:
        return "\n".join(
            [
                *prose(context_detail=context_detail),
                *_artifact_section(result, run_url, artifact_name, inline=inline),
                "",
                marker,
            ]
        )

    # Summary and assessment prose is bounded by the validator (SUMMARY_MAX,
    # ASSESSMENT_MAX), so the only term that still scales without limit is
    # per-area context: every lens must cover every changed file, making the
    # context section grow as files x lenses. Degrade that, then the inline
    # artifact, and never silently — the elision says so and points at a
    # retained artifact. The digest is computed over the result rather than
    # this rendering, so shrinking the body cannot weaken the evidence
    # `verify` matches.
    for inline, context_detail in (
        (True, True),
        (False, True),
        (False, False),
    ):
        rendered = body(inline=inline, context_detail=context_detail)
        if len(rendered.encode("utf-8")) <= _PUBLISHED_BODY_LIMIT:
            return rendered
    raise GateError("rendered review evidence exceeds the published body limit")


def render_finding(finding: Mapping[str, Any], head_sha: str) -> str:
    severity = str(finding["severity"])
    disposition = (
        "fix before merge"
        if severity == "blocking"
        else "fix, or resolve this thread with a written disposition"
    )
    return "\n".join(
        [
            f"### {finding['category']}: {finding['title']}",
            "",
            f"**Severity:** {severity} — {disposition}",
            "",
            f"**What it does:** {finding['what_it_does']}",
            "",
            f"**What goes wrong:** {finding['what_goes_wrong']}",
            "",
            "**Fix:**",
            str(finding["fix"]),
            "",
            f"<!-- archetype-footgun-finding head={head_sha} -->",
        ]
    )


def review_payload(
    result: Mapping[str, Any],
    digest: str,
    run_url: str | None = None,
    artifact_name: str | None = None,
) -> dict[str, Any]:
    head_sha = str(result["head_sha"])
    findings = _expect_list(result.get("findings"), "findings")
    if not findings:
        raise GateError("a review payload requires at least one finding")
    return {
        "commit_id": head_sha,
        "event": "COMMENT",
        "body": render_evidence(result, digest, run_url, artifact_name),
        "comments": [
            {
                "path": finding["path"],
                "line": finding["line"],
                "side": finding["side"],
                "body": render_finding(finding, head_sha),
            }
            for finding in findings
        ],
    }


def _flatten_pages(value: Any, label: str) -> list[Mapping[str, Any]]:
    pages = _expect_list(value, label)
    if pages and all(isinstance(item, Mapping) for item in pages):
        return [item for item in pages if isinstance(item, Mapping)]
    flattened: list[Mapping[str, Any]] = []
    for page_index, page in enumerate(pages):
        for item in _expect_list(page, f"{label}[{page_index}]"):
            flattened.append(_expect_mapping(item, f"{label}[{page_index}] item"))
    return flattened


def _is_bot_authored(item: Mapping[str, Any]) -> bool:
    user = item.get("user")
    return isinstance(user, Mapping) and user.get("login") == BOT_LOGIN


def verify_posted_evidence(
    *,
    issue_comments: Any,
    reviews: Any,
    review_comments: Any,
    head_sha: str,
    finding_count: int,
    digest: str,
) -> None:
    """Verify that this exact run posted the expected bot-authored artifact."""
    marker = evidence_marker(head_sha, finding_count, digest)
    issues = _flatten_pages(issue_comments, "issue_comments")
    pull_reviews = _flatten_pages(reviews, "reviews")
    inline_comments = _flatten_pages(review_comments, "review_comments")

    if finding_count == 0:
        matches = [
            item
            for item in issues
            if _is_bot_authored(item) and marker in str(item.get("body") or "")
        ]
        if not matches:
            raise GateError(
                "the exact no-findings evidence comment was not posted by GitHub Actions"
            )
        return

    matching_reviews = [
        item
        for item in pull_reviews
        if _is_bot_authored(item)
        and item.get("commit_id") == head_sha
        and marker in str(item.get("body") or "")
    ]
    for review in matching_reviews:
        review_id = review.get("id")
        matching_inline = [
            comment
            for comment in inline_comments
            if comment.get("pull_request_review_id") == review_id
            and _is_bot_authored(comment)
            and f"head={head_sha}" in str(comment.get("body") or "")
        ]
        if len(matching_inline) == finding_count:
            return
    raise GateError(
        "the exact findings review and its inline review threads were not posted by GitHub Actions"
    )


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise GateError(f"could not read valid JSON from {path}: {error}") from error


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def _append_github_outputs(path: Path, values: Mapping[str, str | int]) -> None:
    with path.open("a", encoding="utf-8") as output:
        for key, value in values.items():
            output.write(f"{key}={value}\n")


def _scope_command(args: argparse.Namespace) -> None:
    scope, diff = build_scope(args.base, args.head)
    _write_json(args.scope, scope)
    args.diff.write_text(diff, encoding="utf-8")


def _github_scope_command(args: argparse.Namespace) -> None:
    scope = build_github_scope(
        repository=args.repository,
        pr_number=args.pr_number,
        base_sha=args.base,
        head_sha=args.head,
        before=_expect_mapping(_load_json(args.before), "before"),
        after=_expect_mapping(_load_json(args.after), "after"),
        file_pages=_load_json(args.files),
        diff=args.diff.read_text(encoding="utf-8"),
    )
    _write_json(args.scope, scope)


def _selected_categories(args: argparse.Namespace) -> tuple[str, ...]:
    lens = getattr(args, "lens", None)
    return lens_categories(lens) if lens else REQUIRED_CATEGORIES


def _validated_result(args: argparse.Namespace) -> dict[str, Any]:
    scope = _expect_mapping(_load_json(args.scope), "scope")
    raw_result = _expect_mapping(_load_json(args.result), "result")
    diff = args.diff.read_text(encoding="utf-8")
    return validate_result(raw_result, scope, diff, categories=_selected_categories(args))


def _normalize_command(args: argparse.Namespace) -> None:
    result = _validated_result(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    _write_json(args.output, result)


def _attempt_command(args: argparse.Namespace) -> None:
    """Validate one detector response and emit whether one retry is required."""
    try:
        result = _validated_result(args)
    except GateError as error:
        args.output.unlink(missing_ok=True)
        args.feedback.parent.mkdir(parents=True, exist_ok=True)
        args.feedback.write_text(retry_feedback(error), encoding="utf-8")
        _append_github_outputs(args.github_output, {"valid": "false"})
        return

    args.output.parent.mkdir(parents=True, exist_ok=True)
    _write_json(args.output, result)
    args.feedback.unlink(missing_ok=True)
    _append_github_outputs(args.github_output, {"valid": "true"})


def _prepare_command(args: argparse.Namespace) -> None:
    result = _validated_result(args)
    digest = artifact_digest(result)
    finding_count = len(result["findings"])
    blocking_finding_count = sum(
        1 for finding in result["findings"] if finding["severity"] == "blocking"
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    _write_json(args.output_dir / "normalized.json", result)
    (args.output_dir / "evidence.md").write_text(
        render_evidence(result, digest, args.run_url, args.artifact_name) + "\n",
        encoding="utf-8",
    )
    if finding_count:
        _write_json(
            args.output_dir / "review.json",
            review_payload(result, digest, args.run_url, args.artifact_name),
        )

    _append_github_outputs(
        args.github_output,
        {
            "head_sha": str(result["head_sha"]),
            "finding_count": finding_count,
            "blocking_finding_count": blocking_finding_count,
            "artifact_digest": digest,
        },
    )


def _verify_command(args: argparse.Namespace) -> None:
    verify_posted_evidence(
        issue_comments=_load_json(args.issue_comments),
        reviews=_load_json(args.reviews),
        review_comments=_load_json(args.review_comments),
        head_sha=args.head,
        finding_count=args.finding_count,
        digest=args.digest,
    )


def _merge_command(args: argparse.Namespace) -> None:
    scope = _expect_mapping(_load_json(args.scope), "scope")
    diff = args.diff.read_text(encoding="utf-8")
    lens_results: dict[str, Mapping[str, Any]] = {}
    for lens in LENSES:
        result_path = args.results_dir / f"{lens}.json"
        if not result_path.is_file():
            raise GateError(f"missing validated lens result: {result_path}")
        lens_results[lens] = _expect_mapping(_load_json(result_path), f"lens result {lens}")
    merged = merge_lens_results(lens_results, scope, diff)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    _write_json(args.output, merged)


def _extract_command(args: argparse.Namespace) -> None:
    """Write the extracted JSON object, or pass the raw text through.

    Never fails on unparsable model output: the passthrough feeds the
    attempt validator, which turns it into bounded-retry feedback.
    """
    raw = args.raw.read_text(encoding="utf-8")
    extracted = extract_structured_json(raw)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if extracted is None:
        args.output.write_text(raw, encoding="utf-8")
    else:
        _write_json(args.output, extracted)


def _lens_categories_command(args: argparse.Namespace) -> None:
    print(", ".join(lens_categories(args.lens)))


def _schema_command(args: argparse.Namespace) -> None:
    print(json.dumps(result_schema(_selected_categories(args)), separators=(",", ":")))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    schema = subparsers.add_parser("schema", help="print the constrained-output JSON schema")
    schema.add_argument("--lens", default=None)
    schema.set_defaults(handler=_schema_command)

    lens_list = subparsers.add_parser("lens-categories", help="print one lens's category subset")
    lens_list.add_argument("--lens", required=True)
    lens_list.set_defaults(handler=_lens_categories_command)

    extract = subparsers.add_parser(
        "extract", help="extract a structured JSON object from raw model output"
    )
    extract.add_argument("--raw", type=Path, required=True)
    extract.add_argument("--output", type=Path, required=True)
    extract.set_defaults(handler=_extract_command)

    merge = subparsers.add_parser(
        "merge", help="merge validated per-lens results into one full-coverage review"
    )
    merge.add_argument("--scope", type=Path, required=True)
    merge.add_argument("--diff", type=Path, required=True)
    merge.add_argument("--results-dir", type=Path, required=True)
    merge.add_argument("--output", type=Path, required=True)
    merge.set_defaults(handler=_merge_command)

    scope = subparsers.add_parser("scope", help="materialize the exact PR review scope")
    scope.add_argument("--base", required=True)
    scope.add_argument("--head", required=True)
    scope.add_argument("--scope", type=Path, required=True)
    scope.add_argument("--diff", type=Path, required=True)
    scope.set_defaults(handler=_scope_command)

    github_scope = subparsers.add_parser(
        "github-scope", help="validate an API-fetched PR review scope"
    )
    github_scope.add_argument("--repository", required=True)
    github_scope.add_argument("--pr-number", type=int, required=True)
    github_scope.add_argument("--base", required=True)
    github_scope.add_argument("--head", required=True)
    github_scope.add_argument("--before", type=Path, required=True)
    github_scope.add_argument("--after", type=Path, required=True)
    github_scope.add_argument("--files", type=Path, required=True)
    github_scope.add_argument("--diff", type=Path, required=True)
    github_scope.add_argument("--scope", type=Path, required=True)
    github_scope.set_defaults(handler=_github_scope_command)

    normalize = subparsers.add_parser(
        "normalize", help="validate structured output without rendering unpublished evidence"
    )
    normalize.add_argument("--scope", type=Path, required=True)
    normalize.add_argument("--diff", type=Path, required=True)
    normalize.add_argument("--result", type=Path, required=True)
    normalize.add_argument("--output", type=Path, required=True)
    normalize.add_argument("--lens", default=None)
    normalize.set_defaults(handler=_normalize_command)

    attempt = subparsers.add_parser(
        "attempt",
        help="validate one detector response and emit bounded-retry metadata",
    )
    attempt.add_argument("--scope", type=Path, required=True)
    attempt.add_argument("--diff", type=Path, required=True)
    attempt.add_argument("--result", type=Path, required=True)
    attempt.add_argument("--output", type=Path, required=True)
    attempt.add_argument("--feedback", type=Path, required=True)
    attempt.add_argument("--github-output", type=Path, required=True)
    attempt.add_argument("--lens", default=None)
    attempt.set_defaults(handler=_attempt_command)

    prepare = subparsers.add_parser("prepare", help="validate and render structured output")
    prepare.add_argument("--scope", type=Path, required=True)
    prepare.add_argument("--diff", type=Path, required=True)
    prepare.add_argument("--result", type=Path, required=True)
    prepare.add_argument("--output-dir", type=Path, required=True)
    prepare.add_argument("--github-output", type=Path, required=True)
    prepare.add_argument("--artifact-name", default=None)
    prepare.add_argument("--run-url", default=None)
    prepare.set_defaults(handler=_prepare_command)

    verify = subparsers.add_parser("verify", help="verify exact GitHub evidence")
    verify.add_argument("--issue-comments", type=Path, required=True)
    verify.add_argument("--reviews", type=Path, required=True)
    verify.add_argument("--review-comments", type=Path, required=True)
    verify.add_argument("--head", required=True)
    verify.add_argument("--finding-count", type=int, required=True)
    verify.add_argument("--digest", required=True)
    verify.set_defaults(handler=_verify_command)

    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        args.handler(args)
    except GateError as error:
        raise SystemExit(f"footgun review gate failed: {error}") from error
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
