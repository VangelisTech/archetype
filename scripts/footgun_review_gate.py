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

REVIEW_BUNDLE_KIND = "archetype-footgun-review-bundle"
REVIEW_BUNDLE_VERSION = 1
# The final bundle is operational workflow evidence, not permanent history.
# Its digest remains in the PR receipt after GitHub expires the artifact.
FINAL_ARTIFACT_RETENTION_DAYS = 90

# The parallel review matrix runs one detector job per lens. Every lens
# reviews the full diff against its category subset; `merge` packages the
# complete validated lens results without concatenating their narration.
# The partition invariant —
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

# This mirrors the workflow matrix and is held against it by a drift test.
# The final receipt records who performed each validated lens without making
# backend identity part of the model-authored result.
LENS_BACKENDS: dict[str, str] = {
    "daft-shape": "opencode",
    "state-lifecycle": "claude-code",
    "contracts": "claude-code",
    "authority": "claude-code",
    "observability": "opencode",
}

_HUNK_RE = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@(?: .*)?$")
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")


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


def _text(value: Any, label: str, *, minimum: int) -> str:
    if not isinstance(value, str) or len(value.strip()) < minimum:
        raise GateError(f"{label} must contain at least {minimum} non-whitespace characters")
    return value.strip()


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
) -> dict[str, Any]:
    """Validate and normalize model output against the exact reviewed diff.

    ``categories`` narrows the required coverage to one lens's subset; the
    default demands the full detector category list.
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
    summary = _text(raw_result.get("summary"), "summary", minimum=80)

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


def artifact_digest(result: Mapping[str, Any]) -> str:
    """Return the digest of one canonical structured review value."""
    canonical = json.dumps(result, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _validate_lens_partition() -> None:
    assigned = [category for categories in LENSES.values() for category in categories]
    if len(assigned) != len(set(assigned)):
        raise GateError("the lens partition assigns a category to more than one lens")
    if set(assigned) != set(REQUIRED_CATEGORIES):
        missing = sorted(set(REQUIRED_CATEGORIES) - set(assigned))
        extra = sorted(set(assigned) - set(REQUIRED_CATEGORIES))
        raise GateError(
            "the lens partition does not cover the required categories; "
            f"missing={missing}, extra={extra}"
        )
    if set(LENS_BACKENDS) != set(LENSES):
        missing = sorted(set(LENSES) - set(LENS_BACKENDS))
        extra = sorted(set(LENS_BACKENDS) - set(LENSES))
        raise GateError(
            "the lens backend map does not match the lens partition; "
            f"missing={missing}, extra={extra}"
        )


def _deduplicate_findings(results: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Combine validated findings without letting dedupe soften severity."""
    findings_by_key: dict[tuple[str, str, str, int], dict[str, Any]] = {}
    for result in results:
        for raw_finding in _expect_list(result.get("findings"), "findings"):
            finding = dict(_expect_mapping(raw_finding, "findings entry"))
            key = (finding["category"], finding["path"], finding["side"], finding["line"])
            kept = findings_by_key.get(key)
            if kept is None or (
                kept["severity"] == "advisory" and finding["severity"] == "blocking"
            ):
                findings_by_key[key] = finding
    return list(findings_by_key.values())


def validate_review_bundle(
    raw_bundle: Mapping[str, Any],
    scope: Mapping[str, Any],
    diff: str,
) -> dict[str, Any]:
    """Validate and normalize the machine-produced full review bundle.

    The bundle keeps every complete per-lens result as the audit artifact.
    Narration is not concatenated into a second model-shaped result; the
    human-facing publication is derived later as a compact receipt.
    """
    _validate_lens_partition()
    if raw_bundle.get("kind") != REVIEW_BUNDLE_KIND:
        raise GateError(f"review bundle kind must be {REVIEW_BUNDLE_KIND!r}")
    if raw_bundle.get("schema_version") != REVIEW_BUNDLE_VERSION:
        raise GateError(f"review bundle schema_version must be {REVIEW_BUNDLE_VERSION}")

    head_sha = scope.get("head_sha")
    if raw_bundle.get("head_sha") != head_sha:
        raise GateError("review bundle head_sha does not match the pull request head")
    scoped_files = _expect_list(scope.get("files"), "scope.files")
    if any(not isinstance(item, str) for item in scoped_files):
        raise GateError("scope.files must contain only strings")
    files = _exact_unique_strings(
        raw_bundle.get("reviewed_files"),
        scoped_files,
        "review bundle reviewed_files",
    )
    categories = _exact_unique_strings(
        raw_bundle.get("reviewed_categories"),
        REQUIRED_CATEGORIES,
        "review bundle reviewed_categories",
    )

    raw_receipts = _expect_list(raw_bundle.get("lenses"), "review bundle lenses")
    receipts_by_lens: dict[str, Mapping[str, Any]] = {}
    for index, raw_receipt in enumerate(raw_receipts):
        receipt = _expect_mapping(raw_receipt, f"review bundle lenses[{index}]")
        lens = receipt.get("lens")
        if not isinstance(lens, str) or lens not in LENSES:
            raise GateError(f"review bundle lenses[{index}].lens is not a configured lens")
        if lens in receipts_by_lens:
            raise GateError(f"review bundle contains duplicate lens {lens!r}")
        receipts_by_lens[lens] = receipt
    if set(receipts_by_lens) != set(LENSES):
        missing = sorted(set(LENSES) - set(receipts_by_lens))
        extra = sorted(set(receipts_by_lens) - set(LENSES))
        raise GateError(
            f"review bundle lenses do not match the partition; missing={missing}, extra={extra}"
        )

    normalized_receipts: list[dict[str, Any]] = []
    for lens in LENSES:
        receipt = receipts_by_lens[lens]
        backend = receipt.get("backend")
        if backend != LENS_BACKENDS[lens]:
            raise GateError(f"review bundle lens {lens!r} backend must be {LENS_BACKENDS[lens]!r}")
        if receipt.get("status") != "validated":
            raise GateError(f"review bundle lens {lens!r} status must be 'validated'")
        result = validate_result(
            _expect_mapping(receipt.get("result"), f"review bundle lens {lens!r} result"),
            scope,
            diff,
            categories=LENSES[lens],
        )
        digest = artifact_digest(result)
        if receipt.get("artifact_digest") != digest:
            raise GateError(f"review bundle lens {lens!r} digest does not match its result")
        normalized_receipts.append(
            {
                "lens": lens,
                "backend": backend,
                "status": "validated",
                "artifact_digest": digest,
                "result": result,
            }
        )

    return {
        "kind": REVIEW_BUNDLE_KIND,
        "schema_version": REVIEW_BUNDLE_VERSION,
        "head_sha": head_sha,
        "reviewed_files": files,
        "reviewed_categories": categories,
        "lenses": normalized_receipts,
    }


def _bundle_receipts(bundle: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    """Return ordered receipts after checking self-contained bundle integrity."""
    if bundle.get("kind") != REVIEW_BUNDLE_KIND:
        raise GateError("published evidence requires a validated review bundle")
    if bundle.get("schema_version") != REVIEW_BUNDLE_VERSION:
        raise GateError("published evidence has an unsupported review bundle version")
    head_sha = bundle.get("head_sha")
    if not isinstance(head_sha, str) or not _SHA_RE.fullmatch(head_sha):
        raise GateError("review bundle head_sha must be a full lowercase Git SHA")

    raw_receipts = _expect_list(bundle.get("lenses"), "review bundle lenses")
    receipts_by_lens: dict[str, Mapping[str, Any]] = {}
    for raw_receipt in raw_receipts:
        receipt = _expect_mapping(raw_receipt, "review bundle lens receipt")
        lens = receipt.get("lens")
        if not isinstance(lens, str) or lens in receipts_by_lens:
            raise GateError("review bundle lens receipts must have unique string names")
        receipts_by_lens[lens] = receipt
    if set(receipts_by_lens) != set(LENSES):
        raise GateError("review bundle lens receipts do not match the configured partition")

    ordered: list[Mapping[str, Any]] = []
    for lens in LENSES:
        receipt = receipts_by_lens[lens]
        if receipt.get("backend") != LENS_BACKENDS[lens] or receipt.get("status") != "validated":
            raise GateError(f"review bundle lens {lens!r} has invalid completion metadata")
        result = _expect_mapping(receipt.get("result"), f"review bundle lens {lens!r} result")
        if result.get("head_sha") != head_sha:
            raise GateError(f"review bundle lens {lens!r} is bound to a different head")
        digest = receipt.get("artifact_digest")
        if not isinstance(digest, str) or not _DIGEST_RE.fullmatch(digest):
            raise GateError(f"review bundle lens {lens!r} has an invalid digest")
        if artifact_digest(result) != digest:
            raise GateError(f"review bundle lens {lens!r} digest does not match its result")
        ordered.append(receipt)
    return ordered


def review_bundle_findings(bundle: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return the bundle's deterministic, severity-preserving finding set."""
    return _deduplicate_findings(
        _expect_mapping(receipt["result"], "review bundle lens result")
        for receipt in _bundle_receipts(bundle)
    )


def merge_lens_results(
    lens_results: Mapping[str, Mapping[str, Any]], scope: Mapping[str, Any], diff: str
) -> dict[str, Any]:
    """Package per-lens reviews into one full-coverage validated bundle.

    Fails closed unless every lens is present, every lens result validates
    against its own category subset, and the lens partition covers
    REQUIRED_CATEGORIES exactly — so a category without a lens assignment,
    or a missing lens artifact, blocks the merge rather than shrinking the
    reviewed surface.
    """
    _validate_lens_partition()
    if set(lens_results) != set(LENSES):
        missing = sorted(set(LENSES) - set(lens_results))
        extra = sorted(set(lens_results) - set(LENSES))
        raise GateError(
            f"lens results do not match the lens partition; missing={missing}, extra={extra}"
        )

    receipts: list[dict[str, Any]] = []
    for lens in LENSES:
        validated = validate_result(lens_results[lens], scope, diff, categories=LENSES[lens])
        receipts.append(
            {
                "lens": lens,
                "backend": LENS_BACKENDS[lens],
                "status": "validated",
                "artifact_digest": artifact_digest(validated),
                "result": validated,
            }
        )

    bundle = {
        "kind": REVIEW_BUNDLE_KIND,
        "schema_version": REVIEW_BUNDLE_VERSION,
        "head_sha": scope.get("head_sha"),
        "reviewed_files": list(scope.get("files") or []),
        "reviewed_categories": list(REQUIRED_CATEGORIES),
        "lenses": receipts,
    }
    return validate_review_bundle(bundle, scope, diff)


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


def result_schema(categories: Sequence[str] = REQUIRED_CATEGORIES) -> dict[str, Any]:
    """Return the constrained-output schema used by Claude Code."""
    text = {"type": "string", "minLength": 1}
    return {
        "type": "object",
        "properties": {
            "head_sha": {"type": "string", "pattern": "^[0-9a-f]{40}$"},
            "summary": {"type": "string", "minLength": 80},
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
                        "assessment": {"type": "string", "minLength": 30},
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
        f"schema={REVIEW_BUNDLE_VERSION} head={head_sha} "
        f"findings={finding_count} digest={digest} -->"
    )


def _markdown_code(value: str) -> str:
    return value.replace("`", "\\`")


_PUBLISHED_BODY_LIMIT = 60000


def _artifact_reference(run_url: str | None, artifact_name: str | None) -> str:
    """Render controlled artifact metadata without letting it consume the receipt."""
    linkable = (
        isinstance(run_url, str)
        and run_url.startswith("https://")
        and len(run_url) <= 2048
        and not any(character.isspace() for character in run_url)
        and isinstance(artifact_name, str)
        and 0 < len(artifact_name) <= 256
        and "\n" not in artifact_name
        and "\r" not in artifact_name
    )
    if linkable:
        return (
            f"**Review bundle:** [workflow artifact]({run_url}#artifacts) "
            f"(`{_markdown_code(artifact_name)}`; "
            f"{FINAL_ARTIFACT_RETENTION_DAYS}-day operational retention)."
        )
    return (
        "**Review bundle:** artifact link unavailable in this rendering; "
        "the receipt remains bound to the bundle digest."
    )


def render_evidence(
    bundle: Mapping[str, Any],
    digest: str,
    run_url: str | None = None,
    artifact_name: str | None = None,
) -> str:
    """Render a compact receipt; complete lens narration stays in the bundle.

    Publication capacity is deliberately not part of the review oracle. The
    model-authored summaries and context can grow without growing this body,
    and untrusted artifact metadata falls back to a fixed-size receipt rather
    than converting completed analysis into an incomplete review.
    """
    receipts = _bundle_receipts(bundle)
    if not isinstance(digest, str) or not _DIGEST_RE.fullmatch(digest):
        raise GateError("review bundle digest must be a lowercase SHA-256 value")
    if artifact_digest(bundle) != digest:
        raise GateError("published review digest does not match the review bundle")

    files = _expect_list(bundle.get("reviewed_files"), "review bundle reviewed_files")
    categories = _expect_list(
        bundle.get("reviewed_categories"),
        "review bundle reviewed_categories",
    )
    findings = review_bundle_findings(bundle)
    head_sha = str(bundle["head_sha"])
    finding_count = len(findings)
    blocking_count = sum(1 for finding in findings if finding.get("severity") == "blocking")
    outcome = (
        "no findings"
        if finding_count == 0
        else (
            f"{finding_count} finding(s) — "
            f"{blocking_count} blocking, {finding_count - blocking_count} advisory"
        )
    )
    marker = evidence_marker(head_sha, finding_count, digest)

    lens_rows: list[str] = []
    for receipt in receipts:
        lens_result = _expect_mapping(receipt["result"], "review bundle lens result")
        lens_findings = _deduplicate_findings([lens_result])
        lens_blocking = sum(1 for finding in lens_findings if finding.get("severity") == "blocking")
        lens_rows.append(
            f"| `{receipt['lens']}` | `{receipt['backend']}` | {receipt['status']} | "
            f"{len(lens_findings)} ({lens_blocking} blocking, "
            f"{len(lens_findings) - lens_blocking} advisory) | "
            f"`sha256:{receipt['artifact_digest']}` |"
        )

    rendered = "\n".join(
        [
            f"## Footgun review — {outcome}",
            "",
            f"**Exact head:** `{head_sha}`  ",
            f"**Validated scope:** {len(files)} changed file(s), "
            f"{len(categories)} detector categories, {len(receipts)}/{len(LENSES)} lenses  ",
            f"**Bundle digest:** `sha256:{digest}`",
            "",
            "| Lens | Backend | Status | Findings | Lens evidence digest |",
            "|---|---|---:|---:|---|",
            *lens_rows,
            "",
            "Complete per-lens summaries, reviewed context, and structured findings "
            "are retained in the digest-bound bundle; they are not duplicated in this receipt.",
            "",
            _artifact_reference(run_url, artifact_name),
            "",
            marker,
        ]
    )
    if len(rendered.encode("utf-8")) <= _PUBLISHED_BODY_LIMIT:
        return rendered

    # All remaining fields are fixed-size validated values. This rung cannot
    # be inflated by model prose, file names, artifact names, or URLs.
    return "\n".join(
        [
            f"## Footgun review — {outcome}",
            "",
            f"**Exact head:** `{head_sha}`  ",
            f"**Validated lenses:** {len(receipts)}/{len(LENSES)}  ",
            f"**Findings:** {finding_count} total; {blocking_count} blocking  ",
            f"**Bundle digest:** `sha256:{digest}`",
            "",
            "The detailed receipt exceeded the publication budget; complete evidence "
            "remains in the workflow's digest-bound review bundle.",
            "",
            marker,
        ]
    )


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
    bundle: Mapping[str, Any],
    digest: str,
    run_url: str | None = None,
    artifact_name: str | None = None,
) -> dict[str, Any]:
    head_sha = str(bundle["head_sha"])
    findings = review_bundle_findings(bundle)
    if not findings:
        raise GateError("a review payload requires at least one finding")
    return {
        "commit_id": head_sha,
        "event": "COMMENT",
        "body": render_evidence(bundle, digest, run_url, artifact_name),
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


def _validated_review_bundle(args: argparse.Namespace) -> dict[str, Any]:
    scope = _expect_mapping(_load_json(args.scope), "scope")
    raw_bundle = _expect_mapping(_load_json(args.result), "review bundle")
    diff = args.diff.read_text(encoding="utf-8")
    return validate_review_bundle(raw_bundle, scope, diff)


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
    bundle = _validated_review_bundle(args)
    digest = artifact_digest(bundle)
    findings = review_bundle_findings(bundle)
    finding_count = len(findings)
    blocking_finding_count = sum(1 for finding in findings if finding["severity"] == "blocking")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    _write_json(args.output_dir / "review-bundle.json", bundle)
    (args.output_dir / "evidence.md").write_text(
        render_evidence(bundle, digest, args.run_url, args.artifact_name) + "\n",
        encoding="utf-8",
    )
    if finding_count:
        _write_json(
            args.output_dir / "review.json",
            review_payload(bundle, digest, args.run_url, args.artifact_name),
        )

    _append_github_outputs(
        args.github_output,
        {
            "head_sha": str(bundle["head_sha"]),
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
        "merge", help="package validated per-lens results into one full-coverage bundle"
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

    prepare = subparsers.add_parser(
        "prepare", help="validate a review bundle and render its receipt"
    )
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
