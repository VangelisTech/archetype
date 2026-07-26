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

"""Deterministic orchestration, validation, and publication for review CI.

Prompt policy and model return types live in :mod:`review_contracts`.
Evidence-conserving fan-out aggregation lives in :mod:`review_aggregation`.
This module owns exact PR scope, the command-line workflow, and GitHub-facing
rendering and verification.
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import subprocess
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

from review_aggregation import (
    assemble_preliminary_bundle,
    blocking_finding_count,
    design_bundle_findings,
    finalize_review_bundle,
    human_decision_count,
    make_adjudication_receipt,
    make_reviewer_receipt,
    review_bundle_findings,
)
from review_aggregation import (
    validate_review_bundle as validate_aggregate_bundle,
)
from review_contracts import (
    DESIGN_BRIEF_VERSION,
    DESIGN_CATEGORIES,
    REQUIRED_CATEGORIES,
    REVIEW_BUNDLE_KIND,
    REVIEW_BUNDLE_VERSION,
    ReviewError,
    adjudication_result_schema,
    artifact_digest,
    human_design_brief_schema,
    lens_categories,
    lens_result_schema,
    normalize_adjudication_result,
    normalize_human_design_brief,
    normalize_lens_result,
    render_adjudication_prompt,
    render_design_brief_prompt,
    render_lens_retry_prompt,
    render_lens_review_prompt,
    review_matrix,
)

SCHEMA_VERSION = 2
BOT_LOGIN = "github-actions[bot]"
FINAL_ARTIFACT_RETENTION_DAYS = 90
GateError = ReviewError

_HUNK_RE = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@(?: .*)?$")
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_PUBLISHED_BODY_LIMIT = 60000


def _run_git(*args: str) -> bytes:
    completed = subprocess.run(("git", *args), check=True, capture_output=True)
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
        "footgun_categories": list(REQUIRED_CATEGORIES),
        "design_categories": list(DESIGN_CATEGORIES),
    }


def build_scope(base_sha: str, head_sha: str) -> tuple[dict[str, Any], str]:
    """Return the exact file manifest and rename-aware unified diff."""
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
    """Validate an API-fetched PR snapshot against the event identity."""

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
    """Return GitHub-commentable changed lines as ``(path, side, line)``."""
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


def _scope_values(scope: Mapping[str, Any]) -> tuple[str, list[str]]:
    if scope.get("schema_version") != SCHEMA_VERSION:
        raise GateError(f"scope.schema_version must be {SCHEMA_VERSION}")
    head_sha = scope.get("head_sha")
    if not isinstance(head_sha, str) or not _SHA_RE.fullmatch(head_sha):
        raise GateError("scope.head_sha must be a full lowercase Git SHA")
    files = _expect_list(scope.get("files"), "scope.files")
    if not files or any(not isinstance(item, str) for item in files):
        raise GateError("scope.files must contain changed paths")
    if len(files) != len(set(files)):
        raise GateError("scope.files must not contain duplicates")
    if files != sorted(files):
        raise GateError("scope.files must be in canonical sorted order")
    if scope.get("footgun_categories") != list(REQUIRED_CATEGORIES):
        raise GateError("scope footgun categories do not match trusted policy")
    if scope.get("design_categories") != list(DESIGN_CATEGORIES):
        raise GateError("scope design categories do not match trusted policy")
    return head_sha, files


def validate_result(
    raw_result: Mapping[str, Any],
    scope: Mapping[str, Any],
    diff: str,
    *,
    lens: str,
) -> dict[str, Any]:
    """Validate one model-authored lens result against trusted scope."""
    head_sha, files = _scope_values(scope)
    return dict(
        normalize_lens_result(
            raw_result,
            lens=lens,
            head_sha=head_sha,
            scoped_files=files,
            anchors=changed_line_anchors(diff),
        )
    )


def validate_review_bundle(
    raw_bundle: Mapping[str, Any],
    scope: Mapping[str, Any],
    diff: str,
    *,
    expected_phase: str | None = None,
) -> dict[str, Any]:
    head_sha, files = _scope_values(scope)
    return dict(
        validate_aggregate_bundle(
            raw_bundle,
            head_sha=head_sha,
            scoped_files=files,
            anchors=changed_line_anchors(diff),
            expected_phase=expected_phase,
        )
    )


def result_schema(lens: str) -> dict[str, Any]:
    return lens_result_schema(lens)


def extract_structured_json(raw: str) -> dict[str, Any] | None:
    """Extract the last standalone JSON object, preferring one bound to a head."""
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
    return (
        "The first review response did not pass Archetype's exact-scope validator:\n\n"
        f"{error}\n\n"
        "Return one corrected, complete structured result for the same exact head. "
        "Use only the model-authored fields in the supplied schema. Cover every "
        "changed path in review_context, anchor findings to changed lines, and "
        "preserve substantive analysis that remains valid.\n"
    )


def evidence_marker(head_sha: str, finding_count: int, digest: str) -> str:
    return (
        "<!-- archetype-footgun-review "
        f"schema={REVIEW_BUNDLE_VERSION} head={head_sha} "
        f"findings={finding_count} digest={digest} -->"
    )


def design_review_marker(head_sha: str, bundle_digest: str, brief_digest: str) -> str:
    return (
        "<!-- archetype-human-design-review "
        f"schema={DESIGN_BRIEF_VERSION} head={head_sha} "
        f"bundle={bundle_digest} brief={brief_digest} -->"
    )


def _markdown_code(value: str) -> str:
    return value.replace("`", "\\`")


def _artifact_reference(run_url: str | None, artifact_name: str | None) -> str:
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
    """Render a compact aggregate receipt; complete evidence stays in the bundle."""
    if bundle.get("kind") != REVIEW_BUNDLE_KIND or bundle.get("phase") != "final":
        raise GateError("published evidence requires a finalized review bundle")
    if not _DIGEST_RE.fullmatch(digest) or artifact_digest(bundle) != digest:
        raise GateError("published review digest does not match the review bundle")

    findings = review_bundle_findings(bundle)
    blocking = blocking_finding_count(bundle)
    human_decisions = human_decision_count(bundle)
    advisory = len(findings) - blocking - human_decisions
    design_notes = len(design_bundle_findings(bundle))
    head_sha = str(bundle["head_sha"])
    outcome = (
        "no footgun findings"
        if not findings
        else (
            f"{len(findings)} footgun finding(s) — {blocking} blocking, "
            f"{advisory} advisory, {human_decisions} human decision"
        )
    )

    lens_rows: list[str] = []
    for raw_group in _expect_list(bundle.get("lenses"), "review bundle lenses"):
        group = _expect_mapping(raw_group, "review bundle lens")
        reviewers = _expect_list(group.get("reviewers"), "review bundle reviewers")
        reviewer_names = ", ".join(
            f"`{_expect_mapping(item, 'reviewer').get('reviewer_id')}`" for item in reviewers
        )
        member_count = sum(
            len(
                _expect_list(
                    _expect_mapping(item, "reviewer").get("result", {}).get("findings"),
                    "reviewer findings",
                )
            )
            for item in reviewers
        )
        lens_rows.append(
            f"| `{group['lens']}` | {reviewer_names} | {len(reviewers)}/2 | {member_count} |"
        )

    marker = evidence_marker(head_sha, len(findings), digest)
    rendered = "\n".join(
        [
            f"## Footgun review — {outcome}",
            "",
            f"**Exact head:** `{head_sha}`  ",
            f"**Fan-out:** {len(bundle['lenses'])} lenses × 2 independent reviewers  ",
            f"**Design-coherence notes:** {design_notes} (advisory-only)  ",
            f"**Bundle digest:** `sha256:{digest}`",
            "",
            "| Lens | Reviewers | Complete | Original findings |",
            "|---|---|---:|---:|",
            *lens_rows,
            "",
            "Every original result, prompt digest, finding, cluster member, and adjudication "
            "is retained in the digest-bound bundle.",
            "",
            _artifact_reference(run_url, artifact_name),
            "",
            marker,
        ]
    )
    if len(rendered.encode("utf-8")) <= _PUBLISHED_BODY_LIMIT:
        return rendered
    return "\n".join(
        [
            f"## Footgun review — {outcome}",
            "",
            f"**Exact head:** `{head_sha}`  ",
            f"**Bundle digest:** `sha256:{digest}`",
            "",
            "The detailed receipt exceeded the publication budget; complete evidence "
            "remains in the digest-bound workflow artifact.",
            "",
            marker,
        ]
    )


def render_finding(finding: Mapping[str, Any], head_sha: str) -> str:
    disposition = str(finding["gate_disposition"])
    actions = {
        "blocking": "fix before merge",
        "advisory": "fix, or resolve this thread with a written disposition",
        "human-decision": "resolve with the human decision and its rationale",
    }
    return "\n".join(
        [
            f"### {finding['category']}: {finding['title']}",
            "",
            f"**Gate disposition:** {disposition} — {actions[disposition]}",
            f"**Corroboration:** {finding['corroboration']} ({', '.join(finding['reviewer_ids'])})",
            f"**Adjudication:** {finding['adjudication_status']}",
            "",
            f"**What it does:** {finding['what_it_does']}",
            "",
            f"**What goes wrong:** {finding['what_goes_wrong']}",
            "",
            f"**Failing input or sequence:** {finding['failing_input_or_sequence']}",
            "",
            "**Fix:**",
            str(finding["fix"]),
            "",
            f"<!-- archetype-footgun-finding head={head_sha} cluster={finding['cluster_id']} -->",
        ]
    )


def review_payload(
    bundle: Mapping[str, Any],
    digest: str,
    run_url: str | None = None,
    artifact_name: str | None = None,
) -> dict[str, Any]:
    findings = review_bundle_findings(bundle)
    if not findings:
        raise GateError("a review payload requires at least one footgun finding")
    head_sha = str(bundle["head_sha"])
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


def _render_design_note(finding: Mapping[str, Any]) -> list[str]:
    evidence = "; ".join(
        f"`{item['path']}::{item['symbol']}` — {item['explanation']}"
        for item in finding["repository_evidence"]
    )
    return [
        f"### {finding['category']}: {finding['title']}",
        "",
        f"Changed anchor: `{finding['path']}:{finding['line']}`  ",
        f"Confidence: {finding['corroboration']} ({', '.join(finding['reviewer_ids'])})",
        "",
        f"- Introduced shape: {finding['introduced_shape']}",
        f"- Concrete cost: {finding['concrete_cost']}",
        f"- Behavior-preserving alternative: {finding['simpler_alternative']}",
        f"- Repository evidence: {evidence}",
        "",
    ]


def render_human_design_brief(
    brief: Mapping[str, Any],
    bundle: Mapping[str, Any],
    *,
    bundle_digest: str,
    brief_digest: str,
    run_url: str | None = None,
    artifact_name: str | None = None,
) -> str:
    """Render the model-organized brief plus deterministic review evidence."""
    head_sha = str(bundle["head_sha"])
    if brief.get("head_sha") != head_sha or brief.get("bundle_digest") != bundle_digest:
        raise GateError("human design brief is not bound to the finalized review bundle")
    if artifact_digest(brief) != brief_digest:
        raise GateError("human design brief digest does not match its content")

    lines = [
        "## Human design review — ready for human review",
        "",
        f"**Exact head:** `{head_sha}`  ",
        f"**Decision requested:** {brief['decision_requested']}  ",
        f"**Review bundle:** `sha256:{bundle_digest}`  ",
        f"**Brief:** `sha256:{brief_digest}`",
        "",
        "### Intent and behavioral delta",
        "",
        str(brief["intent_and_behavioral_delta"]),
        "",
        "### Suggested reading order",
        "",
    ]
    for cohort in brief["change_cohorts"]:
        files = ", ".join(f"`{path}`" for path in cohort["files"])
        lines.append(
            f"{cohort['reading_order']}. **{cohort['name']}** — {cohort['summary']} ({files})"
        )

    lines.extend(["", "### Design choices", ""])
    if not brief["design_choices"]:
        lines.append("No distinct design choice was asserted by the generator.")
    for choice in brief["design_choices"]:
        alternatives = "; ".join(choice["alternatives"]) or "none recorded"
        tradeoffs = "; ".join(choice["tradeoffs"]) or "none recorded"
        lines.extend(
            [
                f"- **{choice['choice']}** — {choice['rationale']}",
                f"  - Alternatives: {alternatives}",
                f"  - Tradeoffs: {tradeoffs}",
            ]
        )

    lines.extend(["", "### Affected invariants", ""])
    if not brief["affected_invariants"]:
        lines.append("No affected invariant was identified.")
    for invariant in brief["affected_invariants"]:
        lines.append(
            f"- **{invariant['invariant']}** — {invariant['impact']} "
            f"(evidence: {invariant['evidence']})"
        )

    lines.extend(["", "### Design-coherence notes (advisory-only)", ""])
    notes = design_bundle_findings(bundle)
    if not notes:
        lines.append("No design-coherence note survived exact-anchor aggregation.")
    for finding in notes:
        lines.extend(_render_design_note(finding))

    lines.extend(["", "### Human decision queue", ""])
    if not brief["human_decision_queue"]:
        lines.append("No explicit human decision is queued.")
    for decision in brief["human_decision_queue"]:
        clusters = ", ".join(f"`{item}`" for item in decision["related_cluster_ids"])
        options = "; ".join(decision["options"])
        lines.extend(
            [
                f"- **{decision['question']}**",
                f"  - Why now: {decision['why_now']}",
                f"  - Options: {options}",
                f"  - Review clusters: {clusters or 'none'}",
            ]
        )

    lines.extend(["", "### Validation", ""])
    for item in brief["validation"]:
        lines.append(f"- **{item['status']}** — {item['check']}: {item['evidence']}")

    lines.extend(["", "### Open questions", ""])
    if not brief["open_questions"]:
        lines.append("No additional open question was recorded.")
    else:
        lines.extend(f"- {question}" for question in brief["open_questions"])
    lines.extend(
        [
            "",
            _artifact_reference(run_url, artifact_name),
            "",
            design_review_marker(head_sha, bundle_digest, brief_digest),
        ]
    )
    rendered = "\n".join(lines)
    if len(rendered.encode("utf-8")) <= _PUBLISHED_BODY_LIMIT:
        return rendered
    return "\n".join(
        [
            "## Human design review — ready for human review",
            "",
            f"**Exact head:** `{head_sha}`  ",
            f"**Decision requested:** {brief['decision_requested']}  ",
            f"**Review bundle:** `sha256:{bundle_digest}`  ",
            f"**Brief:** `sha256:{brief_digest}`",
            "",
            "The full brief exceeded the comment budget and remains in the "
            "digest-bound workflow artifact.",
            "",
            _artifact_reference(run_url, artifact_name),
            "",
            design_review_marker(head_sha, bundle_digest, brief_digest),
        ]
    )


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
    brief_digest: str,
) -> None:
    """Verify both exact footgun evidence and the human design-review surface."""
    footgun_marker = evidence_marker(head_sha, finding_count, digest)
    design_marker = design_review_marker(head_sha, digest, brief_digest)
    issues = _flatten_pages(issue_comments, "issue_comments")
    pull_reviews = _flatten_pages(reviews, "reviews")
    inline_comments = _flatten_pages(review_comments, "review_comments")

    if not any(
        _is_bot_authored(item) and design_marker in str(item.get("body") or "") for item in issues
    ):
        raise GateError("the exact human design-review brief was not posted by GitHub Actions")

    if finding_count == 0:
        if not any(
            _is_bot_authored(item) and footgun_marker in str(item.get("body") or "")
            for item in issues
        ):
            raise GateError(
                "the exact no-findings footgun evidence was not posted by GitHub Actions"
            )
        return

    matching_reviews = [
        item
        for item in pull_reviews
        if _is_bot_authored(item)
        and item.get("commit_id") == head_sha
        and footgun_marker in str(item.get("body") or "")
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
    path.parent.mkdir(parents=True, exist_ok=True)
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


def _normalized_lens_result(args: argparse.Namespace) -> dict[str, Any]:
    scope = _expect_mapping(_load_json(args.scope), "scope")
    raw_result = _expect_mapping(_load_json(args.result), "result")
    return validate_result(
        raw_result,
        scope,
        args.diff.read_text(encoding="utf-8"),
        lens=args.lens,
    )


def _reviewer_receipt(args: argparse.Namespace) -> dict[str, Any]:
    result = _normalized_lens_result(args)
    prompt = args.prompt.read_text(encoding="utf-8")
    return dict(
        make_reviewer_receipt(
            result,
            lens=args.lens,
            reviewer_id=args.reviewer,
            prompt=prompt,
        )
    )


def _normalize_command(args: argparse.Namespace) -> None:
    _write_json(args.output, _reviewer_receipt(args))


def _attempt_command(args: argparse.Namespace) -> None:
    try:
        receipt = _reviewer_receipt(args)
    except GateError as error:
        args.output.unlink(missing_ok=True)
        args.feedback.parent.mkdir(parents=True, exist_ok=True)
        args.feedback.write_text(retry_feedback(error), encoding="utf-8")
        _append_github_outputs(args.github_output, {"valid": "false"})
        return
    _write_json(args.output, receipt)
    args.feedback.unlink(missing_ok=True)
    _append_github_outputs(args.github_output, {"valid": "true"})


def _receipt_files(directory: Path, kind: str) -> list[Mapping[str, Any]]:
    if not directory.is_dir():
        return []
    receipts: list[Mapping[str, Any]] = []
    for path in sorted(directory.rglob("*.json")):
        value = _expect_mapping(_load_json(path), f"receipt {path}")
        if value.get("kind") == kind:
            receipts.append(value)
    return receipts


def _required_decision_clusters(bundle: Mapping[str, Any]) -> set[str]:
    required: set[str] = set()
    for raw_cluster in _expect_list(bundle.get("clusters"), "review bundle clusters"):
        cluster = _expect_mapping(raw_cluster, "review bundle cluster")
        if cluster.get("gate_disposition") == "human-decision":
            cluster_id = cluster.get("cluster_id")
            if not isinstance(cluster_id, str):
                raise GateError("human-decision cluster_id must be a string")
            required.add(cluster_id)
    return required


def _merge_command(args: argparse.Namespace) -> None:
    scope = _expect_mapping(_load_json(args.scope), "scope")
    head_sha, files = _scope_values(scope)
    receipts = _receipt_files(args.results_dir, "archetype-reviewer-receipt")
    bundle = assemble_preliminary_bundle(
        receipts,
        head_sha=head_sha,
        scoped_files=files,
        anchors=changed_line_anchors(args.diff.read_text(encoding="utf-8")),
    )
    _write_json(args.output, bundle)


def _adjudication_matrix_command(args: argparse.Namespace) -> None:
    scope = _expect_mapping(_load_json(args.scope), "scope")
    bundle = validate_review_bundle(
        _expect_mapping(_load_json(args.bundle), "review bundle"),
        scope,
        args.diff.read_text(encoding="utf-8"),
        expected_phase="preliminary",
    )
    targets = bundle["adjudication_targets"]
    include = (
        [{"cluster_id": cluster_id, "skip": False} for cluster_id in targets]
        if targets
        else [{"cluster_id": "none", "skip": True}]
    )
    _append_github_outputs(
        args.github_output,
        {
            "matrix": json.dumps({"include": include}, separators=(",", ":")),
            "target_count": len(targets),
        },
    )


def _normalize_adjudication_command(args: argparse.Namespace) -> None:
    raw = _expect_mapping(_load_json(args.result), "adjudication result")
    normalized = normalize_adjudication_result(
        raw,
        head_sha=args.head,
        cluster_id=args.cluster,
    )
    receipt = make_adjudication_receipt(
        normalized,
        reviewer_id=args.reviewer,
        prompt=args.prompt.read_text(encoding="utf-8"),
    )
    _write_json(args.output, receipt)


def _finalize_command(args: argparse.Namespace) -> None:
    scope = _expect_mapping(_load_json(args.scope), "scope")
    head_sha, files = _scope_values(scope)
    preliminary = _expect_mapping(_load_json(args.bundle), "preliminary review bundle")
    adjudications = _receipt_files(
        args.adjudications_dir,
        "archetype-review-adjudication-receipt",
    )
    final = finalize_review_bundle(
        preliminary,
        adjudications,
        head_sha=head_sha,
        scoped_files=files,
        anchors=changed_line_anchors(args.diff.read_text(encoding="utf-8")),
    )
    _write_json(args.output, final)


def _normalize_brief_command(args: argparse.Namespace) -> None:
    scope = _expect_mapping(_load_json(args.scope), "scope")
    _, files = _scope_values(scope)
    bundle = validate_review_bundle(
        _expect_mapping(_load_json(args.bundle), "review bundle"),
        scope,
        args.diff.read_text(encoding="utf-8"),
        expected_phase="final",
    )
    digest = artifact_digest(bundle)
    clusters = {
        str(item["cluster_id"])
        for item in _expect_list(bundle.get("clusters"), "review bundle clusters")
    }
    normalized = normalize_human_design_brief(
        _expect_mapping(_load_json(args.result), "human design brief"),
        head_sha=str(bundle["head_sha"]),
        bundle_digest=digest,
        scoped_files=files,
        cluster_ids=clusters,
        required_decision_cluster_ids=_required_decision_clusters(bundle),
    )
    _write_json(args.output, normalized)


def _prepare_command(args: argparse.Namespace) -> None:
    scope = _expect_mapping(_load_json(args.scope), "scope")
    bundle = validate_review_bundle(
        _expect_mapping(_load_json(args.bundle), "review bundle"),
        scope,
        args.diff.read_text(encoding="utf-8"),
        expected_phase="final",
    )
    bundle_digest = artifact_digest(bundle)
    brief = _expect_mapping(_load_json(args.brief), "human design brief")
    _, files = _scope_values(scope)
    cluster_ids = {str(item["cluster_id"]) for item in bundle["clusters"]}
    normalized_brief = normalize_human_design_brief(
        {key: value for key, value in brief.items() if key not in {"schema_version", "kind"}},
        head_sha=str(bundle["head_sha"]),
        bundle_digest=bundle_digest,
        scoped_files=files,
        cluster_ids=cluster_ids,
        required_decision_cluster_ids=_required_decision_clusters(bundle),
    )
    if normalized_brief != brief:
        raise GateError("human design brief is not canonically normalized")
    brief_digest = artifact_digest(normalized_brief)
    findings = review_bundle_findings(bundle)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    _write_json(args.output_dir / "review-bundle.json", bundle)
    _write_json(args.output_dir / "human-design-brief.json", normalized_brief)
    (args.output_dir / "evidence.md").write_text(
        render_evidence(bundle, bundle_digest, args.run_url, args.artifact_name) + "\n",
        encoding="utf-8",
    )
    (args.output_dir / "design-review.md").write_text(
        render_human_design_brief(
            normalized_brief,
            bundle,
            bundle_digest=bundle_digest,
            brief_digest=brief_digest,
            run_url=args.run_url,
            artifact_name=args.artifact_name,
        )
        + "\n",
        encoding="utf-8",
    )
    if findings:
        _write_json(
            args.output_dir / "review.json",
            review_payload(bundle, bundle_digest, args.run_url, args.artifact_name),
        )
    _append_github_outputs(
        args.github_output,
        {
            "head_sha": str(bundle["head_sha"]),
            "finding_count": len(findings),
            "blocking_finding_count": blocking_finding_count(bundle),
            "human_decision_count": human_decision_count(bundle),
            "artifact_digest": bundle_digest,
            "design_brief_digest": brief_digest,
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
        brief_digest=args.brief_digest,
    )


def _extract_command(args: argparse.Namespace) -> None:
    raw = args.raw.read_text(encoding="utf-8")
    extracted = extract_structured_json(raw)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if extracted is None:
        args.output.write_text(raw, encoding="utf-8")
    else:
        _write_json(args.output, extracted)


def _prompt_command(args: argparse.Namespace) -> None:
    if args.kind == "lens-review":
        prompt = render_lens_review_prompt(
            pr_number=args.pr_number,
            head_sha=args.head,
            lens=args.lens,
            reviewer_id=args.reviewer,
        )
    elif args.kind == "lens-retry":
        prompt = render_lens_retry_prompt(
            pr_number=args.pr_number,
            head_sha=args.head,
            lens=args.lens,
            reviewer_id=args.reviewer,
        )
    elif args.kind == "adjudication":
        prompt = render_adjudication_prompt(
            pr_number=args.pr_number,
            head_sha=args.head,
            cluster_id=args.cluster,
        )
    else:
        prompt = render_design_brief_prompt(
            pr_number=args.pr_number,
            head_sha=args.head,
            bundle_digest=args.bundle_digest,
        )
    if args.output is None:
        print(prompt, end="")
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(prompt, encoding="utf-8")


def _schema_command(args: argparse.Namespace) -> None:
    if args.kind == "lens":
        schema = lens_result_schema(args.lens)
    elif args.kind == "adjudication":
        schema = adjudication_result_schema()
    else:
        schema = human_design_brief_schema()
    print(json.dumps(schema, separators=(",", ":")))


def _review_matrix_command(_args: argparse.Namespace) -> None:
    print(json.dumps({"include": review_matrix()}, separators=(",", ":")))


def _digest_command(args: argparse.Namespace) -> None:
    print(artifact_digest(_expect_mapping(_load_json(args.input), "digest input")))


def _lens_categories_command(args: argparse.Namespace) -> None:
    print(", ".join(lens_categories(args.lens)))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    scope = subparsers.add_parser("scope", help="materialize exact PR scope")
    scope.add_argument("--base", required=True)
    scope.add_argument("--head", required=True)
    scope.add_argument("--scope", type=Path, required=True)
    scope.add_argument("--diff", type=Path, required=True)
    scope.set_defaults(handler=_scope_command)

    github_scope = subparsers.add_parser(
        "github-scope",
        help="validate API-fetched PR scope",
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

    matrix = subparsers.add_parser("review-matrix", help="print the configured 6×2 fan-out")
    matrix.set_defaults(handler=_review_matrix_command)

    digest = subparsers.add_parser("digest", help="digest one canonical JSON object")
    digest.add_argument("--input", type=Path, required=True)
    digest.set_defaults(handler=_digest_command)

    categories = subparsers.add_parser(
        "lens-categories",
        help="print one lens category set",
    )
    categories.add_argument("--lens", required=True)
    categories.set_defaults(handler=_lens_categories_command)

    schema = subparsers.add_parser("schema", help="print a model return JSON schema")
    schema.add_argument(
        "--kind",
        choices=("lens", "adjudication", "design-brief"),
        default="lens",
    )
    schema.add_argument("--lens")
    schema.set_defaults(handler=_schema_command)

    prompt = subparsers.add_parser("prompt", help="render one reviewed prompt template")
    prompt.add_argument(
        "--kind",
        choices=("lens-review", "lens-retry", "adjudication", "design-brief"),
        required=True,
    )
    prompt.add_argument("--pr-number", type=int, required=True)
    prompt.add_argument("--head", required=True)
    prompt.add_argument("--lens")
    prompt.add_argument("--reviewer")
    prompt.add_argument("--cluster")
    prompt.add_argument("--bundle-digest")
    prompt.add_argument("--output", type=Path)
    prompt.set_defaults(handler=_prompt_command)

    extract = subparsers.add_parser("extract", help="extract JSON from raw model output")
    extract.add_argument("--raw", type=Path, required=True)
    extract.add_argument("--output", type=Path, required=True)
    extract.set_defaults(handler=_extract_command)

    for name, handler, help_text in (
        ("normalize", _normalize_command, "validate one lens result into a receipt"),
        ("attempt", _attempt_command, "attempt lens validation with retry feedback"),
    ):
        command = subparsers.add_parser(name, help=help_text)
        command.add_argument("--scope", type=Path, required=True)
        command.add_argument("--diff", type=Path, required=True)
        command.add_argument("--result", type=Path, required=True)
        command.add_argument("--prompt", type=Path, required=True)
        command.add_argument("--output", type=Path, required=True)
        command.add_argument("--lens", required=True)
        command.add_argument("--reviewer", required=True)
        if name == "attempt":
            command.add_argument("--feedback", type=Path, required=True)
            command.add_argument("--github-output", type=Path, required=True)
        command.set_defaults(handler=handler)

    merge = subparsers.add_parser("merge", help="build the preliminary aggregate")
    merge.add_argument("--scope", type=Path, required=True)
    merge.add_argument("--diff", type=Path, required=True)
    merge.add_argument("--results-dir", type=Path, required=True)
    merge.add_argument("--output", type=Path, required=True)
    merge.set_defaults(handler=_merge_command)

    adjudication_matrix = subparsers.add_parser(
        "adjudication-matrix",
        help="emit selective adjudication targets",
    )
    adjudication_matrix.add_argument("--scope", type=Path, required=True)
    adjudication_matrix.add_argument("--diff", type=Path, required=True)
    adjudication_matrix.add_argument("--bundle", type=Path, required=True)
    adjudication_matrix.add_argument("--github-output", type=Path, required=True)
    adjudication_matrix.set_defaults(handler=_adjudication_matrix_command)

    normalize_adjudication = subparsers.add_parser(
        "normalize-adjudication",
        help="validate one adjudication into a receipt",
    )
    normalize_adjudication.add_argument("--head", required=True)
    normalize_adjudication.add_argument("--cluster", required=True)
    normalize_adjudication.add_argument("--reviewer", required=True)
    normalize_adjudication.add_argument("--result", type=Path, required=True)
    normalize_adjudication.add_argument("--prompt", type=Path, required=True)
    normalize_adjudication.add_argument("--output", type=Path, required=True)
    normalize_adjudication.set_defaults(handler=_normalize_adjudication_command)

    finalize = subparsers.add_parser("finalize", help="apply adjudications")
    finalize.add_argument("--scope", type=Path, required=True)
    finalize.add_argument("--diff", type=Path, required=True)
    finalize.add_argument("--bundle", type=Path, required=True)
    finalize.add_argument("--adjudications-dir", type=Path, required=True)
    finalize.add_argument("--output", type=Path, required=True)
    finalize.set_defaults(handler=_finalize_command)

    normalize_brief = subparsers.add_parser(
        "normalize-brief",
        help="validate the human design brief",
    )
    normalize_brief.add_argument("--scope", type=Path, required=True)
    normalize_brief.add_argument("--diff", type=Path, required=True)
    normalize_brief.add_argument("--bundle", type=Path, required=True)
    normalize_brief.add_argument("--result", type=Path, required=True)
    normalize_brief.add_argument("--output", type=Path, required=True)
    normalize_brief.set_defaults(handler=_normalize_brief_command)

    prepare = subparsers.add_parser(
        "prepare",
        help="render final review and design evidence",
    )
    prepare.add_argument("--scope", type=Path, required=True)
    prepare.add_argument("--diff", type=Path, required=True)
    prepare.add_argument("--bundle", type=Path, required=True)
    prepare.add_argument("--brief", type=Path, required=True)
    prepare.add_argument("--output-dir", type=Path, required=True)
    prepare.add_argument("--github-output", type=Path, required=True)
    prepare.add_argument("--artifact-name")
    prepare.add_argument("--run-url")
    prepare.set_defaults(handler=_prepare_command)

    verify = subparsers.add_parser("verify", help="verify exact GitHub evidence")
    verify.add_argument("--issue-comments", type=Path, required=True)
    verify.add_argument("--reviews", type=Path, required=True)
    verify.add_argument("--review-comments", type=Path, required=True)
    verify.add_argument("--head", required=True)
    verify.add_argument("--finding-count", type=int, required=True)
    verify.add_argument("--digest", required=True)
    verify.add_argument("--brief-digest", required=True)
    verify.set_defaults(handler=_verify_command)
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        args.handler(args)
    except GateError as error:
        raise SystemExit(f"review gate failed: {error}") from error
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
