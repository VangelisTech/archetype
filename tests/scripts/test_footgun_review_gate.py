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

"""Contract tests for the deterministic footgun review gate."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pytest

_SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import footgun_review_gate as gate  # noqa: E402
from footgun_review_gate import (  # noqa: E402
    BOT_LOGIN,
    REQUIRED_CATEGORIES,
    GateError,
    artifact_digest,
    build_github_scope,
    changed_line_anchors,
    evidence_marker,
    render_evidence,
    review_payload,
    validate_result,
    verify_posted_evidence,
)

HEAD_SHA = "a" * 40
BASE_SHA = "b" * 40
DIFF = """\
diff --git a/old.py b/old.py
index 1111111..2222222 100644
--- a/old.py
+++ b/old.py
@@ -2,3 +2,3 @@
 keep
-unsafe_call()
+safe_call()
 tail
diff --git a/new.py b/new.py
new file mode 100644
index 0000000..3333333
--- /dev/null
+++ b/new.py
@@ -0,0 +1,2 @@
+first = 1
+second = 2
"""

RENAMED_DIFF = """\
diff --git a/old_name.py b/new_name.py
similarity index 80%
rename from old_name.py
rename to new_name.py
index 1111111..2222222 100644
--- a/old_name.py
+++ b/new_name.py
@@ -1 +1 @@
-unsafe_call()
+safe_call()
"""


def _scope() -> dict:
    return {
        "schema_version": 1,
        "base_sha": BASE_SHA,
        "head_sha": HEAD_SHA,
        "files": ["new.py", "old.py"],
        "categories": list(REQUIRED_CATEGORIES),
    }


def _result(*, findings: list[dict] | None = None) -> dict:
    return {
        "head_sha": HEAD_SHA,
        "summary": (
            "The change replaces an unsafe call in old.py and adds a two-line new.py module; "
            "the reviewed paths preserve the surrounding runtime contracts."
        ),
        "reviewed_files": ["old.py", "new.py"],
        "reviewed_categories": list(reversed(REQUIRED_CATEGORIES)),
        "review_context": [
            {
                "area": "Call safety",
                "files": ["old.py"],
                "assessment": "The replacement preserves the call site while removing the unsafe operation.",
            },
            {
                "area": "New module",
                "files": ["new.py"],
                "assessment": "The new assignments are isolated and do not bypass an existing lifecycle.",
            },
        ],
        "findings": findings or [],
    }


def _finding(**overrides) -> dict:
    finding = {
        "category": "fail-open-failure-paths",
        "title": "Validation silently fails open",
        "path": "old.py",
        "side": "RIGHT",
        "line": 3,
        "what_it_does": "The replacement catches a validation error and continues execution.",
        "what_goes_wrong": "Unauthorized rows can pass through when the validation dependency fails.",
        "fix": "Propagate the validation error or return an explicitly empty result.",
    }
    finding.update(overrides)
    return finding


def _bot_item(**values) -> dict:
    return {"user": {"login": BOT_LOGIN}, **values}


def _pr_metadata(**overrides) -> dict:
    metadata = {
        "number": 299,
        "state": "open",
        "changed_files": 2,
        "base": {
            "sha": BASE_SHA,
            "repo": {"full_name": "VangelisTech/archetype"},
        },
        "head": {"sha": HEAD_SHA},
    }
    metadata.update(overrides)
    return metadata


def test_github_scope_binds_api_snapshot_to_event_identity():
    scope = build_github_scope(
        repository="VangelisTech/archetype",
        pr_number=299,
        base_sha=BASE_SHA,
        head_sha=HEAD_SHA,
        before=_pr_metadata(),
        after=_pr_metadata(),
        file_pages=[[{"filename": "old.py"}, {"filename": "new.py"}]],
        diff=DIFF,
    )

    assert scope == _scope()


def test_github_scope_rejects_head_change_during_fetch():
    after = _pr_metadata()
    after["head"] = {"sha": "c" * 40}

    with pytest.raises(GateError, match="after base/head"):
        build_github_scope(
            repository="VangelisTech/archetype",
            pr_number=299,
            base_sha=BASE_SHA,
            head_sha=HEAD_SHA,
            before=_pr_metadata(),
            after=after,
            file_pages=[[{"filename": "old.py"}, {"filename": "new.py"}]],
            diff=DIFF,
        )


def test_github_scope_rejects_incomplete_file_manifest():
    with pytest.raises(GateError, match="changed-file count"):
        build_github_scope(
            repository="VangelisTech/archetype",
            pr_number=299,
            base_sha=BASE_SHA,
            head_sha=HEAD_SHA,
            before=_pr_metadata(),
            after=_pr_metadata(),
            file_pages=[[{"filename": "old.py"}]],
            diff=DIFF,
        )


def test_changed_line_anchors_include_only_added_and_removed_lines():
    assert changed_line_anchors(DIFF) == {
        ("old.py", "LEFT", 3),
        ("old.py", "RIGHT", 3),
        ("new.py", "RIGHT", 1),
        ("new.py", "RIGHT", 2),
    }


def test_renamed_file_anchors_use_githubs_new_path_on_both_sides():
    assert changed_line_anchors(RENAMED_DIFF) == {
        ("new_name.py", "LEFT", 1),
        ("new_name.py", "RIGHT", 1),
    }


def test_no_findings_result_requires_exact_file_category_and_context_coverage():
    normalized = validate_result(_result(), _scope(), DIFF)

    assert normalized["reviewed_files"] == ["new.py", "old.py"]
    assert normalized["reviewed_categories"] == sorted(REQUIRED_CATEGORIES)
    assert normalized["findings"] == []


def test_file_spanning_multiple_context_areas_is_accepted():
    result = _result()
    result["review_context"].append(
        {
            "area": "Cross-cutting notes",
            "files": ["old.py"],
            "assessment": "The same module also carries cross-cutting notes that span both reviewed areas.",
        }
    )

    normalized = validate_result(result, _scope(), DIFF)

    assert normalized["review_context"][2]["files"] == ["old.py"]


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda result: result.update(head_sha="c" * 40), "head_sha"),
        (lambda result: result["reviewed_files"].pop(), "reviewed_files"),
        (lambda result: result["reviewed_categories"].pop(), "reviewed_categories"),
        (lambda result: result["review_context"].pop(), "review_context"),
        (lambda result: result.update(summary="No notes."), "summary"),
    ],
)
def test_result_fails_closed_when_completion_evidence_is_incomplete(mutation, message):
    result = _result()
    mutation(result)

    with pytest.raises(GateError, match=message):
        validate_result(result, _scope(), DIFF)


def test_finding_must_anchor_to_a_changed_line():
    with pytest.raises(GateError, match="not anchored"):
        validate_result(_result(findings=[_finding(line=2)]), _scope(), DIFF)


def test_finding_can_anchor_to_the_removed_side():
    normalized = validate_result(
        _result(findings=[_finding(side="LEFT", line=3)]),
        _scope(),
        DIFF,
    )

    assert normalized["findings"][0]["side"] == "LEFT"


def test_rendered_no_findings_evidence_is_specific_and_digest_bound():
    normalized = validate_result(_result(), _scope(), DIFF)
    digest = artifact_digest(normalized)
    rendered = render_evidence(normalized, digest)

    assert "no findings" in rendered
    assert "2 changed file(s), 22 detector categories" in rendered
    assert "old.py" in rendered
    assert evidence_marker(HEAD_SHA, 0, digest) in rendered


def test_rendered_evidence_inlines_validated_artifact_and_run_link():
    normalized = validate_result(_result(), _scope(), DIFF)
    digest = artifact_digest(normalized)
    rendered = render_evidence(normalized, digest, run_url="https://example.test/runs/7")

    assert "<summary>Validated review artifact</summary>" in rendered
    start = rendered.index("```json\n") + len("```json\n")
    end = rendered.index("\n```\n", start)
    assert json.loads(rendered[start:end]) == normalized
    assert "[workflow run](https://example.test/runs/7)" in rendered


def test_inline_artifact_fence_outruns_backticks_in_findings():
    finding = _finding(fix="Replace the call:\n```python\nsafe_call()\n```\nand keep the guard.")
    normalized = validate_result(_result(findings=[finding]), _scope(), DIFF)
    digest = artifact_digest(normalized)
    rendered = render_evidence(normalized, digest)

    assert "````json\n" in rendered
    assert rendered.count("````") == 2


def test_full_published_body_budget_defers_duplicated_artifact_to_workflow_run():
    normalized = validate_result(_result(), _scope(), DIFF)
    normalized["summary"] = "s" * 20000
    normalized["review_context"][0]["assessment"] = "c" * 20000
    digest = artifact_digest(normalized)
    rendered = render_evidence(normalized, digest, run_url="https://example.test/runs/7")

    assert (
        len(json.dumps(normalized, ensure_ascii=False).encode("utf-8")) < gate._PUBLISHED_BODY_LIMIT
    )
    assert len(rendered.encode("utf-8")) <= gate._PUBLISHED_BODY_LIMIT
    assert "exceeds the inline comment budget" in rendered
    assert "```json" not in rendered
    assert "[workflow run](https://example.test/runs/7)" in rendered
    assert evidence_marker(HEAD_SHA, 0, digest) in rendered


def test_review_payload_batches_each_finding_as_an_inline_thread():
    normalized = validate_result(_result(findings=[_finding()]), _scope(), DIFF)
    digest = artifact_digest(normalized)
    payload = review_payload(normalized, digest)

    assert payload["commit_id"] == HEAD_SHA
    assert payload["event"] == "COMMENT"
    assert evidence_marker(HEAD_SHA, 1, digest) in payload["body"]
    assert payload["comments"] == [
        {
            "path": "old.py",
            "line": 3,
            "side": "RIGHT",
            "body": payload["comments"][0]["body"],
        }
    ]
    assert "**Fix:**" in payload["comments"][0]["body"]


def test_verify_no_findings_requires_exact_bot_authored_marker():
    digest = "d" * 64
    marker = evidence_marker(HEAD_SHA, 0, digest)

    verify_posted_evidence(
        issue_comments=[[_bot_item(body=f"review\n{marker}")]],
        reviews=[[]],
        review_comments=[[]],
        head_sha=HEAD_SHA,
        finding_count=0,
        digest=digest,
    )

    with pytest.raises(GateError, match="no-findings evidence"):
        verify_posted_evidence(
            issue_comments=[[_bot_item(body="## Footgun review\n\nNo notes.")]],
            reviews=[[]],
            review_comments=[[]],
            head_sha=HEAD_SHA,
            finding_count=0,
            digest=digest,
        )


def test_verify_findings_requires_exact_review_head_and_inline_thread_count():
    digest = "e" * 64
    marker = evidence_marker(HEAD_SHA, 2, digest)
    review = _bot_item(id=17, commit_id=HEAD_SHA, body=marker)
    comments = [
        _bot_item(pull_request_review_id=17, body=f"finding head={HEAD_SHA}"),
        _bot_item(pull_request_review_id=17, body=f"finding head={HEAD_SHA}"),
    ]

    verify_posted_evidence(
        issue_comments=[],
        reviews=[review],
        review_comments=comments,
        head_sha=HEAD_SHA,
        finding_count=2,
        digest=digest,
    )

    with pytest.raises(GateError, match="findings review"):
        verify_posted_evidence(
            issue_comments=[],
            reviews=[review],
            review_comments=comments[:1],
            head_sha=HEAD_SHA,
            finding_count=2,
            digest=digest,
        )


def test_digest_is_canonical_across_dictionary_key_order():
    value = {"b": [2], "a": {"d": 4, "c": 3}}
    reordered = json.loads('{"a":{"c":3,"d":4},"b":[2]}')

    assert artifact_digest(value) == artifact_digest(reordered)


def _skill_category_slugs() -> tuple[str, ...]:
    skill = (
        Path(__file__).resolve().parents[2] / ".claude" / "skills" / "footgun-detector" / "SKILL.md"
    )
    section = re.search(
        r"^### Footgun categories$(.*?)^## ",
        skill.read_text(encoding="utf-8"),
        re.MULTILINE | re.DOTALL,
    )
    assert section is not None, "skill file lost its 'Footgun categories' section"
    headings = re.findall(r"^#### (.+)$", section.group(1), re.MULTILINE)
    return tuple(re.sub(r"[^a-z0-9]+", "-", heading.lower()).strip("-") for heading in headings)


def test_required_categories_track_the_skill_rulebook():
    """The skill file is the single source of truth for footgun categories.

    REQUIRED_CATEGORIES is the merge gate's machine-readable copy of the
    category headings in .claude/skills/footgun-detector/SKILL.md. Adding,
    removing, renaming, or reordering a category in one place without the
    other silently changes what the gate enforces — so drift fails here.
    """
    assert _skill_category_slugs() == REQUIRED_CATEGORIES
