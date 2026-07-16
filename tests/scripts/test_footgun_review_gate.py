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
import sys
from pathlib import Path

import pytest

_SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from footgun_review_gate import (  # noqa: E402
    BOT_LOGIN,
    REQUIRED_CATEGORIES,
    GateError,
    artifact_digest,
    changed_line_anchors,
    evidence_marker,
    render_evidence,
    review_payload,
    validate_result,
    verify_posted_evidence,
)

HEAD_SHA = "a" * 40
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
        "base_sha": "b" * 40,
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
