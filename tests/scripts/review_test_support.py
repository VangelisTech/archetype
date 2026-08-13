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

"""Shared exact-scope fixtures for deterministic review contract tests."""

from __future__ import annotations

import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

_SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from review_aggregation import (  # noqa: E402
    assemble_preliminary_bundle,
    finalize_review_bundle,
    make_reviewer_receipt,
)
from review_contracts import (  # noqa: E402
    DESIGN_CATEGORIES,
    REQUIRED_CATEGORIES,
    expected_review_pairs,
    normalize_human_design_brief,
    normalize_lens_result,
    render_lens_review_prompt,
)

HEAD_SHA = "a" * 40
BASE_SHA = "b" * 40
FILES = ["new.py", "old.py"]
ANCHORS = {
    ("new.py", "RIGHT", 1),
    ("new.py", "RIGHT", 2),
    ("old.py", "LEFT", 3),
    ("old.py", "RIGHT", 3),
}
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


def scope() -> dict[str, Any]:
    return {
        "schema_version": 2,
        "base_sha": BASE_SHA,
        "head_sha": HEAD_SHA,
        "files": list(FILES),
        "footgun_categories": list(REQUIRED_CATEGORIES),
        "design_categories": list(DESIGN_CATEGORIES),
    }


def raw_result(*, findings: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {
        "head_sha": HEAD_SHA,
        "review_status": "complete",
        "summary": (
            "The complete review inspected both changed files through the assigned lens "
            "and compared their behavior with the relevant protected-base contracts."
        ),
        "review_context": [
            {
                "area": "Changed behavior",
                "files": list(FILES),
                "assessment": (
                    "Both changed paths were inspected against the assigned categories "
                    "and the repository patterns relevant to their concrete behavior."
                ),
            }
        ],
        "findings": findings or [],
    }


def footgun_finding(**overrides: Any) -> dict[str, Any]:
    finding: dict[str, Any] = {
        "category": "governance-bypass",
        "severity": "blocking",
        "title": "Bypasses governed dispatch",
        "path": "old.py",
        "side": "RIGHT",
        "line": 3,
        "what_it_does": (
            "The changed call reaches the concrete service directly instead of dispatching "
            "the registered operation."
        ),
        "what_goes_wrong": (
            "Authorization and audit policy are skipped when an untrusted actor reaches "
            "this exact call path."
        ),
        "failing_input_or_sequence": (
            "An untrusted actor invokes this path and the service mutates state without "
            "the command gate checking permission."
        ),
        "fix": (
            "Route this operation through the existing command dispatcher and its exact "
            "registered operation model."
        ),
    }
    finding.update(overrides)
    return finding


def design_finding(**overrides: Any) -> dict[str, Any]:
    finding: dict[str, Any] = {
        "category": "single-use-abstraction",
        "title": "Single-use forwarding protocol",
        "path": "new.py",
        "side": "RIGHT",
        "line": 1,
        "repository_evidence": [
            {
                "path": "packages/archetype-ecs/src/archetype/storage/service.py",
                "symbol": "StorageService",
                "explanation": (
                    "The comparable family uses the concrete service directly until a "
                    "second implementation creates real variation."
                ),
            }
        ],
        "introduced_shape": (
            "The change adds a protocol and forwarding adapter around one implementation "
            "and one call site."
        ),
        "concrete_cost": (
            "A reader must cross two extra symbols to discover the only execution path, "
            "and tests now need ceremonial adapter setup."
        ),
        "simpler_alternative": (
            "Inject the existing concrete service directly and introduce a protocol only "
            "when a second behavior actually exists."
        ),
        "behavior_preserved": True,
    }
    finding.update(overrides)
    return finding


FindingFactory = Callable[[str, str], list[dict[str, Any]]]


def reviewer_receipts(
    findings_for: FindingFactory | None = None,
) -> list[dict[str, Any]]:
    receipts: list[dict[str, Any]] = []
    for lens, reviewer in expected_review_pairs():
        findings = findings_for(lens, reviewer) if findings_for else []
        normalized = normalize_lens_result(
            raw_result(findings=findings),
            lens=lens,
            head_sha=HEAD_SHA,
            scoped_files=FILES,
            anchors=ANCHORS,
        )
        prompt = render_lens_review_prompt(
            pr_number=7,
            head_sha=HEAD_SHA,
            lens=lens,
            reviewer_id=reviewer,
        )
        receipts.append(
            dict(
                make_reviewer_receipt(
                    normalized,
                    lens=lens,
                    reviewer_id=reviewer,
                    prompt=prompt,
                )
            )
        )
    return receipts


def preliminary_bundle(
    findings_for: FindingFactory | None = None,
) -> dict[str, Any]:
    return dict(
        assemble_preliminary_bundle(
            reviewer_receipts(findings_for),
            head_sha=HEAD_SHA,
            scoped_files=FILES,
            anchors=ANCHORS,
        )
    )


def final_bundle() -> dict[str, Any]:
    preliminary = preliminary_bundle()
    return dict(
        finalize_review_bundle(
            preliminary,
            [],
            head_sha=HEAD_SHA,
            scoped_files=FILES,
            anchors=ANCHORS,
        )
    )


def raw_design_brief(bundle_digest: str, cluster_ids: list[str] | None = None) -> dict[str, Any]:
    return {
        "head_sha": HEAD_SHA,
        "bundle_digest": bundle_digest,
        "readiness": "ready-for-human-review",
        "decision_requested": "Confirm that the implementation shape matches the intended change.",
        "intent_and_behavioral_delta": (
            "The patch replaces one unsafe call and introduces a small module while "
            "preserving the surrounding runtime behavior and reviewed ownership boundaries."
        ),
        "change_cohorts": [
            {
                "name": "Call and module update",
                "files": list(FILES),
                "summary": (
                    "Read the replacement call first, then the new module that supplies "
                    "the concrete values used by the changed path."
                ),
                "reading_order": 1,
            }
        ],
        "design_choices": [],
        "affected_invariants": [],
        "human_decision_queue": (
            [
                {
                    "question": "Should the disputed review claim change the implementation?",
                    "why_now": (
                        "The aggregate retained conflicting evidence that requires a human "
                        "choice before its thread can be resolved."
                    ),
                    "options": [
                        "Accept the proposed implementation change",
                        "Document why the current implementation is intentional",
                    ],
                    "related_cluster_ids": cluster_ids,
                }
            ]
            if cluster_ids
            else []
        ),
        "validation": [
            {
                "check": "Structured review contract",
                "status": "passed",
                "evidence": "All reviewer receipts and aggregate digests validated.",
            }
        ],
        "open_questions": [],
    }


def normalized_design_brief(bundle: dict[str, Any]) -> dict[str, Any]:
    from review_contracts import artifact_digest

    digest = artifact_digest(bundle)
    cluster_ids = [item["cluster_id"] for item in bundle["clusters"]]
    return dict(
        normalize_human_design_brief(
            raw_design_brief(digest),
            head_sha=HEAD_SHA,
            bundle_digest=digest,
            scoped_files=FILES,
            cluster_ids=set(cluster_ids),
        )
    )
