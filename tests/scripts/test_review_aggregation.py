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

"""Tests for deterministic, evidence-conserving review aggregation."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

_SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from review_aggregation import (  # noqa: E402
    assemble_preliminary_bundle,
    blocking_finding_count,
    design_bundle_findings,
    finalize_review_bundle,
    human_decision_count,
    make_adjudication_receipt,
    review_bundle_findings,
    validate_review_bundle,
)
from review_contracts import (  # noqa: E402
    ReviewError,
    normalize_adjudication_result,
    render_adjudication_prompt,
)
from review_test_support import (  # noqa: E402
    ANCHORS,
    FILES,
    HEAD_SHA,
    design_finding,
    footgun_finding,
    preliminary_bundle,
    reviewer_receipts,
)


def _adjudication(
    cluster_id: str,
    *,
    disposition: str,
    severity: str | None,
) -> dict[str, Any]:
    raw = {
        "head_sha": HEAD_SHA,
        "cluster_id": cluster_id,
        "disposition": disposition,
        "recommended_severity": severity,
        "evidence": [
            {
                "path": "old.py",
                "explanation": (
                    "The protected-base call chain supplies concrete evidence for the "
                    "assigned claim and its exact execution sequence."
                ),
            }
        ],
        "rationale": (
            "The protected-base implementation resolves the claim by tracing the exact "
            "changed call through its owning policy boundary."
        ),
        "recommended_action": (
            "Preserve the claim and this adjudication in the human review surface."
        ),
    }
    normalized = normalize_adjudication_result(
        raw,
        head_sha=HEAD_SHA,
        cluster_id=cluster_id,
    )
    prompt = render_adjudication_prompt(
        pr_number=7,
        head_sha=HEAD_SHA,
        cluster_id=cluster_id,
    )
    return dict(
        make_adjudication_receipt(
            normalized,
            reviewer_id="claude",
            prompt=prompt,
        )
    )


def test_fan_out_fails_closed_when_one_reviewer_receipt_is_missing():
    receipts = reviewer_receipts()
    receipts.pop()

    with pytest.raises(ReviewError, match="configured fan-out"):
        assemble_preliminary_bundle(
            receipts,
            head_sha=HEAD_SHA,
            scoped_files=FILES,
            anchors=ANCHORS,
        )


def test_exact_anchor_reports_from_two_reviewers_are_corroborated():
    def findings(lens: str, _reviewer: str) -> list[dict[str, Any]]:
        return [footgun_finding()] if lens == "authority" else []

    bundle = preliminary_bundle(findings)
    cluster = bundle["clusters"][0]

    assert cluster["corroboration"] == "corroborated"
    assert cluster["reviewer_ids"] == ["claude", "kimi"]
    assert len(cluster["members"]) == 2
    assert cluster["gate_disposition"] == "blocking"
    assert cluster["adjudication_status"] == "not-required"


def test_duplicate_findings_from_one_reviewer_do_not_create_false_corroboration():
    def findings(lens: str, reviewer: str) -> list[dict[str, Any]]:
        if lens == "authority" and reviewer == "claude":
            return [footgun_finding(), footgun_finding()]
        return []

    bundle = preliminary_bundle(findings)
    cluster = bundle["clusters"][0]

    assert cluster["corroboration"] == "singleton"
    assert cluster["reviewer_ids"] == ["claude"]
    assert len(cluster["members"]) == 2
    assert cluster["gate_disposition"] == "pending-adjudication"


def test_aggregation_conserves_every_original_finding():
    def findings(lens: str, reviewer: str) -> list[dict[str, Any]]:
        if lens == "authority" and reviewer == "claude":
            return [footgun_finding(), footgun_finding(severity="advisory")]
        if lens == "authority" and reviewer == "kimi":
            return [footgun_finding()]
        if lens == "design-coherence" and reviewer == "claude":
            return [design_finding()]
        return []

    receipts = reviewer_receipts(findings)
    bundle = assemble_preliminary_bundle(
        receipts,
        head_sha=HEAD_SHA,
        scoped_files=FILES,
        anchors=ANCHORS,
    )
    original_count = sum(
        len(receipt["result"]["findings"])
        for group in bundle["lenses"]
        for receipt in group["reviewers"]
    )
    member_count = sum(len(cluster["members"]) for cluster in bundle["clusters"])

    assert original_count == member_count == 4
    assert (
        sum(
            len(receipt["result"]["findings"])
            for group in bundle["lenses"]
            for receipt in group["reviewers"]
        )
        == 4
    )


def test_absence_is_not_disagreement_but_singleton_blocking_is_adjudicated():
    def findings(lens: str, reviewer: str) -> list[dict[str, Any]]:
        return [footgun_finding()] if lens == "authority" and reviewer == "claude" else []

    bundle = preliminary_bundle(findings)
    cluster = bundle["clusters"][0]

    assert cluster["severity_conflict"] is False
    assert cluster["adjudication_status"] == "pending"
    assert bundle["adjudication_targets"] == [cluster["cluster_id"]]


def test_severity_disagreement_selects_target_without_softening_evidence():
    def findings(lens: str, reviewer: str) -> list[dict[str, Any]]:
        if lens != "authority":
            return []
        severity = "blocking" if reviewer == "claude" else "advisory"
        return [footgun_finding(severity=severity)]

    bundle = preliminary_bundle(findings)
    cluster = bundle["clusters"][0]

    assert cluster["corroboration"] == "corroborated"
    assert cluster["severity_conflict"] is True
    assert cluster["representative"]["severity"] == "blocking"
    assert cluster["gate_disposition"] == "pending-adjudication"


@pytest.mark.parametrize(
    ("disposition", "severity", "gate_disposition", "blocking", "human_decisions"),
    [
        ("confirmed", "blocking", "blocking", 1, 0),
        ("confirmed", "advisory", "advisory", 0, 0),
        ("refuted", None, "human-decision", 0, 1),
        ("unresolved", None, "human-decision", 0, 1),
    ],
)
def test_adjudication_never_deletes_the_original_claim(
    disposition,
    severity,
    gate_disposition,
    blocking,
    human_decisions,
):
    def findings(lens: str, reviewer: str) -> list[dict[str, Any]]:
        return [footgun_finding()] if lens == "authority" and reviewer == "claude" else []

    preliminary = preliminary_bundle(findings)
    cluster_id = preliminary["adjudication_targets"][0]
    final = finalize_review_bundle(
        preliminary,
        [_adjudication(cluster_id, disposition=disposition, severity=severity)],
        head_sha=HEAD_SHA,
        scoped_files=FILES,
        anchors=ANCHORS,
    )

    assert final["clusters"][0]["gate_disposition"] == gate_disposition
    assert len(final["clusters"][0]["members"]) == 1
    assert len(review_bundle_findings(final)) == 1
    assert blocking_finding_count(final) == blocking
    assert human_decision_count(final) == human_decisions


def test_design_coherence_is_always_advisory_and_never_adjudicated():
    def findings(lens: str, reviewer: str) -> list[dict[str, Any]]:
        return [design_finding()] if lens == "design-coherence" else []

    preliminary = preliminary_bundle(findings)
    final = finalize_review_bundle(
        preliminary,
        [],
        head_sha=HEAD_SHA,
        scoped_files=FILES,
        anchors=ANCHORS,
    )

    assert preliminary["adjudication_targets"] == []
    assert review_bundle_findings(final) == []
    assert len(design_bundle_findings(final)) == 1
    assert design_bundle_findings(final)[0]["gate_disposition"] == "design-note"
    assert blocking_finding_count(final) == 0


def test_final_bundle_rejects_cluster_or_receipt_tampering():
    preliminary = preliminary_bundle()
    final = finalize_review_bundle(
        preliminary,
        [],
        head_sha=HEAD_SHA,
        scoped_files=FILES,
        anchors=ANCHORS,
    )
    tampered = json.loads(json.dumps(final))
    tampered["lenses"][0]["reviewers"][0]["result"]["summary"] += " tampered"

    with pytest.raises(ReviewError, match="artifact digest"):
        validate_review_bundle(
            tampered,
            head_sha=HEAD_SHA,
            scoped_files=FILES,
            anchors=ANCHORS,
        )
