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

"""Evidence-conserving aggregation for the deterministic review fan-out.

Models author individual lens results and targeted adjudications. This module
does not summarize their prose or decide whether two merely similar claims are
the same. It clusters only exact anchors, counts distinct reviewers, preserves
every original finding in its digest-bound receipt, and derives gate state by
fixed rules.
"""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Any, cast

from review_contracts import (
    ADJUDICATION_RECEIPT_KIND,
    ADJUDICATOR_REVIEWER,
    DESIGN_CATEGORIES,
    DESIGN_LENS_KIND,
    FOOTGUN_LENS_KIND,
    LENS_KINDS,
    LENS_REVIEWERS,
    LENSES,
    MODEL_RESULT_SCHEMA_VERSION,
    REQUIRED_CATEGORIES,
    REVIEW_BUNDLE_KIND,
    REVIEW_BUNDLE_VERSION,
    REVIEWER_RECEIPT_KIND,
    REVIEWER_RECEIPT_VERSION,
    AdjudicationReceipt,
    AdjudicationResult,
    FindingCluster,
    LensReceiptGroup,
    ReviewBundle,
    ReviewerReceipt,
    ReviewError,
    artifact_digest,
    expected_review_pairs,
    lens_categories,
    lens_kind,
    normalize_adjudication_result,
    normalize_lens_result,
    prompt_digest,
    reviewer_spec,
)

_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_RECEIPT_FIELDS = (
    "kind",
    "schema_version",
    "head_sha",
    "lens",
    "lens_kind",
    "categories",
    "reviewer_id",
    "backend",
    "model",
    "status",
    "prompt_digest",
    "artifact_digest",
    "result",
)
# A seat whose provider failed outside the review's semantics (quota
# exhaustion, schema-invalid output after the bounded retry, runner fault)
# records that fact instead of failing the whole gate. The 2026-07-26 kimi
# billing-cycle exhaustion turned two seats into a repository-wide merge
# brick precisely because seat failure and review verdict shared one exit
# code. An infra receipt is provenance, not a verdict: it carries no model
# result and can never corroborate or contribute findings.
INFRA_FAILURE_CLASSES = ("quota", "schema", "runner")
_INFRA_RECEIPT_FIELDS = (
    "kind",
    "schema_version",
    "head_sha",
    "lens",
    "lens_kind",
    "categories",
    "reviewer_id",
    "backend",
    "model",
    "status",
    "failure_class",
    "detail",
)
_ADJUDICATION_RECEIPT_FIELDS = (
    "kind",
    "schema_version",
    "head_sha",
    "cluster_id",
    "reviewer_id",
    "backend",
    "model",
    "status",
    "prompt_digest",
    "artifact_digest",
    "result",
)
_BUNDLE_FIELDS = (
    "kind",
    "schema_version",
    "phase",
    "head_sha",
    "reviewed_files",
    "footgun_categories",
    "design_categories",
    "lenses",
    "clusters",
    "adjudication_targets",
    "adjudications",
)


def _expect_mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ReviewError(f"{label} must be an object")
    return value


def _expect_list(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise ReviewError(f"{label} must be an array")
    return value


def _exact_keys(value: Mapping[str, Any], expected: Sequence[str], label: str) -> None:
    if set(value) != set(expected):
        missing = sorted(set(expected) - set(value))
        extra = sorted(set(value) - set(expected))
        raise ReviewError(
            f"{label} fields do not match the contract; missing={missing}, extra={extra}"
        )


def _validate_digest(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _DIGEST_RE.fullmatch(value):
        raise ReviewError(f"{label} must be a lowercase SHA-256 digest")
    return value


def make_reviewer_receipt(
    result: Mapping[str, Any],
    *,
    lens: str,
    reviewer_id: str,
    prompt: str,
) -> ReviewerReceipt:
    """Stamp trusted provenance around one already-normalized model result."""
    if reviewer_id not in LENS_REVIEWERS.get(lens, ()):
        raise ReviewError(f"reviewer {reviewer_id!r} is not assigned to lens {lens!r}")
    spec = reviewer_spec(reviewer_id)
    if result.get("schema_version") != MODEL_RESULT_SCHEMA_VERSION:
        raise ReviewError("reviewer result has an unsupported schema version")
    head_sha = result.get("head_sha")
    if not isinstance(head_sha, str):
        raise ReviewError("reviewer result head_sha must be a string")
    result_copy = dict(result)
    return cast(
        ReviewerReceipt,
        {
            "kind": REVIEWER_RECEIPT_KIND,
            "schema_version": REVIEWER_RECEIPT_VERSION,
            "head_sha": head_sha,
            "lens": lens,
            "lens_kind": lens_kind(lens),
            "categories": list(lens_categories(lens)),
            "reviewer_id": reviewer_id,
            "backend": spec["backend"],
            "model": spec["model"],
            "status": "validated",
            "prompt_digest": prompt_digest(prompt),
            "artifact_digest": artifact_digest(result_copy),
            "result": result_copy,
        },
    )


def make_infra_failure_receipt(
    *,
    lens: str,
    reviewer_id: str,
    head_sha: str,
    failure_class: str,
    detail: str,
) -> ReviewerReceipt:
    """Record that a seat failed for infrastructure reasons, not verdict ones."""
    if reviewer_id not in LENS_REVIEWERS.get(lens, ()):
        raise ReviewError(f"reviewer {reviewer_id!r} is not assigned to lens {lens!r}")
    if failure_class not in INFRA_FAILURE_CLASSES:
        raise ReviewError(f"unknown infra failure class {failure_class!r}")
    if not isinstance(detail, str) or not detail.strip():
        raise ReviewError("infra failure detail must be a non-empty string")
    spec = reviewer_spec(reviewer_id)
    return cast(
        ReviewerReceipt,
        {
            "kind": REVIEWER_RECEIPT_KIND,
            "schema_version": REVIEWER_RECEIPT_VERSION,
            "head_sha": head_sha,
            "lens": lens,
            "lens_kind": lens_kind(lens),
            "categories": list(lens_categories(lens)),
            "reviewer_id": reviewer_id,
            "backend": spec["backend"],
            "model": spec["model"],
            "status": "infra_failed",
            "failure_class": failure_class,
            "detail": detail.strip()[:500],
        },
    )


def validate_reviewer_receipt(
    raw_receipt: Mapping[str, Any],
    *,
    head_sha: str,
    scoped_files: Sequence[str],
    anchors: set[tuple[str, str, int]],
) -> ReviewerReceipt:
    """Validate provenance and re-normalize the bound model result."""
    if raw_receipt.get("status") == "infra_failed":
        return _validate_infra_receipt(raw_receipt, head_sha=head_sha)
    _exact_keys(raw_receipt, _RECEIPT_FIELDS, "reviewer receipt")
    if raw_receipt.get("kind") != REVIEWER_RECEIPT_KIND:
        raise ReviewError("reviewer receipt kind is invalid")
    if raw_receipt.get("schema_version") != REVIEWER_RECEIPT_VERSION:
        raise ReviewError("reviewer receipt schema version is invalid")
    if raw_receipt.get("head_sha") != head_sha:
        raise ReviewError("reviewer receipt is bound to a different head")

    lens = raw_receipt.get("lens")
    reviewer_id = raw_receipt.get("reviewer_id")
    if not isinstance(lens, str) or lens not in LENSES:
        raise ReviewError("reviewer receipt lens is invalid")
    if not isinstance(reviewer_id, str) or reviewer_id not in LENS_REVIEWERS[lens]:
        raise ReviewError(f"reviewer receipt has an unassigned reviewer for lens {lens!r}")
    spec = reviewer_spec(reviewer_id)
    expected_metadata = {
        "lens_kind": lens_kind(lens),
        "categories": list(lens_categories(lens)),
        "backend": spec["backend"],
        "model": spec["model"],
        "status": "validated",
    }
    for field, expected in expected_metadata.items():
        if raw_receipt.get(field) != expected:
            raise ReviewError(f"reviewer receipt {field} does not match trusted policy")

    _validate_digest(raw_receipt.get("prompt_digest"), "reviewer receipt prompt_digest")
    result = _expect_mapping(raw_receipt.get("result"), "reviewer receipt result")
    if result.get("schema_version") != MODEL_RESULT_SCHEMA_VERSION:
        raise ReviewError("reviewer receipt result has an unsupported schema version")
    model_result = dict(result)
    model_result.pop("schema_version", None)
    normalized = normalize_lens_result(
        model_result,
        lens=lens,
        head_sha=head_sha,
        scoped_files=scoped_files,
        anchors=anchors,
    )
    if normalized != result:
        raise ReviewError("reviewer receipt result is not canonically normalized")
    digest = artifact_digest(normalized)
    if raw_receipt.get("artifact_digest") != digest:
        raise ReviewError("reviewer receipt artifact digest does not match its result")
    return cast(
        ReviewerReceipt,
        {
            **dict(raw_receipt),
            "result": normalized,
        },
    )


def _validate_infra_receipt(
    raw_receipt: Mapping[str, Any],
    *,
    head_sha: str,
) -> ReviewerReceipt:
    """Validate an infra-failure receipt's binding without inventing a verdict."""
    _exact_keys(raw_receipt, _INFRA_RECEIPT_FIELDS, "infra-failure receipt")
    if raw_receipt.get("kind") != REVIEWER_RECEIPT_KIND:
        raise ReviewError("infra-failure receipt kind is invalid")
    if raw_receipt.get("schema_version") != REVIEWER_RECEIPT_VERSION:
        raise ReviewError("infra-failure receipt schema version is invalid")
    if raw_receipt.get("head_sha") != head_sha:
        raise ReviewError("infra-failure receipt is bound to a different head")
    lens = raw_receipt.get("lens")
    reviewer_id = raw_receipt.get("reviewer_id")
    if not isinstance(lens, str) or lens not in LENSES:
        raise ReviewError("infra-failure receipt lens is invalid")
    if not isinstance(reviewer_id, str) or reviewer_id not in LENS_REVIEWERS[lens]:
        raise ReviewError(f"infra-failure receipt has an unassigned reviewer for lens {lens!r}")
    spec = reviewer_spec(reviewer_id)
    expected_metadata = {
        "lens_kind": lens_kind(lens),
        "categories": list(lens_categories(lens)),
        "backend": spec["backend"],
        "model": spec["model"],
    }
    for field, expected in expected_metadata.items():
        if raw_receipt.get(field) != expected:
            raise ReviewError(f"infra-failure receipt {field} does not match trusted policy")
    if raw_receipt.get("failure_class") not in INFRA_FAILURE_CLASSES:
        raise ReviewError("infra-failure receipt failure_class is invalid")
    detail = raw_receipt.get("detail")
    if not isinstance(detail, str) or not detail.strip():
        raise ReviewError("infra-failure receipt detail must be a non-empty string")
    return cast(ReviewerReceipt, dict(raw_receipt))


def neutral_seats(bundle: Mapping[str, Any]) -> list[dict[str, str]]:
    """Seats that recorded an infra failure instead of a verdict."""
    return [
        {
            "lens": group["lens"],
            "reviewer_id": receipt["reviewer_id"],
            "failure_class": receipt["failure_class"],
        }
        for group in bundle.get("lenses", ())
        for receipt in group.get("reviewers", ())
        if receipt.get("status") == "infra_failed"
    ]


def _cluster_id(key: tuple[str, str, str, str, int]) -> str:
    lens, category, path, side, line = key
    return artifact_digest(
        {
            "lens": lens,
            "category": category,
            "path": path,
            "side": side,
            "line": line,
        }
    )


def _representative(
    members: Sequence[tuple[ReviewerReceipt, int, Mapping[str, Any]]],
) -> dict[str, Any]:
    """Choose display evidence without deleting or merging any member."""

    def rank(member: tuple[ReviewerReceipt, int, Mapping[str, Any]]) -> tuple[int, int, int]:
        receipt, index, finding = member
        severity_rank = 0 if finding.get("severity") == "blocking" else 1
        reviewer_rank = LENS_REVIEWERS[receipt["lens"]].index(receipt["reviewer_id"])
        return severity_rank, reviewer_rank, index

    return dict(min(members, key=rank)[2])


def _build_clusters(receipts: Sequence[ReviewerReceipt]) -> list[FindingCluster]:
    grouped: dict[
        tuple[str, str, str, str, int],
        list[tuple[ReviewerReceipt, int, Mapping[str, Any]]],
    ] = defaultdict(list)
    for receipt in receipts:
        result = receipt["result"]
        for index, raw_finding in enumerate(result["findings"]):
            finding = _expect_mapping(raw_finding, "review finding")
            key = (
                receipt["lens"],
                str(finding["category"]),
                str(finding["path"]),
                str(finding["side"]),
                int(finding["line"]),
            )
            grouped[key].append((receipt, index, finding))

    lens_order = {lens: index for index, lens in enumerate(LENSES)}
    ordered_keys = sorted(
        grouped,
        key=lambda key: (lens_order[key[0]], key[1], key[2], key[3], key[4]),
    )
    clusters: list[FindingCluster] = []
    for key in ordered_keys:
        members = grouped[key]
        lens, category, path, side, line = key
        reviewer_ids = sorted(
            {receipt["reviewer_id"] for receipt, _, _ in members},
            key=LENS_REVIEWERS[lens].index,
        )
        corroboration = "corroborated" if len(reviewer_ids) >= 2 else "singleton"
        representative = _representative(members)
        severity_conflict = False
        if lens_kind(lens) == FOOTGUN_LENS_KIND:
            severities = {str(finding.get("severity")) for _, _, finding in members}
            severity_conflict = len(severities) > 1

        if lens_kind(lens) == DESIGN_LENS_KIND:
            adjudication_status = "not-required"
            gate_disposition = "design-note"
        else:
            singleton_blocking = corroboration == "singleton" and any(
                finding.get("severity") == "blocking" for _, _, finding in members
            )
            if severity_conflict or singleton_blocking:
                adjudication_status = "pending"
                gate_disposition = "pending-adjudication"
            else:
                adjudication_status = "not-required"
                gate_disposition = (
                    "blocking" if representative.get("severity") == "blocking" else "advisory"
                )

        clusters.append(
            cast(
                FindingCluster,
                {
                    "cluster_id": _cluster_id(key),
                    "lens": lens,
                    "lens_kind": lens_kind(lens),
                    "category": category,
                    "path": path,
                    "side": side,
                    "line": line,
                    "corroboration": corroboration,
                    "severity_conflict": severity_conflict,
                    "reviewer_ids": reviewer_ids,
                    "members": [
                        {
                            "reviewer_id": receipt["reviewer_id"],
                            "artifact_digest": receipt["artifact_digest"],
                            "finding_index": index,
                        }
                        for receipt, index, _ in members
                    ],
                    "representative": representative,
                    "adjudication_status": adjudication_status,
                    "gate_disposition": gate_disposition,
                },
            )
        )
    return clusters


def assemble_preliminary_bundle(
    receipts: Sequence[Mapping[str, Any]],
    *,
    head_sha: str,
    scoped_files: Sequence[str],
    anchors: set[tuple[str, str, int]],
) -> ReviewBundle:
    """Validate the complete 6×2 fan-out and derive preliminary clusters."""
    validated: dict[tuple[str, str], ReviewerReceipt] = {}
    for raw_receipt in receipts:
        receipt = validate_reviewer_receipt(
            raw_receipt,
            head_sha=head_sha,
            scoped_files=scoped_files,
            anchors=anchors,
        )
        key = (receipt["lens"], receipt["reviewer_id"])
        if key in validated:
            raise ReviewError(f"duplicate reviewer receipt for {key[0]!r}/{key[1]!r}")
        validated[key] = receipt

    expected = set(expected_review_pairs())
    if set(validated) != expected:
        missing = sorted(expected - set(validated))
        extra = sorted(set(validated) - expected)
        raise ReviewError(
            "reviewer receipts do not match the configured fan-out; "
            f"missing={missing}, extra={extra}"
        )

    # An infra-failed seat is neutral: it contributes provenance, never a
    # verdict. Every lens still needs at least one live seat — a lens whose
    # entire bench failed has no verdict at all, and that fails closed.
    # A survivor's blocking findings become singletons, which routes them
    # to adjudication: a degraded bench earns MORE scrutiny, not less.
    for lens, reviewer_ids in LENS_REVIEWERS.items():
        statuses = [validated[(lens, rid)].get("status") for rid in reviewer_ids]
        if "validated" not in statuses:
            failures = ", ".join(
                f"{rid}={validated[(lens, rid)].get('failure_class')}" for rid in reviewer_ids
            )
            raise ReviewError(
                f"every seat for lens {lens!r} failed for infrastructure reasons "
                f"({failures}); no verdict exists, refusing to proceed"
            )

    ordered_receipts = [
        validated[pair]
        for pair in expected_review_pairs()
        if validated[pair].get("status") == "validated"
    ]
    lens_groups: list[LensReceiptGroup] = []
    for lens, reviewer_ids in LENS_REVIEWERS.items():
        lens_groups.append(
            {
                "lens": lens,
                "lens_kind": LENS_KINDS[lens],
                "categories": list(LENSES[lens]),
                "reviewers": [validated[(lens, reviewer_id)] for reviewer_id in reviewer_ids],
            }
        )

    clusters = _build_clusters(ordered_receipts)
    targets = [
        cluster["cluster_id"] for cluster in clusters if cluster["adjudication_status"] == "pending"
    ]
    return {
        "kind": REVIEW_BUNDLE_KIND,
        "schema_version": REVIEW_BUNDLE_VERSION,
        "phase": "preliminary",
        "head_sha": head_sha,
        "reviewed_files": sorted(scoped_files),
        "footgun_categories": list(REQUIRED_CATEGORIES),
        "design_categories": list(DESIGN_CATEGORIES),
        "lenses": lens_groups,
        "clusters": clusters,
        "adjudication_targets": targets,
        "adjudications": [],
    }


def _bundle_receipts(raw_bundle: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    groups = _expect_list(raw_bundle.get("lenses"), "review bundle lenses")
    receipts: list[Mapping[str, Any]] = []
    for group_index, raw_group in enumerate(groups):
        group = _expect_mapping(raw_group, f"review bundle lenses[{group_index}]")
        reviewers = _expect_list(
            group.get("reviewers"),
            f"review bundle lenses[{group_index}].reviewers",
        )
        receipts.extend(
            _expect_mapping(item, f"review bundle lenses[{group_index}] reviewer")
            for item in reviewers
        )
    return receipts


def _validate_bundle_envelope(
    raw_bundle: Mapping[str, Any],
    *,
    head_sha: str,
    scoped_files: Sequence[str],
) -> str:
    _exact_keys(raw_bundle, _BUNDLE_FIELDS, "review bundle")
    if raw_bundle.get("kind") != REVIEW_BUNDLE_KIND:
        raise ReviewError("review bundle kind is invalid")
    if raw_bundle.get("schema_version") != REVIEW_BUNDLE_VERSION:
        raise ReviewError("review bundle schema version is invalid")
    phase = raw_bundle.get("phase")
    if phase not in {"preliminary", "final"}:
        raise ReviewError("review bundle phase must be preliminary or final")
    if raw_bundle.get("head_sha") != head_sha:
        raise ReviewError("review bundle is bound to a different head")
    if raw_bundle.get("reviewed_files") != sorted(scoped_files):
        raise ReviewError("review bundle file manifest does not match trusted scope")
    if raw_bundle.get("footgun_categories") != list(REQUIRED_CATEGORIES):
        raise ReviewError("review bundle footgun categories do not match trusted policy")
    if raw_bundle.get("design_categories") != list(DESIGN_CATEGORIES):
        raise ReviewError("review bundle design categories do not match trusted policy")
    return str(phase)


def make_adjudication_receipt(
    result: Mapping[str, Any],
    *,
    reviewer_id: str,
    prompt: str,
) -> AdjudicationReceipt:
    """Stamp trusted provenance around a normalized adjudication."""
    if reviewer_id != ADJUDICATOR_REVIEWER:
        raise ReviewError(f"adjudications must use reviewer {ADJUDICATOR_REVIEWER!r}")
    spec = reviewer_spec(reviewer_id)
    if result.get("schema_version") != MODEL_RESULT_SCHEMA_VERSION:
        raise ReviewError("adjudication result has an unsupported schema version")
    head_sha = result.get("head_sha")
    cluster_id = result.get("cluster_id")
    if not isinstance(head_sha, str) or not isinstance(cluster_id, str):
        raise ReviewError("adjudication result must bind a head and cluster")
    result_copy = dict(result)
    return cast(
        AdjudicationReceipt,
        {
            "kind": ADJUDICATION_RECEIPT_KIND,
            "schema_version": REVIEWER_RECEIPT_VERSION,
            "head_sha": head_sha,
            "cluster_id": cluster_id,
            "reviewer_id": reviewer_id,
            "backend": spec["backend"],
            "model": spec["model"],
            "status": "validated",
            "prompt_digest": prompt_digest(prompt),
            "artifact_digest": artifact_digest(result_copy),
            "result": result_copy,
        },
    )


def validate_adjudication_receipt(
    raw_receipt: Mapping[str, Any],
    *,
    head_sha: str,
    cluster_id: str,
    scoped_files: Sequence[str] = (),
) -> AdjudicationReceipt:
    _exact_keys(
        raw_receipt,
        _ADJUDICATION_RECEIPT_FIELDS,
        "adjudication receipt",
    )
    if raw_receipt.get("kind") != ADJUDICATION_RECEIPT_KIND:
        raise ReviewError("adjudication receipt kind is invalid")
    if raw_receipt.get("schema_version") != REVIEWER_RECEIPT_VERSION:
        raise ReviewError("adjudication receipt schema version is invalid")
    reviewer_id = raw_receipt.get("reviewer_id")
    if reviewer_id != ADJUDICATOR_REVIEWER:
        raise ReviewError("adjudication receipt reviewer does not match trusted policy")
    spec = reviewer_spec(ADJUDICATOR_REVIEWER)
    for field, expected in {
        "head_sha": head_sha,
        "cluster_id": cluster_id,
        "backend": spec["backend"],
        "model": spec["model"],
        "status": "validated",
    }.items():
        if raw_receipt.get(field) != expected:
            raise ReviewError(f"adjudication receipt {field} does not match trusted policy")
    _validate_digest(
        raw_receipt.get("prompt_digest"),
        "adjudication receipt prompt_digest",
    )
    result = _expect_mapping(raw_receipt.get("result"), "adjudication receipt result")
    if result.get("schema_version") != MODEL_RESULT_SCHEMA_VERSION:
        raise ReviewError("adjudication result has an unsupported schema version")
    model_result = dict(result)
    model_result.pop("schema_version", None)
    normalized = normalize_adjudication_result(
        model_result,
        head_sha=head_sha,
        cluster_id=cluster_id,
        scoped_files=scoped_files,
    )
    if normalized != result:
        raise ReviewError("adjudication result is not canonically normalized")
    if raw_receipt.get("artifact_digest") != artifact_digest(normalized):
        raise ReviewError("adjudication receipt artifact digest does not match its result")
    return cast(
        AdjudicationReceipt,
        {
            **dict(raw_receipt),
            "result": normalized,
        },
    )


def finalize_review_bundle(
    preliminary: Mapping[str, Any],
    adjudications: Sequence[Mapping[str, Any]],
    *,
    head_sha: str,
    scoped_files: Sequence[str],
    anchors: set[tuple[str, str, int]],
) -> ReviewBundle:
    """Apply exactly one adjudication to each targeted footgun cluster."""
    validated_preliminary = validate_review_bundle(
        preliminary,
        head_sha=head_sha,
        scoped_files=scoped_files,
        anchors=anchors,
        expected_phase="preliminary",
    )
    targets = validated_preliminary["adjudication_targets"]
    receipts_by_cluster: dict[str, AdjudicationReceipt] = {}
    for raw_receipt in adjudications:
        cluster_id = raw_receipt.get("cluster_id")
        if not isinstance(cluster_id, str):
            raise ReviewError("adjudication receipt cluster_id must be a string")
        if cluster_id in receipts_by_cluster:
            raise ReviewError(f"duplicate adjudication for cluster {cluster_id}")
        receipts_by_cluster[cluster_id] = validate_adjudication_receipt(
            raw_receipt,
            head_sha=head_sha,
            cluster_id=cluster_id,
            scoped_files=scoped_files,
        )
    if set(receipts_by_cluster) != set(targets):
        missing = sorted(set(targets) - set(receipts_by_cluster))
        extra = sorted(set(receipts_by_cluster) - set(targets))
        raise ReviewError(
            f"adjudications do not match the targeted clusters; missing={missing}, extra={extra}"
        )

    final_clusters: list[FindingCluster] = []
    for raw_cluster in validated_preliminary["clusters"]:
        cluster = dict(raw_cluster)
        cluster_id = cluster["cluster_id"]
        if cluster_id in receipts_by_cluster:
            result: AdjudicationResult = receipts_by_cluster[cluster_id]["result"]
            disposition = result["disposition"]
            cluster["adjudication_status"] = disposition
            cluster["gate_disposition"] = (
                result["recommended_severity"] if disposition == "confirmed" else "human-decision"
            )
        final_clusters.append(cast(FindingCluster, cluster))

    return {
        **validated_preliminary,
        "phase": "final",
        "clusters": final_clusters,
        "adjudications": [receipts_by_cluster[target] for target in targets],
    }


def validate_review_bundle(
    raw_bundle: Mapping[str, Any],
    *,
    head_sha: str,
    scoped_files: Sequence[str],
    anchors: set[tuple[str, str, int]],
    expected_phase: str | None = None,
) -> ReviewBundle:
    """Rebuild the deterministic bundle and reject any altered derivative."""
    phase = _validate_bundle_envelope(
        raw_bundle,
        head_sha=head_sha,
        scoped_files=scoped_files,
    )
    if expected_phase is not None and phase != expected_phase:
        raise ReviewError(f"review bundle phase must be {expected_phase!r}")
    preliminary = assemble_preliminary_bundle(
        _bundle_receipts(raw_bundle),
        head_sha=head_sha,
        scoped_files=scoped_files,
        anchors=anchors,
    )
    if phase == "preliminary":
        if preliminary != raw_bundle:
            raise ReviewError("preliminary review bundle is not the deterministic aggregate")
        return preliminary

    raw_adjudications = _expect_list(
        raw_bundle.get("adjudications"),
        "review bundle adjudications",
    )
    final = finalize_review_bundle(
        preliminary,
        [_expect_mapping(item, "review bundle adjudication") for item in raw_adjudications],
        head_sha=head_sha,
        scoped_files=scoped_files,
        anchors=anchors,
    )
    if final != raw_bundle:
        raise ReviewError("final review bundle is not the deterministic aggregate")
    return final


def review_bundle_findings(bundle: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Project one publishable footgun thread per cluster."""
    findings: list[dict[str, Any]] = []
    for raw_cluster in _expect_list(bundle.get("clusters"), "review bundle clusters"):
        cluster = _expect_mapping(raw_cluster, "review bundle cluster")
        if cluster.get("lens_kind") != FOOTGUN_LENS_KIND:
            continue
        finding = dict(_expect_mapping(cluster.get("representative"), "cluster representative"))
        finding.update(
            {
                "cluster_id": cluster["cluster_id"],
                "corroboration": cluster["corroboration"],
                "reviewer_ids": list(cluster["reviewer_ids"]),
                "adjudication_status": cluster["adjudication_status"],
                "gate_disposition": cluster["gate_disposition"],
            }
        )
        findings.append(finding)
    return findings


def design_bundle_findings(bundle: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Project advisory design notes for the human review surface."""
    findings: list[dict[str, Any]] = []
    for raw_cluster in _expect_list(bundle.get("clusters"), "review bundle clusters"):
        cluster = _expect_mapping(raw_cluster, "review bundle cluster")
        if cluster.get("lens_kind") != DESIGN_LENS_KIND:
            continue
        finding = dict(_expect_mapping(cluster.get("representative"), "cluster representative"))
        finding.update(
            {
                "cluster_id": cluster["cluster_id"],
                "corroboration": cluster["corroboration"],
                "reviewer_ids": list(cluster["reviewer_ids"]),
                "gate_disposition": "design-note",
            }
        )
        findings.append(finding)
    return findings


def blocking_finding_count(bundle: Mapping[str, Any]) -> int:
    return sum(
        finding.get("gate_disposition") == "blocking" for finding in review_bundle_findings(bundle)
    )


def human_decision_count(bundle: Mapping[str, Any]) -> int:
    return sum(
        finding.get("gate_disposition") == "human-decision"
        for finding in review_bundle_findings(bundle)
    )
