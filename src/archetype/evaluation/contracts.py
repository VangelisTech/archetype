# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Evaluation value contracts (issue #275): outcomes and identity digests.

Identity is three digests:

- subject: the immutable snapshot reference (manifest head + tokens) plus
  the canonical selector. Never row-content hashing — materializing a
  10k-entity x 600-tick trajectory to hash it would break the lazy contract.
- contract: the versioned grader descriptor. Bare callables get no digest;
  persisted receipts REQUIRE a contract.
- evaluation_id: caller-supplied trial identity, deliberately DISTINCT from
  subject+contract — repeated trials of nondeterministic graders are a
  feature, not a replay bug.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field

from pydantic_core import to_jsonable_python

_RECEIPT_DIGEST_DOMAIN = "archetype.receipt.v1"

OUTCOME_STATUSES = frozenset({"pass", "fail", "invalid", "inconclusive"})


@dataclass(frozen=True)
class Outcome:
    """Represent a validated grading conclusion.

    `status` must be `pass`, `fail`, `invalid`, or `inconclusive`. A supplied
    score must be finite.
    """

    status: str
    score: float | None = None
    evidence: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.status not in OUTCOME_STATUSES:
            raise ValueError(
                f"outcome status {self.status!r} is not one of {sorted(OUTCOME_STATUSES)}"
            )
        if self.score is not None and not math.isfinite(self.score):
            raise ValueError("outcome score must be finite when present")


@dataclass(frozen=True)
class GraderContract:
    """Identify the grader configuration used for a durable receipt.

    Two receipts are directly comparable only when their contract digests
    match. Change `implementation_version`, configuration, thresholds, or
    seed whenever that comparison should no longer be valid.
    """

    grader_id: str
    implementation_version: str
    config: dict = field(default_factory=dict)
    thresholds: dict = field(default_factory=dict)
    seed: int | None = None

    def __post_init__(self) -> None:
        if not self.grader_id.strip():
            raise ValueError("grader_id must be a non-empty stable identity")
        if not self.implementation_version.strip():
            raise ValueError(
                "implementation_version must name the grader implementation "
                "(code version, prompt hash, or model id)"
            )

    def digest(self) -> str:
        payload = json.dumps(
            {
                "domain": _RECEIPT_DIGEST_DOMAIN,
                "kind": "grader-contract",
                "grader_id": self.grader_id,
                "implementation_version": self.implementation_version,
                "config": to_jsonable_python(self.config),
                "thresholds": to_jsonable_python(self.thresholds),
                "seed": self.seed,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def subject_digest(
    world_id: str,
    run_id: str,
    *,
    snapshot_tick: int,
    snapshot_tokens: list[str],
    component_names: list[str],
    ticks: list[int] | None = None,
    entity_ids: list[int] | None = None,
) -> str:
    """The pinned-subject identity: snapshot reference + canonical selector.

    The snapshot reference is the manifest head (tick + its commit tokens)
    from the control catalog — immutable by the atomic-visibility contract.
    The selector is what was asked of that snapshot. Together they make a
    receipt recomputable and attributable without hashing row content.
    """
    payload = json.dumps(
        {
            "domain": _RECEIPT_DIGEST_DOMAIN,
            "kind": "subject",
            "world_id": str(world_id),
            "run_id": str(run_id),
            "snapshot": {"tick": snapshot_tick, "tokens": sorted(snapshot_tokens)},
            "selector": {
                "components": sorted(component_names),
                "ticks": sorted(int(t) for t in ticks) if ticks is not None else None,
                "entity_ids": sorted(int(e) for e in entity_ids)
                if entity_ids is not None
                else None,
            },
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def evaluation_identity_digest(subject: str, contract: str) -> str:
    """The claim's payload digest: what this evaluation is OF.

    Deliberately excludes the graded outcome — two trials of a
    nondeterministic grader share subject+contract while concluding
    differently. Same evaluation_id + different identity digest is a loud
    conflict; same identity under a new evaluation_id is a fresh trial.
    """
    payload = json.dumps(
        {
            "domain": _RECEIPT_DIGEST_DOMAIN,
            "kind": "evaluation-identity",
            "subject": subject,
            "contract": contract,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
