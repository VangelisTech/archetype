# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Catalog-bound artifact publication models.

The reusable bundle value contracts moved to ``archetype.artifacts.bundles``
(#558). ``PreparedArtifactBundleRequest`` stays here because its validation
binds to the control catalog's publication-key derivation — a storage
authority — so it is application-owned, not a portable family contract.
"""

from __future__ import annotations

import re

from pydantic import BaseModel, field_validator, model_validator

from archetype.artifacts.bundles import ArtifactBundleRequest

_SHA256_RE = re.compile(r"[0-9a-f]{64}")


class PreparedArtifactBundleRequest(BaseModel):
    """Immutable, scanned identity safe to persist before publication I/O.

    ``request_digest`` authenticates the exact bound canonical JSON while
    ``producer_digest`` preserves the logical publication identity across
    compatible redaction-policy upgrades.
    """

    model_config = dict(frozen=True)

    request_json: str
    request_digest: str
    publication_key: str
    producer_digest: str
    redaction_policy_id: str

    @field_validator("request_json", "redaction_policy_id")
    @classmethod
    def _required_prepared_value(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be empty")
        return value

    @field_validator("request_digest", "publication_key", "producer_digest")
    @classmethod
    def _prepared_sha256(cls, value: str) -> str:
        if not _SHA256_RE.fullmatch(value):
            raise ValueError("must be a lowercase SHA-256 digest")
        return value

    @model_validator(mode="after")
    def _authenticates_exact_request(self) -> PreparedArtifactBundleRequest:
        # Keep the deterministic publication identity owned by the control
        # catalog.  The narrow local import avoids duplicating its domain
        # separator here while keeping the public model import graph acyclic.
        from archetype.app.storage.catalog import artifact_publication_key

        try:
            request = ArtifactBundleRequest.model_validate_json(self.request_json)
        except (ValueError, TypeError) as exc:
            raise ValueError("request_json must encode an ArtifactBundleRequest") from exc
        if request.canonical_json() != self.request_json:
            raise ValueError("request_json must use the canonical artifact request encoding")
        if request.request_digest() != self.request_digest:
            raise ValueError("request_digest does not authenticate request_json")
        if request.producer_digest() != self.producer_digest:
            raise ValueError("producer_digest does not authenticate request_json")
        if request.redaction_policy_id != self.redaction_policy_id:
            raise ValueError("redaction_policy_id does not match request_json")
        expected_key = artifact_publication_key(
            request.world_id,
            request.run_id,
            request.idempotency_key,
        )
        if self.publication_key != expected_key:
            raise ValueError("publication_key does not match request_json")
        return self
