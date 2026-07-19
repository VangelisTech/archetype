# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Cross-family adapter for mission-owned indexed artifact finalization."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from archetype.app.artifacts.bundle_models import PreparedArtifactBundleRequest
from archetype.app.artifacts.interfaces import iArtifactBundleService
from archetype.app.missions.interfaces import iMissionArtifactFinalizer
from archetype.app.missions.models import (
    WORKTREE_ARCHIVE_OUTCOME_CONTRACT_VERSION,
    AttemptArtifactExpiration,
    AttemptArtifactProjection,
    AttemptArtifactPublication,
    MissionArtifactFinalizationExpiredError,
    MissionAttemptRequest,
)
from archetype.artifacts.bundles import (
    ArtifactBundleRequest,
    ArtifactCandidate,
    ArtifactPublicationStatus,
)
from archetype.core.config import StorageConfig
from archetype.missions.transitions import FinalizationPhase

# field, portable path, kind, recursive, required.  Tuple order is fixed for
# debuggability; ArtifactBundleRequest canonicalization independently sorts by
# portable identity so projection identity cannot depend on mapping iteration.
_MISSION_ARTIFACT_CANDIDATES = (
    ("finalization_manifest_ref", "attempt/manifest.json", "attempt_manifest", False, True),
    ("trace_ref", "attempt/agent-output.jsonl", "agent_trace", False, True),
    ("traces_ref", "attempt/traces", "agent_traces", True, False),
    ("live_status_ref", "attempt/live-session.json", "agent_live_status", False, False),
    ("live_events_ref", "attempt/live-events.jsonl", "agent_live_events", False, False),
    (
        "filesystem_start_ref",
        "recovery/filesystem-start.jsonl",
        "filesystem_manifest",
        False,
        True,
    ),
    (
        "filesystem_end_ref",
        "recovery/filesystem-end.jsonl",
        "filesystem_manifest",
        False,
        True,
    ),
    (
        "filesystem_diff_ref",
        "recovery/filesystem-diff.jsonl",
        "filesystem_diff",
        False,
        True,
    ),
    ("git_status_ref", "recovery/git-status.txt", "git_status", False, True),
    ("git_patch_ref", "recovery/worktree.patch", "git_patch", False, True),
    ("git_bundle_ref", "recovery/repository.bundle", "git_bundle", False, True),
    (
        "worktree_archive_ref",
        "recovery/full-worktree.tar",
        "worktree_archive",
        False,
        True,
    ),
    ("context_ref", "context", "context", True, False),
)


class MissionArtifactFinalizer(iMissionArtifactFinalizer):
    """Project mission evidence, then publish only its persisted exact request."""

    def __init__(
        self,
        artifact_bundles: iArtifactBundleService,
        *,
        storage_config: StorageConfig | None = None,
    ) -> None:
        self._artifact_bundles = artifact_bundles
        self._storage_config = storage_config

    def prepare(
        self,
        request: MissionAttemptRequest,
        outcome: Mapping[str, Any],
        *,
        redaction_policy_id: str,
        claim_contract_version: int = WORKTREE_ARCHIVE_OUTCOME_CONTRACT_VERSION,
    ) -> AttemptArtifactProjection:
        """Return a deterministic, scanned projection without publication I/O."""

        bundle_request = self._bundle_request(
            request,
            outcome,
            redaction_policy_id=redaction_policy_id,
            claim_contract_version=claim_contract_version,
        )
        prepared = self._artifact_bundles.prepare(bundle_request)
        if prepared.redaction_policy_id != redaction_policy_id:
            raise ValueError("artifact preparation changed the mission-bound redaction policy")
        return AttemptArtifactProjection(
            request_json=prepared.request_json,
            request_digest=prepared.request_digest,
            publication_key=prepared.publication_key,
            producer_digest=prepared.producer_digest,
            redaction_policy_id=prepared.redaction_policy_id,
        )

    async def publish(
        self,
        projection: AttemptArtifactProjection,
    ) -> AttemptArtifactPublication:
        """Publish one staged request and require its exact authoritative receipt."""

        prepared = PreparedArtifactBundleRequest(
            request_json=projection.request_json,
            request_digest=projection.request_digest,
            publication_key=projection.publication_key,
            producer_digest=projection.producer_digest,
            redaction_policy_id=projection.redaction_policy_id,
        )
        request = ArtifactBundleRequest.model_validate_json(prepared.request_json)
        receipt = await self._artifact_bundles.publish_prepared(
            prepared,
            storage_config=self._storage_config,
        )
        expected = (
            projection.publication_key,
            projection.request_digest,
            projection.producer_digest,
            projection.redaction_policy_id,
        )
        actual = (
            receipt.bundle_id,
            receipt.request_digest,
            receipt.producer_digest,
            receipt.redaction_policy_id,
        )
        if actual != expected:
            raise ValueError("artifact indexed receipt does not match its staged projection")
        if (
            receipt.world_id != request.world_id
            or receipt.run_id != request.run_id
            or receipt.attempt_id != request.attempt_id
        ):
            raise ValueError("artifact indexed receipt does not match its staged attempt identity")
        if receipt.status is ArtifactPublicationStatus.EXPIRED:
            raise MissionArtifactFinalizationExpiredError(
                AttemptArtifactExpiration(
                    status="expired",
                    bundle_id=receipt.bundle_id,
                    request_digest=receipt.request_digest,
                    producer_digest=receipt.producer_digest,
                    redaction_policy_id=receipt.redaction_policy_id,
                )
            )
        if receipt.status is not ArtifactPublicationStatus.INDEXED:
            raise RuntimeError("artifact publication did not reach authoritative indexed status")
        if not receipt.manifest_uri or receipt.index_snapshot_id <= 0:
            raise ValueError("artifact indexed receipt lacks manifest or index-snapshot evidence")
        return AttemptArtifactPublication(
            status=FinalizationPhase.INDEXED,
            bundle_id=receipt.bundle_id,
            manifest_uri=receipt.manifest_uri,
            index_snapshot_id=receipt.index_snapshot_id,
            request_digest=receipt.request_digest,
            producer_digest=receipt.producer_digest,
            redaction_policy_id=receipt.redaction_policy_id,
        )

    @staticmethod
    def _bundle_request(
        request: MissionAttemptRequest,
        outcome: Mapping[str, Any],
        *,
        redaction_policy_id: str,
        claim_contract_version: int,
    ) -> ArtifactBundleRequest:
        if (
            type(claim_contract_version) is not int
            or not 1 <= claim_contract_version <= WORKTREE_ARCHIVE_OUTCOME_CONTRACT_VERSION
        ):
            raise ValueError("mission artifact preparation requires a supported claim contract")
        if str(outcome.get("attempt_id", "")) != request.attempt_id:
            raise ValueError("artifact outcome does not match the mission attempt_id")
        if str(outcome.get("idempotency_key", "")) != request.idempotency_key:
            raise ValueError("artifact outcome does not match the mission idempotency key")
        accepted = outcome.get("accepted")
        if not isinstance(accepted, bool):
            raise TypeError("artifact outcome accepted flag must be boolean")
        restorable = outcome.get("checkpoint_restorable")
        if not isinstance(restorable, bool):
            raise TypeError("artifact checkpoint restorable flag must be boolean")

        correlation = request.correlation
        try:
            world_id = str(correlation["world_id"])
            run_id = str(correlation["run_id"])
            entity_id = int(correlation["entity_id"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(
                "mission artifact preparation requires world/run/entity correlation"
            ) from exc
        if not world_id or not run_id:
            raise ValueError("mission artifact world and run identities must not be empty")

        artifacts: list[ArtifactCandidate] = []
        for field, logical_path, kind, recursive, required in _MISSION_ARTIFACT_CANDIDATES:
            candidate_required = required and not (
                field == "worktree_archive_ref"
                and claim_contract_version < WORKTREE_ARCHIVE_OUTCOME_CONTRACT_VERSION
            )
            source_ref = str(outcome.get(field) or "")
            if not source_ref:
                if candidate_required:
                    raise ValueError(f"mission artifact outcome requires {field}")
                continue
            artifacts.append(
                ArtifactCandidate(
                    source_ref=source_ref,
                    logical_path=logical_path,
                    kind=kind,
                    recursive=recursive,
                    required=candidate_required,
                )
            )

        try:
            checkpoint_created_at_ms = int(outcome["checkpoint_created_at_ms"])
            expires = outcome.get("checkpoint_expires_at_ms")
            if expires is None or expires == 0 or expires == "":
                checkpoint_expires_at_ms = 0
            elif isinstance(expires, bool) or not isinstance(expires, int | str):
                raise TypeError("checkpoint expiration must be an integer")
            else:
                checkpoint_expires_at_ms = int(expires)
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("mission artifact checkpoint timestamps are invalid") from exc

        return ArtifactBundleRequest(
            world_id=world_id,
            run_id=run_id,
            entity_id=entity_id,
            tick=request.observation_tick,
            attempt_id=request.attempt_id,
            idempotency_key=request.idempotency_key,
            redaction_policy_id=redaction_policy_id,
            checkpoint_ref=str(outcome.get("sandbox_state_ref") or ""),
            checkpoint_provider=str(outcome.get("checkpoint_provider") or ""),
            checkpoint_restorable=restorable,
            checkpoint_created_at_ms=checkpoint_created_at_ms,
            checkpoint_expires_at_ms=checkpoint_expires_at_ms,
            accepted=accepted,
            retention="run" if accepted else "attempt",
            artifacts=tuple(artifacts),
        )


__all__ = ["MissionArtifactFinalizer"]
