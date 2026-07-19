# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Durable pre-execution claim and recovery authority for mission attempts."""

from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import time
from collections.abc import Mapping
from dataclasses import replace
from typing import Any, cast

from pydantic import JsonValue

from archetype.app.limits import MAX_ICEBERG_SNAPSHOT_ID
from archetype.app.missions.models import (
    AttemptArtifactExpiration,
    AttemptArtifactProjection,
    AttemptArtifactPublication,
    AttemptClaim,
    AttemptClaimAcquisition,
    AttemptRecoveryDecision,
    FencedExecutionAuthorization,
    FinalizationSettlementKind,
    MissionAttemptRequest,
    PreparedFinalizationSettlement,
    ProviderExecutionCapabilities,
    attempt_invocation_fingerprint,
    mission_attempt_request_fingerprint,
    normalize_attempt_validators,
)
from archetype.app.missions.outcomes import (
    WORKTREE_ARCHIVE_OUTCOME_CONTRACT_VERSION,
    MissionAttemptAssessment,
    assess_attempt_outcome,
    assess_legacy_unbound_settled_outcome,
    assess_pre_worktree_archive_outcome,
)
from archetype.app.missions.transitions import (
    AttemptClaimAcquireOutcome,
    AttemptClaimEvent,
    AttemptClaimStatus,
    AttemptClaimTransitionGraph,
    AttemptRecoveryAction,
)
from archetype.app.redaction.interfaces import iRedactionService
from archetype.app.redaction.models import (
    RedactedRecord,
    RedactionReceipt,
    SecretQuarantineError,
)
from archetype.app.storage.catalog import (
    ArtifactPublicationRecord,
    AttemptClaimConflictError,
    AttemptClaimRecord,
    AttemptClaimStaleError,
    ControlCatalog,
)
from archetype.missions.transitions import (
    AttemptStatus,
    FinalizationPhase,
    MissionStatus,
    MissionTaskState,
    TaskStatus,
)

_CLAIM_DOMAIN = "archetype.mission-attempt-claim.v1"
_CURRENT_CLAIM_CONTRACT_VERSION = WORKTREE_ARCHIVE_OUTCOME_CONTRACT_VERSION
_PRE_WORKTREE_ARCHIVE_CLAIM_CONTRACT_VERSION = 8
_LEGACY_CLAIM_CONTRACT_VERSION = 7
_ARTIFACT_PUBLICATION_EXPIRED = "artifact_publication_expired"
_MAX_LAST_ERROR_CHARS = 4096
_REDACTION_EVIDENCE_KEYS = frozenset(
    {
        "policy_id",
        "request",
        "provider",
        "acknowledgement",
        "outcome",
        "last_error",
    }
)
_REDACTION_EVIDENCE_SCOPES = {
    "request": "mission-attempt-request",
    "provider": "mission-attempt-provider-capabilities",
    "acknowledgement": "mission-attempt-provider-acknowledgement",
    "outcome": "mission-attempt-outcome",
    "last_error": "mission-attempt-last-error",
}
_OUTCOME_IDENTITY_FIELDS = (
    "attempt_id",
    "idempotency_key",
    "request_fingerprint",
    "status",
    "harness",
    "agent_session_id",
    "checkpoint_provider",
    "checkpoint_status",
    "sandbox_state_ref",
    "finalization_phase",
    "finalization_manifest_ref",
    "trace_ref",
    "traces_ref",
    "filesystem_start_ref",
    "filesystem_end_ref",
    "filesystem_diff_ref",
    "git_status_ref",
    "git_patch_ref",
    "git_bundle_ref",
    "worktree_archive_ref",
    "context_ref",
    "sha",
    "artifact_publication_key",
    "artifact_request_digest",
    "artifact_producer_digest",
    "artifact_redaction_policy_id",
    "finalization_bundle_id",
    "finalization_request_digest",
    "finalization_producer_digest",
    "finalization_redaction_policy_id",
    "finalization_index_snapshot_id",
)


class MissionAttemptClaimService:
    """Fence one provider submission before any sandbox or model invocation."""

    def __init__(
        self,
        catalog: ControlCatalog,
        graph: AttemptClaimTransitionGraph | None = None,
        *,
        redaction_service: iRedactionService,
    ) -> None:
        self._catalog = catalog
        self._graph = graph or AttemptClaimTransitionGraph()
        self._redaction_service = redaction_service
        self._finalization_seal_key = secrets.token_bytes(32)

    async def acquire(
        self,
        request: MissionAttemptRequest,
        capabilities: ProviderExecutionCapabilities,
        *,
        claimant: str,
        lease_seconds: float = 900.0,
    ) -> AttemptClaimAcquisition:
        """Persist or recover the deterministic attempt claim and lease."""

        claimant = claimant.strip()
        if not claimant:
            raise ValueError("attempt claim claimant must not be empty")
        self._validate_request(request)
        world_id, run_id = self._request_world_run(request)
        request_json = self._request_json(request)
        claim_key = self.claim_key(
            world_id=world_id,
            mission_id=request.mission_id,
            task_id=request.task_id,
            attempt_id=request.attempt_id,
        )
        existing = await self.get(world_id, claim_key)
        if existing is not None:
            recovered_request = self.recover_request(existing)
            # Observation time is durable evidence, not provider-submission
            # identity. Preserve the first observation when a later tick
            # rediscovers the same deterministic claim.
            if replace(request, observation_tick=recovered_request.observation_tick) != (
                recovered_request
            ):
                raise AttemptClaimConflictError(
                    f"attempt claim {claim_key} was reused with different immutable input"
                )
            # A pre-v8 request has no contract marker. Preserve its exact
            # immutable bytes during lease recovery rather than rewriting it.
            request_json = existing.request_json
        if existing is not None and existing.status is AttemptClaimStatus.SETTLED:
            expected = self._claim_request_fingerprint(
                request,
                capabilities,
                redaction_policy_id=existing.redaction_policy_id,
            )
            if existing.request_fingerprint != expected:
                raise AttemptClaimConflictError(
                    f"attempt claim {claim_key} was reused with different immutable input"
                )
            return AttemptClaimAcquisition(AttemptClaimAcquireOutcome.DUPLICATE, existing)

        redaction_evidence_json = self._acquisition_redaction_evidence(
            request_json=request_json,
            capabilities=capabilities,
        )
        self._redaction_service.assert_safe_metadata(
            claimant,
            field="mission-attempt-claimant",
        )
        fingerprint = self._claim_request_fingerprint(
            request,
            capabilities,
            redaction_policy_id=self._redaction_service.policy_id,
        )
        outcome, record = await self._catalog.acquire_attempt_claim(
            claim_key=claim_key,
            world_id=world_id,
            run_id=run_id,
            mission_id=request.mission_id,
            task_id=request.task_id,
            attempt_id=request.attempt_id,
            idempotency_key=request.idempotency_key,
            request_fingerprint=fingerprint,
            request_json=request_json,
            redaction_policy_id=self._redaction_service.policy_id,
            redaction_evidence_json=redaction_evidence_json,
            provider=capabilities.provider,
            provider_request_fingerprint=capabilities.request_fingerprint,
            supports_idempotent_replay=capabilities.supports_idempotent_replay,
            supports_session_resume=capabilities.supports_session_resume,
            provider_idempotency_key=capabilities.provider_idempotency_key,
            claimant=claimant,
            lease_seconds=lease_seconds,
        )
        return AttemptClaimAcquisition(
            AttemptClaimAcquireOutcome(outcome),
            self._project(record),
        )

    async def decide_recovery(
        self,
        claim: AttemptClaim,
        *,
        lease_seconds: float = 900.0,
    ) -> AttemptRecoveryDecision:
        """Return the sole action allowed by durable state and provider capability.

        A fresh ``claimed`` row is first persisted as ``possibly_submitted``.
        Only then is an ``execute`` authorization returned, conservatively
        covering a crash immediately before or after request send.
        """

        if claim.status is AttemptClaimStatus.SETTLED:
            return self._decision(AttemptRecoveryAction.SETTLED, claim)
        current = await self.renew(claim, lease_seconds=lease_seconds)
        if current.status is AttemptClaimStatus.CLAIMED:
            transition = self._graph.transition(
                current.status,
                AttemptClaimEvent.ARM_SUBMISSION,
            )
            execution_nonce = secrets.token_urlsafe(32)
            try:
                record = await self._catalog.transition_attempt_claim(
                    current.world_id,
                    current.claim_key,
                    current.claimant,
                    current.fence_epoch,
                    expected_status=transition.source.value,
                    target_status=transition.target.value,
                    execution_nonce=execution_nonce,
                )
            except AttemptClaimConflictError:
                # Another decision on this exact fence won the arm CAS. It may
                # reconcile, but it must never receive a second execute grant.
                observed = await self._current_owned(current, allow_settled=True)
                return self._nonexecuting_decision(observed)
            return self._decision(AttemptRecoveryAction.EXECUTE, self._project(record))

        if current.status in {
            AttemptClaimStatus.POSSIBLY_SUBMITTED,
            AttemptClaimStatus.PROVIDER_ACKNOWLEDGED,
        }:
            # Capability declarations are durable evidence, not executable
            # recovery implementations. Until a provider adapter can apply an
            # idempotency operation or retrieve an acknowledged response, both
            # uncertainty states fail closed to reconciliation.
            return self._decision(AttemptRecoveryAction.RECONCILE, current)

        if current.status is AttemptClaimStatus.FINALIZING:
            return self._decision(AttemptRecoveryAction.FINALIZE, current)

        raise AssertionError(f"unhandled attempt claim state: {current.status.value}")

    async def acknowledge_provider(
        self,
        claim: AttemptClaim,
        *,
        provider_session_id: str = "",
        provider_request_id: str = "",
    ) -> AttemptClaim:
        """Persist provider acknowledgement without claiming terminal settlement."""

        if not provider_session_id.strip() and not provider_request_id.strip():
            raise ValueError("provider acknowledgement requires a session or request identity")
        current = await self._current_owned(claim)
        self._require_active_policy(current)
        acknowledgement = self._redaction_service.redact_record(
            cast(
                Mapping[str, JsonValue],
                {
                    "provider_session_id": provider_session_id,
                    "provider_request_id": provider_request_id,
                },
            ),
            scope="mission-attempt-provider-acknowledgement",
        )
        self._quarantine_if_redacted(acknowledgement.receipt)
        if not current.execution_consumed_at:
            raise ValueError("provider acknowledgement requires a consumed execution grant")
        if current.status is AttemptClaimStatus.PROVIDER_ACKNOWLEDGED:
            if (
                current.provider_session_id != provider_session_id
                or current.provider_request_id != provider_request_id
            ):
                raise ValueError("provider acknowledgement identity changed on replay")
            return current
        evidence_json = self._updated_redaction_evidence(
            current,
            acknowledgement=acknowledgement.receipt,
        )
        transition = self._graph.transition(
            current.status,
            AttemptClaimEvent.ACKNOWLEDGE_PROVIDER,
        )
        try:
            record = await self._catalog.transition_attempt_claim(
                current.world_id,
                current.claim_key,
                current.claimant,
                current.fence_epoch,
                expected_status=transition.source.value,
                target_status=transition.target.value,
                redaction_evidence_json=evidence_json,
                provider_session_id=provider_session_id,
                provider_request_id=provider_request_id,
            )
        except (AttemptClaimConflictError, AttemptClaimStaleError) as exc:
            observed = await self._current_owned(current, allow_settled=True)
            if (
                observed.status
                in {AttemptClaimStatus.PROVIDER_ACKNOWLEDGED, AttemptClaimStatus.SETTLED}
                and observed.provider_session_id == provider_session_id
                and observed.provider_request_id == provider_request_id
            ):
                return observed
            if observed.status is AttemptClaimStatus.SETTLED:
                raise ValueError("provider acknowledgement identity changed concurrently") from None
            raise exc
        return self._project(record)

    async def stage_finalization(
        self,
        claim: AttemptClaim,
        *,
        outcome: RedactedRecord,
        projection: AttemptArtifactProjection,
    ) -> AttemptClaim:
        """Persist one sanitized outcome and exact artifact request before I/O."""

        current = await self._current_owned(claim, allow_settled=True)
        if current.status is AttemptClaimStatus.SETTLED:
            raise ValueError("a settled attempt claim cannot stage artifact finalization")
        self._require_active_policy(current)
        durable_outcome = self._coerce_durable_outcome(current, outcome)
        request = self.recover_request(current)
        assessment = self.assess_outcome(current, durable_outcome.value)
        if request.required_finalization_phase is not FinalizationPhase.INDEXED:
            raise ValueError("artifact finalization may only be staged for an indexed gate")
        if assessment.provider_status not in {
            AttemptStatus.ACCEPTED,
            AttemptStatus.REJECTED,
        }:
            raise ValueError("artifact finalization requires an accepted or rejected outcome")
        if not bool(durable_outcome.value["checkpoint_restorable"]):
            raise ValueError("artifact finalization requires a restorable checkpoint")
        if assessment.finalization_phase not in {
            FinalizationPhase.CAPTURED,
            FinalizationPhase.CHECKPOINTED,
            FinalizationPhase.UPLOADED,
            FinalizationPhase.PUBLISHED,
        }:
            raise ValueError("artifact finalization requires captured or checkpointed evidence")
        if (
            assessment.provider_status is AttemptStatus.ACCEPTED
            and not str(durable_outcome.value["sha"]).strip()
        ):
            raise ValueError("artifact finalization requires a commit SHA")
        if projection.redaction_policy_id != current.redaction_policy_id:
            raise ValueError("prepared artifact request changed the bound redaction policy")

        artifact_value = json.loads(projection.request_json)
        scan = self._redaction_service.redact_record(
            cast(Mapping[str, JsonValue], artifact_value),
            scope="mission-attempt-artifact-request",
        )
        self._quarantine_if_redacted(scan.receipt)
        for name, value in (
            ("request_digest", projection.request_digest),
            ("publication_key", projection.publication_key),
            ("producer_digest", projection.producer_digest),
            ("redaction_policy_id", projection.redaction_policy_id),
        ):
            self._redaction_service.assert_safe_metadata(
                value,
                field=f"mission-attempt-artifact.{name}",
            )

        staged_value = dict(durable_outcome.value)
        staged_value.update(
            {
                "artifact_publication_key": projection.publication_key,
                "artifact_request_digest": projection.request_digest,
                "artifact_producer_digest": projection.producer_digest,
                "artifact_redaction_policy_id": projection.redaction_policy_id,
            }
        )
        staged_outcome = RedactedRecord(
            value=cast(dict[str, JsonValue], staged_value),
            receipt=durable_outcome.receipt,
        )
        outcome_json = self._json(staged_outcome.value)
        outcome_digest = hashlib.sha256(outcome_json.encode()).hexdigest()
        if current.status is AttemptClaimStatus.FINALIZING:
            if self._staging_matches(
                current,
                outcome_digest=outcome_digest,
                outcome_json=outcome_json,
                projection=projection,
            ):
                return current
            raise ValueError("attempt artifact finalization changed on replay")
        evidence_json = self._updated_redaction_evidence(
            current,
            outcome=staged_outcome.receipt,
        )
        transition = self._graph.transition(
            current.status,
            AttemptClaimEvent.STAGE_FINALIZATION,
        )
        try:
            record = await self._catalog.transition_attempt_claim(
                current.world_id,
                current.claim_key,
                current.claimant,
                current.fence_epoch,
                expected_status=transition.source.value,
                target_status=transition.target.value,
                redaction_evidence_json=evidence_json,
                outcome_digest=outcome_digest,
                outcome_json=outcome_json,
                artifact_request_json=projection.request_json,
                artifact_request_digest=projection.request_digest,
                artifact_publication_key=projection.publication_key,
            )
        except (AttemptClaimConflictError, AttemptClaimStaleError) as exc:
            observed = await self._current_owned(current, allow_settled=True)
            if observed.status in {
                AttemptClaimStatus.FINALIZING,
                AttemptClaimStatus.SETTLED,
            } and self._staging_matches(
                observed,
                outcome_digest=outcome_digest,
                outcome_json=outcome_json,
                projection=projection,
            ):
                return observed
            if observed.status in {
                AttemptClaimStatus.FINALIZING,
                AttemptClaimStatus.SETTLED,
            }:
                raise ValueError("attempt artifact finalization changed concurrently") from None
            raise exc
        return self._project(record)

    async def settle(
        self,
        claim: AttemptClaim,
        *,
        attempt_status: AttemptStatus | str,
        outcome: Mapping[str, Any] | RedactedRecord,
        last_error: str = "",
    ) -> AttemptClaim:
        """Settle a non-finalizing claim or prove an exact terminal replay."""

        try:
            settlement = AttemptStatus(attempt_status)
        except ValueError as exc:
            raise ValueError(f"invalid attempt settlement status: {attempt_status!r}") from exc
        if settlement is AttemptStatus.PENDING:
            raise ValueError("a pending attempt cannot terminally settle a claim")
        current = await self._current_owned(claim, allow_settled=True)
        if current.status is AttemptClaimStatus.FINALIZING:
            raise ValueError("a finalizing claim requires a prepared finalization settlement")
        return await self._settle_current(
            current,
            settlement=settlement,
            outcome=outcome,
            last_error=last_error,
            finalization_authorized=False,
        )

    async def settle_finalized(
        self,
        claim: AttemptClaim,
        prepared: PreparedFinalizationSettlement,
        *,
        last_error: str = "",
    ) -> AttemptClaim:
        """Settle FINALIZING only from one exact service-sealed preparation."""

        if not isinstance(prepared, PreparedFinalizationSettlement):
            raise TypeError("finalized settlement requires a typed prepared value")
        current = await self._current_owned(claim, allow_settled=True)
        if current.status not in {
            AttemptClaimStatus.FINALIZING,
            AttemptClaimStatus.SETTLED,
        }:
            raise ValueError("prepared finalization can settle only a finalizing claim")
        self._validate_prepared_finalization_settlement(current, prepared)
        return await self._settle_current(
            current,
            settlement=prepared.attempt_status,
            outcome=prepared.outcome,
            last_error=last_error,
            finalization_authorized=True,
        )

    async def _settle_current(
        self,
        current: AttemptClaim,
        *,
        settlement: AttemptStatus,
        outcome: Mapping[str, Any] | RedactedRecord,
        last_error: str,
        finalization_authorized: bool,
    ) -> AttemptClaim:
        if current.status is AttemptClaimStatus.FINALIZING and not finalization_authorized:
            raise ValueError("a finalizing claim requires a prepared finalization settlement")
        if current.status is AttemptClaimStatus.SETTLED:
            if current.legacy_unbound:
                # Compatibility is read-only: compare the caller's raw value
                # with the terminal bytes, without rescanning or normalizing it
                # under current write semantics.
                replay_value = outcome.value if isinstance(outcome, RedactedRecord) else outcome
                outcome_json = self._json(replay_value)
                last_error = current.last_error
            elif current.redaction_policy_id == self._redaction_service.policy_id:
                durable_outcome = self._coerce_durable_outcome(current, outcome)
                outcome_json = self._json(durable_outcome.value)
                last_error = self._redaction_service.redact_text(
                    last_error,
                    scope="mission-attempt-last-error",
                ).text[:_MAX_LAST_ERROR_CHARS]
            else:
                # A terminal record remains readable after a policy rollout,
                # but no old-policy record may be mutated or reinterpreted.
                # The stored error is evidence produced by the old policy, so
                # a caller must not have to reproduce its redacted bytes with
                # the new policy merely to prove an otherwise identical replay.
                replay_value = outcome.value if isinstance(outcome, RedactedRecord) else outcome
                outcome_json = self._json(replay_value)
                last_error = current.last_error
            outcome_digest = hashlib.sha256(outcome_json.encode()).hexdigest()
            if not self._settlement_matches(
                current,
                settlement=settlement,
                outcome_digest=outcome_digest,
                outcome_json=outcome_json,
                last_error=last_error,
            ):
                raise ValueError("attempt claim settlement changed on replay")
            return current
        self._require_active_policy(current)
        durable_outcome = self._coerce_durable_outcome(current, outcome)
        outcome_json = self._json(durable_outcome.value)
        outcome_digest = hashlib.sha256(outcome_json.encode()).hexdigest()
        redacted_error = self._redaction_service.redact_text(
            last_error,
            scope="mission-attempt-last-error",
        )
        last_error = redacted_error.text[:_MAX_LAST_ERROR_CHARS]
        self._validate_outcome(
            current,
            settlement=settlement,
            outcome=durable_outcome.value,
        )
        if current.status is AttemptClaimStatus.CLAIMED and settlement is AttemptStatus.ACCEPTED:
            raise ValueError("an attempt that was never armed cannot settle as accepted")
        evidence_json = self._updated_redaction_evidence(
            current,
            outcome=durable_outcome.receipt,
            last_error=redacted_error.receipt,
        )
        event = {
            AttemptClaimStatus.CLAIMED: AttemptClaimEvent.SETTLE_WITHOUT_SUBMISSION,
            AttemptClaimStatus.POSSIBLY_SUBMITTED: (AttemptClaimEvent.SETTLE_AFTER_RECONCILIATION),
            AttemptClaimStatus.PROVIDER_ACKNOWLEDGED: (AttemptClaimEvent.SETTLE_ACKNOWLEDGED),
            AttemptClaimStatus.FINALIZING: AttemptClaimEvent.SETTLE_FINALIZED,
        }[current.status]
        transition = self._graph.transition(current.status, event)
        try:
            record = await self._catalog.transition_attempt_claim(
                current.world_id,
                current.claim_key,
                current.claimant,
                current.fence_epoch,
                expected_status=transition.source.value,
                target_status=transition.target.value,
                redaction_evidence_json=evidence_json,
                settlement_status=settlement.value,
                outcome_digest=outcome_digest,
                outcome_json=outcome_json,
                last_error=last_error,
            )
        except (AttemptClaimConflictError, AttemptClaimStaleError) as exc:
            observed = await self._current_owned(current, allow_settled=True)
            if observed.status is AttemptClaimStatus.SETTLED and self._settlement_matches(
                observed,
                settlement=settlement,
                outcome_digest=outcome_digest,
                outcome_json=outcome_json,
                last_error=last_error,
            ):
                return observed
            if observed.status is AttemptClaimStatus.SETTLED:
                raise ValueError("attempt claim settlement changed concurrently") from None
            raise exc
        return self._project(record)

    async def renew(
        self,
        claim: AttemptClaim,
        *,
        lease_seconds: float = 900.0,
    ) -> AttemptClaim:
        self._require_active_policy(claim)
        record = await self._catalog.renew_attempt_claim(
            claim.world_id,
            claim.claim_key,
            claim.claimant,
            claim.fence_epoch,
            lease_seconds=lease_seconds,
        )
        return self._project(record)

    async def consume_execution(
        self,
        authorization: FencedExecutionAuthorization,
    ) -> AttemptClaim:
        """Atomically consume the sole provider-call grant for one armed fence."""

        if authorization.action is not AttemptRecoveryAction.EXECUTE:
            raise ValueError("only an execute authorization carries an execution grant")
        if not authorization.execution_nonce:
            raise ValueError("execute authorization is missing its execution nonce")
        current = await self.get(authorization.world_id, authorization.claim_key)
        if current is None:
            raise ValueError(f"attempt claim {authorization.claim_key} no longer exists")
        self._require_active_policy(current)
        if (
            current.claimant != authorization.claimant
            or current.fence_epoch != authorization.fence_epoch
            or current.attempt_id != authorization.attempt_id
            or current.idempotency_key != authorization.idempotency_key
            or current.request_fingerprint != authorization.request_fingerprint
            or current.execution_nonce != authorization.execution_nonce
        ):
            raise ValueError("execution authorization no longer matches its durable claim")
        record = await self._catalog.consume_attempt_execution(
            authorization.world_id,
            authorization.claim_key,
            authorization.claimant,
            authorization.fence_epoch,
            authorization.execution_nonce,
        )
        return self._project(record)

    async def get(self, world_id: str, claim_key: str) -> AttemptClaim | None:
        record = await self._catalog.get_attempt_claim(world_id, claim_key)
        return self._project(record) if record is not None else None

    async def require_settled(self, world_id: str, claim_key: str) -> AttemptClaim:
        """Reread and authenticate the durable terminal winner for projection."""

        current = await self.get(world_id, claim_key)
        if current is None:
            raise ValueError(f"attempt claim {claim_key} no longer exists")
        if current.status is not AttemptClaimStatus.SETTLED:
            raise ValueError("attempt claim is not durably settled")
        # Verify the canonical payload at the same boundary that turns the
        # catalog row into projection authority. The returned DTO is a value,
        # never authority independent of this reread.
        self.settled_outcome(current)
        return current

    async def list_due(
        self,
        world_id: str,
        *,
        now: float | None = None,
        limit: int = 100,
    ) -> list[AttemptClaim]:
        records = await self._catalog.list_due_attempt_claims(
            world_id,
            now=time.time() if now is None else now,
            limit=limit,
        )
        return [self._project(record) for record in records]

    def prepare_durable_outcome(
        self,
        claim: AttemptClaim,
        outcome: Mapping[str, Any],
    ) -> RedactedRecord:
        """Validate, sanitize, and revalidate one outcome before any durable consumer."""

        self._require_active_policy(claim)
        self._validate_outcome(
            claim,
            settlement=self.assess_outcome(claim, outcome).attempt_status,
            outcome=outcome,
        )
        canonical = json.loads(self._json(outcome))
        if not isinstance(canonical, dict):
            raise TypeError("sandbox attempt outcome must be a JSON object")
        self._assert_safe_outcome_identity(canonical)
        redacted = self._redaction_service.redact_record(
            cast(Mapping[str, JsonValue], canonical),
            scope="mission-attempt-outcome",
        )
        self._validate_outcome(
            claim,
            settlement=self.assess_outcome(claim, redacted.value).attempt_status,
            outcome=redacted.value,
        )
        return redacted

    def staged_artifact_projection(self, claim: AttemptClaim) -> AttemptArtifactProjection:
        """Reconstruct the exact prepared request retained by a finalizing claim."""

        if claim.status is not AttemptClaimStatus.FINALIZING:
            raise ValueError("attempt claim is not awaiting artifact finalization")
        staged = self._staged_outcome(claim)
        try:
            producer_digest = str(staged["artifact_producer_digest"])
            policy_id = str(staged["artifact_redaction_policy_id"])
        except KeyError as exc:
            raise ValueError("finalizing attempt claim lacks prepared artifact identity") from exc
        projection = AttemptArtifactProjection(
            request_json=claim.artifact_request_json,
            request_digest=claim.artifact_request_digest,
            publication_key=claim.artifact_publication_key,
            producer_digest=producer_digest,
            redaction_policy_id=policy_id,
        )
        if projection.redaction_policy_id != claim.redaction_policy_id:
            raise ValueError("finalizing artifact request changed the claim redaction policy")
        return projection

    async def prepare_artifact_finalization_outcome(
        self,
        claim: AttemptClaim,
    ) -> PreparedFinalizationSettlement:
        """Derive terminal authority only from the durable artifact outbox row."""

        current = await self._current_owned(claim)
        if current.status is not AttemptClaimStatus.FINALIZING:
            raise ValueError("attempt claim is not awaiting artifact finalization")
        self._require_active_policy(current)
        projection = self.staged_artifact_projection(current)
        publication = await self._catalog.get_artifact_publication(
            current.world_id,
            projection.publication_key,
        )
        if publication is None:
            raise ValueError("durable artifact publication authority is missing")
        self._validate_artifact_publication_authority(
            current,
            projection,
            publication,
        )
        if publication.status == "INDEXED":
            return self._prepare_finalized_outcome(
                current,
                AttemptArtifactPublication(
                    status=FinalizationPhase.INDEXED,
                    bundle_id=publication.publication_key,
                    manifest_uri=publication.manifest_uri,
                    index_snapshot_id=publication.index_snapshot_id,
                    request_digest=projection.request_digest,
                    producer_digest=projection.producer_digest,
                    redaction_policy_id=projection.redaction_policy_id,
                ),
            )
        if publication.status == "EXPIRED":
            return self._prepare_expired_finalization_outcome(
                current,
                AttemptArtifactExpiration(
                    status="expired",
                    bundle_id=publication.publication_key,
                    request_digest=projection.request_digest,
                    producer_digest=projection.producer_digest,
                    redaction_policy_id=projection.redaction_policy_id,
                ),
            )
        raise RuntimeError(
            "durable artifact publication has not reached INDEXED or EXPIRED authority"
        )

    def _prepare_finalized_outcome(
        self,
        claim: AttemptClaim,
        publication: AttemptArtifactPublication,
    ) -> PreparedFinalizationSettlement:
        """Upgrade staged evidence only for the exact authoritative INDEXED receipt."""

        self._require_active_policy(claim)
        projection = self.staged_artifact_projection(claim)
        if publication.status is not FinalizationPhase.INDEXED:
            raise ValueError("artifact publication is not authoritatively indexed")
        expected = (
            projection.publication_key,
            projection.request_digest,
            projection.producer_digest,
            projection.redaction_policy_id,
        )
        actual = (
            publication.bundle_id,
            publication.request_digest,
            publication.producer_digest,
            publication.redaction_policy_id,
        )
        if actual != expected:
            raise ValueError("artifact publication receipt does not match the staged request")
        if type(publication.index_snapshot_id) is not int:
            raise ValueError("indexed artifact publication requires an exact integer snapshot")
        if not 1 <= publication.index_snapshot_id <= MAX_ICEBERG_SNAPSHOT_ID:
            raise ValueError(
                "indexed artifact publication snapshot is outside the positive signed 64-bit range"
            )
        for name, value in (
            ("bundle_id", publication.bundle_id),
            ("manifest_uri", publication.manifest_uri),
            ("request_digest", publication.request_digest),
            ("producer_digest", publication.producer_digest),
            ("redaction_policy_id", publication.redaction_policy_id),
        ):
            field_kind = "source_ref" if name == "manifest_uri" else "metadata"
            self._redaction_service.assert_safe_metadata(
                value,
                field=f"mission-attempt-artifact-publication.{field_kind}.{name}",
            )
        value = self._staged_outcome(claim)
        value.update(
            {
                "finalization_phase": FinalizationPhase.INDEXED.value,
                "finalization_manifest_ref": publication.manifest_uri,
                "finalization_bundle_id": publication.bundle_id,
                "finalization_request_digest": publication.request_digest,
                "finalization_producer_digest": publication.producer_digest,
                "finalization_redaction_policy_id": publication.redaction_policy_id,
                "finalization_index_snapshot_id": publication.index_snapshot_id,
                "finalization_error": "",
            }
        )
        finalized = self._rescan_enriched_outcome(claim, value)
        self._assert_safe_outcome_identity(finalized.value)
        assessment = self.assess_outcome(claim, finalized.value)
        self._validate_outcome(
            claim,
            settlement=assessment.attempt_status,
            outcome=finalized.value,
        )
        if assessment.attempt_status not in {
            AttemptStatus.ACCEPTED,
            AttemptStatus.REJECTED,
        }:
            raise ValueError("indexed artifact receipt did not produce a terminal mission outcome")
        return self._prepare_finalization_settlement(
            claim,
            kind="indexed",
            attempt_status=assessment.attempt_status,
            outcome=finalized,
        )

    def _prepare_expired_finalization_outcome(
        self,
        claim: AttemptClaim,
        expiration: AttemptArtifactExpiration,
    ) -> PreparedFinalizationSettlement:
        """Rescan staged evidence for terminal publication expiry settlement."""

        self._require_active_policy(claim)
        if not isinstance(expiration, AttemptArtifactExpiration):
            raise TypeError("expired finalization requires a typed artifact expiration receipt")
        projection = self.staged_artifact_projection(claim)
        expected = (
            "expired",
            projection.publication_key,
            projection.request_digest,
            projection.producer_digest,
            projection.redaction_policy_id,
        )
        actual = (
            expiration.status,
            expiration.bundle_id,
            expiration.request_digest,
            expiration.producer_digest,
            expiration.redaction_policy_id,
        )
        if actual != expected:
            raise ValueError("artifact expiration receipt does not match the staged request")
        value = self._staged_outcome(claim)
        phase = FinalizationPhase(str(value["finalization_phase"]))
        if phase is FinalizationPhase.INDEXED:
            raise ValueError("an indexed attempt cannot expire during artifact finalization")
        if any(
            str(value.get(field, "")).strip()
            for field in (
                "finalization_bundle_id",
                "finalization_request_digest",
                "finalization_producer_digest",
                "finalization_redaction_policy_id",
            )
        ):
            raise ValueError("expired artifact staging contains indexed authority")
        snapshot = value.get("finalization_index_snapshot_id", 0)
        if isinstance(snapshot, bool) or snapshot not in (None, "", 0, "0"):
            raise ValueError("expired artifact staging contains an index snapshot")
        value["finalization_error"] = _ARTIFACT_PUBLICATION_EXPIRED
        self._assert_safe_outcome_identity(value)
        rescanned = self._rescan_enriched_outcome(claim, value)
        assessment = self.assess_outcome(claim, rescanned.value)
        if assessment.attempt_status not in {
            AttemptStatus.INCOMPLETE,
            AttemptStatus.REJECTED,
        }:
            raise ValueError("expired artifact publication is not a terminal mission outcome")
        self._validate_outcome(
            claim,
            settlement=assessment.attempt_status,
            outcome=rescanned.value,
        )
        return self._prepare_finalization_settlement(
            claim,
            kind="expired",
            attempt_status=assessment.attempt_status,
            outcome=rescanned,
        )

    def _validate_artifact_publication_authority(
        self,
        claim: AttemptClaim,
        projection: AttemptArtifactProjection,
        publication: ArtifactPublicationRecord,
    ) -> None:
        """Authenticate one terminal artifact row against the staged mission claim."""

        identity = (
            publication.publication_key,
            publication.world_id,
            publication.run_id,
            publication.attempt_id,
            publication.idempotency_key,
        )
        expected_identity = (
            projection.publication_key,
            claim.world_id,
            claim.run_id,
            claim.attempt_id,
            claim.idempotency_key,
        )
        if identity != expected_identity:
            raise ValueError("durable artifact publication identity does not match the claim")
        if publication.request_json != projection.request_json:
            raise ValueError("durable artifact request does not match the staged request")
        if hashlib.sha256(publication.request_json.encode()).hexdigest() != (
            projection.request_digest
        ):
            raise ValueError("durable artifact request JSON does not match its staged digest")
        if publication.request_digest != projection.producer_digest:
            raise ValueError("durable artifact producer digest does not match the staged request")
        try:
            durable_request = json.loads(publication.request_json)
        except json.JSONDecodeError as exc:
            raise ValueError("durable artifact request JSON is invalid") from exc
        if not isinstance(durable_request, dict):
            raise ValueError("durable artifact request JSON must be an object")
        if str(durable_request.get("redaction_policy_id", "")) != (projection.redaction_policy_id):
            raise ValueError("durable artifact request changed the staged redaction policy")
        if publication.status in {"INDEXED", "EXPIRED"} and publication.completed_at is None:
            raise ValueError("terminal artifact publication lacks a completion timestamp")

        if publication.status == "INDEXED":
            if not publication.manifest_uri.strip() or not publication.records_json.strip():
                raise ValueError("indexed artifact publication lacks durable upload metadata")
            try:
                records = json.loads(publication.records_json)
            except json.JSONDecodeError as exc:
                raise ValueError("indexed artifact publication records are invalid") from exc
            if not isinstance(records, list) or not records:
                raise ValueError("indexed artifact publication requires durable index records")
            if type(publication.index_snapshot_id) is not int:
                raise ValueError("indexed artifact publication requires an exact integer snapshot")
            if not 1 <= publication.index_snapshot_id <= MAX_ICEBERG_SNAPSHOT_ID:
                raise ValueError(
                    "indexed artifact publication snapshot is outside the positive "
                    "signed 64-bit range"
                )
            return
        if publication.status == "EXPIRED":
            try:
                records = json.loads(publication.records_json)
            except json.JSONDecodeError as exc:
                raise ValueError("expired artifact publication records are invalid") from exc
            if (
                not isinstance(records, list)
                or records
                or publication.manifest_uri
                or type(publication.index_snapshot_id) is not int
                or publication.index_snapshot_id != 0
            ):
                raise ValueError("expired artifact publication contains indexed authority")
            return
        if publication.status not in {"PENDING", "UPLOADED"}:
            raise ValueError("durable artifact publication has an unknown status")

    def _prepare_finalization_settlement(
        self,
        claim: AttemptClaim,
        *,
        kind: FinalizationSettlementKind,
        attempt_status: AttemptStatus,
        outcome: RedactedRecord,
    ) -> PreparedFinalizationSettlement:
        seal = self._finalization_settlement_seal(
            claim,
            kind=kind,
            attempt_status=attempt_status,
            outcome=outcome,
        )
        return PreparedFinalizationSettlement(
            claim_key=claim.claim_key,
            world_id=claim.world_id,
            attempt_id=claim.attempt_id,
            claimant=claim.claimant,
            fence_epoch=claim.fence_epoch,
            attempt_status=attempt_status,
            kind=kind,
            outcome=outcome,
            seal=seal,
        )

    def _validate_prepared_finalization_settlement(
        self,
        claim: AttemptClaim,
        prepared: PreparedFinalizationSettlement,
    ) -> None:
        if claim.legacy_unbound:
            raise ValueError("legacy compatibility claims cannot use current finalization")
        if (
            prepared.claim_key != claim.claim_key
            or prepared.world_id != claim.world_id
            or prepared.attempt_id != claim.attempt_id
            or prepared.claimant != claim.claimant
            or prepared.fence_epoch != claim.fence_epoch
        ):
            raise ValueError("prepared finalization does not match its durable claim fence")
        expected_seal = self._finalization_settlement_seal(
            claim,
            kind=prepared.kind,
            attempt_status=prepared.attempt_status,
            outcome=prepared.outcome,
        )
        if not hmac.compare_digest(prepared.seal, expected_seal):
            raise ValueError("prepared finalization seal does not match its claim and outcome")
        try:
            phase = FinalizationPhase(str(prepared.value["finalization_phase"]))
        except (KeyError, ValueError) as exc:
            raise ValueError("prepared finalization has an invalid phase") from exc
        error = str(prepared.value.get("finalization_error", ""))
        if prepared.kind == "indexed":
            if phase is not FinalizationPhase.INDEXED or error:
                raise ValueError("indexed finalization preparation changed its authority")
            if prepared.attempt_status not in {
                AttemptStatus.ACCEPTED,
                AttemptStatus.REJECTED,
            }:
                raise ValueError("indexed finalization has an invalid terminal status")
        else:
            if phase is FinalizationPhase.INDEXED or error != _ARTIFACT_PUBLICATION_EXPIRED:
                raise ValueError("expired finalization preparation changed its evidence")
            if prepared.attempt_status not in {
                AttemptStatus.INCOMPLETE,
                AttemptStatus.REJECTED,
            }:
                raise ValueError("expired finalization has an invalid terminal status")

    def _finalization_settlement_seal(
        self,
        claim: AttemptClaim,
        *,
        kind: FinalizationSettlementKind,
        attempt_status: AttemptStatus,
        outcome: RedactedRecord,
    ) -> str:
        payload = {
            "domain": "archetype.mission-finalization-settlement.v1",
            "claim_key": claim.claim_key,
            "world_id": claim.world_id,
            "attempt_id": claim.attempt_id,
            "claimant": claim.claimant,
            "fence_epoch": claim.fence_epoch,
            "artifact_request_json": claim.artifact_request_json,
            "artifact_request_digest": claim.artifact_request_digest,
            "artifact_publication_key": claim.artifact_publication_key,
            "redaction_policy_id": claim.redaction_policy_id,
            "finalizing_at": claim.finalizing_at,
            "kind": kind,
            "attempt_status": attempt_status.value,
            "outcome": outcome.model_dump(mode="json"),
        }
        return hmac.new(
            self._finalization_seal_key,
            self._json(payload).encode(),
            hashlib.sha256,
        ).hexdigest()

    @classmethod
    def _staged_outcome(cls, claim: AttemptClaim) -> dict[str, Any]:
        if not claim.outcome_json or not claim.outcome_digest:
            raise ValueError("finalizing attempt claim lacks a staged outcome")
        if hashlib.sha256(claim.outcome_json.encode()).hexdigest() != claim.outcome_digest:
            raise ValueError("finalizing attempt claim outcome digest is corrupt")
        try:
            value = json.loads(claim.outcome_json)
        except json.JSONDecodeError as exc:
            raise ValueError("finalizing attempt claim outcome_json is invalid") from exc
        if not isinstance(value, dict):
            raise ValueError("finalizing attempt claim outcome_json must be an object")
        return value

    def _coerce_durable_outcome(
        self,
        claim: AttemptClaim,
        outcome: Mapping[str, Any] | RedactedRecord,
    ) -> RedactedRecord:
        if not isinstance(outcome, RedactedRecord):
            return self.prepare_durable_outcome(claim, outcome)
        self._require_active_policy(claim)
        if (
            outcome.receipt.policy_id != claim.redaction_policy_id
            or outcome.receipt.scope != "mission-attempt-outcome"
        ):
            raise ValueError("prepared attempt outcome has incompatible redaction evidence")
        canonical = json.loads(self._json(outcome.value))
        if not isinstance(canonical, dict):
            raise TypeError("prepared attempt outcome must be a JSON object")
        self._assert_safe_outcome_identity(canonical)
        defensive = self._redaction_service.redact_record(
            cast(Mapping[str, JsonValue], canonical),
            scope="mission-attempt-outcome",
        )
        if defensive.value != outcome.value:
            raise ValueError("prepared attempt outcome is not safe for durability")
        assessment = self.assess_outcome(claim, outcome.value)
        self._validate_outcome(
            claim,
            settlement=assessment.attempt_status,
            outcome=outcome.value,
        )
        return outcome

    def _rescan_enriched_outcome(
        self,
        claim: AttemptClaim,
        value: Mapping[str, Any],
    ) -> RedactedRecord:
        """Cover exact enriched bytes without erasing prior narrative findings."""

        evidence = self._parse_redaction_evidence(
            claim.redaction_evidence_json,
            redaction_policy_id=claim.redaction_policy_id,
        )
        prior_receipt = RedactionReceipt.model_validate(evidence["outcome"])
        rescanned = self._redaction_service.redact_record(
            cast(Mapping[str, JsonValue], value),
            scope="mission-attempt-outcome",
        )
        self._quarantine_if_redacted(rescanned.receipt)
        receipt = rescanned.receipt
        if prior_receipt.status == "redacted":
            receipt = prior_receipt.model_copy(
                update={"scanned_bytes": rescanned.receipt.scanned_bytes}
            )
        return rescanned.model_copy(update={"receipt": receipt})

    def _acquisition_redaction_evidence(
        self,
        *,
        request_json: str,
        capabilities: ProviderExecutionCapabilities,
    ) -> str:
        request_value = json.loads(request_json)
        if not isinstance(request_value, dict):
            raise TypeError("canonical mission attempt request must be a JSON object")
        request_scan = self._redaction_service.redact_record(
            cast(Mapping[str, JsonValue], request_value),
            scope="mission-attempt-request",
        )
        self._quarantine_if_redacted(request_scan.receipt)
        provider_scan = self._redaction_service.redact_record(
            cast(
                Mapping[str, JsonValue],
                {
                    "provider": capabilities.provider,
                    "provider_request_fingerprint": capabilities.request_fingerprint,
                    "supports_idempotent_replay": capabilities.supports_idempotent_replay,
                    "supports_session_resume": capabilities.supports_session_resume,
                    "provider_idempotency_key": capabilities.provider_idempotency_key,
                },
            ),
            scope="mission-attempt-provider-capabilities",
        )
        self._quarantine_if_redacted(provider_scan.receipt)
        return self._json(
            {
                "policy_id": self._redaction_service.policy_id,
                "request": request_scan.receipt.model_dump(mode="json"),
                "provider": provider_scan.receipt.model_dump(mode="json"),
                "acknowledgement": None,
                "outcome": None,
                "last_error": None,
            }
        )

    def _updated_redaction_evidence(
        self,
        claim: AttemptClaim,
        *,
        acknowledgement: RedactionReceipt | None = None,
        outcome: RedactionReceipt | None = None,
        last_error: RedactionReceipt | None = None,
    ) -> str:
        evidence = self._parse_redaction_evidence(
            claim.redaction_evidence_json,
            redaction_policy_id=claim.redaction_policy_id,
        )
        for name, receipt in (
            ("acknowledgement", acknowledgement),
            ("outcome", outcome),
            ("last_error", last_error),
        ):
            if receipt is not None:
                if receipt.policy_id != claim.redaction_policy_id:
                    raise ValueError("attempt redaction receipt policy changed during transition")
                evidence[name] = receipt.model_dump(mode="json")
        return self._json(evidence)

    @classmethod
    def _parse_redaction_evidence(
        cls,
        evidence_json: str,
        *,
        redaction_policy_id: str,
    ) -> dict[str, JsonValue]:
        try:
            value = json.loads(evidence_json)
        except json.JSONDecodeError as exc:
            raise ValueError("attempt claim redaction evidence is invalid JSON") from exc
        if not isinstance(value, dict) or set(value) != _REDACTION_EVIDENCE_KEYS:
            raise ValueError("attempt claim redaction evidence has an invalid schema")
        if value["policy_id"] != redaction_policy_id:
            raise ValueError("attempt claim redaction evidence policy is inconsistent")
        for name in ("request", "provider", "acknowledgement", "outcome", "last_error"):
            raw_receipt = value[name]
            if raw_receipt is None:
                continue
            try:
                receipt = RedactionReceipt.model_validate(raw_receipt)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"attempt claim {name} redaction receipt is invalid") from exc
            if receipt.policy_id != redaction_policy_id:
                raise ValueError("attempt claim redaction receipt policy is inconsistent")
            if receipt.scope != _REDACTION_EVIDENCE_SCOPES[name]:
                raise ValueError("attempt claim redaction receipt scope is inconsistent")
        return cast(dict[str, JsonValue], value)

    @staticmethod
    def _quarantine_if_redacted(receipt: RedactionReceipt) -> None:
        if receipt.status == "redacted":
            raise SecretQuarantineError(receipt.scope, receipt.rule_ids)

    def _require_active_policy(self, claim: AttemptClaim) -> None:
        if claim.status is not AttemptClaimStatus.SETTLED and (
            claim.redaction_policy_id != self._redaction_service.policy_id
        ):
            raise ValueError(
                "non-terminal attempt claim redaction policy differs from the active policy"
            )

    def _assert_safe_outcome_identity(self, outcome: Mapping[str, Any]) -> None:
        semantic_fields = set(_OUTCOME_IDENTITY_FIELDS)
        semantic_fields.update(
            field
            for field in outcome
            if field == "correlation" or field.endswith(("_ref", "_url", "_id", "_sha"))
        )
        for field in sorted(semantic_fields):
            if field not in outcome:
                continue
            raw_value = outcome[field]
            value = raw_value if isinstance(raw_value, str) else self._json(raw_value)
            field_kind = "source_ref" if field.endswith(("_ref", "_url")) else "metadata"
            self._redaction_service.assert_safe_metadata(
                value,
                field=f"mission-attempt-outcome.{field_kind}.{field}",
            )
        details = outcome["validator_details"]
        if not isinstance(details, (list, tuple)):
            raise ValueError("attempt validator evidence is malformed")
        for index, detail in enumerate(details):
            if not isinstance(detail, Mapping):
                raise ValueError("attempt validator evidence is malformed")
            self._redaction_service.assert_safe_metadata(
                str(detail.get("name", "")),
                field=f"mission-attempt-outcome.validator_details[{index}].name",
            )
            command = detail.get("command", ())
            if not isinstance(command, (list, tuple)):
                raise ValueError("attempt validator command evidence is malformed")
            for command_index, part in enumerate(command):
                self._redaction_service.assert_safe_metadata(
                    str(part),
                    field=(
                        "mission-attempt-outcome.validator_details"
                        f"[{index}].command[{command_index}]"
                    ),
                )
        results = outcome["results"]
        if not isinstance(results, Mapping):
            raise ValueError("attempt outcome results are malformed")
        for name in results:
            self._redaction_service.assert_safe_metadata(
                str(name),
                field="mission-attempt-outcome.results.name",
            )

    @classmethod
    def recover_request(cls, claim: AttemptClaim) -> MissionAttemptRequest:
        """Reconstruct the original request from durable claim state."""

        return cls._recover_request_json(claim.request_json)

    @classmethod
    def _recover_request_json(cls, request_json: str) -> MissionAttemptRequest:
        try:
            value = json.loads(request_json)
            contract_version = cls._request_contract_version(value)
            source = value["source"]
            validators = tuple(value["validators"])
            previous = tuple(value["previous_validator_details"])
            correlation = dict(value["correlation"])
            observation_tick = (
                0
                if contract_version == _LEGACY_CLAIM_CONTRACT_VERSION
                else value["observation_tick"]
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError("persisted attempt claim cannot reconstruct its request") from exc
        if any(not isinstance(item, dict) for item in validators + previous):
            raise ValueError("persisted attempt claim request evidence is malformed")
        request = MissionAttemptRequest(
            prompt=str(value["prompt"]),
            validators=validators,
            step_name=str(value["step_name"]),
            step_index=int(value["step_index"]),
            attempt_index=int(value["attempt_index"]),
            plan_digest=str(value["plan_digest"]),
            max_attempts=int(value["max_attempts"]),
            required_finalization_phase=FinalizationPhase(
                str(value["required_finalization_phase"])
            ),
            idempotency_key=str(value["idempotency_key"]),
            mission_id=str(value["mission_id"]),
            task_id=str(value["task_id"]),
            attempt_id=str(value["attempt_id"]),
            request_fingerprint=str(value["request_fingerprint"]),
            previous_session_id=str(value["previous_session_id"]),
            previous_validator_details=previous,
            correlation=correlation,
            source=MissionTaskState(
                MissionStatus(str(source["mission"])),
                TaskStatus(str(source["task"])),
            ),
            observation_tick=observation_tick,
        )
        expected_value = json.loads(cls._request_json(request))
        if contract_version == _LEGACY_CLAIM_CONTRACT_VERSION:
            expected_value.pop("claim_contract_version")
            expected_value.pop("observation_tick")
        else:
            expected_value["claim_contract_version"] = contract_version
        if cls._json(expected_value) != request_json:
            raise ValueError("persisted attempt claim request is not canonical")
        expected_fingerprint = mission_attempt_request_fingerprint(
            idempotency_key=request.idempotency_key,
            prompt=request.prompt,
            validators=request.validators,
            step_name=request.step_name,
            step_index=request.step_index,
            attempt_index=request.attempt_index,
            plan_digest=request.plan_digest,
            max_attempts=request.max_attempts,
            required_finalization_phase=request.required_finalization_phase,
            previous_session_id=request.previous_session_id,
            previous_validator_details=request.previous_validator_details,
            correlation=request.correlation,
        )
        if request.request_fingerprint != expected_fingerprint:
            raise ValueError("persisted mission attempt request fingerprint is corrupt")
        cls._validate_request(request)
        return request

    @classmethod
    def settled_outcome(cls, claim: AttemptClaim) -> Any:
        """Return and verify the terminal provider outcome retained for replay."""

        if claim.status is not AttemptClaimStatus.SETTLED or not claim.outcome_json:
            raise ValueError("attempt claim does not contain a settled outcome")
        if hashlib.sha256(claim.outcome_json.encode()).hexdigest() != claim.outcome_digest:
            raise ValueError("attempt claim outcome digest is corrupt")
        try:
            return json.loads(claim.outcome_json)
        except json.JSONDecodeError as exc:
            raise ValueError("attempt claim outcome_json is invalid") from exc

    @classmethod
    def outcome_digest(cls, outcome: Any) -> str:
        """Canonical digest used to make terminal settlement replay-safe."""

        return hashlib.sha256(cls._json(outcome).encode()).hexdigest()

    @classmethod
    def claim_key(
        cls,
        *,
        world_id: str,
        mission_id: str,
        task_id: str,
        attempt_id: str,
    ) -> str:
        return hashlib.sha256(
            cls._json(
                {
                    "domain": _CLAIM_DOMAIN,
                    "kind": "identity",
                    "world_id": world_id,
                    "mission_id": mission_id,
                    "task_id": task_id,
                    "attempt_id": attempt_id,
                }
            ).encode()
        ).hexdigest()

    async def _current_owned(
        self,
        claim: AttemptClaim,
        *,
        allow_settled: bool = False,
    ) -> AttemptClaim:
        record = await self._catalog.get_attempt_claim(claim.world_id, claim.claim_key)
        if record is None:
            raise ValueError(f"attempt claim {claim.claim_key} no longer exists")
        current = self._project(record)
        if current.claimant != claim.claimant or current.fence_epoch != claim.fence_epoch:
            raise ValueError("attempt claim lease was taken over by another fence")
        if current.status is AttemptClaimStatus.SETTLED and not allow_settled:
            raise ValueError("attempt claim is already settled")
        return current

    @classmethod
    def _claim_request_fingerprint(
        cls,
        request: MissionAttemptRequest,
        capabilities: ProviderExecutionCapabilities,
        *,
        redaction_policy_id: str,
    ) -> str:
        return hashlib.sha256(
            cls._json(
                {
                    "domain": _CLAIM_DOMAIN,
                    "kind": "request",
                    "mission_request_fingerprint": request.request_fingerprint,
                    "provider": capabilities.provider,
                    "provider_request_fingerprint": capabilities.request_fingerprint,
                    "supports_idempotent_replay": (capabilities.supports_idempotent_replay),
                    "supports_session_resume": capabilities.supports_session_resume,
                    "provider_idempotency_key": capabilities.provider_idempotency_key,
                    "redaction_policy_id": redaction_policy_id,
                }
            ).encode()
        ).hexdigest()

    @classmethod
    def _request_json(cls, request: MissionAttemptRequest) -> str:
        return cls._json(
            {
                "claim_contract_version": _CURRENT_CLAIM_CONTRACT_VERSION,
                "prompt": request.prompt,
                "validators": request.validators,
                "step_name": request.step_name,
                "step_index": request.step_index,
                "attempt_index": request.attempt_index,
                "plan_digest": request.plan_digest,
                "max_attempts": request.max_attempts,
                "required_finalization_phase": request.required_finalization_phase.value,
                "idempotency_key": request.idempotency_key,
                "mission_id": request.mission_id,
                "task_id": request.task_id,
                "attempt_id": request.attempt_id,
                "request_fingerprint": request.request_fingerprint,
                "previous_session_id": request.previous_session_id,
                "previous_validator_details": request.previous_validator_details,
                "correlation": request.correlation,
                "observation_tick": request.observation_tick,
                "source": {
                    "mission": request.source.mission.value,
                    "task": request.source.task.value,
                },
            }
        )

    @staticmethod
    def _request_contract_version(value: Mapping[str, Any]) -> int:
        raw_version = value.get(
            "claim_contract_version",
            _LEGACY_CLAIM_CONTRACT_VERSION,
        )
        if isinstance(raw_version, bool):
            raise ValueError("persisted attempt claim has an invalid contract version")
        try:
            version = int(raw_version)
        except (TypeError, ValueError) as exc:
            raise ValueError("persisted attempt claim has an invalid contract version") from exc
        if version not in {
            _LEGACY_CLAIM_CONTRACT_VERSION,
            _PRE_WORKTREE_ARCHIVE_CLAIM_CONTRACT_VERSION,
            _CURRENT_CLAIM_CONTRACT_VERSION,
        }:
            raise ValueError("persisted attempt claim has an unsupported contract version")
        return version

    @staticmethod
    def _request_world_run(request: MissionAttemptRequest) -> tuple[str, str]:
        world_id = str(request.correlation.get("world_id", "")).strip()
        run_id = str(request.correlation.get("run_id", "")).strip()
        entity_id = str(request.correlation.get("entity_id", "")).strip()
        if not world_id or not run_id or not entity_id:
            raise ValueError("mission attempt correlation requires world_id, run_id, and entity_id")
        if request.mission_id != f"{world_id}:{run_id}:{entity_id}":
            raise ValueError("mission attempt identity disagrees with its correlation")
        try:
            step_index = int(request.correlation["step_index"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("mission attempt correlation requires an integer step_index") from exc
        if step_index != request.step_index:
            raise ValueError("mission task identity disagrees with its correlation")
        return world_id, run_id

    @classmethod
    def _validate_request(cls, request: MissionAttemptRequest) -> None:
        cls._request_world_run(request)
        expected_attempt_id = hashlib.sha256(request.idempotency_key.encode()).hexdigest()
        if request.attempt_id != expected_attempt_id:
            raise ValueError("mission attempt_id disagrees with its idempotency key")
        expected_task_id = hashlib.sha256(
            f"{request.plan_digest}:{request.step_index}:{request.step_name}".encode()
        ).hexdigest()
        if request.task_id != expected_task_id:
            raise ValueError("mission task_id disagrees with its task request")
        if request.max_attempts < 1 or not 1 <= request.attempt_index <= request.max_attempts:
            raise ValueError("mission attempt counters are invalid")
        if normalize_attempt_validators(request.validators) != request.validators:
            raise ValueError("mission attempt validators are not canonical")
        expected_fingerprint = mission_attempt_request_fingerprint(
            idempotency_key=request.idempotency_key,
            prompt=request.prompt,
            validators=request.validators,
            step_name=request.step_name,
            step_index=request.step_index,
            attempt_index=request.attempt_index,
            plan_digest=request.plan_digest,
            max_attempts=request.max_attempts,
            required_finalization_phase=request.required_finalization_phase,
            previous_session_id=request.previous_session_id,
            previous_validator_details=request.previous_validator_details,
            correlation=request.correlation,
        )
        if request.request_fingerprint != expected_fingerprint:
            raise ValueError("mission attempt request fingerprint is invalid")

    @classmethod
    def assess_outcome(
        cls,
        claim: AttemptClaim,
        outcome: Mapping[str, Any],
    ) -> MissionAttemptAssessment:
        request = cls.recover_request(claim)
        if claim.contract_version < _CURRENT_CLAIM_CONTRACT_VERSION:
            return assess_pre_worktree_archive_outcome(request, outcome)
        return assess_attempt_outcome(request, outcome)

    @classmethod
    def _validate_outcome(
        cls,
        claim: AttemptClaim,
        *,
        settlement: AttemptStatus,
        outcome: Mapping[str, Any],
    ) -> None:
        assessment = cls.assess_outcome(claim, outcome)
        if settlement is not assessment.attempt_status:
            raise ValueError(
                "attempt settlement status disagrees with its authoritative mission outcome"
            )
        if assessment.provider_status is AttemptStatus.ACCEPTED and not claim.execution_consumed_at:
            raise ValueError("accepted provider outcome requires a consumed execution grant")
        cls._validate_provider_outcome_binding(
            provider=claim.provider,
            provider_session_id=claim.provider_session_id,
            outcome=outcome,
        )
        if assessment.finalization_phase is FinalizationPhase.INDEXED:
            cls._validate_indexed_outcome_binding(claim, outcome)

    @staticmethod
    def _validate_indexed_outcome_binding(
        claim: AttemptClaim,
        outcome: Mapping[str, Any],
    ) -> None:
        if not (
            claim.artifact_request_json
            and claim.artifact_request_digest
            and claim.artifact_publication_key
            and claim.finalizing_at
        ):
            raise ValueError("indexed attempt outcome lacks a staged artifact request")
        if str(outcome.get("finalization_bundle_id", "")) != claim.artifact_publication_key:
            raise ValueError("indexed attempt bundle does not match its staged publication key")
        if str(outcome.get("finalization_request_digest", "")) != claim.artifact_request_digest:
            raise ValueError("indexed attempt request digest does not match its staged request")
        if str(outcome.get("artifact_publication_key", "")) != claim.artifact_publication_key:
            raise ValueError("indexed attempt staged publication key changed after staging")
        if str(outcome.get("artifact_request_digest", "")) != claim.artifact_request_digest:
            raise ValueError("indexed attempt staged request digest changed after staging")
        if str(outcome.get("finalization_producer_digest", "")) != str(
            outcome.get("artifact_producer_digest", "")
        ):
            raise ValueError("indexed attempt producer digest changed after staging")
        if str(outcome.get("finalization_redaction_policy_id", "")) != str(
            outcome.get("artifact_redaction_policy_id", "")
        ):
            raise ValueError("indexed attempt redaction policy changed after staging")
        if str(outcome.get("finalization_redaction_policy_id", "")) != claim.redaction_policy_id:
            raise ValueError("indexed attempt redaction policy does not match its claim")

    @staticmethod
    def _validate_provider_outcome_binding(
        *,
        provider: str,
        provider_session_id: str,
        outcome: Mapping[str, Any],
    ) -> None:
        if str(outcome["checkpoint_provider"]) != provider:
            raise ValueError("attempt checkpoint provider does not match its claimed runner")
        if str(outcome["agent_session_id"]) != provider_session_id:
            raise ValueError("attempt agent session does not match its provider acknowledgement")

    @classmethod
    def _project(cls, record: AttemptClaimRecord) -> AttemptClaim:
        request = cls._recover_request_json(record.request_json)
        request_value = json.loads(record.request_json)
        if not isinstance(request_value, dict):
            raise ValueError("persisted attempt claim request must be an object")
        contract_version = cls._request_contract_version(request_value)
        if (
            record.run_id != str(request.correlation["run_id"])
            or record.mission_id != request.mission_id
            or record.task_id != request.task_id
            or record.attempt_id != request.attempt_id
            or record.idempotency_key != request.idempotency_key
        ):
            raise ValueError("persisted attempt claim disagrees with its durable request")
        expected_fingerprint = hashlib.sha256(
            cls._json(
                {
                    "domain": _CLAIM_DOMAIN,
                    "kind": "request",
                    "mission_request_fingerprint": request.request_fingerprint,
                    "provider": record.provider,
                    "provider_request_fingerprint": record.provider_request_fingerprint,
                    "supports_idempotent_replay": record.supports_idempotent_replay,
                    "supports_session_resume": record.supports_session_resume,
                    "provider_idempotency_key": record.provider_idempotency_key,
                    "redaction_policy_id": record.redaction_policy_id,
                }
            ).encode()
        ).hexdigest()
        if record.request_fingerprint != expected_fingerprint:
            raise ValueError("persisted attempt claim request fingerprint is corrupt")
        expected_key = cls.claim_key(
            world_id=record.world_id,
            mission_id=record.mission_id,
            task_id=record.task_id,
            attempt_id=record.attempt_id,
        )
        if record.claim_key != expected_key:
            raise ValueError("persisted attempt claim identity is corrupt")
        if record.supports_idempotent_replay != bool(record.provider_idempotency_key):
            raise ValueError("persisted provider idempotency capability is inconsistent")
        evidence = cls._parse_redaction_evidence(
            record.redaction_evidence_json,
            redaction_policy_id=record.redaction_policy_id,
        )
        for phase in ("request", "provider"):
            receipt = RedactionReceipt.model_validate(evidence[phase])
            if receipt.status != "clean":
                raise ValueError("attempt claim acquisition evidence contains a secret finding")
        status = AttemptClaimTransitionGraph.state(record.status)
        has_acknowledgement = bool(record.provider_session_id or record.provider_request_id)
        if (evidence["acknowledgement"] is not None) != has_acknowledgement:
            raise ValueError("provider acknowledgement and redaction evidence are inconsistent")
        if bool(record.acknowledged_at) != has_acknowledgement:
            raise ValueError("provider acknowledgement identity and timestamp are inconsistent")
        if has_acknowledgement and status not in {
            AttemptClaimStatus.PROVIDER_ACKNOWLEDGED,
            AttemptClaimStatus.FINALIZING,
            AttemptClaimStatus.SETTLED,
        }:
            raise ValueError("provider acknowledgement is invalid for the claim state")
        if has_acknowledgement and not record.execution_consumed_at:
            raise ValueError("provider acknowledgement lacks consumed execution evidence")
        has_outcome_evidence = evidence["outcome"] is not None
        if has_outcome_evidence != (
            status
            in {
                AttemptClaimStatus.FINALIZING,
                AttemptClaimStatus.SETTLED,
            }
        ):
            raise ValueError("attempt outcome state and redaction evidence are inconsistent")
        if (evidence["last_error"] is not None) != (status is AttemptClaimStatus.SETTLED):
            raise ValueError("attempt terminal state and error evidence are inconsistent")
        if (
            status
            in {
                AttemptClaimStatus.PROVIDER_ACKNOWLEDGED,
                AttemptClaimStatus.FINALIZING,
            }
            and not has_acknowledgement
        ):
            raise ValueError("provider-acknowledged claim lacks provider identity")
        if status is AttemptClaimStatus.CLAIMED and (
            record.execution_nonce or record.execution_consumed_at
        ):
            raise ValueError("unarmed attempt claim contains execution-grant evidence")
        if record.possibly_submitted_at and not record.execution_nonce:
            raise ValueError("armed attempt claim lacks its execution nonce")
        if record.execution_consumed_at and not record.execution_nonce:
            raise ValueError("consumed attempt claim lacks its execution nonce")
        artifact_values = (
            record.artifact_request_json,
            record.artifact_request_digest,
            record.artifact_publication_key,
        )
        if any(artifact_values) and not all(artifact_values):
            raise ValueError("attempt claim contains partial artifact request evidence")
        has_artifact_request = all(artifact_values)
        if status is AttemptClaimStatus.FINALIZING and not has_artifact_request:
            raise ValueError("finalizing attempt claim lacks its artifact request")
        if has_artifact_request and status not in {
            AttemptClaimStatus.FINALIZING,
            AttemptClaimStatus.SETTLED,
        }:
            raise ValueError("artifact request evidence is invalid for the claim state")
        if bool(record.finalizing_at) != has_artifact_request:
            raise ValueError("artifact request and finalizing timestamp are inconsistent")
        if record.legacy_unbound_eligible and (
            contract_version != _LEGACY_CLAIM_CONTRACT_VERSION
            or status is not AttemptClaimStatus.SETTLED
            or has_artifact_request
        ):
            raise ValueError("legacy unbound eligibility is inconsistent with the claim")
        # Early v8 migrations overmarked every settled v7 claim. Normalize that
        # durable compatibility bit at the read boundary: only an INDEXED gate
        # ever needed the legacy unbound authority exception.
        legacy_unbound_eligible = (
            record.legacy_unbound_eligible
            and request.required_finalization_phase is FinalizationPhase.INDEXED
        )

        settlement: AttemptStatus | None = None
        legacy_unbound = False
        if status is AttemptClaimStatus.SETTLED:
            try:
                settlement = AttemptStatus(record.settlement_status)
            except ValueError as exc:
                raise ValueError("settled attempt claim has invalid settlement status") from exc
            if settlement is AttemptStatus.PENDING or not record.outcome_digest:
                raise ValueError("settled attempt claim lacks terminal outcome evidence")
        elif record.settlement_status or record.settled_at:
            raise ValueError("non-terminal attempt claim contains settlement evidence")
        if status in {AttemptClaimStatus.FINALIZING, AttemptClaimStatus.SETTLED}:
            if not record.outcome_json:
                raise ValueError("durable attempt claim lacks replayable outcome JSON")
            if hashlib.sha256(record.outcome_json.encode()).hexdigest() != record.outcome_digest:
                raise ValueError("durable attempt claim outcome digest is corrupt")
            try:
                outcome = json.loads(record.outcome_json)
            except json.JSONDecodeError as exc:
                raise ValueError("durable attempt claim outcome_json is invalid") from exc
            if not isinstance(outcome, dict):
                raise ValueError("durable attempt claim outcome_json must be an object")
            legacy_candidate = (
                status is AttemptClaimStatus.SETTLED
                and settlement is AttemptStatus.ACCEPTED
                and not has_artifact_request
                and legacy_unbound_eligible
                and contract_version == _LEGACY_CLAIM_CONTRACT_VERSION
            )
            if legacy_candidate:
                # Migration provenance is exclusive. Authority-shaped extra
                # keys accepted by v7 remain inert terminal bytes and must
                # never be reinterpreted under the current indexed contract.
                assessment = assess_legacy_unbound_settled_outcome(request, outcome)
                legacy_unbound = True
            elif contract_version < _CURRENT_CLAIM_CONTRACT_VERSION:
                assessment = assess_pre_worktree_archive_outcome(request, outcome)
            else:
                assessment = assess_attempt_outcome(request, outcome)
            if (
                not legacy_unbound
                and assessment.finalization_phase is FinalizationPhase.INDEXED
                and not has_artifact_request
            ):
                raise ValueError("settled indexed claim lacks a staged artifact request")
            if (
                assessment.provider_status is AttemptStatus.ACCEPTED
                and not record.execution_consumed_at
            ):
                raise ValueError("settled accepted claim lacks execution-grant evidence")
            cls._validate_provider_outcome_binding(
                provider=record.provider,
                provider_session_id=record.provider_session_id,
                outcome=outcome,
            )
            projection: AttemptArtifactProjection | None = None
            if has_artifact_request:
                try:
                    projection = AttemptArtifactProjection(
                        request_json=record.artifact_request_json,
                        request_digest=record.artifact_request_digest,
                        publication_key=record.artifact_publication_key,
                        producer_digest=str(outcome["artifact_producer_digest"]),
                        redaction_policy_id=str(outcome["artifact_redaction_policy_id"]),
                    )
                except KeyError as exc:
                    raise ValueError(
                        "artifact-backed attempt claim lacks its prepared identity"
                    ) from exc
                if projection.redaction_policy_id != record.redaction_policy_id:
                    raise ValueError("artifact request redaction policy differs from its claim")
            if status is AttemptClaimStatus.FINALIZING:
                if request.required_finalization_phase is not FinalizationPhase.INDEXED:
                    raise ValueError("finalizing attempt claim does not require indexed evidence")
                if assessment.provider_status not in {
                    AttemptStatus.ACCEPTED,
                    AttemptStatus.REJECTED,
                }:
                    raise ValueError("finalizing attempt claim has an unsupported provider result")
                if assessment.finalization_phase not in {
                    FinalizationPhase.CAPTURED,
                    FinalizationPhase.CHECKPOINTED,
                    FinalizationPhase.UPLOADED,
                    FinalizationPhase.PUBLISHED,
                }:
                    raise ValueError("finalizing attempt claim has an invalid prepared phase")
                if not bool(outcome["checkpoint_restorable"]):
                    raise ValueError("finalizing attempt claim lacks recovery evidence")
                if assessment.provider_status is AttemptStatus.ACCEPTED and not str(outcome["sha"]):
                    raise ValueError("finalizing accepted claim lacks commit evidence")
            else:
                assert settlement is not None
                if assessment.attempt_status is not settlement:
                    raise ValueError(
                        "settled attempt claim status disagrees with its authoritative outcome"
                    )
                if has_artifact_request:
                    assert projection is not None
                    if outcome.get("finalization_error") == _ARTIFACT_PUBLICATION_EXPIRED:
                        if assessment.finalization_phase not in {
                            FinalizationPhase.CAPTURED,
                            FinalizationPhase.CHECKPOINTED,
                            FinalizationPhase.UPLOADED,
                            FinalizationPhase.PUBLISHED,
                        }:
                            raise ValueError(
                                "expired artifact-backed claim has an invalid finalization phase"
                            )
                        if settlement not in {
                            AttemptStatus.INCOMPLETE,
                            AttemptStatus.REJECTED,
                        }:
                            raise ValueError(
                                "expired artifact-backed claim has an invalid settlement status"
                            )
                        if (
                            str(outcome.get("artifact_publication_key", ""))
                            != record.artifact_publication_key
                            or str(outcome.get("artifact_request_digest", ""))
                            != record.artifact_request_digest
                        ):
                            raise ValueError(
                                "expired artifact-backed claim changed its staged request"
                            )
                        snapshot = outcome.get("finalization_index_snapshot_id", 0)
                        if (
                            any(
                                str(outcome.get(field, "")).strip()
                                for field in (
                                    "finalization_bundle_id",
                                    "finalization_request_digest",
                                    "finalization_producer_digest",
                                    "finalization_redaction_policy_id",
                                )
                            )
                            or isinstance(snapshot, bool)
                            or snapshot not in (None, "", 0, "0")
                        ):
                            raise ValueError(
                                "expired artifact-backed claim contains indexed authority"
                            )
                    else:
                        if assessment.finalization_phase is not FinalizationPhase.INDEXED:
                            raise ValueError("artifact-backed settled claim is not indexed")
                        if (
                            str(outcome["finalization_bundle_id"])
                            != record.artifact_publication_key
                        ):
                            raise ValueError("settled artifact bundle changed after staging")
                        if (
                            str(outcome["finalization_request_digest"])
                            != record.artifact_request_digest
                        ):
                            raise ValueError(
                                "settled artifact request digest changed after staging"
                            )
                        if (
                            str(outcome["finalization_producer_digest"])
                            != projection.producer_digest
                        ):
                            raise ValueError(
                                "settled artifact producer digest changed after staging"
                            )
                        if (
                            str(outcome["finalization_redaction_policy_id"])
                            != projection.redaction_policy_id
                        ):
                            raise ValueError(
                                "settled artifact redaction policy changed after staging"
                            )
        elif record.outcome_json or record.outcome_digest:
            raise ValueError("non-terminal attempt claim contains outcome JSON")
        if has_acknowledgement:
            acknowledgement = RedactionReceipt.model_validate(evidence["acknowledgement"])
            if acknowledgement.status != "clean":
                raise ValueError("provider acknowledgement contains a secret finding")
        return AttemptClaim(
            claim_key=record.claim_key,
            world_id=record.world_id,
            run_id=record.run_id,
            mission_id=record.mission_id,
            task_id=record.task_id,
            attempt_id=record.attempt_id,
            idempotency_key=record.idempotency_key,
            request_fingerprint=record.request_fingerprint,
            request_json=record.request_json,
            redaction_policy_id=record.redaction_policy_id,
            redaction_evidence_json=record.redaction_evidence_json,
            status=status,
            provider=record.provider,
            provider_request_fingerprint=record.provider_request_fingerprint,
            supports_idempotent_replay=record.supports_idempotent_replay,
            supports_session_resume=record.supports_session_resume,
            provider_idempotency_key=record.provider_idempotency_key,
            claimant=record.claimant,
            lease_expires_at=record.lease_expires_at,
            fence_epoch=record.fence_epoch,
            execution_nonce=record.execution_nonce,
            execution_consumed_at=record.execution_consumed_at,
            provider_session_id=record.provider_session_id,
            provider_request_id=record.provider_request_id,
            settlement_status=record.settlement_status,
            outcome_digest=record.outcome_digest,
            outcome_json=record.outcome_json,
            artifact_request_json=record.artifact_request_json,
            artifact_request_digest=record.artifact_request_digest,
            artifact_publication_key=record.artifact_publication_key,
            last_error=record.last_error,
            created_at=record.created_at,
            updated_at=record.updated_at,
            possibly_submitted_at=record.possibly_submitted_at,
            acknowledged_at=record.acknowledged_at,
            finalizing_at=record.finalizing_at,
            settled_at=record.settled_at,
            contract_version=contract_version,
            legacy_unbound_eligible=legacy_unbound_eligible,
            legacy_unbound=legacy_unbound,
        )

    @staticmethod
    def _decision(
        action: AttemptRecoveryAction,
        claim: AttemptClaim,
    ) -> AttemptRecoveryDecision:
        request = MissionAttemptClaimService.recover_request(claim)
        authorization = FencedExecutionAuthorization(
            action=action,
            claim_key=claim.claim_key,
            world_id=claim.world_id,
            run_id=claim.run_id,
            mission_id=claim.mission_id,
            task_id=claim.task_id,
            attempt_id=claim.attempt_id,
            idempotency_key=claim.idempotency_key,
            request_fingerprint=claim.request_fingerprint,
            sandbox_request_fingerprint=attempt_invocation_fingerprint(
                prompt=request.prompt,
                validators=request.validators,
                step_name=request.step_name,
                attempt_index=request.attempt_index,
                previous_session_id=request.previous_session_id,
                previous_validator_details=request.previous_validator_details,
                correlation=request.correlation,
            ),
            execution_nonce=claim.execution_nonce,
            claimant=claim.claimant,
            fence_epoch=claim.fence_epoch,
            lease_expires_at=claim.lease_expires_at,
            provider_session_id=claim.provider_session_id,
            provider_idempotency_key=claim.provider_idempotency_key,
        )
        return AttemptRecoveryDecision(action, claim, authorization)

    @classmethod
    def _nonexecuting_decision(cls, claim: AttemptClaim) -> AttemptRecoveryDecision:
        action = {
            AttemptClaimStatus.FINALIZING: AttemptRecoveryAction.FINALIZE,
            AttemptClaimStatus.SETTLED: AttemptRecoveryAction.SETTLED,
        }.get(claim.status, AttemptRecoveryAction.RECONCILE)
        return cls._decision(action, claim)

    @staticmethod
    def _settlement_matches(
        claim: AttemptClaim,
        *,
        settlement: AttemptStatus,
        outcome_digest: str,
        outcome_json: str,
        last_error: str,
    ) -> bool:
        return (
            claim.settlement_status == settlement.value
            and claim.outcome_digest == outcome_digest
            and claim.outcome_json == outcome_json
            and claim.last_error == last_error
        )

    @staticmethod
    def _staging_matches(
        claim: AttemptClaim,
        *,
        outcome_digest: str,
        outcome_json: str,
        projection: AttemptArtifactProjection,
    ) -> bool:
        return (
            claim.outcome_digest == outcome_digest
            and claim.outcome_json == outcome_json
            and claim.artifact_request_json == projection.request_json
            and claim.artifact_request_digest == projection.request_digest
            and claim.artifact_publication_key == projection.publication_key
        )

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
