# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Durable pre-execution claim and recovery authority for mission attempts."""

from __future__ import annotations

import hashlib
import json
import secrets
import time
from collections.abc import Mapping
from typing import Any, cast

from pydantic import JsonValue

from archetype.app.missions.models import (
    AttemptClaim,
    AttemptClaimAcquisition,
    AttemptRecoveryDecision,
    FencedExecutionAuthorization,
    MissionAttemptRequest,
    ProviderExecutionCapabilities,
    attempt_invocation_fingerprint,
    mission_attempt_request_fingerprint,
    normalize_attempt_validators,
)
from archetype.app.missions.outcomes import assess_attempt_outcome
from archetype.app.missions.transitions import (
    AttemptClaimAcquireOutcome,
    AttemptClaimEvent,
    AttemptClaimStatus,
    AttemptClaimTransitionGraph,
    AttemptRecoveryAction,
    AttemptStatus,
    FinalizationPhase,
    MissionStatus,
    MissionTaskState,
    TaskStatus,
)
from archetype.app.redaction.interfaces import iRedactionService
from archetype.app.redaction.models import (
    RedactedRecord,
    RedactionReceipt,
    SecretQuarantineError,
)
from archetype.app.storage.catalog import (
    AttemptClaimConflictError,
    AttemptClaimRecord,
    AttemptClaimStaleError,
    ControlCatalog,
)

_CLAIM_DOMAIN = "archetype.mission-attempt-claim.v1"
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
    "context_ref",
    "sha",
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
        if existing is not None and existing.status is AttemptClaimStatus.SETTLED:
            expected = self._claim_request_fingerprint(
                request,
                capabilities,
                redaction_policy_id=existing.redaction_policy_id,
            )
            if existing.request_json != request_json or existing.request_fingerprint != expected:
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

    async def settle(
        self,
        claim: AttemptClaim,
        *,
        attempt_status: AttemptStatus | str,
        outcome: Mapping[str, Any] | RedactedRecord,
        last_error: str = "",
    ) -> AttemptClaim:
        """Terminally settle one claim through the edge matching its uncertainty."""

        try:
            settlement = AttemptStatus(attempt_status)
        except ValueError as exc:
            raise ValueError(f"invalid attempt settlement status: {attempt_status!r}") from exc
        if settlement is AttemptStatus.PENDING:
            raise ValueError("a pending attempt cannot terminally settle a claim")
        current = await self._current_owned(claim, allow_settled=True)
        if current.status is AttemptClaimStatus.SETTLED:
            if current.redaction_policy_id == self._redaction_service.policy_id:
                durable_outcome = self._coerce_durable_outcome(current, outcome)
                outcome_json = self._json(durable_outcome.value)
                last_error = self._redaction_service.redact_text(
                    last_error,
                    scope="mission-attempt-last-error",
                ).text[:_MAX_LAST_ERROR_CHARS]
            else:
                # A terminal record remains readable after a policy rollout,
                # but no old-policy record may be mutated or reinterpreted.
                replay_value = outcome.value if isinstance(outcome, RedactedRecord) else outcome
                outcome_json = self._json(replay_value)
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
            settlement=assess_attempt_outcome(self.recover_request(claim), outcome).attempt_status,
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
            settlement=assess_attempt_outcome(
                self.recover_request(claim), redacted.value
            ).attempt_status,
            outcome=redacted.value,
        )
        return redacted

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
        assessment = assess_attempt_outcome(self.recover_request(claim), outcome.value)
        self._validate_outcome(
            claim,
            settlement=assessment.attempt_status,
            outcome=outcome.value,
        )
        return outcome

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
            source = value["source"]
            validators = tuple(value["validators"])
            previous = tuple(value["previous_validator_details"])
            correlation = dict(value["correlation"])
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
        )
        if cls._request_json(request) != request_json:
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
                "source": {
                    "mission": request.source.mission.value,
                    "task": request.source.task.value,
                },
            }
        )

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
    def _validate_outcome(
        cls,
        claim: AttemptClaim,
        *,
        settlement: AttemptStatus,
        outcome: Mapping[str, Any],
    ) -> None:
        assessment = assess_attempt_outcome(cls.recover_request(claim), outcome)
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
            AttemptClaimStatus.SETTLED,
        }:
            raise ValueError("provider acknowledgement is invalid for the claim state")
        if has_acknowledgement and not record.execution_consumed_at:
            raise ValueError("provider acknowledgement lacks consumed execution evidence")
        has_terminal_evidence = (
            evidence["outcome"] is not None or evidence["last_error"] is not None
        )
        if has_terminal_evidence != (status is AttemptClaimStatus.SETTLED):
            raise ValueError("attempt terminal state and redaction evidence are inconsistent")
        if status is AttemptClaimStatus.PROVIDER_ACKNOWLEDGED and not has_acknowledgement:
            raise ValueError("provider-acknowledged claim lacks provider identity")
        if status is AttemptClaimStatus.CLAIMED and (
            record.execution_nonce or record.execution_consumed_at
        ):
            raise ValueError("unarmed attempt claim contains execution-grant evidence")
        if record.possibly_submitted_at and not record.execution_nonce:
            raise ValueError("armed attempt claim lacks its execution nonce")
        if record.execution_consumed_at and not record.execution_nonce:
            raise ValueError("consumed attempt claim lacks its execution nonce")
        if status is AttemptClaimStatus.SETTLED:
            try:
                settlement = AttemptStatus(record.settlement_status)
            except ValueError as exc:
                raise ValueError("settled attempt claim has invalid settlement status") from exc
            if settlement is AttemptStatus.PENDING or not record.outcome_digest:
                raise ValueError("settled attempt claim lacks terminal outcome evidence")
        elif record.settlement_status or record.outcome_digest or record.settled_at:
            raise ValueError("non-terminal attempt claim contains settlement evidence")
        if status is AttemptClaimStatus.SETTLED:
            if not record.outcome_json:
                raise ValueError("settled attempt claim lacks replayable outcome JSON")
            if hashlib.sha256(record.outcome_json.encode()).hexdigest() != record.outcome_digest:
                raise ValueError("settled attempt claim outcome digest is corrupt")
            try:
                outcome = json.loads(record.outcome_json)
            except json.JSONDecodeError as exc:
                raise ValueError("settled attempt claim outcome_json is invalid") from exc
            if not isinstance(outcome, dict):
                raise ValueError("settled attempt claim outcome_json must be an object")
            assessment = assess_attempt_outcome(request, outcome)
            if assessment.attempt_status is not settlement:
                raise ValueError(
                    "settled attempt claim status disagrees with its authoritative outcome"
                )
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
            if evidence["outcome"] is None or evidence["last_error"] is None:
                raise ValueError("settled attempt claim lacks terminal redaction evidence")
        elif record.outcome_json:
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
            last_error=record.last_error,
            created_at=record.created_at,
            updated_at=record.updated_at,
            possibly_submitted_at=record.possibly_submitted_at,
            acknowledged_at=record.acknowledged_at,
            settled_at=record.settled_at,
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
        action = (
            AttemptRecoveryAction.SETTLED
            if claim.status is AttemptClaimStatus.SETTLED
            else AttemptRecoveryAction.RECONCILE
        )
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
    def _json(value: Any) -> str:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
