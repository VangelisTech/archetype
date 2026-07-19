# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Credential-free proof of the mission INDEXED finalization gate."""

from __future__ import annotations

import asyncio
import hashlib
import json
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from archetype.app.application.mission_artifacts import MissionArtifactFinalizer
from archetype.app.artifacts.bundle_models import (
    ArtifactBundleRequest,
    ArtifactPublicationStatus,
    ArtifactPublishReceipt,
    ArtifactStoreConfig,
    PreparedArtifactBundleRequest,
)
from archetype.app.artifacts.bundle_service import ArtifactBundleService
from archetype.app.missions import (
    AttemptClaimAcquireOutcome,
    AttemptClaimStatus,
    AttemptRecoveryAction,
    MissionAttemptClaimService,
    MissionAttemptExecutionService,
    MissionAttemptRequest,
    MissionService,
    ProviderExecutionCapabilities,
    attempt_invocation_fingerprint,
)
from archetype.app.redaction.service import RedactionService
from archetype.app.storage.catalog import SqliteControlCatalog
from archetype.core.config import StorageConfig
from evals.graders import state_check
from evals.harness import EvalHarness
from evals.types import GraderResult

SUITE = "capability"
_INDEX_SNAPSHOT_ID = 73
_INDEXED_MANIFEST = "eval://artifact-index/manifest.json"


class _ForbiddenArtifactIO:
    """Fail if credential-free preparation reaches an I/O collaborator."""

    def __init__(self) -> None:
        self.accesses = 0

    def __getattr__(self, name: str) -> Any:
        self.accesses += 1
        raise AssertionError(f"artifact preparation unexpectedly accessed {name}")


class _ReceiptArtifactBundlePort:
    """Use real preparation, then expose one controlled publication receipt."""

    def __init__(
        self,
        preparer: ArtifactBundleService,
        catalog: SqliteControlCatalog,
        *,
        status: ArtifactPublicationStatus,
    ) -> None:
        self._preparer = preparer
        self._catalog = catalog
        self._status = status
        self.prepare_calls = 0
        self.publish_calls = 0
        self.preparations: list[PreparedArtifactBundleRequest] = []
        self.published: list[PreparedArtifactBundleRequest] = []

    @property
    def enabled(self) -> bool:
        return True

    def prepare(self, request: ArtifactBundleRequest) -> PreparedArtifactBundleRequest:
        self.prepare_calls += 1
        prepared = self._preparer.prepare(request)
        self.preparations.append(prepared)
        return prepared

    async def publish(
        self,
        request: ArtifactBundleRequest,
        *,
        storage_config: StorageConfig | None = None,
    ) -> ArtifactPublishReceipt:
        _ = request, storage_config
        raise AssertionError("mission finalization must publish its persisted preparation")

    async def publish_prepared(
        self,
        prepared: PreparedArtifactBundleRequest,
        *,
        storage_config: StorageConfig | None = None,
    ) -> ArtifactPublishReceipt:
        _ = storage_config
        self.publish_calls += 1
        self.published.append(prepared)
        request = ArtifactBundleRequest.model_validate_json(prepared.request_json)
        claimant = f"eval-artifact-{self._status.value}"
        _, publication = await self._catalog.acquire_artifact_publication(
            world_id=request.world_id,
            run_id=request.run_id,
            attempt_id=request.attempt_id,
            idempotency_key=request.idempotency_key,
            request_digest=prepared.producer_digest,
            request_json=prepared.request_json,
            claimant=claimant,
            retry_until_ms=10**15,
            lease_seconds=0.05,
        )
        if publication.status == "PENDING":
            await self._catalog.record_artifact_uploads(
                request.world_id,
                publication.publication_key,
                claimant,
                json.dumps([{"kind": "bundle_manifest"}]),
                _INDEXED_MANIFEST,
            )
            publication = await self._catalog.get_artifact_publication(
                request.world_id,
                publication.publication_key,
            )
            assert publication is not None
        if self._status is ArtifactPublicationStatus.INDEXED:
            await self._catalog.complete_artifact_publication(
                request.world_id,
                publication.publication_key,
                claimant,
                _INDEX_SNAPSHOT_ID,
            )
        return ArtifactPublishReceipt(
            bundle_id=prepared.publication_key,
            world_id=request.world_id,
            run_id=request.run_id,
            attempt_id=request.attempt_id,
            status=self._status,
            manifest_uri=_INDEXED_MANIFEST,
            index_snapshot_id=(
                _INDEX_SNAPSHOT_ID if self._status is ArtifactPublicationStatus.INDEXED else 0
            ),
            request_digest=prepared.request_digest,
            producer_digest=prepared.producer_digest,
            redaction_policy_id=prepared.redaction_policy_id,
        )

    async def query(
        self,
        world_id: str,
        run_id: str,
        *,
        attempt_id: str | None = None,
        kinds: list[str] | None = None,
    ) -> Any:
        _ = world_id, run_id, attempt_id, kinds
        raise AssertionError("the finalization gate must not query the artifact index")

    async def reconcile(
        self,
        world_id: str,
        *,
        storage_config: StorageConfig | None = None,
        limit: int = 100,
    ) -> Any:
        _ = world_id, storage_config, limit
        raise AssertionError("the finalization gate must not invoke the reconciler")


class _MissionProbe(MissionService):
    """Record the evidence phase at every attempted world-state projection."""

    def __init__(self) -> None:
        super().__init__()
        self.applied_phases: list[str] = []

    def apply_attempt(
        self,
        row: Mapping[str, Any],
        request: MissionAttemptRequest,
        outcome: Mapping[str, Any],
    ) -> dict[str, Any]:
        self.applied_phases.append(str(outcome.get("finalization_phase", "")))
        return super().apply_attempt(row, request, outcome)

    def _apply_settled_attempt(
        self,
        row: Mapping[str, Any],
        request: MissionAttemptRequest,
        outcome: Mapping[str, Any],
        claim: Any,
    ) -> dict[str, Any]:
        self.applied_phases.append(str(outcome.get("finalization_phase", "")))
        return super()._apply_settled_attempt(row, request, outcome, claim)


class _AcceptedRunner:
    """Deterministic local provider stand-in for the initial submission."""

    def __init__(self, request: MissionAttemptRequest) -> None:
        self._request = request
        self.run_calls = 0

    @property
    def provider_execution_capabilities(self) -> ProviderExecutionCapabilities:
        return _capabilities()

    async def run_attempt(self, **kwargs: Any) -> dict[str, Any]:
        self.run_calls += 1
        await kwargs["authorize_execution"](kwargs["authorization"])
        await kwargs["acknowledge_provider"](
            "session-indexed-eval",
            "request-indexed-eval",
        )
        return _accepted_outcome(self._request)


class _NoRunRunner:
    """Prove FINALIZING recovery and terminal replay perform no inference."""

    def __init__(self) -> None:
        self.run_calls = 0

    @property
    def provider_execution_capabilities(self) -> ProviderExecutionCapabilities:
        return _capabilities()

    async def run_attempt(self, **_: Any) -> dict[str, Any]:
        self.run_calls += 1
        raise AssertionError("cold FINALIZING recovery must not invoke the runner/model")


def _capabilities() -> ProviderExecutionCapabilities:
    return ProviderExecutionCapabilities(
        provider="indexed-eval",
        request_fingerprint="indexed-finalization-capability-v1",
    )


def _mission_row() -> dict[str, Any]:
    return {
        "world_id": "world-indexed-eval",
        "run_id": "run-indexed-eval",
        "entity_id": 502,
        "mission__plan_json": json.dumps(
            [
                {
                    "name": "publish-evidence",
                    "prompt": "Produce a validated, checkpointed change.",
                    "validators": [{"name": "tests", "command": ["pytest"]}],
                }
            ]
        ),
        "mission__status": "ready",
        "mission__finished": False,
        "mission__succeeded": False,
        "mission__failure_reason": "",
        "mission__pr_url": "",
        "taskgate__step_index": 0,
        "taskgate__attempts": 0,
        "taskgate__max_attempts": 3,
        "taskgate__status": "ready",
        "taskgate__required_finalization_phase": "indexed",
        "attempt__agent_session_id": "",
        "attempt__validator_details_json": "[]",
        "frictionlog__entries_json": "[]",
    }


def _accepted_outcome(request: MissionAttemptRequest) -> dict[str, Any]:
    return {
        "attempt_id": request.attempt_id,
        "attempt_index": request.attempt_index,
        "idempotency_key": request.idempotency_key,
        "request_fingerprint": attempt_invocation_fingerprint(
            prompt=request.prompt,
            validators=request.validators,
            step_name=request.step_name,
            attempt_index=request.attempt_index,
            previous_session_id=request.previous_session_id,
            previous_validator_details=request.previous_validator_details,
            correlation=request.correlation,
        ),
        "status": "accepted",
        "accepted": True,
        "harness": "indexed-finalization-eval",
        "agent_session_id": "session-indexed-eval",
        "validator_details": [
            {
                "name": "tests",
                "command": ["pytest"],
                "expected_returncode": 0,
                "returncode": 0,
                "passed": True,
                "stdout": "",
                "stderr": "",
            }
        ],
        "checkpoint_provider": "indexed-eval",
        "checkpoint_status": "ready",
        "checkpoint_restorable": True,
        "checkpoint_created_at_ms": 1,
        "checkpoint_expires_at_ms": 2,
        "sandbox_state_ref": "eval://checkpoint/indexed-attempt",
        "finalization_phase": "checkpointed",
        "finalization_manifest_ref": "eval://attempt/manifest.json",
        "finalization_error": "",
        "results": {"tests": True},
        "trace_ref": "eval://attempt/agent-output.jsonl",
        "traces_ref": "eval://attempt/traces",
        "live_status_ref": "eval://attempt/live-session.json",
        "live_events_ref": "eval://attempt/live-events.jsonl",
        "filesystem_start_ref": "eval://recovery/filesystem-start.jsonl",
        "filesystem_end_ref": "eval://recovery/filesystem-end.jsonl",
        "filesystem_diff_ref": "eval://recovery/filesystem-diff.jsonl",
        "git_status_ref": "eval://recovery/git-status.txt",
        "git_patch_ref": "eval://recovery/worktree.patch",
        "git_bundle_ref": "eval://recovery/repository.bundle",
        "context_ref": "eval://context",
        "friction": [],
        "sha": "e" * 40,
        "message": "fix: publish indexed attempt evidence",
        "pushed": False,
    }


def _artifact_port(
    root: Path,
    redaction_service: RedactionService,
    catalog: SqliteControlCatalog,
    *,
    status: ArtifactPublicationStatus,
) -> tuple[_ReceiptArtifactBundlePort, _ForbiddenArtifactIO]:
    forbidden = _ForbiddenArtifactIO()
    forbidden_port: Any = forbidden
    preparer = ArtifactBundleService(
        forbidden_port,
        forbidden_port,
        ArtifactStoreConfig.local(root / "artifact-store"),
        forbidden_port,
        redaction_service=redaction_service,
    )
    return _ReceiptArtifactBundlePort(preparer, catalog, status=status), forbidden


def task_mission_indexed_finalization_gate() -> list[GraderResult]:
    """Hold mission progress until cold recovery receives exact INDEXED evidence."""

    return asyncio.run(_task_mission_indexed_finalization_gate())


async def _task_mission_indexed_finalization_gate() -> list[GraderResult]:
    row = _mission_row()
    initial_missions = _MissionProbe()
    request = initial_missions.prepare_attempt(row, tick=1)
    assert request is not None

    with tempfile.TemporaryDirectory(prefix="archetype-indexed-finalization-eval-") as root:
        root_path = Path(root)
        catalog_path = root_path / "control.db"
        initial_redaction = RedactionService()
        initial_catalog = SqliteControlCatalog(catalog_path)
        initial_claims = MissionAttemptClaimService(
            initial_catalog,
            redaction_service=initial_redaction,
        )
        uploaded_port, initial_forbidden = _artifact_port(
            root_path / "initial",
            initial_redaction,
            initial_catalog,
            status=ArtifactPublicationStatus.UPLOADED,
        )
        initial_runner = _AcceptedRunner(request)
        gate_error = ""
        try:
            await MissionAttemptExecutionService(
                initial_claims,
                initial_missions,
                MissionArtifactFinalizer(uploaded_port),
            ).run(
                row,
                tick=1,
                claimant="worker-before-index",
                runner=initial_runner,
                lease_seconds=0.05,
            )
        except RuntimeError as exc:
            gate_error = str(exc)

        claim_key = initial_claims.claim_key(
            world_id=str(row["world_id"]),
            mission_id=request.mission_id,
            task_id=request.task_id,
            attempt_id=request.attempt_id,
        )
        staged = await initial_claims.get(str(row["world_id"]), claim_key)
        assert staged is not None
        staged_projection = initial_claims.staged_artifact_projection(staged)
        staged_outcome = json.loads(staged.outcome_json)
        prepared = uploaded_port.preparations[0]
        await initial_catalog.close()
        await asyncio.sleep(0.06)

        cold_redaction = RedactionService()
        cold_catalog = SqliteControlCatalog(catalog_path)
        cold_claims = MissionAttemptClaimService(
            cold_catalog,
            redaction_service=cold_redaction,
        )
        indexed_port, cold_forbidden = _artifact_port(
            root_path / "cold",
            cold_redaction,
            cold_catalog,
            status=ArtifactPublicationStatus.INDEXED,
        )
        cold_missions = _MissionProbe()
        cold_runner = _NoRunRunner()
        cold_execution_service = MissionAttemptExecutionService(
            cold_claims,
            cold_missions,
            MissionArtifactFinalizer(indexed_port),
        )
        recovered = await cold_execution_service.run(
            row,
            tick=2,
            claimant="worker-after-restart",
            runner=cold_runner,
            lease_seconds=1,
        )
        assert recovered is not None
        settled_at = recovered.claim.settled_at

        duplicate = await cold_execution_service.run(
            row,
            tick=3,
            claimant="worker-after-settlement",
            runner=cold_runner,
            lease_seconds=1,
        )
        assert duplicate is not None
        terminal_noop = await cold_execution_service.run(
            recovered.updated_row,
            tick=4,
            claimant="worker-after-world-commit",
            runner=cold_runner,
            lease_seconds=1,
        )
        persisted = await cold_claims.get(str(row["world_id"]), claim_key)
        remaining_due = await cold_claims.list_due(str(row["world_id"]), now=10**12)
        await cold_catalog.close()

    published = indexed_port.published[0]
    outcome = recovered.outcome
    updated = recovered.updated_row
    exact_request_digest = hashlib.sha256(staged_projection.request_json.encode()).hexdigest()
    parsed_request = ArtifactBundleRequest.model_validate_json(staged_projection.request_json)

    return [
        state_check(
            {
                "uploaded_row_is_rejected": ("indexed" in gate_error.lower()),
                "claim_stays_finalizing": (
                    staged.status is AttemptClaimStatus.FINALIZING and staged.settled_at is None
                ),
                "mission_does_not_project_before_index": initial_missions.applied_phases == [],
                "original_row_does_not_advance": (
                    row["taskgate__attempts"] == 0
                    and not row["mission__finished"]
                    and not row["mission__succeeded"]
                ),
                "preparation_precedes_publication": (
                    initial_runner.run_calls == 1
                    and uploaded_port.prepare_calls == 1
                    and uploaded_port.publish_calls == 1
                    and uploaded_port.published == [prepared]
                ),
                "preparation_is_credential_free": initial_forbidden.accesses == 0,
            },
            name="indexed_row_gate_holds",
        ),
        state_check(
            {
                "cold_claim_is_recovered": (
                    recovered.acquisition.outcome is AttemptClaimAcquireOutcome.RECOVERED
                    and recovered.decision.action is AttemptRecoveryAction.FINALIZE
                    and recovered.replayed
                ),
                "runner_model_is_not_invoked": cold_runner.run_calls == 0,
                "stored_projection_is_not_reprepared": indexed_port.prepare_calls == 0,
                "stored_projection_is_published_exactly": (
                    indexed_port.publish_calls == 1
                    and published.request_json == staged_projection.request_json
                    and published.request_digest == staged_projection.request_digest
                    and published.publication_key == staged_projection.publication_key
                    and published.producer_digest == staged_projection.producer_digest
                    and published.redaction_policy_id == staged_projection.redaction_policy_id
                ),
                "cold_path_is_credential_free": cold_forbidden.accesses == 0,
            },
            name="cold_finalizing_recovery",
        ),
        state_check(
            {
                "request_digest_authenticates_exact_json": (
                    exact_request_digest == staged_projection.request_digest
                    and parsed_request.canonical_json() == staged_projection.request_json
                ),
                "producer_digest_is_exact": (
                    parsed_request.producer_digest() == staged_projection.producer_digest
                    and outcome["finalization_producer_digest"] == staged_projection.producer_digest
                ),
                "policy_is_exact": (
                    staged_projection.redaction_policy_id == cold_redaction.policy_id
                    and outcome["finalization_redaction_policy_id"]
                    == staged_projection.redaction_policy_id
                    and staged_outcome["artifact_redaction_policy_id"]
                    == staged_projection.redaction_policy_id
                ),
                "indexed_snapshot_is_exact": (
                    outcome["finalization_index_snapshot_id"] == _INDEX_SNAPSHOT_ID
                    and updated["finalization__index_snapshot_id"] == _INDEX_SNAPSHOT_ID
                    and outcome["finalization_manifest_ref"] == _INDEXED_MANIFEST
                ),
                "publication_key_is_exact": (
                    outcome["finalization_bundle_id"] == staged_projection.publication_key
                    and updated["finalization__bundle_id"] == staged_projection.publication_key
                    and outcome["finalization_request_digest"] == staged_projection.request_digest
                ),
            },
            name="indexed_evidence_identity",
        ),
        state_check(
            {
                "claim_settles_once": (
                    persisted is not None
                    and persisted.status is AttemptClaimStatus.SETTLED
                    and bool(settled_at)
                    and persisted.settled_at == settled_at
                    and duplicate.claim.settled_at == settled_at
                ),
                "mission_advances_once": (
                    updated["taskgate__attempts"] == 1
                    and updated["mission__finished"]
                    and updated["mission__succeeded"]
                    and duplicate.updated_row == updated
                    and terminal_noop is None
                ),
                "only_indexed_outcomes_are_applied": (
                    bool(cold_missions.applied_phases)
                    and set(cold_missions.applied_phases) == {"indexed"}
                ),
                "terminal_replay_is_side_effect_free": (
                    duplicate.acquisition.outcome is AttemptClaimAcquireOutcome.DUPLICATE
                    and duplicate.decision.action is AttemptRecoveryAction.SETTLED
                    and duplicate.outcome == outcome
                    and indexed_port.publish_calls == 1
                    and cold_runner.run_calls == 0
                ),
                "settled_claim_is_not_due": remaining_due == [],
            },
            name="indexed_settlement_exactly_once",
        ),
    ]


def register(harness: EvalHarness) -> None:
    harness.add(
        "mission_indexed_finalization_gate",
        suite=SUITE,
        fn=task_mission_indexed_finalization_gate,
        desc=(
            "INDEXED evidence gates mission progress and cold FINALIZING recovery "
            "settles exact staged identity without another runner/model call"
        ),
    )
