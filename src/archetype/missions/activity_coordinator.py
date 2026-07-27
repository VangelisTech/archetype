# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Agent Mission adapter over generic durable activity coordination."""

from __future__ import annotations

from archetype.activities import (
    ActivityAdmission,
    ActivityClaim,
    ActivityConflictError,
    ActivityResultRef,
    ActivityRetryGuard,
    ActivitySettlement,
    claim_next_pending,
    collect_pending_results,
    iActivityCoordinator,
)
from archetype.core.interfaces import CommittedTickReceipt
from archetype.missions.activities import (
    AUTHOR_ACTIVITY_KIND,
    AuthorActivityRequestRef,
    AuthorActivityResultRef,
    AuthorActivityRetryGuard,
)
from archetype.missions.author_activity import (
    AuthorActivityClaim,
    AuthorActivityResultDelivery,
)

# Pending-scan page size, not a scan bound: claim and result scans page
# until the catalog is exhausted.  A module constant so tests can shrink
# it and prove the pagination property with few rows.
_CLAIM_SCAN_PAGE = 1_000


class MissionAuthorActivityCoordinator:
    """Translate generic lease mechanics into the author-workflow port."""

    def __init__(
        self,
        coordinator: iActivityCoordinator,
        *,
        lease_seconds: float = 300.0,
    ) -> None:
        if lease_seconds <= 0:
            raise ValueError("mission author activity lease must be positive")
        self._coordinator = coordinator
        self._lease_seconds = lease_seconds
        self._claims: dict[tuple[str, str, int], ActivityClaim] = {}

    async def admit_author(
        self,
        *,
        world_id: str,
        receipt: CommittedTickReceipt,
        activity_id: str,
        request: AuthorActivityRequestRef,
    ) -> None:
        if world_id != receipt.world_id:
            raise ValueError("author activity admission belongs to another world")
        admission = ActivityAdmission(
            activity_id=activity_id,
            kind=AUTHOR_ACTIVITY_KIND,
            source=receipt,
            input_ref=request.ref,
            input_digest=request.digest,
        )
        existing = await self._coordinator.get(
            kind=AUTHOR_ACTIVITY_KIND,
            world_id=world_id,
            activity_id=activity_id,
        )
        if existing is not None:
            self._validate_existing(existing.admission, admission)
            return
        try:
            await self._coordinator.admit(admission)
        except ActivityConflictError:
            existing = await self._coordinator.get(
                kind=AUTHOR_ACTIVITY_KIND,
                world_id=world_id,
                activity_id=activity_id,
            )
            if existing is None:
                raise
            self._validate_existing(existing.admission, admission)

    async def claim_author(
        self,
        *,
        world_id: str,
        owner: str,
    ) -> AuthorActivityClaim | None:
        # Page until the catalog is exhausted: the old finite 10,000-row
        # prefix stranded claimable author work behind leased rows.
        generic = await claim_next_pending(
            self._coordinator,
            kind=AUTHOR_ACTIVITY_KIND,
            world_id=world_id,
            owner=owner,
            lease_seconds=self._lease_seconds,
            page_size=_CLAIM_SCAN_PAGE,
        )
        if generic is None:
            return None
        return self._remember(generic)

    async def bind_provider_operation(
        self,
        claim: AuthorActivityClaim,
        *,
        provider: str,
        operation_id: str,
    ) -> AuthorActivityClaim:
        generic = await self._coordinator.bind_provider_operation(
            self._resolve(claim),
            provider,
            operation_id,
        )
        return self._remember(generic)

    async def confirm_provider_operation_absent(
        self,
        claim: AuthorActivityClaim,
        guard: AuthorActivityRetryGuard,
    ) -> AuthorActivityClaim:
        generic = await self._coordinator.confirm_provider_operation_absent(
            self._resolve(claim),
            ActivityRetryGuard(ref=guard.ref, digest=guard.digest),
            lease_seconds=self._lease_seconds,
        )
        return self._remember(generic)

    async def record_author_result(
        self,
        claim: AuthorActivityClaim,
        result: AuthorActivityResultRef,
    ) -> None:
        await self._coordinator.record_result(
            self._resolve(claim),
            ActivityResultRef(
                ref=result.ref,
                digest=result.digest,
                media_type=result.media_type,
                size_bytes=result.size_bytes,
            ),
        )

    async def pending_author_results(
        self,
        *,
        world_id: str,
    ) -> tuple[AuthorActivityResultDelivery, ...]:
        # Results-side twin of the claim scan: page to exhaustion so a
        # durable result beyond the first page still reaches observation.
        snapshots = await collect_pending_results(
            self._coordinator,
            kind=AUTHOR_ACTIVITY_KIND,
            world_id=world_id,
            page_size=_CLAIM_SCAN_PAGE,
        )
        deliveries: list[AuthorActivityResultDelivery] = []
        for snapshot in snapshots:
            result = snapshot.result
            if result is None:
                raise AssertionError("pending activity result has no result reference")
            admission = snapshot.admission
            deliveries.append(
                AuthorActivityResultDelivery(
                    world_id=admission.source.world_id,
                    activity_id=admission.activity_id,
                    request=AuthorActivityRequestRef(
                        ref=admission.input_ref,
                        digest=admission.input_digest,
                    ),
                    result=AuthorActivityResultRef(
                        ref=result.ref,
                        digest=result.digest,
                        media_type=result.media_type,
                        size_bytes=result.size_bytes,
                    ),
                )
            )
        return tuple(deliveries)

    async def settle_author_observation(
        self,
        *,
        world_id: str,
        activity_id: str,
        result_digest: str,
        receipt: CommittedTickReceipt,
    ) -> None:
        await self._coordinator.settle_observation(
            kind=AUTHOR_ACTIVITY_KIND,
            world_id=world_id,
            activity_id=activity_id,
            settlement=ActivitySettlement(
                receipt=receipt,
                result_digest=result_digest,
            ),
        )

    async def has_unsettled_work(self, world_id: str) -> bool:
        """Expose the generic world-scoped orphan-prevention oracle."""

        return await self._coordinator.has_unsettled(world_id)

    def _remember(self, claim: ActivityClaim) -> AuthorActivityClaim:
        if not claim.acquired or claim.attempt is None or claim.fence is None:
            raise ValueError("mission author adapter requires an acquired claim")
        operation_id = (
            claim.reconciles_provider_operation_id
            if claim.reconciliation_required
            else claim.provider_operation_id
        )
        provider = claim.reconciles_provider if claim.reconciliation_required else claim.provider
        semantic = AuthorActivityClaim(
            world_id=claim.world_id,
            activity_id=claim.activity_id,
            attempt=claim.attempt,
            fence=claim.fence,
            request=AuthorActivityRequestRef(
                ref=claim.snapshot.admission.input_ref,
                digest=claim.snapshot.admission.input_digest,
            ),
            provider=provider or "",
            provider_operation_id=operation_id or "",
            reconciliation_required=claim.reconciliation_required,
            retry_guard=(
                AuthorActivityRetryGuard(
                    ref=claim.retry_guard.ref,
                    digest=claim.retry_guard.digest,
                )
                if claim.retry_guard is not None
                else None
            ),
        )
        self._claims[(claim.world_id, claim.activity_id, claim.fence)] = claim
        return semantic

    def _resolve(self, claim: AuthorActivityClaim) -> ActivityClaim:
        try:
            return self._claims[(claim.world_id, claim.activity_id, claim.fence)]
        except KeyError:
            raise ValueError("author activity claim was not issued by this adapter") from None

    @staticmethod
    def _validate_existing(
        existing: ActivityAdmission,
        candidate: ActivityAdmission,
    ) -> None:
        if (
            existing.kind != candidate.kind
            or existing.input_ref != candidate.input_ref
            or existing.input_digest != candidate.input_digest
        ):
            raise ActivityConflictError(
                "mission author activity identity has different immutable request content"
            )


__all__ = ["MissionAuthorActivityCoordinator"]
