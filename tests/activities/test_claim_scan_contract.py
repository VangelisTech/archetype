# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Claim-scan starvation contract for coordinator claim and result scans.

The retired implementations scanned one finite prefix of the pending set
(10,000 rows for the author and hosted coordinators, the 100-row default
for the critic coordinator).  When other workers held leases on the whole
prefix, a claimable Activity beyond it was stranded: the scan reported no
work until an unrelated change reshuffled the prefix.  These contracts pin
the shared replacement: page until exhaustion, never conclude from a
prefix.
"""

from __future__ import annotations

import pytest

from archetype.activities import (
    ActivityAdmission,
    ActivityClaim,
    ActivityResultRef,
    ActivitySnapshot,
    claim_next_pending,
    collect_pending_results,
)
from archetype.core.interfaces import CommittedTickReceipt

_WORLD_ID = "world-a"
_KIND = "contract.kind"


def _snapshot(activity_id: str) -> ActivitySnapshot:
    return ActivitySnapshot(
        admission=ActivityAdmission(
            activity_id=activity_id,
            kind=_KIND,
            source=CommittedTickReceipt(_WORLD_ID, "run-a", 1, "token-1", 0),
            input_ref=f"contract-input:{activity_id}",
            input_digest="0" * 64,
        )
    )


def _result_snapshot(activity_id: str) -> ActivitySnapshot:
    base = _snapshot(activity_id)
    return ActivitySnapshot(
        admission=base.admission,
        result=ActivityResultRef(
            ref=f"contract-result:{activity_id}",
            digest="1" * 64,
            media_type="application/json",
            size_bytes=1,
        ),
        result_attempt=1,
        result_fence=1,
    )


class _ScanCoordinator:
    """In-memory pending/claim surface honoring limit and offset.

    ``pending`` and ``pending_results`` slice a stable admission order the
    way the catalog's ``ORDER BY sequence LIMIT ? OFFSET ?`` queries do;
    ``claim`` acquires only rows no other worker holds a lease on.
    """

    def __init__(
        self,
        *,
        leased_ids: list[str],
        claimable_ids: list[str],
        result_ids: list[str] | None = None,
    ) -> None:
        self._order = [*leased_ids, *claimable_ids]
        self._leased = set(leased_ids)
        self._results = [_result_snapshot(rid) for rid in result_ids or []]
        self.pending_offsets: list[int] = []
        self.result_offsets: list[int] = []

    async def pending(
        self,
        *,
        kind: str | None = None,
        world_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[ActivitySnapshot, ...]:
        assert kind == _KIND and world_id == _WORLD_ID
        self.pending_offsets.append(offset)
        return tuple(_snapshot(aid) for aid in self._order[offset : offset + limit])

    async def claim(
        self,
        world_id: str,
        kind: str,
        activity_id: str,
        owner: str,
        *,
        lease_seconds: float = 300.0,
    ) -> ActivityClaim:
        assert kind == _KIND and world_id == _WORLD_ID
        if activity_id in self._leased:
            return ActivityClaim(snapshot=_snapshot(activity_id), acquired=False)
        return ActivityClaim(
            snapshot=_snapshot(activity_id),
            acquired=True,
            attempt=1,
            fence=1,
            owner=owner,
            lease_expires_at=lease_seconds,
        )

    async def pending_results(
        self,
        *,
        kind: str | None = None,
        world_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[ActivitySnapshot, ...]:
        assert kind == _KIND and world_id == _WORLD_ID
        self.result_offsets.append(offset)
        return tuple(self._results[offset : offset + limit])


@pytest.mark.asyncio
async def test_claim_scan_claims_beyond_the_retired_finite_prefix() -> None:
    """More leased rows than the old 10,000-row scan bound cannot strand work.

    Every row the retired prefix could see is leased by other workers; the
    single claimable Activity sits just beyond row 10,000 and must still be
    claimed.
    """

    coordinator = _ScanCoordinator(
        leased_ids=[f"leased-{index:05d}" for index in range(10_001)],
        claimable_ids=["claimable-beyond-the-prefix"],
    )

    claim = await claim_next_pending(
        coordinator,
        kind=_KIND,
        world_id=_WORLD_ID,
        owner="available-worker",
        lease_seconds=300.0,
    )

    assert claim is not None
    assert claim.acquired
    assert claim.activity_id == "claimable-beyond-the-prefix"


@pytest.mark.asyncio
async def test_claim_scan_reports_none_only_after_exhaustion() -> None:
    """No-work is honest only once every page of the pending set was scanned."""

    coordinator = _ScanCoordinator(
        leased_ids=[f"leased-{index:04d}" for index in range(2_500)],
        claimable_ids=[],
    )

    claim = await claim_next_pending(
        coordinator,
        kind=_KIND,
        world_id=_WORLD_ID,
        owner="available-worker",
        lease_seconds=300.0,
        page_size=1_000,
    )

    assert claim is None
    assert coordinator.pending_offsets == [0, 1_000, 2_000]


@pytest.mark.asyncio
async def test_result_scan_collects_beyond_one_page() -> None:
    """Durable results beyond the first page still reach delivery."""

    coordinator = _ScanCoordinator(
        leased_ids=[],
        claimable_ids=[],
        result_ids=[f"result-{index:04d}" for index in range(2_050)],
    )

    snapshots = await collect_pending_results(
        coordinator,
        kind=_KIND,
        world_id=_WORLD_ID,
        page_size=1_000,
    )

    assert len(snapshots) == 2_050
    assert coordinator.result_offsets == [0, 1_000, 2_000]
    assert snapshots[-1].admission.activity_id == "result-2049"


@pytest.mark.asyncio
async def test_scan_page_size_must_be_positive() -> None:
    coordinator = _ScanCoordinator(leased_ids=[], claimable_ids=[])

    with pytest.raises(ValueError, match="page size must be positive"):
        await claim_next_pending(
            coordinator,
            kind=_KIND,
            world_id=_WORLD_ID,
            owner="worker",
            lease_seconds=300.0,
            page_size=0,
        )
    with pytest.raises(ValueError, match="page size must be positive"):
        await collect_pending_results(
            coordinator,
            kind=_KIND,
            world_id=_WORLD_ID,
            page_size=0,
        )
