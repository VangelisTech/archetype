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

from collections.abc import Callable

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


def _snapshot(activity_id: str, sequence: int | None = None) -> ActivitySnapshot:
    return ActivitySnapshot(
        admission=ActivityAdmission(
            activity_id=activity_id,
            kind=_KIND,
            source=CommittedTickReceipt(_WORLD_ID, "run-a", 1, "token-1", 0),
            input_ref=f"contract-input:{activity_id}",
            input_digest="0" * 64,
        ),
        sequence=sequence,
    )


def _result_snapshot(activity_id: str, sequence: int | None = None) -> ActivitySnapshot:
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
        sequence=sequence,
    )


class _ScanCoordinator:
    """In-memory pending/claim surface honoring limit and the keyset cursor.

    ``pending`` and ``pending_results`` filter the current mutable set the
    way the catalog's ``WHERE sequence > ? ORDER BY sequence LIMIT ?``
    queries do; ``claim`` acquires only rows no other worker holds a lease
    on.  ``complete_pending`` and ``observe_result`` remove rows the way a
    concurrent worker's completion or observation does.
    """

    def __init__(
        self,
        *,
        leased_ids: list[str],
        claimable_ids: list[str],
        result_ids: list[str] | None = None,
    ) -> None:
        self._pending = {
            sequence: activity_id
            for sequence, activity_id in enumerate([*leased_ids, *claimable_ids], start=1)
        }
        self._leased = set(leased_ids)
        self._results = {
            sequence: _result_snapshot(rid, sequence)
            for sequence, rid in enumerate(result_ids or [], start=1)
        }
        self.pending_cursors: list[int] = []
        self.result_cursors: list[int] = []
        self.after_pending_page: Callable[[], None] | None = None
        self.after_result_page: Callable[[], None] | None = None

    def complete_pending(self, activity_id: str) -> None:
        self._pending = {
            sequence: aid for sequence, aid in self._pending.items() if aid != activity_id
        }

    def observe_result(self, activity_id: str) -> None:
        self._results = {
            sequence: snapshot
            for sequence, snapshot in self._results.items()
            if snapshot.admission.activity_id != activity_id
        }

    async def pending(
        self,
        *,
        kind: str | None = None,
        world_id: str | None = None,
        limit: int = 100,
        after_sequence: int = 0,
    ) -> tuple[ActivitySnapshot, ...]:
        assert kind == _KIND and world_id == _WORLD_ID
        self.pending_cursors.append(after_sequence)
        page = tuple(
            _snapshot(aid, sequence)
            for sequence, aid in sorted(self._pending.items())
            if sequence > after_sequence
        )[:limit]
        if self.after_pending_page is not None:
            self.after_pending_page()
        return page

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
        after_sequence: int = 0,
    ) -> tuple[ActivitySnapshot, ...]:
        assert kind == _KIND and world_id == _WORLD_ID
        self.result_cursors.append(after_sequence)
        page = tuple(
            snapshot
            for sequence, snapshot in sorted(self._results.items())
            if sequence > after_sequence
        )[:limit]
        if self.after_result_page is not None:
            self.after_result_page()
        return page


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
    assert coordinator.pending_cursors == [0, 1_000, 2_000]


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
    assert coordinator.result_cursors == [0, 1_000, 2_000]
    assert snapshots[-1].admission.activity_id == "result-2049"


@pytest.mark.asyncio
async def test_claim_scan_survives_mid_scan_completions() -> None:
    """Rows completed by other workers mid-scan cannot hide claimable work.

    The retired offset paging sliced the *current* pending set at an
    absolute position: when concurrent completions shrank the set between
    pages, the slice shifted past the still-claimable tail and the scan
    returned ``None`` to a drain loop that treats ``None`` as exhaustion.
    The keyset cursor keys pages by admission sequence, so removed rows
    never shift the rows behind them.
    """

    leased_ids = [f"leased-{index:02d}" for index in range(10)]
    coordinator = _ScanCoordinator(
        leased_ids=leased_ids,
        claimable_ids=["claimable-tail"],
    )

    def _complete_first_five() -> None:
        coordinator.after_pending_page = None
        for activity_id in leased_ids[:5]:
            coordinator.complete_pending(activity_id)

    coordinator.after_pending_page = _complete_first_five

    claim = await claim_next_pending(
        coordinator,
        kind=_KIND,
        world_id=_WORLD_ID,
        owner="draining-worker",
        lease_seconds=300.0,
        page_size=10,
    )

    assert claim is not None
    assert claim.acquired
    assert claim.activity_id == "claimable-tail"
    assert coordinator.pending_cursors == [0, 10]


@pytest.mark.asyncio
async def test_result_scan_survives_mid_scan_observations() -> None:
    """Results observed by other workers mid-scan cannot drop later results."""

    result_ids = [f"result-{index:02d}" for index in range(15)]
    coordinator = _ScanCoordinator(
        leased_ids=[],
        claimable_ids=[],
        result_ids=result_ids,
    )

    def _observe_first_five() -> None:
        coordinator.after_result_page = None
        for activity_id in result_ids[:5]:
            coordinator.observe_result(activity_id)

    coordinator.after_result_page = _observe_first_five

    snapshots = await collect_pending_results(
        coordinator,
        kind=_KIND,
        world_id=_WORLD_ID,
        page_size=10,
    )

    collected = [snapshot.admission.activity_id for snapshot in snapshots]
    assert collected == result_ids
    assert coordinator.result_cursors == [0, 10]


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
