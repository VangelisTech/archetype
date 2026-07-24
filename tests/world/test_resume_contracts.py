# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for world-owned durable resume interpretation."""

from __future__ import annotations

from importlib import import_module

import pytest

from archetype.storage.catalog import SignatureRecord
from archetype.storage.service import PinnedVisibility

pytestmark = pytest.mark.contract("world.tick.atomic_visibility")


def _signature(table_id: str) -> SignatureRecord:
    return SignatureRecord(
        table_id=table_id,
        component_names=(table_id,),
        schema_json="{}",
        fingerprint=f"fingerprint:{table_id}",
    )


def test_latest_row_wins_and_same_tick_active_row_wins() -> None:
    resume = import_module("archetype.world.resume")
    apply_row_to_directory = resume.apply_row_to_directory

    first = _signature("first")
    second = _signature("second")
    directory: dict[int, SignatureRecord] = {}
    latest_seen: dict[int, int] = {}

    assert (
        apply_row_to_directory(
            directory,
            latest_seen,
            first,
            {"entity_id": -1, "tick": 99, "is_active": True},
        )
        is None
    )
    assert directory == {}
    assert latest_seen == {}

    assert (
        apply_row_to_directory(
            directory,
            latest_seen,
            first,
            {"entity_id": 7, "tick": 2, "is_active": True},
        )
        == 7
    )
    assert directory == {7: first}

    apply_row_to_directory(
        directory,
        latest_seen,
        first,
        {"entity_id": 7, "tick": 3, "is_active": False},
    )
    assert directory == {}

    # Migration writes an inactive row in the old table and an active row in
    # the new table at one tick. Active must win regardless of table order.
    apply_row_to_directory(
        directory,
        latest_seen,
        second,
        {"entity_id": 7, "tick": 4, "is_active": True},
    )
    apply_row_to_directory(
        directory,
        latest_seen,
        first,
        {"entity_id": 7, "tick": 4, "is_active": False},
    )
    assert directory == {7: second}

    # Older lineage or physical rows cannot overwrite a newer decision.
    apply_row_to_directory(
        directory,
        latest_seen,
        first,
        {"entity_id": 7, "tick": 1, "is_active": True},
    )
    assert directory == {7: second}
    assert latest_seen == {7: 4}


@pytest.mark.parametrize(
    ("visibility", "lineage", "latest_physical_tick", "expected"),
    [
        (
            PinnedVisibility(
                world_id="world",
                run_id="run",
                head_tick=6,
                head_tokens=("manifest-6",),
                visibility_tokens=("manifest-6",),
                max_tick=None,
            ),
            [],
            99,
            7,
        ),
        (
            PinnedVisibility(
                world_id="world",
                run_id="run",
                head_tick=None,
                head_tokens=(),
                visibility_tokens=(),
                max_tick=None,
            ),
            [("ancestor", "ancestor-run", 3)],
            99,
            4,
        ),
        (
            PinnedVisibility(
                world_id="world",
                run_id="run",
                head_tick=None,
                head_tokens=(),
                visibility_tokens=None,
                max_tick=None,
            ),
            [],
            9,
            10,
        ),
        (
            PinnedVisibility(
                world_id="world",
                run_id="run",
                head_tick=None,
                head_tokens=(),
                visibility_tokens=None,
                max_tick=None,
            ),
            [],
            None,
            0,
        ),
    ],
)
def test_resume_tick_uses_manifest_authority_or_legacy_physical_head(
    visibility: PinnedVisibility,
    lineage: list[tuple[str, str, int]],
    latest_physical_tick: int | None,
    expected: int,
) -> None:
    derive_resume_tick = import_module("archetype.world.resume").derive_resume_tick

    assert (
        derive_resume_tick(
            visibility,
            lineage=lineage,
            latest_physical_tick=latest_physical_tick,
        )
        == expected
    )
