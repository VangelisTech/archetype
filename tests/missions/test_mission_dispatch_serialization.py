# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Deterministic single-branch dispatch contracts for Agent Missions."""

from __future__ import annotations

import daft
import pytest

from archetype.core.hooks import PostTick
from archetype.core.resources import Resources
from archetype.graph import GraphView
from archetype.missions.components import TaskDispatch, TaskState
from archetype.missions.processors import TaskDispatchProcessor
from archetype.missions.relations import PartOfMission
from archetype.missions.transitions import TaskStatus


def _tasks(rows: list[tuple[int, TaskStatus, str, int]]) -> daft.DataFrame:
    state = TaskState.get_prefix()
    dispatch = TaskDispatch.get_prefix()
    return daft.from_pydict(
        {
            "entity_id": [row[0] for row in rows],
            "tick": [4 for _ in rows],
            f"{state}status": [row[1].value for row in rows],
            f"{state}reason": ["" for _ in rows],
            f"{dispatch}dispatch_id": [row[2] for row in rows],
            f"{dispatch}sequence": [row[3] for row in rows],
        }
    )


async def _process(
    rows: list[tuple[int, TaskStatus, str, int]],
) -> dict[int, dict[str, object]]:
    relation = PartOfMission.get_prefix()
    memberships = daft.from_pydict(
        {
            "entity_id": list(range(100, 100 + len(rows))),
            "tick": [4 for _ in rows],
            f"{relation}source": [row[0] for row in rows],
            f"{relation}target": [1 for _ in rows],
        }
    )
    view = GraphView()
    await view.on_post_tick(
        PostTick(
            world_id="world",
            tick=5,
            results={(PartOfMission,): memberships},
        )
    )
    resources = Resources()
    resources.insert(view)
    result = await TaskDispatchProcessor().process(_tasks(rows), resources=resources)
    return {int(row["entity_id"]): row for row in result.to_pylist()}


@pytest.mark.asyncio
async def test_two_independent_roots_dispatch_one_deterministic_task() -> None:
    state = TaskState.get_prefix()
    dispatch = TaskDispatch.get_prefix()

    rows = await _process(
        [
            (3, TaskStatus.READY, "", 0),
            (2, TaskStatus.READY, "", 0),
        ]
    )

    assert rows[2][f"{state}status"] == TaskStatus.DISPATCHED.value
    assert rows[2][f"{dispatch}sequence"] == 1
    assert rows[3][f"{state}status"] == TaskStatus.READY.value
    assert rows[3][f"{dispatch}sequence"] == 0


@pytest.mark.asyncio
async def test_repair_precedes_fresh_root_and_terminal_failure_blocks_dispatch() -> None:
    state = TaskState.get_prefix()
    dispatch = TaskDispatch.get_prefix()

    repair = await _process(
        [
            (2, TaskStatus.READY, "", 0),
            (3, TaskStatus.READY, "prior-repair", 1),
        ]
    )
    failed = await _process(
        [
            (2, TaskStatus.FAILED, "failed-dispatch", 1),
            (3, TaskStatus.READY, "", 0),
        ]
    )

    assert repair[2][f"{state}status"] == TaskStatus.READY.value
    assert repair[3][f"{state}status"] == TaskStatus.DISPATCHED.value
    assert repair[3][f"{dispatch}sequence"] == 2
    assert failed[3][f"{state}status"] == TaskStatus.READY.value
    assert failed[3][f"{dispatch}sequence"] == 0
