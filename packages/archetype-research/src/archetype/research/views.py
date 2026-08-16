# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Read-only projections over one AutoResearch ledger world."""

from __future__ import annotations

from typing import Any

from archetype.research.components import BranchHead, Experiment, Run, RunStatus
from archetype.research.models import AutoResearchConfig
from archetype.storage.interfaces import iStorageService


async def read_experiment(
    lab: Any,
    storage: iStorageService,
) -> dict[str, Any] | None:
    """Return the active Experiment row, or None when the lab has no genesis."""

    if lab.tick == 0:
        return None
    frame = (await lab.query_archetype(sig=(Experiment,), ticks=[lab.tick - 1])).select(
        "experiment__name",
        "experiment__metadata_json",
    )
    materialized = await storage.materialize(frame)
    rows = materialized.to_pylist()
    return rows[0] if rows else None


async def read_head(
    lab: Any,
    storage: iStorageService,
) -> dict[str, Any] | None:
    """Return the latest persisted BranchHead row, or None for a fresh world."""

    if lab.tick == 0:
        return None
    frame = (await lab.query_archetype(sig=(BranchHead,), ticks=[lab.tick - 1])).select(
        "entity_id",
        "branchhead__descriptor_json",
    )
    materialized = await storage.materialize(frame)
    rows = materialized.to_pylist()
    return rows[0] if rows else None


async def next_iteration(
    lab: Any,
    storage: iStorageService,
    config: AutoResearchConfig,
) -> int:
    """Return one past the greatest terminal iteration Run id.

    An active row is not silently abandoned: starting another attempt without
    reconciliation could duplicate work and compare against an ambiguous
    incumbent.
    """

    if lab.tick == 0:
        return 0
    frame = (await lab.query_archetype(sig=(Run,), ticks=[lab.tick - 1])).select(
        "run__run_id",
        "run__status",
    )
    materialized = await storage.materialize(frame)
    rows = materialized.to_pylist()
    prefix = f"{config.experiment_id}:iter"
    indices: list[int] = []
    for row in rows:
        run_id = row["run__run_id"]
        if not run_id.startswith(prefix):
            raise ValueError(
                "experiment identity collision: attached lab contains an unexpected Run id"
            )
        suffix = run_id[len(prefix) :]
        if not suffix.isdigit():
            raise ValueError(
                "experiment identity collision: attached lab contains a malformed Run id"
            )
        status = row["run__status"]
        if RunStatus.is_active(status):
            raise RuntimeError(
                f"experiment has an active attempt {run_id!r}; reconcile it before resuming"
            )
        if not RunStatus.is_terminal(status):
            raise ValueError(
                f"experiment history contains an unknown Run status {status!r} for {run_id!r}"
            )
        indices.append(int(suffix))
    ordered_indices = sorted(indices)
    if ordered_indices != list(range(len(ordered_indices))):
        raise ValueError("experiment history contains duplicate or non-contiguous iteration ids")
    return len(ordered_indices)


__all__ = ["next_iteration", "read_experiment", "read_head"]
