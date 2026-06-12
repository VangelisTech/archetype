# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""World fork lineage: durable provenance for forked worlds.

A fork reads pre-fork ticks through its lineage — ascending
(world_id, run_id, up_to_tick) ancestor segments. The live segments hang
off ``AsyncWorld.lineage``; this module makes them durable by appending
one ``WorldLineage`` row per segment to the store at fork time, under the
fork's own (world_id, run_id).

The store is append-only, so provenance written here is never deleted:
ancestry remains resolvable after the fork is destroyed or the process
restarts. Lineage rows use negative entity ids — they are world metadata,
never live entities, and never enter ``entity2sig`` or the tick loop.
"""

from __future__ import annotations

import daft
import pyarrow as pa

from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.interfaces import iAsyncStore


class WorldLineage(Component):
    """One ancestor segment of a fork's read lineage."""

    parent_world_id: str = ""
    parent_run_id: str = ""
    up_to_tick: int = 0
    position: int = 0


LINEAGE_SIG = (WorldLineage,)


async def persist_lineage(
    store: iAsyncStore,
    *,
    world_id: str,
    run_id: str,
    tick: int,
    lineage: list[tuple[str, str, int]],
) -> None:
    """Append the fork's full ancestor chain to the store.

    Written once at fork time. The chain is already flattened (the source's
    lineage plus the source's own segment), so a single read recovers the
    complete ancestry without recursion.
    """
    if not lineage:
        return
    rows = [
        Archetype.to_row_dict(
            entity_id=-(position + 1),
            tick=tick,
            components=[
                WorldLineage(
                    parent_world_id=str(parent_world_id),
                    parent_run_id=str(parent_run_id),
                    up_to_tick=int(up_to_tick),
                    position=position,
                )
            ],
            world_id=str(world_id),
            run_id=str(run_id),
        )
        for position, (parent_world_id, parent_run_id, up_to_tick) in enumerate(lineage)
    ]
    schema = Archetype.get_archetype_schema(LINEAGE_SIG)
    df = daft.from_arrow(pa.Table.from_pylist(rows, schema=schema))
    await store.append(LINEAGE_SIG, df)


async def load_lineage(
    store: iAsyncStore,
    *,
    world_id: str,
    run_id: str,
) -> list[tuple[str, str, int]] | None:
    """Recover a world's ancestor chain from its persisted lineage rows.

    Returns None when no lineage was recorded (root worlds, pre-lineage
    data). Works for destroyed worlds: the rows are append-only.
    """
    df = await store.get_archetype_df(
        sig=LINEAGE_SIG,
        world_id=str(world_id),
        run_id=str(run_id),
    )
    rows = df.to_pylist()
    if not rows:
        return None
    rows.sort(key=lambda r: r["worldlineage__position"])
    return [
        (
            str(r["worldlineage__parent_world_id"]),
            str(r["worldlineage__parent_run_id"]),
            int(r["worldlineage__up_to_tick"]),
        )
        for r in rows
    ]
