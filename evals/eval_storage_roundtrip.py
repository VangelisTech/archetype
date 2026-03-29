# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Eval: Storage round-trip integrity.

Verifies that entities written to storage can be queried back with
identical field values — no data corruption or silent column drops.
"""

from __future__ import annotations

import asyncio
import sys
import tempfile

from archetype.app.storage_service import StorageService
from archetype.app.world_service import WorldService
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig

# ---------------------------------------------------------------------------
# Components — deliberately varied field types
# ---------------------------------------------------------------------------


class Stats(Component):
    hp: int
    name: str


class Flags(Component):
    active: bool
    level: int


# ---------------------------------------------------------------------------
# Eval
# ---------------------------------------------------------------------------

PASS = 0
FAIL = 1


async def eval_storage_roundtrip() -> int:
    """Write entities, flush to storage, read back and compare."""
    failures = []

    with tempfile.TemporaryDirectory() as tmp:
        storage_cfg = StorageConfig(uri=f"{tmp}/store", namespace="eval_rt")
        orch = WorldService(StorageService())

        try:
            world = await orch.create_world(WorldConfig(name="roundtrip-eval"), storage_cfg)

            # Write a batch of entities
            expected = []
            for i in range(20):
                comps = [Stats(hp=100 + i, name=f"entity_{i}"), Flags(active=i % 2 == 0, level=i)]
                eid = await world.create_entity(comps)
                expected.append(
                    {
                        "entity_id": eid,
                        "hp": 100 + i,
                        "name": f"entity_{i}",
                        "active": i % 2 == 0,
                        "level": i,
                    }
                )

            # Run one step to flush to storage
            rc = RunConfig.benchmark(steps=1)
            await world.run(rc)

            # Query back
            sig = Archetype.sig_from_components(
                [Stats(hp=0, name=""), Flags(active=False, level=0)]
            )
            df = await world.query_archetype(sig, run_config=rc)
            rows = df.collect().to_pydict()

            returned_ids = set(rows.get("entity_id", []))
            expected_ids = {e["entity_id"] for e in expected}

            if returned_ids != expected_ids:
                missing = expected_ids - returned_ids
                extra = returned_ids - expected_ids
                failures.append(f"FAIL: entity ID mismatch — missing={missing}, extra={extra}")

            # Verify field values
            for idx, eid in enumerate(rows.get("entity_id", [])):
                exp = next((e for e in expected if e["entity_id"] == eid), None)
                if exp is None:
                    continue
                for col_name, key in [
                    ("stats__hp", "hp"),
                    ("stats__name", "name"),
                    ("flags__level", "level"),
                ]:
                    actual = rows[col_name][idx]
                    if actual != exp[key]:
                        failures.append(
                            f"FAIL: entity {eid} field {col_name}: expected {exp[key]!r}, got {actual!r}"
                        )

        finally:
            await orch.shutdown()

    if failures:
        for f in failures:
            print(f, file=sys.stderr)
        print(f"\neval_storage_roundtrip: {len(failures)} failure(s)", file=sys.stderr)
        return FAIL

    print("eval_storage_roundtrip: PASS")
    return PASS


def main() -> int:
    return asyncio.run(eval_storage_roundtrip())


if __name__ == "__main__":
    sys.exit(main())
