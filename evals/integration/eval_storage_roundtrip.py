# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Integration eval: Storage round-trip integrity.

Multi-step eval that writes entities to storage, flushes via a simulation
step, reads them back, and verifies field-level data integrity.

Tests the interaction between: Component → Archetype → AsyncWorld → Store → Querier.
"""

from __future__ import annotations

import asyncio
import tempfile

from archetype.app.storage_service import StorageService
from archetype.app.world_service import WorldService
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from evals.types import EvalResult, Tier, binary, rubric


class Stats(Component):
    hp: int
    name: str


class Flags(Component):
    active: bool
    level: int


async def _run() -> list[EvalResult]:
    results = []

    with tempfile.TemporaryDirectory() as tmp:
        storage_cfg = StorageConfig(uri=f"{tmp}/store", namespace="eval_rt")
        orch = WorldService(StorageService())

        try:
            world = await orch.create_world(WorldConfig(name="roundtrip-eval"), storage_cfg)

            # --- write entities ---
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

            # --- flush to storage ---
            rc = RunConfig.benchmark(steps=1)
            await world.run(rc)

            # --- read back ---
            sig = Archetype.sig_from_components(
                [Stats(hp=0, name=""), Flags(active=False, level=0)]
            )
            df = await world.query_archetype(sig, run_config=rc)
            rows = df.collect().to_pydict()

            # --- grade: entity count ---
            returned_ids = set(rows.get("entity_id", []))
            expected_ids = {e["entity_id"] for e in expected}
            results.append(
                binary(
                    returned_ids,
                    expected_ids,
                    case_name="roundtrip_entity_ids_match",
                    tier=Tier.INTEGRATION,
                )
            )

            # --- grade: field-level integrity (rubric across all entities) ---
            field_checks = {}
            for idx, eid in enumerate(rows.get("entity_id", [])):
                exp = next((e for e in expected if e["entity_id"] == eid), None)
                if exp is None:
                    field_checks[f"entity_{eid}_found"] = False
                    continue
                field_checks[f"entity_{eid}_hp"] = rows["stats__hp"][idx] == exp["hp"]
                field_checks[f"entity_{eid}_name"] = rows["stats__name"][idx] == exp["name"]
                field_checks[f"entity_{eid}_level"] = rows["flags__level"][idx] == exp["level"]

            results.append(
                rubric(
                    field_checks,
                    case_name="roundtrip_field_integrity",
                    tier=Tier.INTEGRATION,
                    threshold=1.0,
                )
            )

        finally:
            await orch.shutdown()

    return results


def run() -> list[EvalResult]:
    return asyncio.run(_run())
