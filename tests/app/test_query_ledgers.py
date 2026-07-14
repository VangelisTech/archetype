# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import pytest

from archetype.app.ledger_service import LedgerService
from archetype.app.query_service import QueryService
from archetype.app.storage_service import StorageService
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.ledger import (
    ComponentRegistry,
    ComponentResolutionError,
    LedgerMetadataUnavailableError,
)


class ColdScore(Component):
    value: float


@pytest.mark.asyncio
async def test_generation_zero_pinned_query_is_typed_empty_and_creates_no_table(
    tmp_path: Path,
) -> None:
    config = StorageConfig(uri=tmp_path / "db", namespace="test")
    storage = StorageService()
    ledgers = LedgerService(storage)
    queries = QueryService(storage, ledger_service=ledgers)
    registry = ComponentRegistry()
    registry.register(ColdScore)
    ref = await ledgers.create_ledger(
        name="empty", storage_config=config, world_id="world", run_id="run"
    )

    result = await queries.query_ledger(
        ref,
        [ColdScore],
        storage_config=config,
        component_registry=registry,
    )

    materialized = result.collect()
    assert materialized.count_rows() == 0
    assert materialized.column_names == Archetype.get_archetype_schema((ColdScore,)).names
    assert not (tmp_path / "db" / "test" / "lance").exists()


@pytest.mark.asyncio
async def test_catalog_query_requires_explicit_ledger_wiring(tmp_path: Path) -> None:
    config = StorageConfig(uri=tmp_path / "db", namespace="test")
    storage = StorageService()
    ledgers = LedgerService(storage)
    ref = await ledgers.create_ledger(
        name=None, storage_config=config, world_id="world", run_id="run"
    )
    registry = ComponentRegistry()
    registry.register(ColdScore)

    with pytest.raises(LedgerMetadataUnavailableError):
        await QueryService(storage).query_ledger(
            ref,
            [ColdScore],
            storage_config=config,
            component_registry=registry,
        )


@pytest.mark.asyncio
async def test_pinned_query_requires_trusted_component_registration(tmp_path: Path) -> None:
    config = StorageConfig(uri=tmp_path / "db", namespace="test")
    storage = StorageService()
    ledgers = LedgerService(storage)
    ref = await ledgers.create_ledger(
        name=None, storage_config=config, world_id="world", run_id="run"
    )

    with pytest.raises(ComponentResolutionError):
        await QueryService(storage, ledger_service=ledgers).query_ledger(
            ref,
            [ColdScore],
            storage_config=config,
            component_registry=ComponentRegistry(),
        )


@pytest.mark.asyncio
async def test_describe_ledger_does_not_register_or_open_a_world(tmp_path: Path) -> None:
    config = StorageConfig(uri=tmp_path / "db", namespace="test")
    storage = StorageService()
    ledgers = LedgerService(storage)
    queries = QueryService(storage, ledger_service=ledgers)
    ref = await ledgers.create_ledger(
        name="description", storage_config=config, world_id="world", run_id="run"
    )

    info = await queries.describe_ledger(ref, storage_config=config)

    assert info.ref == ref
    assert info.name == "description"
    assert not (tmp_path / "db" / "test" / "lance").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "components,ticks,entity_ids,match",
    [
        ([ColdScore, ColdScore], None, None, "unique"),
        ([ColdScore], [-1], None, "ticks must be nonnegative"),
        ([ColdScore], None, [-1], "entity_ids must be nonnegative"),
    ],
)
async def test_pinned_query_rejects_ambiguous_or_negative_selectors(
    tmp_path: Path,
    components,
    ticks,
    entity_ids,
    match: str,
) -> None:
    config = StorageConfig(uri=tmp_path / "db", namespace="test")
    storage = StorageService()
    ledgers = LedgerService(storage)
    queries = QueryService(storage, ledger_service=ledgers)
    registry = ComponentRegistry()
    registry.register(ColdScore)
    ref = await ledgers.create_ledger(
        name=None, storage_config=config, world_id="world", run_id="run"
    )

    with pytest.raises(ValueError, match=match):
        await queries.query_ledger(
            ref,
            components,
            storage_config=config,
            component_registry=registry,
            ticks=ticks,
            entity_ids=entity_ids,
        )
