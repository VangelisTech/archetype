# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the physical storage substrate introduced in PR-1."""

from __future__ import annotations

from types import SimpleNamespace

import daft
import pytest

from archetype.core.config import StorageBackend, StorageConfig
from archetype.storage.catalog import SignatureRecord, WorldRecord
from archetype.storage.commit import CommitCoordinatorIdentity
from archetype.storage.config import ControlCatalogConfig
from archetype.storage.service import PinnedVisibility, StorageService


def _storage(tmp_path) -> StorageConfig:
    return StorageConfig(
        uri=str(tmp_path / "warehouse"),
        namespace="substrate",
        backend=StorageBackend.ICEBERG,
    )


class _VisibilityCatalog:
    def __init__(self, world: WorldRecord) -> None:
        self.world = world
        self.manifests = [
            SimpleNamespace(tick=1, commit_token="token-1"),
            SimpleNamespace(tick=2, commit_token="token-2"),
        ]
        self.visible = {1: ["token-1"], 2: ["token-2"]}
        self.signatures: list[SignatureRecord] = []

    async def get_world(self, world_id: str):
        return self.world if world_id == self.world.world_id else None

    async def list_manifests(self, world_id: str, run_id: str | None = None):
        return list(self.manifests)

    async def visible_tokens(
        self,
        world_id: str,
        run_id: str,
        ticks: list[int] | None = None,
    ):
        selected = self.visible
        if selected is None:
            return None
        if ticks is not None:
            selected = {tick: tokens for tick, tokens in selected.items() if tick in ticks}
        return {tick: list(tokens) for tick, tokens in selected.items()}

    async def list_signatures(self):
        return list(self.signatures)


def test_storage_binds_commit_coordinator_to_durable_identity(monkeypatch, tmp_path) -> None:
    storage = _storage(tmp_path)
    service = StorageService(control_catalog_config=ControlCatalogConfig())
    catalog = object()
    monkeypatch.setattr(service, "get_control_catalog", lambda _storage: catalog)

    coordinator = service.bind_commit_coordinator(
        storage,
        world_id="world-1",
        run_id="run-1",
        writer_epoch=9,
    )

    assert coordinator.identity == CommitCoordinatorIdentity(
        world_id="world-1",
        run_id="run-1",
        writer_epoch=9,
    )


@pytest.mark.asyncio
async def test_pin_visibility_is_an_immutable_snapshot(monkeypatch, tmp_path) -> None:
    storage = _storage(tmp_path)
    record = WorldRecord(
        world_id="world-1",
        name="one",
        run_id="run-1",
        parent_world_id=None,
        status="active",
        tick_head=2,
    )
    catalog = _VisibilityCatalog(record)
    service = StorageService(control_catalog_config=ControlCatalogConfig())
    monkeypatch.setattr(service, "get_control_catalog", lambda _storage: catalog)

    pinned = await service.pin_visibility(storage, record.world_id)

    assert pinned == PinnedVisibility(
        world_id="world-1",
        run_id="run-1",
        head_tick=2,
        head_tokens=("token-2",),
        visibility_tokens=("token-1", "token-2"),
        max_tick=None,
    )

    catalog.manifests.append(SimpleNamespace(tick=3, commit_token="token-3"))
    catalog.visible[3] = ["token-3"]
    assert pinned.head_tick == 2
    assert pinned.head_tokens == ("token-2",)
    assert pinned.visibility_tokens == ("token-1", "token-2")


@pytest.mark.asyncio
async def test_pin_visibility_preserves_legacy_and_fenced_empty_states(
    monkeypatch, tmp_path
) -> None:
    storage = _storage(tmp_path)
    record = WorldRecord(
        world_id="world-1",
        name="one",
        run_id="run-1",
        parent_world_id=None,
        status="active",
        tick_head=-1,
    )
    catalog = _VisibilityCatalog(record)
    catalog.manifests = []
    service = StorageService(control_catalog_config=ControlCatalogConfig())
    monkeypatch.setattr(service, "get_control_catalog", lambda _storage: catalog)

    catalog.visible = {}
    fenced = await service.pin_visibility(storage, record.world_id)
    assert fenced.visibility_tokens == ()
    assert fenced.head_tick is None

    catalog.visible = None
    legacy = await service.pin_visibility(storage, record.world_id)
    assert legacy.visibility_tokens is None
    assert legacy.head_tick is None


@pytest.mark.asyncio
async def test_pin_visibility_caps_both_head_and_token_allowlist(monkeypatch, tmp_path) -> None:
    storage = _storage(tmp_path)
    record = WorldRecord(
        world_id="world-1",
        name="one",
        run_id="run-1",
        parent_world_id=None,
        status="active",
        tick_head=2,
    )
    catalog = _VisibilityCatalog(record)
    service = StorageService(control_catalog_config=ControlCatalogConfig())
    monkeypatch.setattr(service, "get_control_catalog", lambda _storage: catalog)

    pinned = await service.pin_visibility(storage, record.world_id, max_tick=1)

    assert pinned.head_tick == 1
    assert pinned.head_tokens == ("token-1",)
    assert pinned.visibility_tokens == ("token-1",)
    assert pinned.max_tick == 1


@pytest.mark.asyncio
async def test_pin_visibility_excludes_commit_published_after_manifest_snapshot(
    monkeypatch, tmp_path
) -> None:
    storage = _storage(tmp_path)
    record = WorldRecord(
        world_id="world-1",
        name="one",
        run_id="run-1",
        parent_world_id=None,
        status="active",
        tick_head=2,
    )

    class _RacingCatalog(_VisibilityCatalog):
        async def visible_tokens(self, world_id, run_id, ticks=None):
            # Simulate another writer publishing a replacement token at an
            # already-observed tick between list_manifests() and this call.
            self.visible[2].append("token-published-after-pin")
            return await super().visible_tokens(world_id, run_id, ticks)

    catalog = _RacingCatalog(record)
    service = StorageService(control_catalog_config=ControlCatalogConfig())
    monkeypatch.setattr(service, "get_control_catalog", lambda _storage: catalog)

    pinned = await service.pin_visibility(storage, record.world_id)

    assert pinned.head_tokens == ("token-2",)
    assert pinned.visibility_tokens == ("token-1", "token-2")


@pytest.mark.asyncio
async def test_physical_scan_filters_visibility_but_keeps_raw_liveness_ties(
    monkeypatch, tmp_path
) -> None:
    storage = _storage(tmp_path)
    record = WorldRecord(
        world_id="world-1",
        name="one",
        run_id="run-1",
        parent_world_id=None,
        status="active",
        tick_head=4,
    )
    signature = SignatureRecord(
        table_id="table-a",
        component_names=("Agent",),
        schema_json="{}",
        fingerprint="fingerprint",
    )
    catalog = _VisibilityCatalog(record)
    catalog.signatures = [signature]
    service = StorageService(control_catalog_config=ControlCatalogConfig())
    monkeypatch.setattr(service, "get_control_catalog", lambda _storage: catalog)

    class _Store:
        async def get_existing_table_df(self, table_id: str, world_id: str, run_id: str):
            assert (table_id, world_id, run_id) == ("table-a", "world-1", "run-1")
            return daft.from_pydict(
                {
                    "entity_id": [7, 7, 8, 9],
                    "tick": [4, 4, 3, 4],
                    "is_active": [False, True, False, True],
                    "commit_token": ["token-2", "token-2", "token-1", "unpublished"],
                }
            )

    async def _store(_storage, _cache=None):
        return _Store()

    monkeypatch.setattr(service, "get_or_create_store", _store)
    real_materialize = service.materialize
    reduction_shapes: list[tuple[str, ...]] = []

    async def _materialize(frame):
        reduction_shapes.append(tuple(frame.column_names))
        return await real_materialize(frame)

    monkeypatch.setattr(service, "materialize", _materialize)
    visibility = PinnedVisibility(
        world_id="world-1",
        run_id="run-1",
        head_tick=4,
        head_tokens=("token-2",),
        visibility_tokens=("token-1", "token-2"),
        max_tick=None,
    )

    scanned = await service.scan_visible_world_rows(storage, record, visibility)

    assert scanned.latest_physical_tick == 4
    assert reduction_shapes == [("latest_tick",)]
    assert len(scanned.tables) == 1
    assert scanned.tables[0].signature == signature
    rows = (await real_materialize(scanned.tables[0].frame)).to_pylist()
    assert sorted(
        (int(row["entity_id"]), int(row["tick"]), bool(row["is_active"])) for row in rows
    ) == [(7, 4, False), (7, 4, True), (8, 3, False)]


@pytest.mark.asyncio
async def test_world_row_append_owns_envelope_and_extends_dedupe_key(monkeypatch, tmp_path) -> None:
    storage = _storage(tmp_path)
    record = WorldRecord(
        world_id="world-1",
        name="one",
        run_id="run-1",
        parent_world_id=None,
        status="active",
        tick_head=0,
    )
    catalog = _VisibilityCatalog(record)
    service = StorageService(control_catalog_config=ControlCatalogConfig())
    monkeypatch.setattr(service, "get_control_catalog", lambda _storage: catalog)
    captured: dict[str, object] = {}

    async def _append_missing(
        effective_storage,
        table_name,
        rows,
        *,
        key_columns,
    ):
        captured.update(
            storage=effective_storage,
            table_name=table_name,
            rows=rows,
            key_columns=key_columns,
        )
        return 1

    monkeypatch.setattr(service, "append_missing", _append_missing)
    written = await service.append_world_rows(
        storage,
        record.world_id,
        "events",
        daft.from_pydict({"event_id": ["event-1"], "value": [3]}),
        key_columns=("event_id",),
    )

    assert written == 1
    assert captured["key_columns"] == ("world_id", "run_id", "event_id")
    rows = (await service.materialize(captured["rows"])).to_pylist()  # type: ignore[arg-type]
    assert rows == [
        {
            "world_id": "world-1",
            "run_id": "run-1",
            "event_id": "event-1",
            "value": 3,
        }
    ]


@pytest.mark.asyncio
async def test_world_row_append_delegates_plain_append(monkeypatch, tmp_path) -> None:
    storage = _storage(tmp_path)
    record = WorldRecord(
        world_id="world-1",
        name="one",
        run_id="run-1",
        parent_world_id=None,
        status="active",
        tick_head=0,
    )
    catalog = _VisibilityCatalog(record)
    service = StorageService(control_catalog_config=ControlCatalogConfig())
    monkeypatch.setattr(service, "get_control_catalog", lambda _storage: catalog)
    captured: dict[str, object] = {}

    async def _append_table(effective_storage, table_name, rows):
        captured.update(storage=effective_storage, table_name=table_name, rows=rows)
        return 1

    monkeypatch.setattr(service, "append_table", _append_table)
    written = await service.append_world_rows(
        storage,
        record.world_id,
        "events",
        daft.from_pydict({"value": [3]}),
    )

    assert written == 1
    assert captured["storage"] == storage
    assert captured["table_name"] == "events"
    rows = (await service.materialize(captured["rows"])).to_pylist()  # type: ignore[arg-type]
    assert rows == [{"world_id": "world-1", "run_id": "run-1", "value": 3}]


@pytest.mark.asyncio
async def test_world_row_append_rejects_caller_owned_envelope(monkeypatch, tmp_path) -> None:
    storage = _storage(tmp_path)
    record = WorldRecord(
        world_id="world-1",
        name="one",
        run_id="run-1",
        parent_world_id=None,
        status="active",
        tick_head=0,
    )
    service = StorageService(control_catalog_config=ControlCatalogConfig())
    catalog = _VisibilityCatalog(record)
    monkeypatch.setattr(service, "get_control_catalog", lambda _storage: catalog)

    with pytest.raises(ValueError, match="world_id and run_id"):
        await service.append_world_rows(
            storage,
            record.world_id,
            "events",
            daft.from_pydict({"world_id": ["forged"], "event_id": ["event-1"]}),
        )


@pytest.mark.asyncio
async def test_world_row_read_is_scoped_to_durable_world_run(monkeypatch, tmp_path) -> None:
    storage = _storage(tmp_path)
    record = WorldRecord(
        world_id="world-1",
        name="one",
        run_id="run-1",
        parent_world_id=None,
        status="active",
        tick_head=0,
    )
    service = StorageService(control_catalog_config=ControlCatalogConfig())
    catalog = _VisibilityCatalog(record)
    monkeypatch.setattr(service, "get_control_catalog", lambda _storage: catalog)

    async def _read_table(_storage, table_name):
        assert table_name == "events"
        return daft.from_pydict(
            {
                "world_id": ["world-1", "world-1", "world-2"],
                "run_id": ["run-1", "old-run", "run-1"],
                "event_id": ["keep", "old", "other"],
            }
        )

    monkeypatch.setattr(service, "read_table", _read_table)
    rows = await service.read_world_rows(storage, record.world_id, "events")

    assert (await service.materialize(rows)).to_pylist() == [
        {"world_id": "world-1", "run_id": "run-1", "event_id": "keep"}
    ]
