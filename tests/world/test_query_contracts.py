# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Canonical durable world-query boundary contracts."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

import pytest

from archetype.core.archetype import Archetype
from archetype.core.config import StorageConfig
from archetype.core.lineage import LINEAGE_SIG
from archetype.world import query


def test_world_query_has_no_application_or_audit_dependency() -> None:
    module_path = Path(query.__file__)
    tree = ast.parse(module_path.read_text())
    imports = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}

    assert not any(name == "archetype.app" or name.startswith("archetype.app.") for name in imports)
    assert not any("audit" in name for name in imports)
    assert "get_command_history" not in {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


@pytest.mark.asyncio
async def test_explicit_visibility_tokens_cannot_be_combined_with_lineage() -> None:
    with pytest.raises(ValueError, match="cannot be combined"):
        await query.query_components(
            object(),
            components=[],
            world_id="world",
            run_id="run",
            lineage=[("ancestor", "ancestor-run", 3)],
            visibility_tokens=["token"],
        )


@pytest.mark.asyncio
async def test_root_snapshot_never_reads_or_creates_lineage(monkeypatch) -> None:
    @dataclass(frozen=True)
    class _Record:
        run_id: str = "root-run"
        parent_world_id: None = None

    @dataclass(frozen=True)
    class _Visibility:
        run_id: str = "root-run"
        head_tick: int = 0
        head_tokens: tuple[str, ...] = ("root-head",)
        visibility_tokens: tuple[str, ...] = ("root-visible",)

    class _Catalog:
        async def get_world(self, world_id):
            assert world_id == "root"
            return _Record()

    class _Storage:
        def get_control_catalog(self, storage_config):
            del storage_config
            return _Catalog()

        async def pin_visibility(self, *_args, **_kwargs):
            return _Visibility()

    async def unexpected_lineage_read(*_args, **_kwargs):
        raise AssertionError("a root world must not probe lineage storage")

    monkeypatch.setattr(query, "get_lineage", unexpected_lineage_read)

    snapshot = await query.pin_query_snapshot(
        _Storage(),  # type: ignore[arg-type]
        "root",
        storage_config=StorageConfig(),
    )

    assert (snapshot.world_id, snapshot.run_id) == ("root", "root-run")
    assert (snapshot.head_tick, snapshot.head_tokens) == (0, ("root-head",))
    assert snapshot.lineage == ()


@pytest.mark.asyncio
async def test_missing_lineage_table_is_an_open_never_create_read(monkeypatch) -> None:
    class _Store:
        def __init__(self) -> None:
            self.table_ids: list[str] = []

        async def get_existing_table_df(self, table_id, *_args, **_kwargs):
            self.table_ids.append(table_id)
            raise KeyError(table_id)

        async def get_archetype_df(self, *_args, **_kwargs):
            raise AssertionError("missing lineage must not use create-on-read")

    store = _Store()

    async def querier_for(*_args):
        return StorageConfig(), store, object()

    monkeypatch.setattr(query, "_querier_for", querier_for)

    assert (
        await query.get_lineage(
            object(),  # type: ignore[arg-type]
            "child",
            "child-run",
            StorageConfig(),
        )
        is None
    )
    assert store.table_ids == [
        Archetype.get_name(LINEAGE_SIG),
        Archetype.get_legacy_name(LINEAGE_SIG),
    ]


@pytest.mark.asyncio
async def test_existing_lineage_table_preserves_lineage_read(monkeypatch) -> None:
    expected = [("parent", "parent-run", 4)]

    class _Rows:
        def to_pylist(self):
            return [
                {
                    "worldlineage__parent_world_id": "parent",
                    "worldlineage__parent_run_id": "parent-run",
                    "worldlineage__up_to_tick": 4,
                    "worldlineage__position": 0,
                }
            ]

    class _Store:
        async def get_existing_table_df(self, table_id, *_args, **_kwargs):
            if table_id == Archetype.get_name(LINEAGE_SIG):
                return _Rows()
            raise KeyError(table_id)

    store = _Store()

    async def querier_for(*_args):
        return StorageConfig(), store, object()

    monkeypatch.setattr(query, "_querier_for", querier_for)

    assert (
        await query.get_lineage(
            object(),  # type: ignore[arg-type]
            "child",
            "child-run",
            StorageConfig(),
        )
        == expected
    )


@pytest.mark.asyncio
async def test_legacy_only_lineage_table_remains_readable(monkeypatch) -> None:
    class _Rows:
        def to_pylist(self):
            return [
                {
                    "worldlineage__parent_world_id": "legacy-parent",
                    "worldlineage__parent_run_id": "legacy-run",
                    "worldlineage__up_to_tick": 2,
                    "worldlineage__position": 0,
                }
            ]

    class _Store:
        async def get_existing_table_df(self, table_id, *_args, **_kwargs):
            if table_id == Archetype.get_legacy_name(LINEAGE_SIG):
                return _Rows()
            raise KeyError(table_id)

    async def querier_for(*_args):
        return StorageConfig(), _Store(), object()

    monkeypatch.setattr(query, "_querier_for", querier_for)

    assert await query.get_lineage(
        object(),  # type: ignore[arg-type]
        "child",
        "child-run",
        StorageConfig(),
    ) == [("legacy-parent", "legacy-run", 2)]


@pytest.mark.asyncio
async def test_nested_fresh_fork_snapshot_uses_last_widening_head(monkeypatch) -> None:
    @dataclass(frozen=True)
    class _Visibility:
        run_id: str
        head_tick: int | None
        head_tokens: tuple[str, ...]
        visibility_tokens: tuple[str, ...] | None

    class _Storage:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str | None, int | None]] = []

        def get_control_catalog(self, storage_config):
            del storage_config

            class _Catalog:
                async def get_world(self, world_id):
                    assert world_id == "child"
                    return type(
                        "_Record",
                        (),
                        {"run_id": "child-run", "parent_world_id": "parent"},
                    )()

            return _Catalog()

        async def pin_visibility(
            self,
            storage_config,
            world_id,
            *,
            run_id=None,
            max_tick=None,
        ):
            del storage_config
            self.calls.append((world_id, run_id, max_tick))
            if world_id == "child":
                return _Visibility("child-run", None, (), ())
            if world_id == "parent":
                return _Visibility("parent-run", None, (), ())
            return _Visibility("source-run", 2, ("source-head",), ("source-0", "source-2"))

    async def lineage(*_args):
        return [("source", "source-run", 2), ("parent", "parent-run", 2)]

    storage = _Storage()
    monkeypatch.setattr(query, "get_lineage", lineage)

    snapshot = await query.pin_query_snapshot(
        storage,  # type: ignore[arg-type]
        "child",
        storage_config=StorageConfig(),
    )

    assert (snapshot.world_id, snapshot.run_id) == ("child", "child-run")
    assert (snapshot.head_tick, snapshot.head_tokens) == (2, ("source-head",))
    assert snapshot.current.visibility_tokens == ()
    assert snapshot.lineage[0].visibility_tokens == ("source-0", "source-2")
    assert snapshot.lineage[1].visibility_tokens == ()
    assert snapshot.effective_lineage == (snapshot.lineage[0],)
    assert storage.calls == [
        ("child", "child-run", None),
        ("source", "source-run", 2),
        ("parent", "parent-run", 2),
    ]


@pytest.mark.parametrize(
    ("ancestor_tokens", "accepted"),
    [(None, True), ((), False)],
    ids=["legacy-prefix", "fenced-empty-prefix"],
)
@pytest.mark.asyncio
async def test_fresh_fork_snapshot_preserves_legacy_visibility_semantics(
    monkeypatch,
    ancestor_tokens,
    accepted,
) -> None:
    @dataclass(frozen=True)
    class _Record:
        run_id: str = "child-run"
        parent_world_id: str = "source"

    @dataclass(frozen=True)
    class _Visibility:
        run_id: str
        head_tick: int | None
        head_tokens: tuple[str, ...]
        visibility_tokens: tuple[str, ...] | None

    class _Catalog:
        async def get_world(self, world_id):
            assert world_id == "child"
            return _Record()

    class _Storage:
        def get_control_catalog(self, storage_config):
            del storage_config
            return _Catalog()

        async def pin_visibility(
            self,
            _storage_config,
            world_id,
            *,
            run_id=None,
            max_tick=None,
        ):
            if world_id == "child":
                return _Visibility(str(run_id), None, (), ())
            assert (world_id, run_id, max_tick) == ("source", "source-run", 2)
            return _Visibility("source-run", None, (), ancestor_tokens)

    async def lineage(*_args):
        return [("source", "source-run", 2)]

    monkeypatch.setattr(query, "get_lineage", lineage)
    pending = query.pin_query_snapshot(
        _Storage(),  # type: ignore[arg-type]
        "child",
        storage_config=StorageConfig(),
    )
    if not accepted:
        with pytest.raises(RuntimeError, match="lineage visibility is incomplete"):
            await pending
        return

    snapshot = await pending
    assert (snapshot.head_tick, snapshot.head_tokens) == (2, ())
    assert snapshot.current.visibility_tokens == ()
    assert snapshot.lineage[0].visibility_tokens is None


@pytest.mark.parametrize(
    ("lineage", "message"),
    [
        (None, "has no persisted lineage"),
        ([("wrong-parent", "parent-run", 2)], "direct lineage segment"),
        (
            [("ancestor", "ancestor-run", 2), ("parent", "parent-run", 1)],
            "tick caps must be monotonic",
        ),
        ([("parent", "parent-run", 3)], "overlaps inherited lineage"),
    ],
)
@pytest.mark.asyncio
async def test_fork_snapshot_rejects_corrupt_parent_lineage(
    monkeypatch,
    lineage,
    message,
) -> None:
    @dataclass(frozen=True)
    class _Record:
        run_id: str = "child-run"
        parent_world_id: str = "parent"

    @dataclass(frozen=True)
    class _Visibility:
        run_id: str = "child-run"
        head_tick: int = 3
        head_tokens: tuple[str, ...] = ("child-head",)
        visibility_tokens: tuple[str, ...] = ("child-3",)

    class _Catalog:
        async def get_world(self, world_id):
            if world_id == "child":
                return _Record()
            assert world_id == "parent"
            return object()

    class _Storage:
        def get_control_catalog(self, storage_config):
            del storage_config
            return _Catalog()

        async def pin_visibility(
            self,
            _storage_config,
            world_id,
            *,
            run_id=None,
            max_tick=None,
        ):
            if world_id == "child":
                return _Visibility()
            return _Visibility(
                run_id=str(run_id),
                head_tick=int(max_tick),
                head_tokens=(f"{world_id}-head",),
                visibility_tokens=(f"{world_id}-{max_tick}",),
            )

    async def load_lineage(*_args):
        return lineage

    monkeypatch.setattr(query, "get_lineage", load_lineage)

    with pytest.raises(RuntimeError, match=message):
        await query.pin_query_snapshot(
            _Storage(),  # type: ignore[arg-type]
            "child",
            storage_config=StorageConfig(),
        )


@pytest.mark.asyncio
async def test_no_lineage_parent_does_not_treat_legacy_current_as_empty(
    monkeypatch,
) -> None:
    @dataclass(frozen=True)
    class _Record:
        run_id: str = "child-run"
        parent_world_id: str = "parent"
        tick_head: int = 0

    @dataclass(frozen=True)
    class _Visibility:
        run_id: str = "child-run"
        head_tick: None = None
        head_tokens: tuple[str, ...] = ()
        visibility_tokens: None = None

    class _Catalog:
        async def get_world(self, world_id):
            return _Record() if world_id == "child" else object()

    class _Storage:
        def get_control_catalog(self, storage_config):
            del storage_config
            return _Catalog()

        async def pin_visibility(self, *_args, **_kwargs):
            return _Visibility()

    async def no_lineage(*_args):
        return None

    monkeypatch.setattr(query, "get_lineage", no_lineage)

    with pytest.raises(RuntimeError, match="child-owned tick-zero origin"):
        await query.pin_query_snapshot(
            _Storage(),  # type: ignore[arg-type]
            "child",
            storage_config=StorageConfig(),
        )


@pytest.mark.parametrize(
    ("requested_run", "returned_run", "message"),
    [
        ("wrong-run", "child-run", "records run child-run"),
        (None, "wrong-run", "expected recorded run child-run"),
    ],
    ids=["requested-run-mismatch", "returned-run-mismatch"],
)
@pytest.mark.asyncio
async def test_query_snapshot_rejects_run_identity_mismatch(
    monkeypatch,
    requested_run,
    returned_run,
    message,
) -> None:
    @dataclass(frozen=True)
    class _Record:
        run_id: str = "child-run"
        parent_world_id: None = None

    @dataclass(frozen=True)
    class _Visibility:
        run_id: str
        head_tick: int = 0
        head_tokens: tuple[str, ...] = ("head",)
        visibility_tokens: tuple[str, ...] = ("visible",)

    class _Catalog:
        async def get_world(self, world_id):
            assert world_id == "child"
            return _Record()

    class _Storage:
        def get_control_catalog(self, storage_config):
            del storage_config
            return _Catalog()

        async def pin_visibility(self, *_args, **_kwargs):
            return _Visibility(returned_run)

    async def no_lineage(*_args):
        return None

    monkeypatch.setattr(query, "get_lineage", no_lineage)

    with pytest.raises((ValueError, RuntimeError), match=message):
        await query.pin_query_snapshot(
            _Storage(),  # type: ignore[arg-type]
            "child",
            run_id=requested_run,
            storage_config=StorageConfig(),
        )


@pytest.mark.asyncio
async def test_lineage_segments_are_clipped_to_their_owned_ticks() -> None:
    class _Frame:
        def __init__(self, label: str) -> None:
            self.labels = [label]

        def concat(self, other: _Frame) -> _Frame:
            result = _Frame("")
            result.labels = [*self.labels, *other.labels]
            return result

    calls: list[tuple[str, str, list[int] | None]] = []

    async def segment(world_id: str, run_id: str, ticks: list[int] | None) -> _Frame:
        calls.append((world_id, run_id, ticks))
        return _Frame(world_id)

    result = await query._union_lineage(
        _Frame("child"),
        [("root", "root-run", 2), ("parent", "parent-run", 5)],
        [0, 2, 3, 5, 8],
        segment,
    )

    assert result.labels == ["child", "root", "parent"]
    assert calls == [
        ("root", "root-run", [0, 2]),
        ("parent", "parent-run", [3, 5]),
    ]


@pytest.mark.asyncio
async def test_pinned_snapshot_keeps_segment_tokens_and_filters_exact(monkeypatch) -> None:
    class _Frame:
        def __init__(self, label: str) -> None:
            self.labels = [label]

        def concat(self, other: _Frame) -> _Frame:
            result = _Frame("")
            result.labels = [*self.labels, *other.labels]
            return result

    snapshot = query.PinnedWorldQuerySnapshot(
        world_id="child",
        run_id="child-run",
        head_tick=6,
        head_tokens=("child-head",),
        current=query.PinnedQuerySegment(
            world_id="child",
            run_id="child-run",
            up_to_tick=None,
            head_tick=6,
            head_tokens=("child-head",),
            visibility_tokens=("child-6",),
        ),
        lineage=(
            query.PinnedQuerySegment(
                world_id="root",
                run_id="root-run",
                up_to_tick=2,
                head_tick=2,
                head_tokens=("root-head",),
                visibility_tokens=("root-0", "root-1", "root-2"),
            ),
            query.PinnedQuerySegment(
                world_id="parent",
                run_id="parent-run",
                up_to_tick=5,
                head_tick=5,
                head_tokens=("parent-head",),
                visibility_tokens=("parent-3", "parent-4", "parent-5"),
            ),
        ),
    )
    calls: list[tuple[str, str, list[int] | None, list[int] | None, list[str] | None]] = []

    async def querier_for(*_args):
        return StorageConfig(), object(), object()

    async def candidates(*_args):
        return []

    async def components_frame(
        _querier,
        _store,
        _records,
        _components,
        world_id,
        run_id,
        *,
        ticks,
        entity_ids,
        commit_tokens,
    ):
        calls.append((world_id, run_id, ticks, entity_ids, commit_tokens))
        return _Frame(world_id)

    async def unexpected_repin(*_args, **_kwargs):
        raise AssertionError("an exact snapshot must not repin during its read")

    monkeypatch.setattr(query, "_querier_for", querier_for)
    monkeypatch.setattr(query, "_catalog_candidates", candidates)
    monkeypatch.setattr(query, "_components_frame", components_frame)
    monkeypatch.setattr(query, "_visible_tokens", unexpected_repin)

    result = await query.query_components(
        object(),  # type: ignore[arg-type]
        components=[],
        world_id="child",
        run_id="child-run",
        ticks=[0, 2, 3, 5, 6],
        entity_ids=[7, 9],
        snapshot=snapshot,
    )

    assert result.labels == ["child", "root", "parent"]
    assert calls == [
        ("child", "child-run", [0, 2, 3, 5, 6], [7, 9], ["child-6"]),
        ("root", "root-run", [0, 2], [7, 9], ["root-0", "root-1", "root-2"]),
        (
            "parent",
            "parent-run",
            [3, 5],
            [7, 9],
            ["parent-3", "parent-4", "parent-5"],
        ),
    ]
    with pytest.raises(ValueError, match="does not identify the requested world/run"):
        await query.query_components(
            object(),  # type: ignore[arg-type]
            components=[],
            world_id="other-child",
            run_id="child-run",
            snapshot=snapshot,
        )


@pytest.mark.asyncio
async def test_visibility_is_pinned_through_storage_at_requested_tick_head() -> None:
    @dataclass(frozen=True)
    class _Visibility:
        visibility_tokens: tuple[str, ...] | None

    class _Storage:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str, int | None]] = []

        async def pin_visibility(
            self,
            storage_config: StorageConfig,
            world_id: str,
            *,
            run_id: str,
            max_tick: int | None,
        ) -> _Visibility:
            del storage_config
            self.calls.append((world_id, run_id, max_tick))
            return _Visibility(("token-2", "token-5"))

    storage = _Storage()
    tokens = await query._visible_tokens(
        storage,  # type: ignore[arg-type]
        StorageConfig(),
        "world",
        "run",
        [2, 5],
    )

    assert tokens == ["token-2", "token-5"]
    assert storage.calls == [("world", "run", 5)]
