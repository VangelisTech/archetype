# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Regression tests for γ2/B5 footgun fixes.

Acceptance criteria:
  1. Signature canonicalization: unsorted tuples return correct rows;
     unknown signatures raise UnknownSignatureError distinctly from
     'no rows at this tick'.
  2. ArchetypeRuntime is non-interactive without logfire config (no EOFError,
     no prompt).
  3. Missing-column error in query_components names the providing component
     (ManipProprio-vs-ManipAction trap).
"""

from __future__ import annotations

import pytest

from archetype.core.aio import AsyncSystem, UnknownSignatureError
from archetype.core.aio.async_querier import _canonicalize
from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from tests.conftest import make_world_service

# ─────────────────────────────────────────────────────────────────────────────
# Shared test components
# ─────────────────────────────────────────────────────────────────────────────


class Alpha(Component):
    a: float = 1.0


class Beta(Component):
    b: float = 2.0


class Gamma(Component):
    g: float = 3.0


class Delta(Component):
    d: float = 4.0


# Two components that simulate the ManipProprio / ManipAction trap:
# one is on the entity being queried, one is NOT.
class ManipProprio(Component):
    position: float = 0.0


class ManipAction(Component):
    torque: float = 0.0


# ─────────────────────────────────────────────────────────────────────────────
# 1. Signature canonicalization
# ─────────────────────────────────────────────────────────────────────────────


def test_canonicalize_helper_sorts_by_class_name():
    """_canonicalize must produce a stable sorted order regardless of input order."""
    sig_fwd = (Alpha, Beta, Gamma, Delta)
    sig_rev = (Delta, Gamma, Beta, Alpha)
    assert _canonicalize(sig_fwd) == _canonicalize(sig_rev)
    names = [t.__name__ for t in _canonicalize(sig_fwd)]
    assert names == sorted(names)


@pytest.mark.asyncio
async def test_unsorted_4component_query_returns_rows(tmp_path):
    """An unsorted 4-component signature tuple must resolve to the same table
    as the canonical (sorted) form and return the correct rows.

    Regression: before canonicalization, unsorted sigs silently resolved to a
    different (empty) table name and returned 0 rows.
    """
    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        system = AsyncSystem()
        world = await ws.create_world(WorldConfig(name="w"), storage_config=storage, system=system)

        # Spawn an entity with all four components
        await world.create_entity([Alpha(a=10.0), Beta(b=20.0), Gamma(g=30.0), Delta(d=40.0)])

        # Advance one tick so the entity is persisted
        await world.run(RunConfig(num_steps=1))

        # The canonical sig (sorted by class name)
        canonical_sig = (Alpha, Beta, Delta, Gamma)  # sorted: Alpha<Beta<Delta<Gamma

        # Query with the canonical form first to verify rows exist
        df_canon = await world.query_archetype(
            sig=canonical_sig,
            run_id=world.run_id,
            ticks=[0],
        )
        canon_rows = df_canon.collect().to_pylist()
        assert len(canon_rows) >= 1, "canonical query returned no rows"

        # Now query with a deliberately UNSORTED tuple — must return same rows
        unsorted_sig = (Delta, Gamma, Alpha, Beta)  # reversed alphabetical order
        df_unsorted = await world.query_archetype(
            sig=unsorted_sig,
            run_id=world.run_id,
            ticks=[0],
        )
        unsorted_rows = df_unsorted.collect().to_pylist()
        assert len(unsorted_rows) == len(canon_rows), (
            f"Unsorted sig returned {len(unsorted_rows)} rows but canonical returned "
            f"{len(canon_rows)} — canonicalization is broken"
        )

        # Entity IDs must match
        canon_ids = {r["entity_id"] for r in canon_rows}
        unsorted_ids = {r["entity_id"] for r in unsorted_rows}
        assert unsorted_ids == canon_ids

    finally:
        await ws.shutdown()


@pytest.mark.asyncio
async def test_unknown_signature_raises_distinctly_from_empty_tick(tmp_path):
    """Querying a signature that was never spawned must raise UnknownSignatureError.

    This is distinct from 'the signature exists but has zero rows at this tick',
    which returns an empty DataFrame without raising.
    """
    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        system = AsyncSystem()
        world = await ws.create_world(WorldConfig(name="w"), storage_config=storage, system=system)

        # Spawn an entity with Alpha + Beta
        await world.create_entity([Alpha(a=1.0), Beta(b=2.0)])
        await world.run(RunConfig(num_steps=1))

        # Querying a sig that WAS spawned at a tick with no rows returns empty df (OK)
        known_sig = (Alpha, Beta)
        df_empty_tick = await world.query_archetype(
            sig=known_sig,
            run_id=world.run_id,
            ticks=[999],  # no rows at tick 999
        )
        assert df_empty_tick.collect().count_rows() == 0, "expected empty df for future tick"

        # Querying a sig that was NEVER spawned must raise UnknownSignatureError
        never_spawned_sig = (Gamma, Delta)  # these components were never spawned
        with pytest.raises(UnknownSignatureError, match="never been registered"):
            await world.query_archetype(
                sig=never_spawned_sig,
                run_id=world.run_id,
                ticks=[0],
            )

    finally:
        await ws.shutdown()


@pytest.mark.asyncio
async def test_unknown_signature_message_is_descriptive(tmp_path):
    """The UnknownSignatureError message must name the component types so the
    user can diagnose the problem without a debugger."""
    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await ws.create_world(
            WorldConfig(name="w"), storage_config=storage, system=AsyncSystem()
        )
        await world.create_entity([Alpha(a=1.0)])
        await world.run(RunConfig(num_steps=1))

        with pytest.raises(UnknownSignatureError) as exc_info:
            await world.query_archetype(
                sig=(Beta, Gamma),  # never spawned
                run_id=world.run_id,
                ticks=[0],
            )

        msg = str(exc_info.value)
        assert "Beta" in msg or "Gamma" in msg, f"error message does not name components: {msg!r}"
        assert "never been registered" in msg or "never" in msg

    finally:
        await ws.shutdown()


# ─────────────────────────────────────────────────────────────────────────────
# 2. ArchetypeRuntime non-interactive logfire
# ─────────────────────────────────────────────────────────────────────────────


def test_runtime_construction_non_interactive_no_eoferror(monkeypatch, tmp_path):
    """ArchetypeRuntime() must never block on an interactive logfire prompt.

    In a CI environment (no LOGFIRE_TOKEN, no LOGFIRE_API_KEY, no credentials
    file), constructing ArchetypeRuntime must complete without raising EOFError
    (which would indicate it tried to read from stdin) or any other error.
    """
    # Ensure no logfire credentials are visible in this test environment.
    monkeypatch.delenv("LOGFIRE_TOKEN", raising=False)
    monkeypatch.delenv("LOGFIRE_API_KEY", raising=False)
    monkeypatch.delenv("LOGFIRE_SEND_TO_LOGFIRE", raising=False)

    # Patch stdin to raise EOFError if read — proves no prompt is issued.
    import io
    import sys

    monkeypatch.setattr(sys, "stdin", io.StringIO())  # empty stdin → EOFError on read

    # Should not raise.
    from archetype.runtime.runtime import ArchetypeRuntime

    rt = ArchetypeRuntime()
    assert rt is not None


def test_runtime_construction_non_interactive_repeated(monkeypatch):
    """ArchetypeRuntime() can be constructed multiple times in one process
    without side effects from logfire initialization."""
    monkeypatch.delenv("LOGFIRE_TOKEN", raising=False)
    monkeypatch.delenv("LOGFIRE_API_KEY", raising=False)
    monkeypatch.delenv("LOGFIRE_SEND_TO_LOGFIRE", raising=False)

    from archetype.runtime.runtime import ArchetypeRuntime

    rt1 = ArchetypeRuntime()
    rt2 = ArchetypeRuntime()
    assert rt1 is not rt2  # distinct objects


def test_runtime_send_to_logfire_with_token_env(monkeypatch):
    """When LOGFIRE_TOKEN is set (and logfire is installed), the first runtime
    of the process routes tracing to logfire with send_to_logfire=True.

    Backend selection runs once per process (archetype._obs.configure_tracing),
    so the guard is reset here to simulate being that first runtime; logfire
    is imported inside the selection path, so monkeypatching logfire.configure
    needs no module reload.
    """
    logfire = pytest.importorskip("logfire")

    from archetype import _obs

    monkeypatch.setenv("LOGFIRE_TOKEN", "test-fake-token-for-unit-test")
    monkeypatch.delenv("LOGFIRE_API_KEY", raising=False)
    monkeypatch.delenv("LOGFIRE_SEND_TO_LOGFIRE", raising=False)
    monkeypatch.delenv("ARCHETYPE_LOG", raising=False)
    monkeypatch.setattr(_obs, "_configured", False)

    captured: dict = {}
    monkeypatch.setattr(logfire, "configure", lambda **kwargs: captured.update(kwargs))

    from archetype.runtime.runtime import ArchetypeRuntime

    ArchetypeRuntime()
    assert captured.get("send_to_logfire") is True, (
        f"Expected send_to_logfire=True when LOGFIRE_TOKEN is set, got: {captured}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# 3. Query ergonomics: missing-column error names the providing component
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_query_components_missing_component_raises_with_name(tmp_path):
    """Querying a component that does NOT exist in any archetype must raise a
    KeyError that names the missing component type.

    This catches the ManipProprio-vs-ManipAction trap: the user queries
    ManipAction but the entity only has ManipProprio (or ManipAction is on a
    completely different archetype).

    We use the world's own querier (populated with the world's sigs) so that
    list_signatures() correctly reflects what the world has committed.
    """
    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await ws.create_world(
            WorldConfig(name="w"), storage_config=storage, system=AsyncSystem()
        )

        # Spawn an entity with ManipProprio ONLY (not ManipAction)
        await world.create_entity([ManipProprio(position=1.0)])
        await world.run(RunConfig(num_steps=1))

        # Use the world's own querier which has populated _known_sigs
        querier = world.querier

        with pytest.raises(KeyError) as exc_info:
            await querier.query_components(
                components=[ManipAction],  # never spawned
                world_id=world.world_id,
                run_id=world.run_id,
                ticks=[0],
            )

        msg = str(exc_info.value)
        # The error must name the missing component
        assert "ManipAction" in msg, f"error message does not name ManipAction: {msg!r}"

    finally:
        await ws.shutdown()


@pytest.mark.asyncio
async def test_query_components_wrong_combination_raises_with_hint(tmp_path):
    """When a component EXISTS in some archetype but not TOGETHER with the
    requested co-types, the error message must hint at which archetype provides
    that component — the ManipProprio-vs-ManipAction trap at the co-type level.

    Entity has ManipProprio. Another entity has ManipAction. Querying
    (ManipProprio, ManipAction) together should fail with a hint naming the
    mismatched component.
    """
    ws = make_world_service()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
        world = await ws.create_world(
            WorldConfig(name="w"), storage_config=storage, system=AsyncSystem()
        )

        # Spawn two entities with different components
        await world.create_entity([ManipProprio(position=1.0)])
        await world.create_entity([ManipAction(torque=2.0)])
        await world.run(RunConfig(num_steps=1))

        # Use the world's own querier which has populated _known_sigs
        querier = world.querier

        # Querying both together should fail because no archetype has both
        with pytest.raises(KeyError) as exc_info:
            await querier.query_components(
                components=[ManipProprio, ManipAction],  # never on the same entity
                world_id=world.world_id,
                run_id=world.run_id,
                ticks=[0],
            )

        msg = str(exc_info.value)
        # Either component name should appear to identify the problem
        assert "ManipProprio" in msg or "ManipAction" in msg, (
            f"error message does not name either component: {msg!r}"
        )

    finally:
        await ws.shutdown()
