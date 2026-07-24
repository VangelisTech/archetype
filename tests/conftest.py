"""Shared test fixtures for Archetype service wiring."""

from __future__ import annotations

import os
from dataclasses import dataclass

import pytest

# Disable Daft's Scarf telemetry before anything imports daft. It does a blocking
# urlopen per runner/op; with Daft on every world.step it added seconds of stall
# per gated op (a single test: 44s -> 1.6s). Set in the root conftest so it lands
# before pytest imports any test module that pulls daft. (archetype/__init__ also
# sets it, but a test module may `import daft` before importing archetype.)
os.environ.setdefault("DO_NOT_TRACK", "1")

from archetype.core.config import StorageBackend, StorageConfig  # noqa: E402
from archetype.storage.service import StorageService  # noqa: E402
from archetype.world.lifecycle import WorldLifecycle  # noqa: E402
from archetype.world.registry import WorldRegistry  # noqa: E402


@pytest.fixture(autouse=True)
def isolate_default_audit_storage(tmp_path, monkeypatch):
    """Keep each test's default audit catalog isolated and disposable."""
    config = StorageConfig(
        uri=str(tmp_path / "audit"),
        namespace="audit",
        backend=StorageBackend.ICEBERG,
    )
    monkeypatch.setattr(
        "archetype.app.audit.service.default_audit_storage",
        lambda: config,
    )


def make_storage_service() -> StorageService:
    """Create a StorageService for tests."""
    return StorageService()


@dataclass(frozen=True, slots=True)
class WorldHarness:
    """Canonical world-family composition for focused implementation tests."""

    storage: StorageService
    registry: WorldRegistry
    lifecycle: WorldLifecycle

    async def close(self) -> None:
        await self.storage.shutdown()


def make_world_harness() -> WorldHarness:
    """Construct storage, registry, and lifecycle without application delegates."""
    storage = StorageService()
    registry = WorldRegistry()
    return WorldHarness(
        storage=storage,
        registry=registry,
        lifecycle=WorldLifecycle(storage, registry),
    )
