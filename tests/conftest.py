"""Shared test fixtures for Archetype service wiring."""

from __future__ import annotations

from archetype.app.storage_service import StorageService
from archetype.app.world_service import WorldService


def make_storage_service() -> StorageService:
    """Create a StorageService for tests."""
    return StorageService()


def make_world_service(**kwargs) -> WorldService:
    """Create a properly wired WorldService for tests."""
    return WorldService(StorageService(), **kwargs)
