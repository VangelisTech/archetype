"""Shared test fixtures for Archetype service wiring."""

from __future__ import annotations

from archetype.app.storage_service import StorageService
from archetype.app.world_service import WorldOrchestrator


def make_storage_service() -> StorageService:
    """Create a StorageService for tests."""
    return StorageService()


def make_world_orchestrator(**kwargs) -> WorldOrchestrator:
    """Create a properly wired WorldOrchestrator for tests."""
    return WorldOrchestrator(StorageService(), **kwargs)
