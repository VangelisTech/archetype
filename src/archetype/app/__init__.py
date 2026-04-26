# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Archetype Application Layer
===========================

Service-oriented infrastructure for multi-world simulation orchestration.

Services:
- StorageService: Store creation + pooling + lifecycle
- WorldOrchestrator: World CRUD and lifecycle
- CommandService: Auth routing + command dispatch
- SimulationService: Execution (step/run with broker drain)
- QueryService: Time-travel reads
- ServiceContainer: Wires everything together
"""

from archetype.app.broker import CommandBroker
from archetype.app.command_service import CommandService
from archetype.app.container import ServiceContainer
from archetype.app.models import (
    Command,
    CommandType,
    ProcessorInfo,
    RunResult,
    WorldInfo,
    WorldSnapshot,
)
from archetype.app.query_service import QueryService
from archetype.app.simulation_service import SimulationService
from archetype.app.storage_service import StorageService
from archetype.app.world_service import WorldOrchestrator, WorldRegistry

# Backward-compat alias — prefer WorldOrchestrator in new code
WorldService = WorldOrchestrator

# Core config (re-export for convenience)
from archetype.core import AsyncLancedbStore, CacheConfig, RunConfig, StorageConfig, WorldConfig

__all__ = [
    # Services
    "CommandService",
    "WorldOrchestrator",
    "WorldService",
    "SimulationService",
    "QueryService",
    "StorageService",
    "ServiceContainer",
    # Infrastructure
    "AsyncLancedbStore",
    "CommandBroker",
    "WorldRegistry",
    # Models
    "Command",
    "CommandType",
    "WorldInfo",
    "RunResult",
    "ProcessorInfo",
    "WorldSnapshot",
    # Config (re-exports)
    "CacheConfig",
    "RunConfig",
    "StorageConfig",
    "WorldConfig",
]
