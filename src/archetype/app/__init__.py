# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Archetype Application Layer
===========================

Service-oriented infrastructure for multi-world simulation orchestration.

Services:
- CommandService: Auth routing + command dispatch
- WorldService: World CRUD and lifecycle
- SimulationService: Execution (step/run with broker drain)
- QueryService: Time-travel reads
- StorageService: Backend pooling
- ServiceContainer: Wires everything together
"""

from archetype.app.broker import CommandBroker
from archetype.app.command_service import CommandService
from archetype.app.container import ServiceContainer
from archetype.app.factory import WorldFactory
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
from archetype.app.world_service import WorldService

# Core config (re-export for convenience)
from archetype.core import CacheConfig, RunConfig, StorageConfig, WorldConfig

__all__ = [
    # Services
    "CommandService",
    "WorldService",
    "SimulationService",
    "QueryService",
    "StorageService",
    "ServiceContainer",
    # Infrastructure
    "CommandBroker",
    "WorldFactory",
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
