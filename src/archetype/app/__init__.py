# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Archetype Application Layer
===========================

Service-oriented infrastructure for multi-world simulation orchestration.

Services:
- StorageService: Store creation + pooling + lifecycle
- WorldService: World lifecycle management
- CommandService: Auth routing + command dispatch
- SimulationService: Execution (step/run with broker drain)
- QueryService: Time-travel reads
- ServiceContainer: Wires everything together
"""

from archetype.app.broker import CommandBroker
from archetype.app.command_service import CommandService
from archetype.app.container import ServiceContainer
from archetype.app.models import (
    AuditRow,
    Command,
    CommandType,
    HookInfo,
    ProcessorInfo,
    ResourceInfo,
    RunResult,
    WorldInfo,
)
from archetype.app.query_service import QueryService
from archetype.app.simulation_service import SimulationService
from archetype.app.storage_service import StorageService
from archetype.app.world_service import WorldService

# Core config (re-export for convenience)
from archetype.core import AsyncLancedbStore, CacheConfig, RunConfig, StorageConfig, WorldConfig

__all__ = [
    # Services
    "CommandService",
    "WorldService",
    "SimulationService",
    "QueryService",
    "StorageService",
    "ServiceContainer",
    # Infrastructure
    "AsyncLancedbStore",
    "CommandBroker",
    # Models
    "Command",
    "CommandType",
    "AuditRow",
    "WorldInfo",
    "RunResult",
    "ProcessorInfo",
    "HookInfo",
    "ResourceInfo",
    # Config (re-exports)
    "CacheConfig",
    "RunConfig",
    "StorageConfig",
    "WorldConfig",
]
