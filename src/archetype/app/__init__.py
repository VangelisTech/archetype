# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
