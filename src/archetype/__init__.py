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
Archetype — dataframe-first, append-only ECS runtime for simulations and AI agents.

A data-centric Entity-Component-System (ECS) runtime for agent simulations.

Architecture (see assets/archetype_diagram.png):

    ┌─────────┐         ┌─────────┐         ┌─────────┐
    │ System  │──runs──▶│  World  │◀───────▶│  Store  │──append──▶ LanceDB
    └─────────┘         │ step()  │         │archetypes│
         │              └─────────┘         └─────────┘
         │ priority          │                   ▲
         ▼                   ▼                   │
    ┌─────────┐         ┌─────────┐         ┌─────────┐
    │Processor│         │ Querier │─query──▶│         │
    │Processor│         │ Updater │─update─▶│         │
    │   ...   │         └─────────┘         └─────────┘
    └─────────┘              │
                          daft df

Quick Start:
    >>> from archetype.app import ServiceContainer
    >>> container = ServiceContainer()
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__version__ = "0.1.1"

__all__ = [
    # Core types
    "Component",
    "Archetype",
    "ArchetypeSignature",
    "Resources",
    # Processor (with alias)
    "Processor",
    "SyncProcessor",
    "AsyncProcessor",
    # World (with alias)
    "World",
    "SyncWorld",
    "AsyncWorld",
    # System (with alias)
    "System",
    "SyncSystem",
    "AsyncSystem",
    # Store (with alias)
    "Store",
    "SyncStore",
    "AsyncStore",
    "AsyncCachedStore",
    "AsyncLancedbStore",
    # Querier/Updater (with aliases)
    "Querier",
    "Updater",
    "QueryManager",
    "UpdateManager",
    "AsyncQueryManager",
    "AsyncUpdateManager",
    # Config
    "StorageConfig",
    "CacheConfig",
    "RunConfig",
    "WorldConfig",
    # Runtime
    "ArchetypeRuntime",
    "RuntimeWorld",
    "SyncArchetypeRuntime",
    "SyncRuntimeWorld",
    "run_sync",
    "configure_session",
    # App layer services
    "CommandBroker",
    "WorldService",
    "StorageService",
    "CommandService",
    "SimulationService",
    "QueryService",
    "LedgerService",
    "ServiceContainer",
    # Models
    "Command",
    "CommandType",
    # AutoResearch
    "AutoResearchConfig",
    "AutoResearchResult",
    "CandidateContext",
    "EvaluationResult",
    # Durable ledger contracts
    "StorageRef",
    "ComponentRef",
    "SignatureRef",
    "LedgerIdentity",
    "LedgerManifest",
    "LedgerRef",
    "LedgerInfo",
    "ManifestHead",
    "ComponentRegistry",
]

_EXPORTS: dict[str, tuple[str, str]] = {
    # Core types
    "Component": ("archetype.core", "Component"),
    "Archetype": ("archetype.core", "Archetype"),
    "ArchetypeSignature": ("archetype.core", "ArchetypeSignature"),
    "Resources": ("archetype.core", "Resources"),
    # Processors (and aliases)
    "SyncProcessor": ("archetype.core", "SyncProcessor"),
    "AsyncProcessor": ("archetype.core", "AsyncProcessor"),
    "Processor": ("archetype.core", "SyncProcessor"),
    # Worlds (and aliases)
    "SyncWorld": ("archetype.core", "SyncWorld"),
    "AsyncWorld": ("archetype.core", "AsyncWorld"),
    "World": ("archetype.core", "SyncWorld"),
    # Systems (and aliases)
    "SyncSystem": ("archetype.core", "SyncSystem"),
    "AsyncSystem": ("archetype.core", "AsyncSystem"),
    "System": ("archetype.core", "SyncSystem"),
    # Stores (and aliases)
    "SyncStore": ("archetype.core", "SyncStore"),
    "AsyncStore": ("archetype.core", "AsyncStore"),
    "AsyncCachedStore": ("archetype.core", "AsyncCachedStore"),
    "AsyncLancedbStore": ("archetype.core", "AsyncLancedbStore"),
    "Store": ("archetype.core", "SyncStore"),
    # Query/update managers (and aliases)
    "QueryManager": ("archetype.core", "QueryManager"),
    "UpdateManager": ("archetype.core", "UpdateManager"),
    "AsyncQueryManager": ("archetype.core", "AsyncQueryManager"),
    "AsyncUpdateManager": ("archetype.core", "AsyncUpdateManager"),
    "Querier": ("archetype.core", "QueryManager"),
    "Updater": ("archetype.core", "UpdateManager"),
    # Config
    "StorageConfig": ("archetype.core", "StorageConfig"),
    "CacheConfig": ("archetype.core", "CacheConfig"),
    "RunConfig": ("archetype.core", "RunConfig"),
    "WorldConfig": ("archetype.core", "WorldConfig"),
    # Runtime
    "ArchetypeRuntime": ("archetype.runtime", "ArchetypeRuntime"),
    "RuntimeWorld": ("archetype.runtime", "RuntimeWorld"),
    "SyncArchetypeRuntime": ("archetype.runtime", "SyncArchetypeRuntime"),
    "SyncRuntimeWorld": ("archetype.runtime", "SyncRuntimeWorld"),
    "run_sync": ("archetype.runtime", "run_sync"),
    "configure_session": ("archetype.runtime", "configure_session"),
    # App layer
    "CommandBroker": ("archetype.app", "CommandBroker"),
    "WorldService": ("archetype.app", "WorldService"),
    "StorageService": ("archetype.app", "StorageService"),
    "CommandService": ("archetype.app", "CommandService"),
    "SimulationService": ("archetype.app", "SimulationService"),
    "QueryService": ("archetype.app", "QueryService"),
    "LedgerService": ("archetype.app", "LedgerService"),
    "ServiceContainer": ("archetype.app", "ServiceContainer"),
    "Command": ("archetype.app", "Command"),
    "CommandType": ("archetype.app", "CommandType"),
    # AutoResearch
    "AutoResearchConfig": ("archetype.app.autoresearch_service", "AutoResearchConfig"),
    "AutoResearchResult": ("archetype.app.autoresearch_service", "AutoResearchResult"),
    "CandidateContext": ("archetype.app.autoresearch_service", "CandidateContext"),
    "EvaluationResult": ("archetype.app.autoresearch_service", "EvaluationResult"),
    # Durable ledger contracts
    "StorageRef": ("archetype.ledger", "StorageRef"),
    "ComponentRef": ("archetype.ledger", "ComponentRef"),
    "SignatureRef": ("archetype.ledger", "SignatureRef"),
    "LedgerIdentity": ("archetype.ledger", "LedgerIdentity"),
    "LedgerManifest": ("archetype.ledger", "LedgerManifest"),
    "LedgerRef": ("archetype.ledger", "LedgerRef"),
    "LedgerInfo": ("archetype.ledger", "LedgerInfo"),
    "ManifestHead": ("archetype.ledger", "ManifestHead"),
    "ComponentRegistry": ("archetype.ledger", "ComponentRegistry"),
}


def __getattr__(name: str) -> Any:
    """
    Lazy public API loader.

    Archetype has heavy (and sometimes optional) dependencies. Avoid importing
    the entire world at `import archetype` time so small utilities can run in
    minimal environments.
    """

    target = _EXPORTS.get(name)
    if not target:
        raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
    module_name, attr_name = target
    module = import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(list(globals().keys()) + list(_EXPORTS.keys())))
