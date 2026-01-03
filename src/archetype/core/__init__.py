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
Archetype Core

Pure ECS primitives: Component, Archetype, World, System, Processor.
Storage interfaces and implementations.
Resources: Type-safe DI container for passing services to processors.

Application-level orchestration (WorldOrchestrator, WorldFactory, StorageBackendManager)
lives in archetype.app.
"""

# Async Module
from .aio import (
    AsyncCachedStore,
    AsyncProcessor,
    AsyncQueryManager,
    AsyncStore,
    AsyncSystem,
    AsyncUpdateManager,
    AsyncWorld,
)
from .archetype import Archetype
from .component import Component
from .config import CacheConfig, RunConfig, StorageConfig, WorldConfig
from .interfaces import ArchetypeSignature
from .resources import Resources
from .storage import AsyncLancedbStore

# Sync Module
from .sync import (
    QueryManager,
    SyncProcessor,
    SyncStore,
    SyncSystem,
    SyncWorld,
    UpdateManager,
)

__all__ = [
    # Core types
    "Component",
    "ArchetypeSignature",
    "Archetype",
    "Resources",
    # Config
    "RunConfig",
    "StorageConfig",
    "CacheConfig",
    "WorldConfig",
    # Sync API
    "SyncWorld",
    "SyncStore",
    "SyncSystem",
    "SyncProcessor",
    "QueryManager",
    "UpdateManager",
    # Async API
    "AsyncWorld",
    "AsyncSystem",
    "AsyncStore",
    "AsyncQueryManager",
    "AsyncUpdateManager",
    "AsyncCachedStore",
    "AsyncProcessor",
    # Storage backends
    "AsyncLancedbStore",
]
