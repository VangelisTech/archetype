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

"""Dataframe-first simulations and agent workflows.

`ArchetypeRuntime` is the recommended entry point for applications. Concrete
services and the service container are internal and are not re-exported here.

Examples:
    >>> from archetype import ArchetypeRuntime
    >>> async with ArchetypeRuntime() as runtime:
    ...     world = runtime.world("experiment")
    ...     await world.run(steps=10)
"""

from __future__ import annotations

import os
from importlib import import_module
from typing import Any

# Daft phones home to Scarf (daft.gateway.scarf.sh + osstelemetry.io) at import
# and per-runner, via a *blocking* urllib.urlopen. Archetype runs Daft on every
# world.step, so on a slow or firewalled network that telemetry adds seconds of
# event-loop stall per operation — it dominated the test suite (one test was 44s,
# 1.6s with it off) and taxes every real run. Opt out before Daft is imported;
# `setdefault` honors an explicit `DO_NOT_TRACK=0` if a user wants telemetry on.
os.environ.setdefault("DO_NOT_TRACK", "1")

__version__ = "0.4.1"

__all__ = [
    # Core types
    "Component",
    "Archetype",
    "ArchetypeSignature",
    "Resources",
    # Step failure contract
    "TickExecutionError",
    "TickFailure",
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
    "entrypoint",
    # Public-API marker (machine-enforced: no raw-service params)
    "public_api",
    # Runtime configuration and result types
    "WorldInfo",
    "RunResult",
    "EpisodeConfig",
    "EpisodeResult",
    "RolloutConfig",
    "RolloutResult",
    "ArtifactReceipt",
    "ArtifactWriteReceipt",
    "ArtifactProcessor",
    "ArtifactBundleRequest",
    "ArtifactCandidate",
    "ArtifactIndexRecord",
    "ArtifactPublishReceipt",
    "ArtifactReconcileResult",
    "ArtifactSourceResolver",
    "BoundedArtifactSourceResolver",
    "ArtifactStoreConfig",
    "MaterializedArtifact",
    # Models
    "Command",
    "CommandType",
    # AutoResearch
    "AutoResearchConfig",
    "AutoResearchResult",
    "CandidateContext",
    "EvaluationResult",
    # Durable evaluation types
    "Outcome",
    "GraderContract",
    "EvalReceipt",
]

_EXPORTS: dict[str, tuple[str, str]] = {
    # Core types
    "Component": ("archetype.core", "Component"),
    "Archetype": ("archetype.core", "Archetype"),
    "ArchetypeSignature": ("archetype.core", "ArchetypeSignature"),
    "Resources": ("archetype.core", "Resources"),
    # Step failure contract (issue #444)
    "TickExecutionError": ("archetype.core", "TickExecutionError"),
    "TickFailure": ("archetype.core", "TickFailure"),
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
    "entrypoint": ("archetype.runtime.entrypoint", "entrypoint"),
    "public_api": ("archetype._api", "public_api"),
    "RuntimeWorld": ("archetype.runtime", "RuntimeWorld"),
    "SyncArchetypeRuntime": ("archetype.runtime", "SyncArchetypeRuntime"),
    "SyncRuntimeWorld": ("archetype.runtime", "SyncRuntimeWorld"),
    "run_sync": ("archetype.runtime", "run_sync"),
    "configure_session": ("archetype.runtime", "configure_session"),
    # Runtime configuration and result types
    "WorldInfo": ("archetype.app.models", "WorldInfo"),
    "RunResult": ("archetype.app.models", "RunResult"),
    "EpisodeConfig": ("archetype.app.models", "EpisodeConfig"),
    "EpisodeResult": ("archetype.app.models", "EpisodeResult"),
    "RolloutConfig": ("archetype.app.models", "RolloutConfig"),
    "RolloutResult": ("archetype.app.models", "RolloutResult"),
    "ArtifactReceipt": ("archetype.app.artifacts.models", "ArtifactReceipt"),
    "ArtifactWriteReceipt": ("archetype.app.artifacts.models", "ArtifactWriteReceipt"),
    "ArtifactProcessor": ("archetype.app.artifacts.models", "ArtifactProcessor"),
    "ArtifactBundleRequest": (
        "archetype.app.artifacts.bundle_models",
        "ArtifactBundleRequest",
    ),
    "ArtifactCandidate": ("archetype.app.artifacts.bundle_models", "ArtifactCandidate"),
    "ArtifactIndexRecord": ("archetype.app.artifacts.bundle_models", "ArtifactIndexRecord"),
    "ArtifactPublishReceipt": (
        "archetype.app.artifacts.bundle_models",
        "ArtifactPublishReceipt",
    ),
    "ArtifactReconcileResult": (
        "archetype.app.artifacts.bundle_models",
        "ArtifactReconcileResult",
    ),
    "ArtifactSourceResolver": (
        "archetype.app.artifacts.bundle_models",
        "ArtifactSourceResolver",
    ),
    "BoundedArtifactSourceResolver": (
        "archetype.app.artifacts.bundle_models",
        "BoundedArtifactSourceResolver",
    ),
    "ArtifactStoreConfig": ("archetype.app.artifacts.bundle_models", "ArtifactStoreConfig"),
    "MaterializedArtifact": (
        "archetype.app.artifacts.bundle_models",
        "MaterializedArtifact",
    ),
    # Command models used by supported host adapters
    "Command": ("archetype.app.models", "Command"),
    "CommandType": ("archetype.app.models", "CommandType"),
    # AutoResearch
    "AutoResearchConfig": ("archetype.app.research.contracts", "AutoResearchConfig"),
    "AutoResearchResult": ("archetype.app.research.contracts", "AutoResearchResult"),
    "CandidateContext": ("archetype.app.research.contracts", "CandidateContext"),
    "EvaluationResult": ("archetype.app.research.contracts", "EvaluationResult"),
    # Durable evaluation types
    "Outcome": ("archetype.app.evaluation.models", "Outcome"),
    "GraderContract": ("archetype.app.evaluation.models", "GraderContract"),
    "EvalReceipt": ("archetype.app.evaluation.models", "EvalReceipt"),
}


# Sync-engine exports are the educational tier (issue #278): stable, kept,
# and deprecated at the TOP-LEVEL alias only. `archetype.core.sync` imports
# stay silent — the tier is for reading and teaching; the async stack (and
# the Rust core beyond it) is the performance path.
_SYNC_TIER_DEPRECATED = frozenset(
    {
        "World",
        "Processor",
        "System",
        "Store",
        "SyncWorld",
        "SyncProcessor",
        "SyncSystem",
        "SyncStore",
    }
)


def __getattr__(name: str) -> Any:
    """
    Lazy public API loader.

    Archetype has heavy (and sometimes optional) dependencies. Avoid importing
    the entire world at `import archetype` time so small utilities can run in
    minimal environments.
    """
    if name in _SYNC_TIER_DEPRECATED:
        import warnings

        warnings.warn(
            f"archetype.{name} is the educational sync tier: stable but frozen. "
            "New code should use ArchetypeRuntime (or the Async* classes); see "
            "docs/guide/runtime.md.",
            DeprecationWarning,
            stacklevel=2,
        )

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
