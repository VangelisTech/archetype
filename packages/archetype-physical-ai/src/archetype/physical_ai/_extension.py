# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Private trusted-extension adapter for the Physical-AI world library."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, cast

from pydantic import BaseModel

from archetype.activities import ActivityCoordinator
from archetype.commands.registry import OperationSpec
from archetype.errors import WorldNotFoundError
from archetype.physical_ai import hosted_workflow
from archetype.physical_ai.config import PhysicalAIExtensionConfig
from archetype.physical_ai.hosted_activities import PhysicalHostedActivityCoordinator
from archetype.physical_ai.hosted_activity_values import LocalHostedEpisodeValueStore
from archetype.physical_ai.hosted_activity_world import (
    PhysicalHostedActivityBinding,
    StoragePhysicalCommittedIntentReader,
    WorldHostedEpisodeObservationStager,
)
from archetype.physical_ai.hosted_modal import ModalHostedEpisodeProvider
from archetype.physical_ai.models import RunHostedEpisode, summarize_physical_ai_operation
from archetype.physical_ai.runtime import PhysicalAI
from archetype.storage.activity_catalog import (
    SqliteActivityCatalog,
    activity_catalog_path_for,
)
from archetype.world_libraries import (
    InstalledWorldLibrary,
    WorldLibraryContext,
    WorldLibraryManifest,
)

_LIBRARY_NAME = "physical-ai"


def _world_key(operation: RunHostedEpisode) -> object:
    return operation.world_id


async def _handle_run_hosted_episode(
    worlds: Any,
    hosted_activity_for: Callable[[RunHostedEpisode], Awaitable[PhysicalHostedActivityBinding]],
    operation: RunHostedEpisode,
) -> Any:
    binding = await hosted_activity_for(operation)
    return await hosted_workflow.run_hosted_episode(worlds, binding, operation)


def _install(context: WorldLibraryContext) -> InstalledWorldLibrary:
    config = context.config
    if config is None:
        physical_config = PhysicalAIExtensionConfig()
    elif isinstance(config, PhysicalAIExtensionConfig):
        physical_config = config
    else:
        raise TypeError("physical-ai config must be a PhysicalAIExtensionConfig")

    worlds = context.worlds
    storage = context.storage
    resources = context.resources
    required_projectors = context.required_projectors
    hosted_bindings: dict[str, tuple[object, PhysicalHostedActivityBinding]] = {}
    hosted_bindings_lock = asyncio.Lock()

    async def hosted_activity_for(
        operation: RunHostedEpisode,
    ) -> PhysicalHostedActivityBinding:
        world_id = str(operation.world_id)
        async with hosted_bindings_lock:
            retained = hosted_bindings.get(world_id)
            if retained is not None:
                retained_config, binding = retained
                if retained_config != operation.provider:
                    raise ValueError("one world cannot change its hosted Modal provider namespace")
                return binding

            storage_record = await worlds.storage_record(world_id)
            if storage_record is None:
                raise WorldNotFoundError(world_id)
            storage_config = storage_record[0]
            if storage_config != operation.storage_config:
                raise ValueError("hosted operation storage does not match the live world")
            catalog_path = activity_catalog_path_for(
                storage_config,
                context.control_catalog_config,
            )
            physical = SqliteActivityCatalog(catalog_path)
            reservation = resources.reserve_owner(
                f"physical-ai:hosted:{world_id}",
                phase="workflow-handles",
                closed_message="hosted Physical-AI worker is closed",
            )

            async def construct() -> PhysicalHostedActivityBinding:
                coordinator = PhysicalHostedActivityCoordinator(
                    ActivityCoordinator(physical),
                    lease_seconds=physical_config.hosted_activity_lease_seconds,
                )
                provider_factory = physical_config.hosted_episode_provider_factory
                provider = (
                    provider_factory(operation.provider)
                    if provider_factory is not None
                    else ModalHostedEpisodeProvider(operation.provider)
                )
                if provider.provider != operation.provider.provider_identity:
                    raise ValueError(
                        "hosted provider does not implement the requested Modal namespace"
                    )
                binding: PhysicalHostedActivityBinding

                async def close_binding() -> None:
                    await required_projectors.unbind(world_id, binding)
                    await physical.close()

                binding = PhysicalHostedActivityBinding(
                    world_id=world_id,
                    owner=f"physical-hosted:{reservation.owner}",
                    reader=StoragePhysicalCommittedIntentReader(
                        storage,
                        storage_config,
                    ),
                    catalog=coordinator,
                    values=LocalHostedEpisodeValueStore(
                        catalog_path.with_name(f"{catalog_path.stem}-physical-values")
                    ),
                    provider=provider,
                    stager=WorldHostedEpisodeObservationStager(
                        storage=storage,
                        registry=worlds,
                    ),
                    close=close_binding,
                )
                await required_projectors.bind(world_id, binding)
                routed = required_projectors.required_projector_for(world_id)
                if worlds.required_projector(world_id) is not routed:
                    await worlds.bind_required_projector(world_id, routed)
                return binding

            try:
                binding = await reservation.construct(construct)
            except BaseException:
                await physical.close()
                raise
            hosted_bindings[world_id] = (operation.provider, binding)
            return binding

    async def handle(operation: RunHostedEpisode) -> Any:
        return await _handle_run_hosted_episode(worlds, hosted_activity_for, operation)

    context.registry.register(
        OperationSpec(
            name="run_hosted_episode",
            model=RunHostedEpisode,
            # OperationSpec is intentionally non-generic. The registry binds
            # this exact model and validates the complete callable bundle.
            handler=cast(Callable[[BaseModel], Awaitable[Any]], handle),
            permission="run_hosted_episode",
            summarize=cast(
                Callable[[BaseModel], Mapping[str, Any]],
                summarize_physical_ai_operation,
            ),
            quota_scope="live_world",
            world_key=cast(Callable[[BaseModel], object], _world_key),
            durable=None,
            trusted=True,
            untrusted=False,
            token_cost=0,
        )
    )
    return InstalledWorldLibrary(
        name=_LIBRARY_NAME,
        world_adapter=PhysicalAI,
    )


def get_manifest() -> WorldLibraryManifest:
    """Return the side-effect-free Physical-AI extension declaration."""

    return WorldLibraryManifest(
        name=_LIBRARY_NAME,
        distribution="archetype-physical-ai",
        version="0.6.0",
        requires_framework=">=0.6,<0.7",
        operation_models=(RunHostedEpisode,),
        install=_install,
        root_exports={
            "HostedEpisodeObservation": (
                "archetype.physical_ai.models",
                "HostedEpisodeObservation",
            ),
            "HostedEpisodeRequest": (
                "archetype.physical_ai.models",
                "HostedEpisodeRequest",
            ),
            "ModalHostedEpisodeConfig": (
                "archetype.physical_ai.models",
                "ModalHostedEpisodeConfig",
            ),
        },
        world_method_aliases={"run_hosted_episode": "run_hosted_episode"},
    )


__all__ = ["get_manifest"]
