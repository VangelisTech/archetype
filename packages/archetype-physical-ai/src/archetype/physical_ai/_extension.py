# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Private trusted-extension adapter for the Physical-AI world library."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import Any, cast

from pydantic import BaseModel

from archetype.commands.registry import OperationSpec
from archetype.physical_ai.config import PhysicalAIExtensionConfig
from archetype.physical_ai.models import RunHostedEpisode, summarize_physical_ai_operation
from archetype.physical_ai.runtime import PhysicalAI
from archetype.world_libraries import (
    InstalledWorldLibrary,
    WorldLibraryContext,
    WorldLibraryManifest,
)

_LIBRARY_NAME = "physical-ai"


def _world_key(operation: RunHostedEpisode) -> object:
    return operation.world_id


async def _handle_run_hosted_episode(operation: RunHostedEpisode) -> Any:
    """Reject the removed SQLite-backed hosted activity route.

    Hosted Physical-AI execution used the generic SQLite claim/lease system.
    That system is being removed in favor of Temporal; this operation remains
    registered only so callers receive an explicit migration error instead of
    silently receiving the obsolete durability model.
    """

    raise RuntimeError(
        "run_hosted_episode is unavailable during the Temporal cutover; "
        "the SQLite-backed hosted Physical-AI route has been removed"
    )


def _install(context: WorldLibraryContext) -> InstalledWorldLibrary:
    if context.config is not None and not isinstance(
        context.config,
        PhysicalAIExtensionConfig,
    ):
        raise TypeError("physical-ai config must be a PhysicalAIExtensionConfig")

    async def handle(operation: RunHostedEpisode) -> Any:
        return await _handle_run_hosted_episode(operation)

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
        version="0.6.3",
        requires_framework=">=0.6,<0.7",
        operation_models=(RunHostedEpisode,),
        install=_install,
    )


__all__ = ["get_manifest"]
