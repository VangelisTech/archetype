# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Private trusted-extension adapter for the Research world library."""

from __future__ import annotations

from collections.abc import Mapping
from functools import partial
from typing import Any, cast

from pydantic import BaseModel

from archetype.commands.registry import OperationSpec
from archetype.research import handlers
from archetype.research.models import AutoResearch, summarize_research_operation
from archetype.research.runtime import Research
from archetype.world_libraries import (
    InstalledWorldLibrary,
    WorldLibraryContext,
    WorldLibraryManifest,
)

_LIBRARY_NAME = "research"


def _world_key(operation: BaseModel) -> object:
    return cast(AutoResearch, operation).world_id


def _token_cost(operation: BaseModel) -> int:
    research = cast(AutoResearch, operation)
    return 200 * max(int(research.config.max_iterations), 1)


def _summarize(operation: BaseModel) -> Mapping[str, Any]:
    return summarize_research_operation(cast(AutoResearch, operation))


def _install(context: WorldLibraryContext) -> InstalledWorldLibrary:
    if context.config is not None:
        raise TypeError("research does not accept world-library configuration")
    admissions = handlers.AutoResearchAdmissions()
    handler = cast(
        Any,
        partial(
            handlers.handle_autoresearch,
            admissions,
            context.worlds,
            context.lifecycle,
            context.storage,
            context.destroy_world,
        ),
    )
    context.registry.register(
        OperationSpec(
            name="autoresearch",
            model=AutoResearch,
            handler=handler,
            permission="autoresearch",
            summarize=_summarize,
            quota_scope="live_world",
            world_key=_world_key,
            durable=None,
            trusted=True,
            untrusted=True,
            token_cost=_token_cost,
        )
    )
    return InstalledWorldLibrary(
        name=_LIBRARY_NAME,
        world_adapter=Research,
    )


def get_manifest() -> WorldLibraryManifest:
    """Return the side-effect-free Research extension declaration."""

    return WorldLibraryManifest(
        name=_LIBRARY_NAME,
        distribution="archetype-research",
        version="0.6.2",
        requires_framework=">=0.6,<0.7",
        operation_models=(AutoResearch,),
        install=_install,
    )


__all__ = ["get_manifest"]
