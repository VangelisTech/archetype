# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Physical storage, visibility, and durable control authority."""

from importlib import import_module
from typing import Any

__all__ = [
    "AmbiguousCommitError",
    "ControlCatalogConfig",
    "PinnedVisibility",
    "StorageService",
    "VisibleTableRows",
    "VisibleWorldRows",
    "create_async_store",
]


def __getattr__(name: str) -> Any:
    # A storage port must not initialize the retained execution engine merely
    # because Python first imports its parent package.
    if name not in __all__:
        raise AttributeError(name)
    module = "config" if name == "ControlCatalogConfig" else "service"
    value = getattr(import_module(f"archetype.storage.{module}"), name)
    globals()[name] = value
    return value
