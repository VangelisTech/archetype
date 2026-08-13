# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Value contracts for deterministic world-library composition."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from pydantic import BaseModel

_LIBRARY_NAME = re.compile(r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*\Z")
_EXPORT_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


@dataclass(frozen=True, slots=True, kw_only=True)
class WorldLibraryContext:
    """Framework capabilities available during one trusted installation.

    The context is intentionally a value bag rather than a second composition
    container. World libraries may retain the narrow capabilities they use,
    while the framework remains the sole owner of process construction and
    shutdown ordering.
    """

    registry: Any
    resources: Any
    worlds: Any
    lifecycle: Any
    scheduler: Any
    storage: Any
    redaction: Any
    required_projectors: Any
    control_catalog_config: Any
    artifact_store_config: Any | None
    destroy_world: Callable[[object], Any]
    runtime_world_factory: Callable[..., Any]
    config: object | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class InstalledWorldLibrary:
    """Runtime and transport adapters retained after successful installation."""

    name: str
    runtime_adapter: Callable[..., Any] | None = None
    world_adapter: Callable[..., Any] | None = None
    api_routers: tuple[object, ...] = ()

    def __post_init__(self) -> None:
        _validate_library_name(self.name)
        if self.runtime_adapter is not None and not callable(self.runtime_adapter):
            raise TypeError("runtime_adapter must be callable")
        if self.world_adapter is not None and not callable(self.world_adapter):
            raise TypeError("world_adapter must be callable")
        object.__setattr__(self, "api_routers", tuple(self.api_routers))


@dataclass(frozen=True, slots=True, kw_only=True)
class WorldLibraryManifest:
    """Side-effect-free declaration for one trusted world-library distribution."""

    name: str
    distribution: str
    version: str
    requires_framework: str
    operation_models: tuple[type[BaseModel], ...]
    install: Callable[[WorldLibraryContext], InstalledWorldLibrary]
    root_exports: Mapping[str, tuple[str, str]] = field(default_factory=dict)
    runtime_method_aliases: Mapping[str, str] = field(default_factory=dict)
    world_method_aliases: Mapping[str, str] = field(default_factory=dict)
    sync_world_method_aliases: Mapping[str, str] = field(default_factory=dict)
    api_router_factories: tuple[Callable[[], object], ...] = ()

    def __post_init__(self) -> None:
        _validate_library_name(self.name)
        if not isinstance(self.distribution, str) or not self.distribution.strip():
            raise ValueError("world-library distribution must be non-empty")
        if not isinstance(self.version, str) or not self.version.strip():
            raise ValueError("world-library version must be non-empty")
        if not isinstance(self.requires_framework, str) or not self.requires_framework.strip():
            raise ValueError("world-library framework range must be non-empty")
        if not callable(self.install):
            raise TypeError("world-library installer must be callable")

        models = tuple(self.operation_models)
        if len(models) != len(set(models)):
            raise ValueError(f"world library {self.name!r} declares duplicate operation models")
        for model in models:
            if not isinstance(model, type) or not issubclass(model, BaseModel):
                raise TypeError("world-library operation models must be BaseModel subclasses")
            operation = model.model_fields.get("operation")
            if operation is None or not isinstance(operation.default, str) or not operation.default:
                raise ValueError(
                    f"world-library operation model {model.__name__} has no fixed discriminator"
                )
        names = tuple(model.model_fields["operation"].default for model in models)
        if len(names) != len(set(names)):
            raise ValueError(f"world library {self.name!r} declares duplicate operation names")

        root_exports = _validated_mapping(self.root_exports, values_are_paths=True)
        runtime_aliases = _validated_mapping(self.runtime_method_aliases)
        world_aliases = _validated_mapping(self.world_method_aliases)
        sync_world_aliases = _validated_mapping(self.sync_world_method_aliases)
        unknown_sync_aliases = set(sync_world_aliases) - set(world_aliases)
        if unknown_sync_aliases:
            raise ValueError(
                "world-library sync aliases must refine declared world aliases: "
                + ", ".join(sorted(unknown_sync_aliases))
            )
        factories = tuple(self.api_router_factories)
        if any(not callable(factory) for factory in factories):
            raise TypeError("world-library API router factories must be callable")

        object.__setattr__(self, "operation_models", models)
        object.__setattr__(self, "root_exports", MappingProxyType(root_exports))
        object.__setattr__(self, "runtime_method_aliases", MappingProxyType(runtime_aliases))
        object.__setattr__(self, "world_method_aliases", MappingProxyType(world_aliases))
        object.__setattr__(
            self,
            "sync_world_method_aliases",
            MappingProxyType(sync_world_aliases),
        )
        object.__setattr__(self, "api_router_factories", factories)

    @property
    def operation_names(self) -> tuple[str, ...]:
        """Return declared operation discriminators in manifest order."""

        return tuple(
            str(model.model_fields["operation"].default) for model in self.operation_models
        )


def _validate_library_name(value: object) -> None:
    if not isinstance(value, str) or _LIBRARY_NAME.fullmatch(value) is None:
        raise ValueError("world-library name must be a lowercase kebab-case identifier")


def _validated_mapping(
    value: Mapping[str, Any],
    *,
    values_are_paths: bool = False,
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("world-library adapter metadata must be a mapping")
    result: dict[str, Any] = {}
    for name, target in value.items():
        if not isinstance(name, str) or _EXPORT_NAME.fullmatch(name) is None:
            raise ValueError("world-library adapter names must be Python identifiers")
        if values_are_paths:
            if not (
                isinstance(target, tuple)
                and len(target) == 2
                and all(isinstance(item, str) and item for item in target)
            ):
                raise ValueError("world-library root exports must be (module, attribute) pairs")
        elif not isinstance(target, str) or _EXPORT_NAME.fullmatch(target) is None:
            raise ValueError("world-library method aliases must target Python identifiers")
        result[name] = target
    return result


__all__ = [
    "InstalledWorldLibrary",
    "WorldLibraryContext",
    "WorldLibraryManifest",
]
