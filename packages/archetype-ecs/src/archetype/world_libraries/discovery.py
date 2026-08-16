# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Deterministic installed-distribution discovery for trusted world libraries."""

from __future__ import annotations

from collections.abc import Iterable
from importlib import metadata
from typing import Any

from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.version import InvalidVersion, Version

from archetype.world_libraries.models import WorldLibraryManifest

WORLD_LIBRARY_ENTRY_POINT_GROUP = "archetype.world_libraries"


def discover_world_libraries(
    *,
    framework_version: str | None = None,
) -> tuple[WorldLibraryManifest, ...]:
    """Load installed world-library manifests in canonical name order."""

    discovered: list[WorldLibraryManifest] = []
    entry_points = metadata.entry_points(group=WORLD_LIBRARY_ENTRY_POINT_GROUP)
    ordered = sorted(
        entry_points,
        key=lambda value: (
            value.name,
            value.value,
            str(getattr(value.dist, "name", "")),
        ),
    )
    for entry_point in ordered:
        loaded = entry_point.load()
        candidate = loaded() if callable(loaded) and not isinstance(loaded, type) else loaded
        if not isinstance(candidate, WorldLibraryManifest):
            raise TypeError(f"entry point {entry_point.name!r} did not load a WorldLibraryManifest")
        if entry_point.name != candidate.name:
            raise ValueError(
                f"world-library entry point {entry_point.name!r} loaded manifest {candidate.name!r}"
            )
        distribution_name = str(getattr(entry_point.dist, "name", "") or "")
        if distribution_name and _normalized(distribution_name) != _normalized(
            candidate.distribution
        ):
            raise ValueError(
                f"world library {candidate.name!r} declares distribution "
                f"{candidate.distribution!r}, loaded from {distribution_name!r}"
            )
        distribution_version = str(getattr(entry_point.dist, "version", "") or "")
        if distribution_version:
            try:
                declared_version = Version(candidate.version)
                installed_version = Version(distribution_version)
            except InvalidVersion as error:
                raise ValueError(
                    f"world library {candidate.name!r} has invalid declared or installed version"
                ) from error
            if declared_version != installed_version:
                raise ValueError(
                    f"world library {candidate.name!r} declares version "
                    f"{candidate.version!r}, loaded from {distribution_version!r}"
                )
        discovered.append(candidate)
    return resolve_world_libraries(discovered, framework_version=framework_version)


def resolve_world_libraries(
    manifests: Iterable[WorldLibraryManifest] | None = None,
    *,
    framework_version: str | None = None,
) -> tuple[WorldLibraryManifest, ...]:
    """Validate explicit manifests or discover the installed deterministic set."""

    if manifests is None:
        return discover_world_libraries(framework_version=framework_version)
    resolved = tuple(manifests)
    for manifest in resolved:
        if not isinstance(manifest, WorldLibraryManifest):
            raise TypeError("world_libraries must contain WorldLibraryManifest values")
    ordered = tuple(sorted(resolved, key=lambda value: value.name))
    _validate_unique(ordered)
    if framework_version is not None:
        _validate_compatibility(ordered, framework_version)
    return ordered


def _validate_unique(manifests: tuple[WorldLibraryManifest, ...]) -> None:
    libraries: dict[str, WorldLibraryManifest] = {}
    operation_names: dict[str, str] = {}
    operation_models: dict[type[Any], str] = {}
    for manifest in manifests:
        previous = libraries.get(manifest.name)
        if previous is not None:
            raise ValueError(f"duplicate world-library name {manifest.name!r}")
        libraries[manifest.name] = manifest
        for name, model in zip(
            manifest.operation_names,
            manifest.operation_models,
            strict=True,
        ):
            if name in operation_names:
                raise ValueError(
                    f"operation name {name!r} is declared by world libraries "
                    f"{operation_names[name]!r} and {manifest.name!r}"
                )
            if model in operation_models:
                raise ValueError(
                    f"operation model {model.__name__} is declared by world libraries "
                    f"{operation_models[model]!r} and {manifest.name!r}"
                )
            operation_names[name] = manifest.name
            operation_models[model] = manifest.name


def _validate_compatibility(
    manifests: tuple[WorldLibraryManifest, ...],
    framework_version: str,
) -> None:
    try:
        version = Version(framework_version)
    except InvalidVersion as error:
        raise ValueError(f"invalid framework version {framework_version!r}") from error
    for manifest in manifests:
        try:
            supported = SpecifierSet(manifest.requires_framework)
        except InvalidSpecifier as error:
            raise ValueError(
                f"world library {manifest.name!r} declares invalid framework range "
                f"{manifest.requires_framework!r}"
            ) from error
        if version not in supported:
            raise ValueError(
                f"world library {manifest.name!r} {manifest.version} requires "
                f"archetype-ecs{manifest.requires_framework}; found {framework_version}"
            )


def _normalized(value: str) -> str:
    return value.lower().replace("_", "-").replace(".", "-")


__all__ = [
    "WORLD_LIBRARY_ENTRY_POINT_GROUP",
    "discover_world_libraries",
    "resolve_world_libraries",
]
