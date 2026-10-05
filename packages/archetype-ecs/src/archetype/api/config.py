# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Immutable operator configuration for the supported HTTP/MCP server."""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from archetype_native.config import RemoteData
from archetype_native.ingress import (
    Component,
    ContextResource,
    Grant,
    LogicalResource,
    ProgramResource,
    validate_configuration,
)
from archetype_native.values import fields


@dataclass(frozen=True, slots=True)
class ServerConfig:
    native: tuple[tuple[str, str | None], ...]
    resources: tuple[Any, ...]
    grants: tuple[Grant, ...]
    remote_data: RemoteData | None = None

    def __post_init__(self):
        if type(self.native) is not tuple or any(
            type(item) is not tuple or len(item) != 2 for item in self.native
        ):
            raise ValueError("Expected immutable native configuration")
        values = dict(self.native)
        if len(values) != len(self.native) or set(values) != {
            "library",
            "store",
            "registry",
            "builds",
            "driver",
        }:
            raise ValueError("Native configuration fields differ from supported contract")
        if any(
            value is not None and (type(value) is not str or not Path(value).is_absolute())
            for value in values.values()
        ):
            raise ValueError("Native operator paths must be absolute strings or None")
        if any(values[key] is not None for key in ("registry", "builds", "driver")) and any(
            values[key] is None for key in ("registry", "builds", "driver")
        ):
            raise ValueError("Live native paths must be supplied together")
        if self.remote_data is not None and type(self.remote_data) is not RemoteData:
            raise ValueError("Expected immutable RemoteData configuration")
        validate_configuration(self.resources, self.grants)

    @classmethod
    def from_env(cls) -> ServerConfig:
        configured = os.environ.get("ARCHETYPE_RESOURCES_PATH")
        if not configured:
            raise ValueError("Configure ARCHETYPE_RESOURCES_PATH")
        document = tomllib.loads(Path(configured).read_text())
        fields(document, "resource grant")
        resources = []
        for raw in document["resource"]:
            kind = raw.get("kind")
            if kind == "program":
                fields(raw, "kind name")
                resources.append(ProgramResource(raw["name"]))
            elif kind == "world":
                fields(raw, "kind name world run components inputs")
                components = []
                for component in raw["components"]:
                    fields(component, "name output fields entity_field")
                    components.append(
                        Component(
                            component["name"],
                            component["output"],
                            tuple(component["fields"]),
                            component["entity_field"],
                        )
                    )
                resources.append(
                    LogicalResource(
                        raw["name"],
                        raw["world"],
                        raw["run"],
                        tuple(components),
                        tuple((n, tuple(ts)) for n, ts in raw["inputs"].items()),
                    )
                )
            elif kind == "context":
                fields(raw, "kind name world run source_resource")
                resources.append(
                    ContextResource(
                        raw["name"], raw["world"], raw["run"], raw["source_resource"] or None
                    )
                )
            else:
                raise ValueError("Unknown configured resource kind")
        grants = []
        for grant in document["grant"]:
            fields(grant, "principal resource capabilities")
            grants.append(
                Grant(grant["principal"], grant["resource"], frozenset(grant["capabilities"]))
            )
        remote_path = os.environ.get("ARCHETYPE_REMOTE_DATA_PATH")
        remote = (
            None
            if remote_path is None
            else RemoteData.from_dict(tomllib.loads(Path(remote_path).read_text()))
        )
        return cls(
            tuple(
                (key, os.environ.get(env))
                for key, env in (
                    ("library", "ARCHETYPE_NATIVE_LIBRARY"),
                    ("store", "ARCHETYPE_STORE"),
                    ("registry", "ARCHETYPE_REGISTRY"),
                    ("builds", "ARCHETYPE_BUILDS"),
                    ("driver", "ARCHETYPE_NATIVE_DRIVER"),
                )
            ),
            tuple(resources),
            tuple(grants),
            remote,
        )


__all__ = ["ServerConfig", "ContextResource", "Grant", "LogicalResource", "ProgramResource"]
