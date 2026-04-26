# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""FastAPI dependency injection from ServiceContainer."""

from __future__ import annotations

from uuid_utils import uuid7

from archetype.app.auth.models import ActorCtx
from archetype.app.broker import CommandBroker
from archetype.app.command_service import CommandService
from archetype.app.container import ServiceContainer
from archetype.app.query_service import QueryService
from archetype.app.simulation_service import SimulationService
from archetype.app.world_service import WorldService, default_registry_path

# Singleton container — initialized once at app startup
_container: ServiceContainer | None = None

# Default admin context for API requests (v0.1 — no real auth yet)
_default_ctx = ActorCtx(id=uuid7(), roles={"admin"})


def get_container() -> ServiceContainer:
    global _container
    if _container is None:
        _container = ServiceContainer(registry_path=default_registry_path())
    return _container


def set_container(container: ServiceContainer) -> None:
    global _container
    _container = container


def get_world_service() -> WorldService:
    return get_container().world_service


def get_command_service() -> CommandService:
    return get_container().command_service


def get_simulation_service() -> SimulationService:
    return get_container().simulation_service


def get_query_service() -> QueryService:
    return get_container().query_service


def get_broker() -> CommandBroker:
    return get_container().broker


def get_actor_ctx() -> ActorCtx:
    return _default_ctx
