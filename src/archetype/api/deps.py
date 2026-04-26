# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""FastAPI dependency injection from ServiceContainer."""

from __future__ import annotations

from fastapi import Header, HTTPException, Request
from uuid_utils import uuid7

from archetype.app.auth.models import ActorCtx
from archetype.app.command_service import CommandService
from archetype.app.container import ServiceContainer

# Test/development override. The lifespan handler attaches the resolved
# container to app.state; request dependencies read from app.state.
_container: ServiceContainer | None = None


def get_container() -> ServiceContainer:
    global _container
    if _container is None:
        _container = ServiceContainer()
    return _container


def set_container(container: ServiceContainer) -> None:
    global _container
    _container = container


async def get_command_service(request: Request) -> CommandService:
    return request.app.state.container.command_service


async def get_actor_ctx(authorization: str | None = Header(None)) -> ActorCtx:
    """Build the request actor context.

    v1 developer mode:
    - no Authorization header means default single-tenant admin
    - "Bearer <role>" accepts admin/operator/player/viewer
    """
    if authorization is None:
        return ActorCtx(id=uuid7(), roles={"admin"})

    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise HTTPException(status_code=401, detail="Invalid Authorization header")

    role = token.strip().lower()
    if role not in {"admin", "operator", "player", "viewer"}:
        raise HTTPException(status_code=401, detail="Unknown bearer role")

    return ActorCtx(id=uuid7(), roles={role})
