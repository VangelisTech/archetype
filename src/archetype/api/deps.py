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

"""FastAPI dependency injection from ServiceContainer."""

from __future__ import annotations

from fastapi import Header, HTTPException, Request
from uuid_utils import uuid7

from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.app.gateway.gate import CommandService

# Test/development override. The lifespan handler attaches the resolved
# container to app.state; request dependencies read from app.state.
_container: ServiceContainer | None = None


def get_container() -> ServiceContainer:
    global _container
    if _container is None:
        _container = ServiceContainer()
    return _container


def set_container(container: ServiceContainer | None) -> None:
    # Accepts None to reset the override (used by tests/teardown).
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
