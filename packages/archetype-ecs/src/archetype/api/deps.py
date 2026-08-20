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

"""FastAPI dependencies over the process-owned command dispatcher."""

from __future__ import annotations

from fastapi import Header, HTTPException, Request
from uuid_utils import NAMESPACE_URL, uuid5

from archetype.api.principals import (
    AuthenticationError,
    MissionPrincipal,
    MissionPrincipalDirectory,
    parse_bearer_credential,
)
from archetype.commands.dispatch import CommandDispatcher
from archetype.commands.models import ActorCtx


async def get_dispatcher(request: Request) -> CommandDispatcher:
    """Return only the governed ingress surface from lifespan-owned state."""
    return request.app.state.resources.dispatcher


async def get_actor_ctx(authorization: str | None = Header(None)) -> ActorCtx:
    """Build the request actor context.

    v1 developer mode:
    - no Authorization header means default single-tenant admin
    - "Bearer <role>" accepts admin/operator/player/viewer
    """
    if authorization is None:
        role = "admin"
        return ActorCtx(
            id=uuid5(NAMESPACE_URL, f"archetype:development-principal:{role}"),
            roles={role},
        )

    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise HTTPException(status_code=401, detail="Invalid Authorization header")

    role = token.strip().lower()
    if role not in {"admin", "operator", "player", "viewer"}:
        raise HTTPException(status_code=401, detail="Unknown bearer role")

    return ActorCtx(
        id=uuid5(NAMESPACE_URL, f"archetype:development-principal:{role}"),
        roles={role},
    )


async def get_mission_principal(
    request: Request,
    authorization: str | None = Header(None),
) -> MissionPrincipal:
    """Authenticate a future mission-control route with a verified principal."""

    directory = getattr(request.app.state, "mission_principals", None)
    if not isinstance(directory, MissionPrincipalDirectory):
        directory = MissionPrincipalDirectory.empty()
    try:
        return directory.authenticate(parse_bearer_credential(authorization))
    except AuthenticationError:
        raise HTTPException(status_code=401, detail="Authentication required") from None
