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

import inspect
import logging
from collections.abc import Awaitable, Callable

from fastapi import Header, HTTPException, Request
from uuid_utils import NAMESPACE_URL, uuid5

from archetype.commands.dispatch import CommandDispatcher
from archetype.commands.models import ActorCtx

Authenticator = Callable[[str], ActorCtx | Awaitable[ActorCtx]]

logger = logging.getLogger(__name__)

_DEVELOPMENT_ROLES = frozenset({"admin", "operator", "player", "viewer"})
_PUBLIC_AUTH_DETAIL = "Authentication failed"


async def get_dispatcher(request: Request) -> CommandDispatcher:
    """Return only the governed ingress surface from lifespan-owned state."""
    return request.app.state.resources.dispatcher


def _reject_authentication(reason: str) -> HTTPException:
    """Return one generic public rejection while retaining a fixed diagnostic."""
    logger.info("API authentication rejected: %s", reason)
    return HTTPException(
        status_code=401,
        detail=_PUBLIC_AUTH_DETAIL,
        headers={"WWW-Authenticate": "Bearer"},
    )


def _bearer_token(authorization: str | None) -> str:
    if authorization is None:
        raise _reject_authentication("credentials are absent")

    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise _reject_authentication("the authorization header is malformed")
    token = token.strip()
    if not token:
        raise _reject_authentication("the authorization header is malformed")
    return token


def _development_actor(token: str) -> ActorCtx:
    role = token.lower()
    if role not in _DEVELOPMENT_ROLES:
        raise _reject_authentication("the development role is not supported")

    return ActorCtx(
        id=uuid5(NAMESPACE_URL, f"archetype:development-principal:{role}"),
        roles={role},
    )


async def get_actor_ctx(
    request: Request,
    authorization: str | None = Header(None),
) -> ActorCtx:
    """Authenticate one untrusted request into a commands-owned actor context."""
    development_auth = bool(getattr(request.app.state, "development_auth", False))
    authenticator: Authenticator | None = getattr(request.app.state, "authenticator", None)

    if authorization is None and development_auth:
        return _development_actor("admin")

    token = _bearer_token(authorization)
    if development_auth:
        return _development_actor(token)

    if authenticator is None:
        raise _reject_authentication("no authenticator is configured")

    try:
        actor = authenticator(token)
        if inspect.isawaitable(actor):
            actor = await actor
    except Exception as exc:
        # Authentication callbacks receive credentials. Log only the exception
        # class, never its message, arguments, or the supplied token.
        logger.info(
            "API authentication callback rejected credentials (%s)",
            type(exc).__name__,
        )
        raise HTTPException(
            status_code=401,
            detail=_PUBLIC_AUTH_DETAIL,
            headers={"WWW-Authenticate": "Bearer"},
        ) from None

    if not isinstance(actor, ActorCtx):
        raise _reject_authentication("the authenticator returned an invalid actor context")
    return actor


__all__ = ["Authenticator", "get_actor_ctx", "get_dispatcher"]
