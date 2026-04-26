# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Default ActorCtx factory for the runtime layer."""

from uuid_utils import uuid7

from archetype.app.auth.models import ActorCtx


def default_actor_ctx() -> ActorCtx:
    """Create the default runtime ActorCtx.

    The runtime is the script boundary. In a single-tenant context,
    that boundary IS the platform admin. Users who want constrained
    roles rebind explicitly via ``world.as_actor(ctx)``.
    """
    return ActorCtx(id=uuid7(), roles={"admin"})
