from __future__ import annotations

from contextvars import ContextVar
from typing import Optional

from .models import ActorCtx


# Holds the current request's ActorCtx for downstream code (stores, updaters)
current_actor_ctx: ContextVar[Optional[ActorCtx]] = ContextVar("current_actor_ctx", default=None)

__all__ = ["current_actor_ctx"]


