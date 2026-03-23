# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Auth model for RBAC guardrails on the command broker."""

from archetype.app.auth.guard import ROLE_PERMS, guardrail_allow, reset_tick_counters
from archetype.app.auth.models import ActorCtx

__all__ = [
    "ActorCtx",
    "ROLE_PERMS",
    "guardrail_allow",
    "reset_tick_counters",
]
