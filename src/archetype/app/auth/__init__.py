# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Auth: RBAC guardrails and the four-role permissions model."""

from archetype.app.auth.guard import guardrail_allow, reset_tick_counters
from archetype.app.auth.models import ActorCtx
from archetype.app.auth.permissions import COMMANDS_BY_ROLE

__all__ = [
    "ActorCtx",
    "COMMANDS_BY_ROLE",
    "guardrail_allow",
    "reset_tick_counters",
]
