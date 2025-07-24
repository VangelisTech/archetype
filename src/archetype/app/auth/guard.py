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

import uuid_utils as uuid  # Use uuid-utils for UUID handling
from uuid import UUID
from typing import Set, Dict
from pydantic import BaseModel, Field
from datetime import date
from archetype.core.command import Command

# 2. Static role-permission table
ROLE_PERMS: dict[str, set[str]] = {
    "viewer":     {"get_state"},
    "coder":      {"add_component", "remove_component", "patch_components"},
    "maintainer": {"spawn_entity", "delete_entity",
                   "add_component", "remove_component",
                   "add_processor", "remove_processor"},
    "admin":      {"*"},   # wildcard – full power
}

# Quotas
DAILY_TOKEN_BUDGET   = 50_000       # model-tokens or cost units
PER_TICK_CMD_BUDGET  = 25           # soft limit per actor per simulation tick

# 3. Ephemeral counters (in-memory dicts)
_TOKENS_USED_TODAY: dict[UUID, int] = {}
_CMD_COUNT_TICK:    dict[UUID, int] = {}
_TODAY = date.today()

# 4. Guardrail function
def estimate_token_cost(cmd: Command) -> int:
    """Cheap heuristic; tune per op."""
    if cmd.op.endswith("_processor"):
        return 200          # assume larger payload
    return 50               # most ECS ops are tiny

async def guardrail_allow(cmd: Command, ctx: ActorCtx) -> bool:
    global _TODAY

    # Day rollover resets token counters
    if date.today() != _TODAY:
        _TOKENS_USED_TODAY.clear()
        _TODAY = date.today()

    # 1. Permission check
    allowed = any(
        cmd.op in ROLE_PERMS.get(role, set()) or "*" in ROLE_PERMS.get(role, set())
        for role in ctx.roles
    )
    if not allowed:
        return False

    # 2. Per-tick command quota
    cnt = _CMD_COUNT_TICK.get(ctx.id, 0) + 1
    if cnt > PER_TICK_CMD_BUDGET:
        return False
    _CMD_COUNT_TICK[ctx.id] = cnt

    # 3. Daily token budget
    cost  = estimate_token_cost(cmd)
    used  = _TOKENS_USED_TODAY.get(ctx.id, 0) + cost
    if used > DAILY_TOKEN_BUDGET:
        return False
    _TOKENS_USED_TODAY[ctx.id] = used

    return True

def reset_tick_counters():
    """Call this once per World.tick to reset per-tick quotas."""
    _CMD_COUNT_TICK.clear()