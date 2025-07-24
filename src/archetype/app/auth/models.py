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

# 1. ActorCtx – identity & runtime counters
class ActorCtx(BaseModel):
    """
    Carries identity + per-run counters.
    Immutable (frozen) so nobody mutates it after enqueue.
    """
    id:   UUID
    roles: Set[str] = Field(default_factory=set)
    org:  str | None = None

    # runtime counters – broker mutates these in-memory dicts, not the model
    tokens_today: int = 0
    cmd_count_tick: int = 0

    model_config = dict(frozen=True)

class SimCtx(BaseModel):
    pass