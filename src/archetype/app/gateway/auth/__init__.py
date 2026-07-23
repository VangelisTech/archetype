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

"""Auth: RBAC guardrails and the four-role permissions model."""

from archetype.app.gateway.auth.errors import GuardrailError
from archetype.app.gateway.auth.guard import guardrail_allow
from archetype.app.gateway.auth.models import ActorCtx
from archetype.app.gateway.auth.permissions import COMMANDS_BY_ROLE

__all__ = [
    "ActorCtx",
    "COMMANDS_BY_ROLE",
    "GuardrailError",
    "guardrail_allow",
]
