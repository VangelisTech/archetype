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

"""Actor identity model."""

from pydantic import BaseModel, Field
from uuid_utils import UUID


class ActorCtx(BaseModel):
    """Identity and permissions of the caller."""

    id: UUID = Field(description="Stable actor identifier used by audit records.")
    roles: set[str] = Field(
        default_factory=lambda: {"viewer"}, description="Roles granted to this actor."
    )

    model_config = dict(frozen=True, arbitrary_types_allowed=True)
