# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Actor identity model."""

from pydantic import BaseModel, Field
from uuid_utils import UUID


class ActorCtx(BaseModel):
    """Identity and permissions of the caller."""

    id: UUID
    roles: set[str] = Field(default_factory=lambda: {"viewer"})

    model_config = dict(frozen=True, arbitrary_types_allowed=True)
