# auth.py  (or inside async_command.py)
from uuid import UUID
from typing import Set
from pydantic import BaseModel, Field

class ActorCtx(BaseModel):
    id: UUID
    roles: Set[str] = Field(default_factory=set)
    org: str | None = None
    tokens_today: int = 0
    cmd_count_tick: int = 0

    model_config = {
        "frozen": True,  # Make immutable for hashability
        "arbitrary_types_allowed": True,  # Allow UUID and Set types
    }