from typing import Dict, Any
from uuid_utils import UUID
from pydantic import BaseModel, Field, field_serializer, FieldSerializationInfo
import uuid
import json
import pyarrow as pa
from typing import Literal

# Global sequence counter for command ordering
_SEQ = count()



class Command(BaseModel):
    id: UUID                       = Field(default_factory=uuid.uuid7())
    tick: int                      = 0
    actor_id: UUID
    op: Literal[
        "create_entity",
        "delete_entity",
        "add_component",
        "remove_component",
        "add_processor",
        "remove_processor",
        "custom",
    ]
    payload: Dict[str, Any]        = Field(default_factory=dict)
    priority: int                  = 0
    version: int                   = 1
    seq: int                       = Field(default_factory=lambda: next(_SEQ))

    model_config = dict(frozen=True)       # hashable & heap-friendly

    # ------------------------------------------------------------------ #
    # Provide Arrow-friendly serialisers
    # ------------------------------------------------------------------ #
    @field_serializer("id", "actor_id")
    def serialize_uuids(self, v: UUID, info: FieldSerializationInfo):
        if info.mode == 'json':
            return str(v)
        return v.bytes

    @classmethod
    def arrow_schema(cls) -> pa.schema:
        """Return a canonical Arrow schema suitable for Parquet."""
        return pa.schema(
            [
                ("id",        pa.binary(16)),
                ("tick",      pa.int32()),
                ("actor_id",  pa.binary(16)),
                ("op",        pa.string()),
                ("payload",   pa.string()),     # JSON-encoded; keep simple
                ("priority",  pa.int16()),
                ("version",   pa.int8()),
                ("seq",       pa.int64()),
            ]
        )

    def to_arrow(self) -> pa.RecordBatch:
        return pa.record_batch(
            [
                [self.id.bytes],
                [self.tick],
                [self.actor_id.bytes],
                [self.op],
                [json.dumps(self.payload)],
                [self.priority],
                [self.version],
                [self.seq],
            ],
            schema=self.arrow_schema(),
        )

    # ------------------------------------------------------------------ #
    # Keep the heap ordering contract
    # ------------------------------------------------------------------ #
    def __lt__(self, other: "Command") -> bool:       # noqa: Dunder
        return (self.tick, self.priority, self.seq) < (
            other.tick,
            other.priority,
            other.seq,
        )

