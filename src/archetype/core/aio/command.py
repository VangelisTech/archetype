import asyncio

from archetype.core.interfaces import Component
from archetype.core.aio.async_processor import AsyncProcessor
from typing import Type, Dict, List, Any, Union

from __future__ import annotations
from typing import Dict, Any, Literal
from uuid import UUID, uuid4
from typing import Any, Dict, Literal
from itertools import count
import json

from pydantic import BaseModel, Field, FieldSerializationInfo, field_serializer
import pyarrow as pa

from .broker import InMemoryBroker
from .async_interfaces import iBroker


_SEQ = count()          # global tie-break counter


class Command(BaseModel):
    id: UUID                       = Field(default_factory=uuid4)
    tick: int                      = 0
    actor_id: UUID                 = Field(default_factory=uuid4)
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
    @staticmethod
    def _uuid_to_bytes(v: UUID, _: FieldSerializationInfo) -> bytes:
        return v.bytes

    @field_serializer("id", "actor_id", when_used="json")
    def _uuid_json(self, v: UUID, _: FieldSerializationInfo):
        return str(v)

    @field_serializer("id", "actor_id", when_used="arrow")
    def _uuid_arrow(self, v: UUID, _: FieldSerializationInfo):
        return v.bytes       # 16-byte fixed width column

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

