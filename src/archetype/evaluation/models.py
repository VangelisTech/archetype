# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Import-light evaluation values, callbacks, and exact direct operations."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Awaitable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    ClassVar,
    Literal,
    Protocol,
    runtime_checkable,
)

from pydantic import (
    BaseModel,
    ConfigDict,
    PlainSerializer,
    WithJsonSchema,
    field_validator,
)
from pydantic_core import to_jsonable_python
from uuid_utils import UUID

if TYPE_CHECKING:
    from daft import DataFrame

    from archetype.core.component import Component
    from archetype.core.config import StorageConfig
else:
    # Operation construction validates these values lazily. Merely importing
    # evaluation contracts must not load Daft, Arrow, LanceDB, or core config.
    Component = Any
    DataFrame = Any
    StorageConfig = Any

_RECEIPT_DIGEST_DOMAIN = "archetype.receipt.v1"
_JsonUUID = Annotated[
    UUID,
    PlainSerializer(lambda value: str(value), return_type=str, when_used="json"),
    WithJsonSchema({"type": "string", "format": "uuid"}),
]

OUTCOME_STATUSES = frozenset({"pass", "fail", "invalid", "inconclusive"})

GraderOutput = object
GraderReturn = GraderOutput | Sequence[GraderOutput]


@runtime_checkable
class FrameGrader(Protocol):
    """Grade one lazy subject frame, synchronously or asynchronously."""

    def __call__(
        self,
        frame: DataFrame,
        /,
    ) -> GraderReturn | Awaitable[GraderReturn]: ...


# One current episodes consumer still uses the historical callback noun.
# Keep this as an identity alias, not a second protocol.
TrajectoryGrader = FrameGrader

if TYPE_CHECKING:
    GraderField = FrameGrader
else:
    # Pydantic cannot build an isinstance validator for a callable Protocol.
    # Exact callability is enforced by the field validators below.
    GraderField = Any


@dataclass(frozen=True)
class Outcome:
    """Represent a validated grading conclusion.

    ``status`` must be ``pass``, ``fail``, ``invalid``, or ``inconclusive``.
    A supplied score must be finite.
    """

    status: str
    score: float | None = None
    evidence: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.status not in OUTCOME_STATUSES:
            raise ValueError(
                f"outcome status {self.status!r} is not one of {sorted(OUTCOME_STATUSES)}"
            )
        if self.score is not None and not math.isfinite(self.score):
            raise ValueError("outcome score must be finite when present")


@dataclass(frozen=True)
class GraderContract:
    """Identify the grader configuration used for a durable receipt.

    Two receipts are directly comparable only when their contract digests
    match. Change ``implementation_version``, configuration, thresholds, or
    seed whenever that comparison should no longer be valid.
    """

    grader_id: str
    implementation_version: str
    config: dict = field(default_factory=dict)
    thresholds: dict = field(default_factory=dict)
    seed: int | None = None

    def __post_init__(self) -> None:
        if not self.grader_id.strip():
            raise ValueError("grader_id must be a non-empty stable identity")
        if not self.implementation_version.strip():
            raise ValueError(
                "implementation_version must name the grader implementation "
                "(code version, prompt hash, or model id)"
            )

    def digest(self) -> str:
        """Return the byte-stable versioned grader identity."""

        payload = json.dumps(
            {
                "domain": _RECEIPT_DIGEST_DOMAIN,
                "kind": "grader-contract",
                "grader_id": self.grader_id,
                "implementation_version": self.implementation_version,
                "config": to_jsonable_python(self.config),
                "thresholds": to_jsonable_python(self.thresholds),
                "seed": self.seed,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class _EvaluationOperation(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        arbitrary_types_allowed=True,
        extra="forbid",
    )

    direct_only: ClassVar[bool] = True
    operation: str


class RunGraders(_EvaluationOperation):
    """Run ephemeral grader callbacks over one lazy frame."""

    operation: Literal["run_graders"] = "run_graders"
    df: DataFrame
    graders: tuple[GraderField, ...]

    @field_validator("df")
    @classmethod
    def _require_frame(cls, value: Any) -> Any:
        from daft import DataFrame as DaftDataFrame

        if not isinstance(value, DaftDataFrame):
            raise ValueError("df must be a Daft DataFrame")
        return value

    @field_validator("graders")
    @classmethod
    def _require_graders(
        cls,
        value: tuple[GraderField, ...],
    ) -> tuple[GraderField, ...]:
        if not all(callable(grader) for grader in value):
            raise ValueError("graders must contain only callbacks")
        return value


class Evaluate(_EvaluationOperation):
    """Evaluate and persist a receipt for one pinned world snapshot."""

    operation: Literal["evaluate"] = "evaluate"
    world_id: str | _JsonUUID
    components: tuple[type[Component], ...]
    contract: GraderContract
    grader: GraderField
    evaluation_id: str
    storage_config: StorageConfig
    ticks: tuple[int, ...] | None = None
    entity_ids: tuple[int, ...] | None = None

    @field_validator("components")
    @classmethod
    def _require_components(
        cls,
        value: tuple[type[Component], ...],
    ) -> tuple[type[Component], ...]:
        from archetype.core.component import Component as RuntimeComponent

        if not all(
            isinstance(component, type) and issubclass(component, RuntimeComponent)
            for component in value
        ):
            raise ValueError("components must contain only Component subclasses")
        return value

    @field_validator("grader")
    @classmethod
    def _require_grader(cls, value: GraderField) -> GraderField:
        if not callable(value):
            raise ValueError("grader must be callable")
        return value

    @field_validator("storage_config")
    @classmethod
    def _require_storage_config(cls, value: Any) -> Any:
        from archetype.core.config import StorageConfig as RuntimeStorageConfig

        if not isinstance(value, RuntimeStorageConfig):
            raise ValueError("storage_config must be an explicit StorageConfig")
        return value


def summarize_evaluation_operation(
    operation: _EvaluationOperation,
) -> Mapping[str, Any]:
    """Return bounded routing identity without frames, callbacks, or evidence."""

    summary: dict[str, Any] = {"operation": operation.operation}
    if isinstance(operation, Evaluate):
        summary["world_id"] = str(operation.world_id)
    return summary


__all__ = [
    "Evaluate",
    "FrameGrader",
    "GraderContract",
    "GraderOutput",
    "GraderReturn",
    "OUTCOME_STATUSES",
    "Outcome",
    "RunGraders",
    "TrajectoryGrader",
    "summarize_evaluation_operation",
]
