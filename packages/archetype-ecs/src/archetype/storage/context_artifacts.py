# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Storage port for explicit nonexecuting contexts and optional cut attribution."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from archetype.storage.cut_artifacts import (
    CutArtifactStorage,
    CutCoordinates,
    CutHost,
    OccurrenceMetadata,
)


@dataclass(frozen=True)
class PublishedContextRef:
    world: str
    run: str
    context_id: str

    def __post_init__(self) -> None:
        if any(
            re.fullmatch(r"[a-z][a-z0-9_]{0,63}", value) is None for value in (self.world, self.run)
        ):
            raise ValueError("Invalid context scope")
        if re.fullmatch(r"[0-9a-f]{64}", self.context_id) is None:
            raise ValueError("Invalid context identity")

    @classmethod
    def from_publication(cls, publication: dict[str, Any]) -> PublishedContextRef:
        if publication["version"] != 1:
            raise ValueError("Unsupported context version")
        return cls(**{key: publication[key] for key in ("world", "run", "context_id")})

    def as_dict(self) -> dict[str, str]:
        return {"world": self.world, "run": self.run, "context_id": self.context_id}


@dataclass(frozen=True)
class ExactContextCut:
    tick: int
    cut_id: str

    def __post_init__(self) -> None:
        # Reuse the established exact Int64/hash contract; no cut is fabricated.
        if type(self.tick) is not int or not 1 <= self.tick < 2**63:
            raise ValueError("Expected exact positive Int64 tick")
        if re.fullmatch(r"[0-9a-f]{64}", self.cut_id) is None:
            raise ValueError("Invalid cut identity")

    @classmethod
    def from_cut(cls, cut: CutCoordinates) -> ExactContextCut:
        return cls(cut.tick, cut.cut_id)

    def as_dict(self) -> dict[str, int | str]:
        return {"tick": self.tick, "cut_id": self.cut_id}


@dataclass(frozen=True)
class ArtifactTarget:
    context: PublishedContextRef
    exact_cut: ExactContextCut | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "context": self.context.as_dict(),
            "exact_cut": None if self.exact_cut is None else self.exact_cut.as_dict(),
        }


class ContextArtifactStorage:
    """Borrow a Host or storage-only Store; native storage verifies authority."""

    def __init__(self, host: CutHost, context: PublishedContextRef) -> None:
        self._host = host
        self.context = context

    # The same deliberate Daft execution/encoding boundary serves both versions.
    materialize = staticmethod(CutArtifactStorage.materialize)
    encode = staticmethod(CutArtifactStorage.encode)
    decode = staticmethod(CutArtifactStorage.decode)

    def _target(self, target: ArtifactTarget) -> dict[str, Any]:
        if target.context != self.context:
            raise ValueError("Target differs from storage context")
        return target.as_dict()

    def verify(self, target: ArtifactTarget) -> Path:
        return Path(
            self._host.request("context_artifact_target", target=self._target(target))[
                "object_root"
            ]
        )

    def publish(
        self, target: ArtifactTarget, occurrences: tuple[OccurrenceMetadata, ...]
    ) -> tuple[dict[str, Any], ...]:
        return tuple(
            self._host.request(
                "attach_context_artifacts",
                target=self._target(target),
                attachments=[item.as_dict() for item in occurrences],
            )
        )

    def read(self, target: ArtifactTarget, *, offset: int = 0, limit: int = 32) -> dict[str, Any]:
        self._target(target)
        return self._host.request(
            "read_context_artifacts",
            context=self.context.as_dict(),
            selection={"kind": "target", "exact_cut": target.as_dict()["exact_cut"]},
            offset=offset,
            limit=limit,
        )

    def read_all(self, *, offset: int = 0, limit: int = 32) -> dict[str, Any]:
        return self._host.request(
            "read_context_artifacts",
            context=self.context.as_dict(),
            selection={"kind": "all"},
            offset=offset,
            limit=limit,
        )
