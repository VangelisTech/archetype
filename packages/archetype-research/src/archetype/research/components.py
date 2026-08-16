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

"""Generic ECS state for resumable world-library optimization.

The ledger deliberately knows nothing about repositories, coding agents,
virtual machines, workspaces, or commits. Candidate construction is supplied
by the caller and results remain opaque JSON evidence.
"""

from __future__ import annotations

import json
import time
from enum import StrEnum
from typing import Any

from archetype.core.component import Component

# ============================================================================
# Helper types — NOT Components. Used for type-safe construction and for
# decoding JSON-encoded fields back into friendly Python objects.
# ============================================================================


class RunStatus(StrEnum):
    """Generic lifecycle of one research candidate evaluation."""

    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"

    @classmethod
    def is_active(cls, status: str) -> bool:
        """True while the run is still in flight."""
        return status == cls.RUNNING.value

    @classmethod
    def is_terminal(cls, status: str) -> bool:
        """True once the run has reached a final state."""
        return status in (cls.SUCCEEDED.value, cls.FAILED.value)


def _now_ms() -> int:
    """Current time as Unix milliseconds."""
    return int(time.time() * 1000)


# ============================================================================
# Components
# ============================================================================


class Experiment(Component):
    """Identity and opaque configuration for one optimization experiment."""

    name: str = ""
    created_at_ms: int = 0
    metadata_json: str = "{}"

    @classmethod
    def make(
        cls,
        name: str,
        *,
        metadata: dict[str, Any] | None = None,
        created_at_ms: int | None = None,
    ) -> Experiment:
        """Convenience constructor that JSON-encodes metadata."""
        return cls(
            name=name,
            created_at_ms=created_at_ms if created_at_ms is not None else _now_ms(),
            metadata_json=json.dumps(metadata or {}),
        )

    def get_metadata(self) -> dict[str, Any]:
        """Decode the metadata JSON back to a dict."""
        return json.loads(self.metadata_json)


class Run(Component):
    """One candidate evaluation in an experiment ledger."""

    run_id: str = ""
    experiment_name: str = ""
    status: str = RunStatus.RUNNING.value
    candidate_world_id: str = ""
    started_at_ms: int = 0
    finished_at_ms: int = 0

    @property
    def is_active(self) -> bool:
        """True while the candidate evaluation remains in flight."""
        return RunStatus.is_active(self.status)

    @property
    def is_terminal(self) -> bool:
        """True once the candidate evaluation succeeded or failed."""
        return RunStatus.is_terminal(self.status)


class Result(Component):
    """An opaque eval envelope for a Run.

    The library persists; it does not interpret. User eval code puts
    whatever is meaningful into ``outputs_json`` — a scalar metric, a
    Pareto point, an LLM judge verdict, a full pytest report, a
    tournament record. Multiple Results per run are allowed (same
    ``run_id``, different ``evaluator``) so several eval harnesses can
    score the same run independently.

    Fields:
        run_id:         Foreign key to Run.run_id
        evaluator:      Free-form name of the eval source ('pytest', 'ruff', 'llm-judge', ...)
        outputs_json:   JSON dict of opaque user-defined eval output
        evaluated_at_ms: When the eval ran, in Unix milliseconds
    """

    run_id: str = ""
    evaluator: str = ""
    outputs_json: str = "{}"
    evaluated_at_ms: int = 0

    @classmethod
    def make(
        cls,
        run_id: str,
        outputs: dict[str, Any],
        *,
        evaluator: str = "",
        evaluated_at_ms: int | None = None,
    ) -> Result:
        """Convenience constructor that JSON-encodes outputs."""
        return cls(
            run_id=run_id,
            evaluator=evaluator,
            outputs_json=json.dumps(outputs),
            evaluated_at_ms=evaluated_at_ms if evaluated_at_ms is not None else _now_ms(),
        )

    def get_outputs(self) -> dict[str, Any]:
        """Decode the outputs JSON back to a dict."""
        return json.loads(self.outputs_json)


class BranchHead(Component):
    """Persisted incumbent candidate for one experiment."""

    experiment_name: str = ""
    candidate_world_id: str = ""
    run_id: str = ""
    descriptor_json: str = "{}"
    updated_at_ms: int = 0

    @classmethod
    def make(
        cls,
        experiment_name: str,
        *,
        candidate_world_id: str = "",
        run_id: str = "",
        descriptor: dict[str, Any] | None = None,
        updated_at_ms: int | None = None,
    ) -> BranchHead:
        """Convenience constructor that JSON-encodes the descriptor."""
        return cls(
            experiment_name=experiment_name,
            candidate_world_id=candidate_world_id,
            run_id=run_id,
            descriptor_json=json.dumps(descriptor or {}),
            updated_at_ms=updated_at_ms if updated_at_ms is not None else _now_ms(),
        )

    def get_descriptor(self) -> dict[str, Any]:
        """Decode the descriptor JSON back to a dict."""
        return json.loads(self.descriptor_json)
