# Copyright 2026 Vangelis Technologies Inc.
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

"""Public step-failure contract (issue #444).

``step()`` aggregates per-table failures into one ``TickExecutionError``
instead of flattening them into message text. The whole-tick failure
semantics are unchanged — the step raises, the tick counter does not
advance, nothing is appended, staged mutations survive for retry — what
this module adds is inspectable composition: every failed table identity
and original exception object crosses the boundary intact, so callers
classify a provider timeout or rate limit with ``isinstance`` on
``failure.error`` rather than parsing strings.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

TickPhase = Literal["compute", "commit"]


class AmbiguousTickCommitError(RuntimeError):
    """A prepared tick's manifest outcome could not be read authoritatively.

    The world retains the exact prepared commit identity and refuses to
    recompute or re-append the tick. A later ``step`` retries publication with
    that same identity.
    """

    public_detail = "Tick commit status is temporarily unavailable; retry the request"

    def __init__(self, *, tick: int, commit_token: str) -> None:
        self.tick = tick
        self.commit_token = commit_token
        super().__init__(
            f"manifest outcome for prepared tick {tick} is ambiguous; "
            "exact publication retry is required"
        )


@dataclass(frozen=True)
class TickFailure:
    """One archetype table's original failure within a failed tick.

    ``error`` is the exception object the table's compute or commit raised,
    unwrapped — its type, args, and traceback survive step aggregation.
    """

    table_id: str
    error: Exception


class TickExecutionError(RuntimeError):
    """One or more archetype tables failed while stepping a world tick.

    ``failures`` preserves every failed table identity and original
    exception object in ascending ``table_id`` order (the step's
    deterministic archetype order). ``phase`` names the step phase that
    aggregated them: ``"compute"`` (processor execution; nothing was
    written anywhere) or ``"commit"`` (persistence; the failed tables'
    staged mutations survive for retry).

    The message carries table identities and the phase only — never the
    original exception text, which may embed provider payloads or paths.
    Raise sites chain the originals as ``__cause__`` (an ``ExceptionGroup``
    at async aggregation boundaries, the single original in the fail-fast
    sync stack), so tracebacks still render every underlying failure.

    Subclasses ``RuntimeError``: callers that caught the old flattened
    aggregate keep catching this one; they gain typed classification.
    """

    def __init__(self, *, phase: TickPhase, failures: Sequence[TickFailure]) -> None:
        self.phase: TickPhase = phase
        self.failures: tuple[TickFailure, ...] = tuple(failures)
        tables = ", ".join(f.table_id for f in self.failures)
        super().__init__(
            f"{len(self.failures)} archetype table(s) failed during the {phase} phase: {tables}"
        )

    def __reduce__(self) -> tuple[object, ...]:
        # BaseException pickles as cls(*self.args), which cannot satisfy the
        # keyword-only constructor; rebuild from the structured state instead.
        return (_rebuild_tick_execution_error, (self.phase, self.failures))


def _rebuild_tick_execution_error(
    phase: TickPhase, failures: tuple[TickFailure, ...]
) -> TickExecutionError:
    return TickExecutionError(phase=phase, failures=failures)
