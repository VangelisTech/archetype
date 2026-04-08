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

"""
SideEffectCollector: Deferred mutation buffer for tick-level atomicity.

Processors that need to mutate Resources (e.g., ChatGraphRegistry) should
defer those mutations via the collector rather than applying them inline.
The collector is committed after successful persist, or rolled back on failure.

ACID mapping:
    Atomicity   — commit() applies ALL mutations or NONE. On partial failure,
                  already-applied mutations are reverted via compensating actions.
    Consistency — Processors validate via join predicates before deferring.
    Isolation   — Each archetype gets a forked Resources with its own collector.
    Durability  — DataFrame persist happens before commit(); mutations apply
                  to in-memory Resources only after data is safe.

Usage in processors:
    collector = resources.get(SideEffectCollector)
    if collector:
        collector.defer(
            apply=lambda: graph.append(cmd),
            rollback=lambda node: graph.remove(node.cmd.id),
        )
    else:
        graph.append(cmd)  # fallback for tests without collector
"""

import logging
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)


class SideEffectCollector:
    """Buffers deferred mutations for tick-level atomicity.

    Each deferred mutation is a pair: (apply, rollback). On commit, mutations
    are applied in order. If any mutation raises, all previously applied
    mutations in the batch are rolled back in reverse order.
    """

    __slots__ = ("_pending",)

    def __init__(self) -> None:
        self._pending: list[tuple[Callable[[], Any], Callable[[Any], None] | None]] = []

    def defer(
        self,
        apply: Callable[[], Any],
        rollback: Callable[[Any], None] | None = None,
    ) -> None:
        """Queue a mutation with an optional compensating rollback.

        Args:
            apply: Callable that performs the mutation. Its return value is
                   passed to rollback if a later mutation fails.
            rollback: Optional callable that undoes the mutation. Receives
                      the return value of apply(). If None, the mutation
                      is treated as non-reversible (best-effort).
        """
        self._pending.append((apply, rollback))

    def commit(self) -> None:
        """Apply all pending mutations atomically.

        If any mutation raises, all previously applied mutations in this
        batch are rolled back in reverse order via their compensating actions.
        The buffer is always cleared regardless of outcome.
        """
        applied: list[tuple[Any, Callable[[Any], None] | None]] = []
        try:
            for apply_fn, rollback_fn in self._pending:
                result = apply_fn()
                applied.append((result, rollback_fn))
        except Exception:
            # Roll back in reverse order
            for result, rollback_fn in reversed(applied):
                if rollback_fn is not None:
                    try:
                        rollback_fn(result)
                    except Exception:
                        logger.warning(
                            "Rollback failed for mutation; state may be inconsistent",
                            exc_info=True,
                        )
            raise
        finally:
            self._pending.clear()

    def rollback(self) -> None:
        """Discard all pending mutations without applying them."""
        self._pending.clear()

    def __len__(self) -> int:
        """Return the number of pending mutations."""
        return len(self._pending)
