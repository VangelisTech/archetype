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

Usage in processors:
    collector = resources.get(SideEffectCollector)
    if collector:
        collector.defer(lambda: graph.append(cmd))
    else:
        graph.append(cmd)  # fallback for tests without collector
"""

from collections.abc import Callable


class SideEffectCollector:
    """Buffers deferred mutations for tick-level atomicity."""

    __slots__ = ("_pending",)

    def __init__(self) -> None:
        self._pending: list[Callable[[], None]] = []

    def defer(self, fn: Callable[[], None]) -> None:
        """Queue a mutation (sync callable) to be applied on commit."""
        self._pending.append(fn)

    def commit(self) -> None:
        """Apply all pending mutations in order, then clear the buffer.

        Always clears the buffer, even if a mutation raises. This prevents
        stale mutations from being re-committed on a subsequent call.
        """
        try:
            for fn in self._pending:
                fn()
        finally:
            self._pending.clear()

    def rollback(self) -> None:
        """Discard all pending mutations without applying them."""
        self._pending.clear()

    def __len__(self) -> int:
        """Return the number of pending mutations."""
        return len(self._pending)
