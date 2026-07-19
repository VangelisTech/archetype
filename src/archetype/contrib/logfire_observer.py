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

"""Deprecated compatibility surface for the former Logfire hook observer.

The former implementation opened dynamic tick spans, retained global span
state, and sent content-bearing hook values directly to a vendor SDK. Hosts
now select an exporter through the vendor-neutral observability adapter. Tick
execution attribution remains owned by the world path and #518/#519 rather
than by an optional hook list.
"""

from __future__ import annotations

import warnings


def logfire_hooks() -> list[tuple]:
    """Return no alternate hooks; configure observability at the host."""
    warnings.warn(
        "logfire_hooks() is deprecated; configure Logfire or OTLP at the "
        "Archetype process-host boundary instead",
        DeprecationWarning,
        stacklevel=2,
    )
    return []
