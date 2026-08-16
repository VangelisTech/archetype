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

"""Path safety for user-influenced storage locations (issue #327).

Storage URIs and catalog namespaces arrive from the API and CLI, then flow
into filesystem paths (lance directories, SQLite catalog files, Iceberg
warehouses). Every local path normalizes through :func:`resolve_local_root`
so one containment rule applies everywhere, and a namespace must pass
:func:`require_safe_namespace` before it may join a path.

``ARCHETYPE_DATA_ROOT`` is the deployment containment control: when set,
every local storage path must resolve inside it, and escapes fail closed.
Unset — the local-development default — paths resolve unconstrained,
matching the development-grade API-auth posture documented in the
specification's durability table.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

from archetype._storage_uri import (
    local_storage_path,
)
from archetype._storage_uri import (
    normalized_storage_uri as normalized_storage_uri,
)

_NAMESPACE_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*")


def require_safe_namespace(namespace: str) -> str:
    """Return *namespace* when it is a single separator-free path segment.

    Raises:
        ValueError: If the namespace contains path separators, starts with a
            dot, or is otherwise able to traverse a directory when joined
            into a filesystem path.
    """
    if not _NAMESPACE_RE.fullmatch(namespace):
        raise ValueError(
            f"namespace {namespace!r} is not a safe catalog namespace: expected a "
            "single segment matching [A-Za-z0-9][A-Za-z0-9_.-]* (no path separators)"
        )
    return namespace


def resolve_local_root(path_like: str) -> Path:
    """Resolve a local storage path with fail-closed deployment containment.

    Canonicalization delegates to ``archetype._storage_uri.local_storage_path``
    (the shared URI normalizer); this function layers the security checks on
    top for filesystem sinks.

    Raises:
        ValueError: On an embedded NUL byte, a non-local URI, or — when
            ``ARCHETYPE_DATA_ROOT`` is set — a resolved path escaping it.
    """
    if "\x00" in path_like:
        raise ValueError("storage path contains a NUL byte")
    base = local_storage_path(path_like)
    if base is None:
        raise ValueError(f"{path_like!r} is not a local storage path")
    root = os.environ.get("ARCHETYPE_DATA_ROOT", "").strip()
    if root:
        root_path = Path(root).expanduser().resolve()
        if not base.is_relative_to(root_path):
            raise ValueError(
                f"storage path {base} escapes ARCHETYPE_DATA_ROOT {root_path} (fail closed)"
            )
    return base
