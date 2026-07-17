# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""The public-API marker.

``@public_api`` declares a callable part of archetype's supported surface.
The marker carries a machine-enforced contract (checked by
``scripts/check_api_import_boundaries.py``):

    A public callable may not accept raw services. Public capability must be
    expressible through ``ArchetypeRuntime`` and its gated handles.

Rationale: a public function with ``world_service=`` / ``simulation_service=``
parameters forces every caller to hand-roll a ``ServiceContainer`` and drive
services directly — bypassing the command gateway, so worlds mutate with no
command-audit provenance. That bypass entered ``src/`` twice through exactly
this signature shape (validate_r2_substrate 2026-07-15; eval_rollouts
2026-07-17). Convention did not hold; enforcement does.

Deprecated service-shaped parameters that exist only as migration bridges are
allowlisted (with a removal deadline) in the checker itself, next to
``ALLOWED_APP_IMPORTS`` — auditable in one place, like the lazy-audit ledger.
"""

from __future__ import annotations

from typing import Any

#: Names of callables that declared themselves public, for docs/audit tooling.
PUBLIC_API_REGISTRY: list[str] = []


def public_api[F](obj: F) -> F:
    """Mark a callable or class as archetype public API (see module docstring)."""
    qualname = getattr(obj, "__qualname__", getattr(obj, "__name__", repr(obj)))
    module = getattr(obj, "__module__", "?")
    PUBLIC_API_REGISTRY.append(f"{module}.{qualname}")
    try:
        obj.__archetype_public_api__ = True  # type: ignore[attr-defined]
    except (AttributeError, TypeError):  # pragma: no cover - exotic callables
        pass
    return obj


def is_public_api(obj: Any) -> bool:
    """True when ``obj`` was marked with :func:`public_api`."""
    return bool(getattr(obj, "__archetype_public_api__", False))
