# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""The public-API marker.

``@public_api`` declares a callable part of archetype's supported surface.
The marker carries a machine-enforced contract (checked by
``scripts/check_api_import_boundaries.py``):

    A public callable may not accept raw services. Public capability must be
    expressible through ``ArchetypeRuntime`` or a supported adapter.

Rationale: a public function with ``world_service=`` / ``simulation_service=``
parameters forces every caller to hand-roll the composition graph and drive
services directly, bypassing ``RuntimeResources`` ownership and its lifecycle
and durability wiring. That bypass entered ``src/`` twice through exactly this
signature shape; both escape hatches have since been removed. Convention did
not hold; enforcement does. Untrusted adapters additionally pass through the
authenticated command dispatcher for RBAC and access audit.

Deprecated service-shaped parameters that exist only as migration bridges are
allowlisted (with a removal deadline) in the checker itself, next to
``ALLOWED_APP_IMPORTS`` — auditable in one place, like the lazy-audit ledger.
"""

from __future__ import annotations

from typing import Any


def public_api[F](obj: F) -> F:
    """Mark a callable or class as archetype public API (see module docstring)."""
    try:
        # setattr keeps the marker invisible to static generics (ty flags a
        # direct attribute write on the TypeVar-bound object).
        setattr(obj, "__archetype_public_api__", True)  # noqa: B010
    except (AttributeError, TypeError):  # pragma: no cover - exotic callables
        pass
    return obj


def is_public_api(obj: Any) -> bool:
    """True when ``obj`` was marked with :func:`public_api`."""
    return bool(getattr(obj, "__archetype_public_api__", False))
