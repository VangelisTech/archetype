# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Stable Temporal Workflow identity derived from an admitted command."""

from __future__ import annotations

import hashlib
import json


def durable_workflow_id(
    namespace: str,
    principal: str,
    idempotency_key: str,
    *,
    prefix: str,
) -> str:
    """Derive one stable Workflow ID from a namespaced idempotency identity.

    The hash deliberately contains no process-local or wall-clock values. A
    family chooses the namespace and human-readable prefix while the shared
    adapter owns canonicalization of the durable identity.
    """

    namespace = namespace.strip()
    principal = principal.strip()
    idempotency_key = idempotency_key.strip()
    prefix = prefix.strip()
    if not namespace or not principal or not idempotency_key or not prefix:
        raise ValueError(
            "durable Workflow identity requires namespace, principal, idempotency_key, and prefix"
        )
    material = json.dumps(
        {
            "idempotency_key": idempotency_key,
            "kind": namespace,
            "principal": principal,
            "schema_version": 1,
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return f"{prefix}-{hashlib.sha256(material).hexdigest()}"


__all__ = ["durable_workflow_id"]
