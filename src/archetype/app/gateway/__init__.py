# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Gateway — policy enforcement, command queue, audit log.

The gateway is the boundary between external callers and internal services.
Every external operation flows through ``CommandService``, which checks RBAC,
delegates to the appropriate service, and emits audit rows.
"""

from archetype.app.gateway.audit_log import AuditLog
from archetype.app.gateway.broker import CommandBroker
from archetype.app.gateway.gate import CommandService

__all__ = [
    "AuditLog",
    "CommandBroker",
    "CommandService",
]
