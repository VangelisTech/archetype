# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Pure audit-row construction helpers shared with the access gateway."""

from __future__ import annotations

from datetime import UTC, datetime

from uuid_utils import UUID

from archetype.commands.models import AuditRow


def make_audit_row(
    ctx,
    command_type: str,
    world_id: str | UUID | None = None,
    *,
    command_id: UUID | None = None,
    status: str = "applied",
    payload_json: str = "{}",
) -> AuditRow:
    """Build an ``AuditRow`` from one gateway decision."""
    now = datetime.now(UTC).isoformat()
    return AuditRow(
        command_id=command_id,
        world_id=world_id,
        actor_id=ctx.id,
        command_type=command_type,
        status=status,
        payload_json=payload_json,
        accepted_at=now,
        applied_at=now,
    )
