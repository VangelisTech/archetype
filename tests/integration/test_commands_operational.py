# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Credential-free source/wheel receipt for the commands execution boundary."""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from archetype.api.app import create_app
from archetype.api.deps import set_container
from archetype.app.container import ServiceContainer
from archetype.core.config import StorageBackend, StorageConfig
from tests.commands.test_durable_runtime_contracts import (
    test_manifest_failure_keeps_command_leased_and_retry_does_not_restage as _post_stage_retry,
)
from tests.commands.test_integration_contracts import (
    test_reserved_spawn_direct_and_deferred_share_locked_family_behavior as _reserved_spawn_parity,
)
from tests.integration.test_command_flow import (
    test_command_materializer_infrastructure_failure_fails_tick_before_settlement as _materializer_failure,
)
from tests.integration.test_command_flow import (
    test_submit_spawn_reserved_id_survives_drain as _due_tick_signature,
)

pytestmark = [
    pytest.mark.contract("gateway.authorization.rbac"),
    pytest.mark.contract("commands.identity.idempotent"),
    pytest.mark.contract("commands.settlement.atomic"),
    pytest.mark.contract("commands.failure.preserves_progress"),
    pytest.mark.integration,
]


@pytest.mark.asyncio
async def test_trusted_direct_and_deferred_reserved_spawn_share_family_behavior(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Direct and deferred entry preserve one reserved ID through one family seam."""

    await _reserved_spawn_parity(monkeypatch)


@pytest.mark.asyncio
async def test_due_spawn_creates_its_new_signature_in_the_due_tick(tmp_path) -> None:
    """A due spawn is visible to signature discovery and persistence in that tick."""

    await _due_tick_signature(tmp_path)


def test_actor_aware_api_denial_emits_one_bounded_redacted_access_row(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A full-policy HTTP denial is a 403 with one payload-free access receipt."""

    audit_storage = StorageConfig(
        uri=str(tmp_path / "audit-store"),
        namespace="commands_operational",
        backend=StorageBackend.ICEBERG,
    )
    container = ServiceContainer(audit_storage_config=audit_storage)
    monkeypatch.setattr(container.policy, "_max_tokens_per_day", 0)
    set_container(container)
    try:
        with TestClient(create_app()) as client:
            response = client.post(
                "/worlds",
                headers={"Authorization": "Bearer admin"},
                json={
                    "name": "WORLD_NAME_SENTINEL",
                    "storage_uri": str(tmp_path / "STORAGE_URI_SENTINEL"),
                },
            )

            assert response.status_code == 403
            assert response.json() == {"detail": "actor exceeded daily token budget (0 tokens)"}
            (evidence,) = container.audit_log._pending  # noqa: SLF001 - exact receipt seam
            encoded = json.dumps(
                evidence.model_dump(mode="python"),
                sort_keys=True,
                default=str,
            )
            assert evidence.command_type == "create_world"
            assert evidence.status == "denied"
            assert evidence.world_id is None
            assert evidence.actor_id is not None
            assert evidence.payload_json == "{}"
            assert len(encoded.encode("utf-8")) <= 4096
            assert "WORLD_NAME_SENTINEL" not in encoded
            assert "STORAGE_URI_SENTINEL" not in encoded
    finally:
        set_container(None)


@pytest.mark.asyncio
async def test_materializer_failure_leaves_tick_and_command_unsettled_for_retry(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Infrastructure failure advances neither tick nor durable settlement."""

    await _materializer_failure(tmp_path, monkeypatch)


@pytest.mark.asyncio
async def test_post_stage_retry_does_not_duplicate_the_reserved_spawn(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A manifest failure retries the staged identity without restaging mutation."""

    await _post_stage_retry(tmp_path, monkeypatch)
