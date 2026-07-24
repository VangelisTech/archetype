# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for mapping public app errors at the HTTP adapter boundary."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi import HTTPException

from archetype.api.errors import raise_api_error
from archetype.app import errors as compatibility_errors
from archetype.commands.audit import AuditBackpressureError
from archetype.core.errors import AmbiguousTickCommitError, TickExecutionError, TickFailure
from archetype.errors import (
    AvailabilityError,
    ConflictError,
    PayloadRejectedError,
    WorldNotFoundError,
)
from archetype.redaction import SecretQuarantineError
from archetype.storage.catalog import (
    CatalogConflictError,
    CatalogSchemaMismatchError,
    SqliteControlCatalog,
)


def test_app_error_shim_preserves_canonical_class_identity() -> None:
    assert compatibility_errors.AvailabilityError is AvailabilityError
    assert compatibility_errors.ConflictError is ConflictError
    assert compatibility_errors.PayloadRejectedError is PayloadRejectedError
    assert compatibility_errors.WorldNotFoundError is WorldNotFoundError


@pytest.mark.parametrize(
    ("error_type", "public_detail"),
    [
        (CatalogConflictError, "Catalog entry conflicts with existing state"),
    ],
)
def test_catalog_conflicts_map_through_public_contract(
    error_type: type[ConflictError],
    public_detail: str,
) -> None:
    private_detail = "catalog /srv/private/archetype/catalog.sqlite already claimed"
    error = error_type(private_detail)

    assert isinstance(error, RuntimeError)
    assert str(error) == private_detail
    with pytest.raises(HTTPException) as raised:
        raise_api_error(error)

    assert raised.value.status_code == 409
    assert raised.value.detail == public_detail
    assert "/srv/private" not in raised.value.detail


def test_conflict_contract_defaults_to_a_safe_public_detail() -> None:
    with pytest.raises(HTTPException) as raised:
        raise_api_error(ConflictError("internal path: /srv/private/catalog.sqlite"))

    assert raised.value.status_code == 409
    assert raised.value.detail == "Request conflicts with existing state"


def test_audit_backpressure_maps_through_public_availability_contract() -> None:
    private_detail = "audit flush failed for /srv/private/audit"
    error = AuditBackpressureError(private_detail)

    assert isinstance(error, AvailabilityError)
    assert isinstance(error, RuntimeError)
    assert str(error) == private_detail
    with pytest.raises(HTTPException) as raised:
        raise_api_error(error)

    assert raised.value.status_code == 503
    assert raised.value.detail == "Audit log is temporarily unavailable"
    assert "/srv/private" not in raised.value.detail


def test_availability_contract_defaults_to_a_safe_public_detail() -> None:
    with pytest.raises(HTTPException) as raised:
        raise_api_error(AvailabilityError("private dependency failure"))

    assert raised.value.status_code == 503
    assert raised.value.detail == "Service is temporarily unavailable"


def test_ambiguous_tick_commit_is_public_and_maps_to_bounded_retry_signal() -> None:
    import archetype

    secret_token = "private-commit-token"
    error = AmbiguousTickCommitError(tick=7, commit_token=secret_token)

    assert archetype.AmbiguousTickCommitError is AmbiguousTickCommitError
    assert isinstance(error, RuntimeError)
    with pytest.raises(HTTPException) as raised:
        raise_api_error(error)

    assert raised.value.status_code == 503
    assert raised.value.detail == (
        "Tick commit status is temporarily unavailable; retry the request"
    )
    assert secret_token not in raised.value.detail


def test_payload_rejection_contract_maps_to_safe_unprocessable_content() -> None:
    secret = "sk-proj-" + "example-secret-never-expose"
    error = SecretQuarantineError(
        f"artifact metadata containing {secret}",
        ("openai-api-key",),
    )

    assert isinstance(error, PayloadRejectedError)
    assert secret not in str(error)
    with pytest.raises(HTTPException) as raised:
        raise_api_error(error)

    assert raised.value.status_code == 422
    assert raised.value.detail == "Payload rejected by safety policy"
    assert secret not in raised.value.detail


def test_catalog_schema_mismatch_remains_an_internal_error() -> None:
    with pytest.raises(HTTPException) as raised:
        raise_api_error(CatalogSchemaMismatchError("private schema detail"))

    assert raised.value.status_code == 500
    assert raised.value.detail == "Internal server error"


def test_tick_execution_error_remains_a_redacted_internal_error() -> None:
    """#444: the structured step failure is a Python-boundary contract; the
    REST adapter fails closed without leaking table errors or payloads."""
    secret = "sk-proj-" + "example-secret-never-expose"
    error = TickExecutionError(
        phase="commit",
        failures=(
            TickFailure(
                table_id="a_1c_safe_table_id",
                error=TimeoutError(f"provider failed with {secret}"),
            ),
        ),
    )

    with pytest.raises(HTTPException) as raised:
        raise_api_error(error)

    assert raised.value.status_code == 500
    assert raised.value.detail == "Internal server error"
    assert secret not in raised.value.detail


@pytest.mark.asyncio
async def test_newer_catalog_version_remains_a_redacted_internal_error(
    tmp_path: Path,
) -> None:
    path = tmp_path / "catalog.sqlite"
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE catalog_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        connection.execute("INSERT INTO catalog_meta (key, value) VALUES ('schema_version', '999')")

    catalog = SqliteControlCatalog(path)
    with pytest.raises(CatalogSchemaMismatchError) as mismatch:
        await catalog.list_worlds()

    assert "schema_version=999" in str(mismatch.value)
    with pytest.raises(HTTPException) as raised:
        raise_api_error(mismatch.value)

    assert raised.value.status_code == 500
    assert raised.value.detail == "Internal server error"
