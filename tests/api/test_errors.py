# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for mapping public app errors at the HTTP adapter boundary."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi import HTTPException

from archetype.api.errors import raise_api_error
from archetype.app._catalog import (
    CatalogConflictError,
    CatalogSchemaMismatchError,
    ClaimConflictError,
    ClaimPendingError,
    SqliteControlCatalog,
)
from archetype.app.audit_log import AuditBackpressureError
from archetype.app.errors import AvailabilityError, ConflictError


@pytest.mark.parametrize(
    ("error_type", "public_detail"),
    [
        (CatalogConflictError, "Catalog entry conflicts with existing state"),
        (ClaimConflictError, "Claim conflicts with existing state"),
        (ClaimPendingError, "Claim is currently pending"),
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


def test_catalog_schema_mismatch_remains_an_internal_error() -> None:
    with pytest.raises(HTTPException) as raised:
        raise_api_error(CatalogSchemaMismatchError("private schema detail"))

    assert raised.value.status_code == 500
    assert raised.value.detail == "Internal server error"


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
