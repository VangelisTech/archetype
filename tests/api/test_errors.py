# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for mapping public app errors at the HTTP adapter boundary."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from archetype.api.errors import raise_api_error
from archetype.app._catalog import (
    CatalogConflictError,
    CatalogSchemaMismatchError,
    ClaimConflictError,
    ClaimPendingError,
)
from archetype.app.errors import ConflictError


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


def test_catalog_schema_mismatch_remains_an_internal_error() -> None:
    with pytest.raises(HTTPException) as raised:
        raise_api_error(CatalogSchemaMismatchError("private schema detail"))

    assert raised.value.status_code == 500
    assert raised.value.detail == "Internal server error"
