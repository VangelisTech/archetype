# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Focused contracts for the storage-family signature resolver move."""

import json

import pytest

from archetype.app.storage.signatures import (
    match_signature_records as compatibility_match_signature_records,
)
from archetype.app.storage.signatures import (
    resolve_signature_records as compatibility_resolve_signature_records,
)
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.storage.catalog import (
    SignatureRecord,
    arrow_schema_descriptor,
    schema_fingerprint,
)
from archetype.storage.signatures import match_signature_records, resolve_signature_records


class SignatureMoveProbeComponent(Component):
    value: int = 0


def _record(
    *,
    table_id: str | None = None,
    component_names: tuple[str, ...] = ("SignatureMoveProbeComponent",),
) -> SignatureRecord:
    signature = (SignatureMoveProbeComponent,)
    schema = Archetype.get_archetype_schema(signature)
    return SignatureRecord(
        table_id=table_id or Archetype.get_name(signature),
        component_names=component_names,
        schema_json=json.dumps(arrow_schema_descriptor(schema)),
        fingerprint=schema_fingerprint(schema),
    )


def test_compatibility_module_reexports_canonical_resolvers() -> None:
    assert compatibility_match_signature_records is match_signature_records
    assert compatibility_resolve_signature_records is resolve_signature_records


def test_match_signature_records_resolves_exact_durable_identity() -> None:
    record = _record()

    resolved, problems = match_signature_records([record, record])

    assert resolved == {record.table_id: (SignatureMoveProbeComponent,)}
    assert problems == {}


def test_match_signature_records_diagnoses_compatible_schema_with_wrong_table_id() -> None:
    record = _record(table_id="stored-under-wrong-table-id")

    resolved, problems = match_signature_records([record])

    assert resolved == {}
    assert "resolves to different table identity" in problems[record.table_id]


def test_resolve_signature_records_fails_when_component_class_is_not_imported() -> None:
    record = _record(component_names=("MissingSignatureMoveProbeComponent",))

    with pytest.raises(
        RuntimeError,
        match=r"cannot resume world: .*MissingSignatureMoveProbeComponent.*not imported",
    ):
        resolve_signature_records([record], operation="resume world")
