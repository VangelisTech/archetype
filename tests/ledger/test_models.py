# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Strict A1 ledger reference and durable-record model contracts."""

import pytest
from pydantic import ValidationError

from archetype.core.config import StorageBackend
from archetype.ledger.canonical import internal_digest
from archetype.ledger.models import (
    BatchRef,
    ComponentRef,
    EntitySignatureRef,
    LedgerIdentity,
    LedgerInfo,
    LedgerManifest,
    LedgerRef,
    LineageSegment,
    SignatureRef,
    StorageRef,
)
from archetype.ledger.records import (
    AtomicPutResult,
    DurableRecord,
    iAsyncAtomicRecordStore,
    iAsyncReadExistingStore,
)


def _digest(kind: str) -> str:
    return internal_digest(kind, {"value": kind})


def _storage() -> StorageRef:
    return StorageRef.create(
        backend=StorageBackend.LANCEDB,
        data_uri="file:///var/lib/archetype",
        namespace="evals",
        catalog_uri="file:///var/lib/archetype/evals/.archetype/catalog-v1.sqlite3",
    )


def _identity() -> LedgerIdentity:
    return LedgerIdentity(storage=_storage(), world_id="world-1", run_id="run-1")


def _signature() -> SignatureRef:
    component = ComponentRef(component_id="test:position", schema_digest=_digest("component"))
    return SignatureRef(
        table_id="a_1c_s0123456789abcdef",
        components=(component,),
        signature_digest=_digest("signature"),
        schema_digest=_digest("signature-schema"),
    )


def test_storage_ref_is_strict_frozen_and_credential_free():
    storage = _storage()
    with pytest.raises(ValidationError, match="frozen"):
        storage.namespace = "other"  # type: ignore[misc]
    with pytest.raises(ValidationError, match="extra"):
        StorageRef.model_validate({**storage.model_dump(), "token": "secret"})

    with pytest.raises(ValueError, match="credentials"):
        StorageRef.create(
            backend=StorageBackend.LANCEDB,
            data_uri="https://user:secret@example.test/data",
            namespace="evals",
        )
    with pytest.raises(ValueError, match="query or fragment"):
        StorageRef.create(
            backend=StorageBackend.LANCEDB,
            data_uri="https://example.test/data?token=secret",
            namespace="evals",
        )


def test_storage_ref_normalizes_relative_uri_and_rejects_spoofed_digest(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    relative = StorageRef.create(
        backend=StorageBackend.LANCEDB,
        data_uri="./relative",
        namespace="evals",
        catalog_uri="./relative/evals/.archetype/catalog-v1.sqlite3",
    )

    assert relative.data_uri == (tmp_path / "relative").as_uri()
    assert (
        relative.catalog_uri == (tmp_path / "relative/evals/.archetype/catalog-v1.sqlite3").as_uri()
    )

    storage = _storage()
    with pytest.raises(ValidationError, match="storage_id mismatch"):
        StorageRef.model_validate(
            {**storage.model_dump(mode="python"), "storage_id": "sha256:" + "0" * 64}
        )


def test_storage_ref_golden_vector_and_strict_json_round_trip():
    storage = StorageRef.create(
        backend=StorageBackend.LANCEDB,
        data_uri="file:///opt/archetype-golden",
        namespace="evals",
        catalog_uri="file:///opt/archetype-golden/evals/.archetype/catalog-v1.sqlite3",
    )

    assert storage.storage_id == (
        "sha256:591b0c1b058c153b7762793ce2bfa1a8c30659e3325a4184518233ae4b516b9b"
    )
    assert StorageRef.model_validate_json(storage.model_dump_json()) == storage
    with pytest.raises(ValidationError, match="StorageBackend"):
        StorageRef.model_validate({**storage.model_dump(mode="json")})


def test_storage_ref_rejects_noncanonical_file_uri_even_with_matching_digest():
    payload = {
        "schema_version": 1,
        "backend": StorageBackend.LANCEDB.value,
        "data_uri": "file://localhost/opt/archetype",
        "namespace": "evals",
        "catalog_uri": None,
    }

    with pytest.raises(ValidationError, match="canonical absolute URI"):
        StorageRef(
            backend=StorageBackend.LANCEDB,
            data_uri=payload["data_uri"],
            namespace=payload["namespace"],
            catalog_uri=None,
            storage_id=internal_digest("archetype-storage-ref-v1", payload),
        )


def test_identifiers_reject_non_nfc_whitespace_and_controls():
    digest = _digest("schema")
    for value in ("e\u0301", " padded", "line\nbreak"):
        with pytest.raises(ValidationError):
            ComponentRef(component_id=value, schema_digest=digest)


def test_signature_components_are_nonempty_sorted_and_unique():
    a = ComponentRef(component_id="a", schema_digest=_digest("a"))
    b = ComponentRef(component_id="b", schema_digest=_digest("b"))
    common = {
        "table_id": "table",
        "signature_digest": _digest("sig"),
        "schema_digest": _digest("schema"),
    }

    with pytest.raises(ValidationError, match="nonempty"):
        SignatureRef(components=(), **common)
    with pytest.raises(ValidationError, match="sorted"):
        SignatureRef(components=(b, a), **common)
    with pytest.raises(ValidationError, match="unique"):
        SignatureRef(components=(a, a), **common)


def test_ledger_ref_uses_unambiguous_empty_and_nonempty_tick_contracts():
    empty = LedgerRef(
        identity=_identity(),
        manifest_digest=_digest("manifest-0"),
        manifest_generation=0,
        committed_through_tick=None,
        next_tick=0,
    )
    assert empty.committed_through_tick is None

    with pytest.raises(ValidationError, match="next_tick=0"):
        LedgerRef.model_validate({**empty.model_dump(), "next_tick": 2})
    with pytest.raises(ValidationError, match=r"committed_through_tick \+ 1"):
        LedgerRef(
            identity=_identity(),
            manifest_digest=_digest("manifest-1"),
            manifest_generation=1,
            committed_through_tick=4,
            next_tick=7,
        )


def test_generation_zero_manifest_digest_excludes_only_timestamp():
    values = dict(
        identity=_identity(),
        name="empty-ledger",
        generation=0,
        previous_manifest_digest=None,
        commit_id=_digest("commit-0"),
        committed_through_tick=None,
        next_tick=0,
        next_entity_id=1,
        signatures=(),
        entity_directory=(),
        lineage=(),
        batches=(),
        writer_epoch=0,
        execution_contract_digest=None,
    )
    first = LedgerManifest.create(**values, committed_at_ms=100)
    second = LedgerManifest.create(**values, committed_at_ms=200)

    assert first.manifest_digest == second.manifest_digest
    assert first.committed_at_ms != second.committed_at_ms
    with pytest.raises(ValidationError, match="manifest_digest mismatch"):
        LedgerManifest.model_validate({**first.model_dump(mode="python"), "next_entity_id": 2})
    assert LedgerManifest.model_validate_json(first.model_dump_json()) == first


@pytest.mark.parametrize(
    "update",
    [
        {"committed_through_tick": 0, "next_tick": 1},
        {"signatures": (_signature(),)},
        {
            "entity_directory": (
                EntitySignatureRef(entity_id=0, signature_digest=_digest("signature")),
            )
        },
        {"lineage": (LineageSegment(world_id="parent", run_id="run", up_to_tick=0),)},
        {
            "batches": (
                BatchRef(
                    commit_id=_digest("genesis"),
                    table_id="table",
                    tick=0,
                    writer_epoch=0,
                    row_count=0,
                    content_digest=_digest("empty-batch"),
                ),
            )
        },
        {"writer_epoch": 1},
    ],
)
def test_generation_zero_rejects_non_genesis_state(update):
    values = {
        "identity": _identity(),
        "name": " genesis display name ",
        "generation": 0,
        "previous_manifest_digest": None,
        "commit_id": _digest("genesis"),
        "committed_through_tick": None,
        "next_tick": 0,
        "next_entity_id": 1,
        "signatures": (),
        "entity_directory": (),
        "lineage": (),
        "batches": (),
        "writer_epoch": 0,
        "execution_contract_digest": None,
        **update,
    }

    with pytest.raises(ValidationError, match="generation zero"):
        LedgerManifest.create(**values, committed_at_ms=1)


def test_manifest_and_durable_record_golden_vectors():
    storage = StorageRef.create(
        backend=StorageBackend.LANCEDB,
        data_uri="file:///opt/archetype-golden",
        namespace="evals",
        catalog_uri="file:///opt/archetype-golden/evals/.archetype/catalog-v1.sqlite3",
    )
    manifest = LedgerManifest.create(
        identity=LedgerIdentity(storage=storage, world_id="world-1", run_id="run-1"),
        name="golden",
        generation=0,
        previous_manifest_digest=None,
        commit_id=internal_digest("golden-commit", {"value": "genesis"}),
        committed_through_tick=None,
        next_tick=0,
        next_entity_id=1,
        signatures=(),
        entity_directory=(),
        lineage=(),
        batches=(),
        writer_epoch=0,
        execution_contract_digest=None,
        committed_at_ms=123,
    )
    record = DurableRecord.create(
        kind="ledger-manifest",
        scope=storage.storage_id,
        key="golden",
        revision=0,
        payload={"manifest_digest": manifest.manifest_digest, "nullable": None},
        committed_at_ms=123,
    )

    assert manifest.manifest_digest == (
        "sha256:78823d7f999e37817ef618b91f504263b9bceaec8abef029b2d4230755d44cda"
    )
    assert record.payload_json == (
        '{"manifest_digest":"sha256:78823d7f999e37817ef618b91f504263b9bceaec8abef029b2d4230755d44cda",'
        '"nullable":null}'
    )
    assert record.content_digest == (
        "sha256:a2fd76d0916ea95b9be73170add1427b84ebf90c60baa1c1743c902f9d38f041"
    )


def test_manifest_binds_catalog_directory_lineage_and_batches():
    signature = _signature()
    batch = BatchRef(
        commit_id=_digest("commit-1"),
        table_id=signature.table_id,
        tick=0,
        writer_epoch=3,
        row_count=1,
        content_digest=_digest("batch"),
    )
    manifest = LedgerManifest.create(
        identity=_identity(),
        name="ledger",
        generation=1,
        previous_manifest_digest=_digest("manifest-0"),
        commit_id=batch.commit_id,
        committed_through_tick=0,
        next_tick=1,
        next_entity_id=2,
        signatures=(signature,),
        entity_directory=(
            EntitySignatureRef(entity_id=1, signature_digest=signature.signature_digest),
        ),
        lineage=(LineageSegment(world_id="parent", run_id="parent-run", up_to_tick=0),),
        batches=(batch,),
        writer_epoch=3,
        execution_contract_digest="b3:" + "a" * 64,
        committed_at_ms=123,
    )

    assert manifest.batches == (batch,)
    assert manifest.next_tick == 1

    with pytest.raises(ValidationError, match="undeclared signature"):
        LedgerManifest.create(
            **{
                **manifest.model_dump(exclude={"manifest_digest", "committed_at_ms"}),
                "entity_directory": (
                    EntitySignatureRef(entity_id=1, signature_digest=_digest("unknown")),
                ),
            },
            committed_at_ms=123,
        )


def test_ledger_info_requires_canonical_sequences():
    signature = _signature()
    ref = LedgerRef(
        identity=_identity(),
        manifest_digest=_digest("manifest"),
        manifest_generation=0,
        committed_through_tick=None,
        next_tick=0,
    )
    info = LedgerInfo(
        ref=ref,
        name=" ledger display name ",
        next_entity_id=1,
        signatures=(signature,),
        lineage=(),
    )
    assert info.ref == ref
    assert info.name == " ledger display name "


def test_ledger_info_rejects_cross_signature_component_schema_drift():
    first = SignatureRef(
        table_id="table-a",
        components=(ComponentRef(component_id="shared", schema_digest=_digest("schema-a")),),
        signature_digest=_digest("signature-a"),
        schema_digest=_digest("composite-a"),
    )
    second = SignatureRef(
        table_id="table-b",
        components=(ComponentRef(component_id="shared", schema_digest=_digest("schema-b")),),
        signature_digest=_digest("signature-b"),
        schema_digest=_digest("composite-b"),
    )
    signatures = tuple(
        sorted((first, second), key=lambda value: (value.signature_digest, value.table_id))
    )
    ref = LedgerRef(
        identity=_identity(),
        manifest_digest=_digest("manifest"),
        manifest_generation=0,
        committed_through_tick=None,
        next_tick=0,
    )

    with pytest.raises(ValidationError, match="conflicting schema digests"):
        LedgerInfo(
            ref=ref,
            name=None,
            next_entity_id=1,
            signatures=signatures,
            lineage=(),
        )


def test_durable_record_binds_payload_and_excludes_commit_timestamp():
    first = DurableRecord.create(
        kind="manifest-head",
        scope="storage/world/run",
        key="head",
        revision=0,
        payload={"generation": 0, "optional": None},
        committed_at_ms=100,
    )
    second = DurableRecord.create(
        kind="manifest-head",
        scope="storage/world/run",
        key="head",
        revision=0,
        payload={"optional": None, "generation": 0},
        committed_at_ms=200,
    )

    assert first.payload_json == '{"generation":0,"optional":null}'
    assert first.content_digest == second.content_digest
    with pytest.raises(ValidationError, match="content_digest mismatch"):
        DurableRecord.model_validate(
            {**first.model_dump(), "payload_json": '{"generation":1,"optional":null}'}
        )
    with pytest.raises(ValidationError, match="canonical JSON"):
        DurableRecord.model_validate({**first.model_dump(), "payload_json": '{"optional": null}'})


def test_durable_record_accepts_its_rfc_8785_fixed_number_output():
    record = DurableRecord.create(
        kind="number-vector",
        scope="golden",
        key="fixed",
        revision=0,
        payload={"value": 9.999999999999999e20},
    )

    assert record.payload_json == '{"value":999999999999999900000}'
    assert DurableRecord.model_validate(record.model_dump()) == record


def test_durable_record_revision_chain_and_atomic_result_are_strict():
    first = DurableRecord.create(
        kind="lease",
        scope="ledger",
        key="writer",
        revision=0,
        payload={"epoch": 0},
    )
    second = DurableRecord.create(
        kind="lease",
        scope="ledger",
        key="writer",
        revision=1,
        previous_digest=first.content_digest,
        payload={"epoch": 1},
    )
    result = AtomicPutResult(record=second, replayed=False)

    assert result.record.previous_digest == first.content_digest
    with pytest.raises(ValidationError, match="previous digest"):
        DurableRecord.create(
            kind="lease", scope="ledger", key="writer", revision=1, payload={"epoch": 1}
        )
    with pytest.raises(ValidationError, match="extra"):
        AtomicPutResult.model_validate({**result.model_dump(), "inserted": True})


def test_atomic_record_store_protocol_is_runtime_checkable():
    class CompleteStore:
        async def put_if_absent(self, record): ...
        async def get(self, *, kind, scope, key, revision=0): ...
        async def get_latest(self, *, kind, scope, key): ...
        async def compare_and_swap(self, record, *, expected_revision, expected_digest): ...
        async def scan(self, *, kind, scope=None): ...

        async def scan_latest(self, *, kind, scope=None): ...

    assert isinstance(CompleteStore(), iAsyncAtomicRecordStore)


def test_read_existing_store_protocol_is_runtime_checkable():
    class CompleteReader:
        async def table_exists(self, table_id): ...
        async def list_existing_table_ids(self): ...
        async def get_table_schema(self, table_id): ...
        async def get_table_df(
            self,
            table_id,
            world_id,
            run_id,
            *,
            ticks=None,
            entity_ids=None,
            active_only=False,
        ): ...

    assert isinstance(CompleteReader(), iAsyncReadExistingStore)
