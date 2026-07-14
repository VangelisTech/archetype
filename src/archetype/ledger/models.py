# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Strict, transport-safe contracts for durable Archetype ledgers."""

from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Literal
from urllib.parse import unquote, urlsplit

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    model_validator,
)

from archetype.core.config import StorageBackend
from archetype.ledger.canonical import internal_digest

if TYPE_CHECKING:
    from archetype.core.interfaces import iWorld

InternalDigest = Annotated[
    str,
    StringConstraints(pattern=r"^sha256:[0-9a-f]{64}$"),
]
ContentDigest = Annotated[
    str,
    StringConstraints(pattern=r"^(sha256|b3):[0-9a-f]{64}$"),
]


def _validate_identifier(value: str) -> str:
    if not 1 <= len(value) <= 512:
        raise ValueError("identifier length must be between 1 and 512 characters")
    if value != unicodedata.normalize("NFC", value):
        raise ValueError("identifier must be Unicode NFC")
    if value != value.strip():
        raise ValueError("identifier must not contain leading or trailing whitespace")
    if any(unicodedata.category(character) == "Cc" for character in value):
        raise ValueError("identifier must not contain control characters")
    return value


Identifier = Annotated[str, AfterValidator(_validate_identifier)]


class _StrictFrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)


def _validate_uri(value: str, *, field_name: str) -> str:
    parsed = urlsplit(value)
    if not parsed.scheme:
        raise ValueError(f"{field_name} must be an absolute URI")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError(f"{field_name} must not contain credentials")
    if parsed.query or parsed.fragment:
        raise ValueError(f"{field_name} must not contain a query or fragment")
    if parsed.scheme == "file" and not parsed.path.startswith("/"):
        raise ValueError(f"{field_name} file URI must contain an absolute path")
    return value


def _normalize_storage_uri(value: str, *, field_name: str) -> str:
    """Normalize caller-local paths while keeping persisted references URI-only."""

    if not value:
        raise ValueError(f"{field_name} must not be empty")
    parsed = urlsplit(value)
    if not parsed.scheme:
        return Path(value).expanduser().resolve(strict=False).as_uri()
    _validate_uri(value, field_name=field_name)
    if parsed.scheme.lower() != "file":
        return value
    if parsed.netloc not in {"", "localhost"}:
        raise ValueError(f"{field_name} file URI must not name a remote host")
    return Path(unquote(parsed.path)).expanduser().resolve(strict=False).as_uri()


class StorageRef(_StrictFrozenModel):
    schema_version: Literal[1] = 1
    storage_id: InternalDigest
    backend: StorageBackend
    data_uri: str
    namespace: Identifier
    catalog_uri: str | None = None

    @model_validator(mode="after")
    def validate_identity(self) -> StorageRef:
        normalized_data_uri = _normalize_storage_uri(self.data_uri, field_name="data_uri")
        if normalized_data_uri != self.data_uri:
            raise ValueError("data_uri must use its canonical absolute URI form")
        if self.catalog_uri is not None:
            normalized_catalog_uri = _normalize_storage_uri(
                self.catalog_uri, field_name="catalog_uri"
            )
            if normalized_catalog_uri != self.catalog_uri:
                raise ValueError("catalog_uri must use its canonical absolute URI form")
        if self.backend is StorageBackend.ICEBERG:
            if self.catalog_uri is None:
                raise ValueError("Iceberg storage references require a durable catalog_uri")
            data_scheme = urlsplit(self.data_uri).scheme.lower()
            catalog_scheme = urlsplit(self.catalog_uri).scheme.lower()
            if data_scheme != "file" and catalog_scheme == "file":
                raise ValueError("remote Iceberg storage cannot use a process-local file catalog")
        expected = internal_digest(
            "archetype-storage-ref-v1",
            {
                "schema_version": self.schema_version,
                "backend": self.backend.value,
                "data_uri": self.data_uri,
                "namespace": self.namespace,
                "catalog_uri": self.catalog_uri,
            },
        )
        if self.storage_id != expected:
            raise ValueError(f"storage_id mismatch: expected {expected}, got {self.storage_id}")
        return self

    @classmethod
    def create(
        cls,
        *,
        backend: StorageBackend,
        data_uri: str,
        namespace: str,
        catalog_uri: str | None = None,
    ) -> StorageRef:
        normalized_data_uri = _normalize_storage_uri(data_uri, field_name="data_uri")
        normalized_catalog_uri = (
            _normalize_storage_uri(catalog_uri, field_name="catalog_uri")
            if catalog_uri is not None
            else None
        )
        payload = {
            "schema_version": 1,
            "backend": backend.value,
            "data_uri": normalized_data_uri,
            "namespace": namespace,
            "catalog_uri": normalized_catalog_uri,
        }
        return cls(
            backend=backend,
            data_uri=normalized_data_uri,
            namespace=namespace,
            catalog_uri=normalized_catalog_uri,
            storage_id=internal_digest("archetype-storage-ref-v1", payload),
        )


class ComponentRef(_StrictFrozenModel):
    component_id: Identifier
    schema_digest: InternalDigest


class SignatureRef(_StrictFrozenModel):
    table_id: Identifier
    components: tuple[ComponentRef, ...]
    signature_digest: InternalDigest
    schema_digest: InternalDigest

    @model_validator(mode="after")
    def validate_components(self) -> SignatureRef:
        if not self.components:
            raise ValueError("signature components must be nonempty")
        component_ids = [component.component_id for component in self.components]
        if component_ids != sorted(component_ids):
            raise ValueError("signature components must be sorted by component_id")
        if len(component_ids) != len(set(component_ids)):
            raise ValueError("signature components must have unique component_id values")
        return self


def _validate_signature_catalog(
    signatures: tuple[SignatureRef, ...],
) -> tuple[set[str], set[str]]:
    signature_keys = [(signature.signature_digest, signature.table_id) for signature in signatures]
    signature_digests = [key[0] for key in signature_keys]
    table_ids = [key[1] for key in signature_keys]
    if (
        signature_keys != sorted(signature_keys)
        or len(signature_digests) != len(set(signature_digests))
        or len(table_ids) != len(set(table_ids))
    ):
        raise ValueError("signatures must be sorted and unique")

    component_schemas: dict[str, str] = {}
    for signature in signatures:
        for component in signature.components:
            known_digest = component_schemas.setdefault(
                component.component_id, component.schema_digest
            )
            if known_digest != component.schema_digest:
                raise ValueError(
                    f"component_id {component.component_id!r} has conflicting schema digests"
                )
    return set(signature_digests), set(table_ids)


class LedgerIdentity(_StrictFrozenModel):
    storage: StorageRef
    world_id: Identifier
    run_id: Identifier


class LineageSegment(_StrictFrozenModel):
    world_id: Identifier
    run_id: Identifier
    up_to_tick: int = Field(ge=0)


class EntitySignatureRef(_StrictFrozenModel):
    entity_id: int = Field(ge=0)
    signature_digest: InternalDigest


class BatchRef(_StrictFrozenModel):
    commit_id: InternalDigest
    table_id: Identifier
    tick: int = Field(ge=0)
    writer_epoch: int = Field(ge=0)
    row_count: int = Field(ge=0)
    content_digest: InternalDigest


class _LedgerManifestSemantic(_StrictFrozenModel):
    schema_version: Literal[1] = 1
    identity: LedgerIdentity
    name: str | None
    generation: int = Field(ge=0)
    previous_manifest_digest: InternalDigest | None
    commit_id: InternalDigest
    committed_through_tick: int | None = Field(default=None, ge=0)
    next_tick: int = Field(ge=0)
    next_entity_id: int = Field(ge=0)
    signatures: tuple[SignatureRef, ...]
    entity_directory: tuple[EntitySignatureRef, ...]
    lineage: tuple[LineageSegment, ...]
    batches: tuple[BatchRef, ...]
    writer_epoch: int = Field(ge=0)
    execution_contract_digest: ContentDigest | None


class LedgerManifest(_LedgerManifestSemantic):
    manifest_digest: InternalDigest
    committed_at_ms: int = Field(ge=0)

    @staticmethod
    def digest_payload(values: dict[str, Any]) -> dict[str, Any]:
        return {
            key: value
            for key, value in values.items()
            if key not in {"manifest_digest", "committed_at_ms"}
        }

    @model_validator(mode="after")
    def validate_manifest(self) -> LedgerManifest:
        if self.generation == 0:
            if self.previous_manifest_digest is not None:
                raise ValueError("generation zero must not have a previous manifest")
            if self.committed_through_tick is not None or self.next_tick != 0:
                raise ValueError("generation zero must represent an empty ledger")
            if self.signatures or self.entity_directory or self.lineage or self.batches:
                raise ValueError("generation zero must not contain committed ledger metadata")
            if self.writer_epoch != 0:
                raise ValueError("generation zero must have writer_epoch=0")
        if self.generation > 0 and self.previous_manifest_digest is None:
            raise ValueError("later generations require a previous manifest digest")
        if self.committed_through_tick is None:
            if self.next_tick != 0:
                raise ValueError("an empty ledger must have next_tick=0")
            if self.batches:
                raise ValueError("an empty ledger must not reference component batches")
        elif self.next_tick != self.committed_through_tick + 1:
            raise ValueError("next_tick must equal committed_through_tick + 1")

        signature_digests, table_ids = _validate_signature_catalog(self.signatures)

        entity_ids = [entry.entity_id for entry in self.entity_directory]
        if entity_ids != sorted(entity_ids) or len(entity_ids) != len(set(entity_ids)):
            raise ValueError("entity_directory must be sorted by unique entity_id")
        if entity_ids and entity_ids[-1] >= self.next_entity_id:
            raise ValueError("next_entity_id must exceed every active entity_id")
        if any(entry.signature_digest not in signature_digests for entry in self.entity_directory):
            raise ValueError("entity_directory references an undeclared signature")

        lineage_keys = [
            (segment.up_to_tick, segment.world_id, segment.run_id) for segment in self.lineage
        ]
        if lineage_keys != sorted(lineage_keys):
            raise ValueError("lineage must have strictly increasing tick bounds")
        bounds = [segment.up_to_tick for segment in self.lineage]
        if len(bounds) != len(set(bounds)):
            raise ValueError("lineage must have strictly increasing tick bounds")
        identities = [(segment.world_id, segment.run_id) for segment in self.lineage]
        if len(identities) != len(set(identities)):
            raise ValueError("lineage must not contain duplicate identities")
        if (self.identity.world_id, self.identity.run_id) in identities:
            raise ValueError("lineage must not contain the ledger itself")

        batch_keys = [(batch.tick, batch.table_id, batch.commit_id) for batch in self.batches]
        if batch_keys != sorted(batch_keys) or len(batch_keys) != len(set(batch_keys)):
            raise ValueError("batches must be sorted and unique")
        for batch in self.batches:
            if batch.commit_id != self.commit_id:
                raise ValueError("every batch must carry the manifest commit_id")
            if batch.writer_epoch != self.writer_epoch:
                raise ValueError("every batch must carry the manifest writer_epoch")
            if batch.table_id not in table_ids:
                raise ValueError("batch references an undeclared signature table")
            if self.committed_through_tick is None or batch.tick != self.committed_through_tick:
                raise ValueError("every batch must carry the manifest committed tick")

        payload = self.digest_payload(self.model_dump(mode="json", exclude_none=False))
        expected = internal_digest("archetype-ledger-manifest-v1", payload)
        if self.manifest_digest != expected:
            raise ValueError(
                f"manifest_digest mismatch: expected {expected}, got {self.manifest_digest}"
            )
        return self

    @classmethod
    def create(cls, **values: Any) -> LedgerManifest:
        if "manifest_digest" in values:
            raise ValueError("LedgerManifest.create computes manifest_digest")
        if "committed_at_ms" not in values:
            raise ValueError("LedgerManifest.create requires committed_at_ms")
        committed_at_ms = values.pop("committed_at_ms")
        semantic = _LedgerManifestSemantic(**values)
        payload = semantic.model_dump(mode="json", exclude_none=False)
        return cls(
            **semantic.model_dump(mode="python", exclude_none=False),
            manifest_digest=internal_digest("archetype-ledger-manifest-v1", payload),
            committed_at_ms=committed_at_ms,
        )


class ManifestHead(_StrictFrozenModel):
    identity: LedgerIdentity
    generation: int = Field(ge=0)
    manifest_digest: InternalDigest
    writer_epoch: int = Field(ge=0)
    published_at_ms: int = Field(ge=0)


class LedgerRef(_StrictFrozenModel):
    schema_version: Literal[1] = 1
    identity: LedgerIdentity
    manifest_digest: InternalDigest
    manifest_generation: int = Field(ge=0)
    committed_through_tick: int | None = Field(default=None, ge=0)
    next_tick: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_ticks(self) -> LedgerRef:
        if self.committed_through_tick is None and self.next_tick != 0:
            raise ValueError("an empty ledger reference must have next_tick=0")
        if (
            self.committed_through_tick is not None
            and self.next_tick != self.committed_through_tick + 1
        ):
            raise ValueError("next_tick must equal committed_through_tick + 1")
        return self


class LedgerWriteToken(_StrictFrozenModel):
    identity: LedgerIdentity
    writer_id: Identifier
    epoch: int = Field(ge=0)
    lease_expires_at_ms: int = Field(ge=0)
    expected_manifest_digest: InternalDigest


@dataclass(frozen=True, slots=True)
class ResumedWorld:
    world: iWorld
    writer_token: LedgerWriteToken


class LedgerInfo(_StrictFrozenModel):
    ref: LedgerRef
    name: str | None
    next_entity_id: int = Field(ge=0)
    signatures: tuple[SignatureRef, ...]
    lineage: tuple[LineageSegment, ...]

    @model_validator(mode="after")
    def validate_canonical_sequences(self) -> LedgerInfo:
        _validate_signature_catalog(self.signatures)
        bounds = [segment.up_to_tick for segment in self.lineage]
        if bounds != sorted(bounds) or len(bounds) != len(set(bounds)):
            raise ValueError("lineage must have strictly increasing tick bounds")
        return self
