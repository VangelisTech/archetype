# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Typed public failures for durable ledger contracts."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from archetype.ledger.models import LedgerRef
    from archetype.ledger.records import DurableRecord


class LedgerNotFoundError(LookupError):
    """The requested durable ledger identity does not exist."""


class LedgerMetadataUnavailableError(RuntimeError):
    """A legacy store lacks metadata required to prove the requested operation."""


class ManifestConflictError(RuntimeError):
    """A manifest generation already has different committed content."""


class ManifestCorruptionError(RuntimeError):
    """Persisted manifest state violates its digest or structural contract."""


class LedgerRecoveryRequiredError(RuntimeError):
    """An uncommitted tail cannot be reconciled without explicit intervention."""


class StaleLedgerRefError(RuntimeError):
    """A mutation targeted an older committed head."""

    def __init__(self, latest_ref: LedgerRef) -> None:
        self.latest_ref = latest_ref
        super().__init__(
            f"ledger reference is stale; latest manifest is {latest_ref.manifest_digest}"
        )


class StorageRefMismatchError(ValueError):
    """Caller-supplied storage does not match a credential-free storage reference."""


class ComponentResolutionError(ValueError):
    """A persisted component identity is absent from the trusted registry."""


class ComponentSchemaConflictError(ValueError):
    """A stable component identity was reused with a different persisted schema."""

    def __init__(self, component_id: str, expected_digest: str, actual_digest: str) -> None:
        self.component_id = component_id
        self.expected_digest = expected_digest
        self.actual_digest = actual_digest
        super().__init__(
            f"component {component_id!r} schema conflict: "
            f"expected {expected_digest}, got {actual_digest}"
        )


class WriterLeaseConflictError(RuntimeError):
    """Another live writer currently owns the ledger."""


class StaleWriterError(RuntimeError):
    """A writer attempted to publish with an obsolete fencing epoch."""


class UnsupportedAtomicInsertError(RuntimeError):
    """The selected backend cannot provide the required atomic record contract."""


class DurableRecordConflictError(RuntimeError):
    """The same durable business key was presented with different content."""

    def __init__(
        self,
        *,
        kind: str,
        scope: str,
        key: str,
        revision: int,
        expected_digest: str,
        actual_digest: str,
        latest_record: DurableRecord | None = None,
    ) -> None:
        self.kind = kind
        self.scope = scope
        self.key = key
        self.revision = revision
        self.expected_digest = expected_digest
        self.actual_digest = actual_digest
        self.latest_record = latest_record
        super().__init__(
            f"durable record conflict for ({kind!r}, {scope!r}, {key!r}, {revision}): "
            f"expected {expected_digest}, found {actual_digest}"
        )


class DurableRecordCASMismatchError(RuntimeError):
    """The current record head did not match compare-and-swap expectations."""

    def __init__(
        self,
        *,
        kind: str,
        scope: str,
        key: str,
        expected_revision: int | None,
        expected_digest: str | None,
        latest_record: DurableRecord | None,
    ) -> None:
        self.kind = kind
        self.scope = scope
        self.key = key
        self.expected_revision = expected_revision
        self.expected_digest = expected_digest
        self.latest_record = latest_record
        actual_revision = latest_record.revision if latest_record is not None else None
        actual_digest = latest_record.content_digest if latest_record is not None else None
        super().__init__(
            f"durable record CAS mismatch for ({kind!r}, {scope!r}, {key!r}): "
            f"expected revision/digest {expected_revision!r}/{expected_digest!r}, "
            f"found {actual_revision!r}/{actual_digest!r}"
        )
