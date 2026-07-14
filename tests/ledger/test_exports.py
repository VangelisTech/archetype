# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

import archetype
from archetype.app import LedgerService
from archetype.ledger import (
    ComponentRef,
    ComponentRegistry,
    LedgerIdentity,
    LedgerInfo,
    LedgerManifest,
    LedgerRef,
    ManifestHead,
    SignatureRef,
    StorageRef,
)


def test_public_ledger_exports_are_lazy_and_exact() -> None:
    expected = {
        "LedgerService": LedgerService,
        "StorageRef": StorageRef,
        "ComponentRef": ComponentRef,
        "SignatureRef": SignatureRef,
        "LedgerIdentity": LedgerIdentity,
        "LedgerManifest": LedgerManifest,
        "LedgerRef": LedgerRef,
        "LedgerInfo": LedgerInfo,
        "ManifestHead": ManifestHead,
        "ComponentRegistry": ComponentRegistry,
    }
    assert all(name in archetype.__all__ for name in expected)
    assert {name: getattr(archetype, name) for name in expected} == expected
