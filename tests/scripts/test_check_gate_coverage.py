# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

CHECKER_PATH = Path(__file__).resolve().parents[2] / "scripts" / "check_gate_coverage.py"
SPEC = importlib.util.spec_from_file_location("check_gate_coverage", CHECKER_PATH)
assert SPEC is not None and SPEC.loader is not None
checker = importlib.util.module_from_spec(SPEC)
sys.modules["check_gate_coverage"] = checker
SPEC.loader.exec_module(checker)


def test_error_taxonomy_governs_registered_family_exceptions() -> None:
    classes = checker._owned_exception_classes()

    assert {
        "archetype.commands.audit.AuditBackpressureError",
        "archetype.redaction.models.SecretQuarantineError",
        "archetype.storage.catalog.records.CatalogConflictError",
        "archetype.storage.catalog.records.CatalogSchemaMismatchError",
        "archetype.storage.catalog.records.CommandConflictError",
        "archetype.world.errors.WorldClosingError",
        "archetype.world.simulation.PostCommitProjectionError",
    } <= classes.keys()
    assert set(checker.INTERNAL_ONLY_EXCEPTIONS) == {
        "archetype.missions.coding_agents.app_server.CodexAppServerError",
        "archetype.missions.coding_agents.app_server.CodexTurnCompletionBarrierError",
        "archetype.missions.critics.harness._UnverifiableReview",
        "archetype.missions.sandboxes._subprocess._CleanupTimeout",
        "archetype.missions.sandboxes._subprocess._JoinTimeout",
    }
    assert checker.check_error_taxonomy() == []


def test_full_composition_discovers_every_first_party_world_library() -> None:
    checker._configure_source_paths()

    registry, installed = checker._composed_registry()

    assert installed == checker.EXPECTED_WORLD_LIBRARIES
    assert len(registry.specs) == 49
