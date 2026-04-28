# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Registry mapping pre-merge audit checklist ids to pytest test nodes.

Single source of truth for cross-referencing red tests back to the original
checklist in `docs/audits/categorization.md`. The categorization doc points
at this manifest; together they answer "which checklist items are covered?".

Each entry: id → (test_node_id, severity, source_doc).
A future meta-test will assert that every entry's `node_id` resolves and that
no checklist id is missing from the manifest.
"""

from __future__ import annotations

from typing import Final, Literal, NamedTuple

Severity = Literal["critical", "high", "medium", "low"]


class AuditEntry(NamedTuple):
    node_id: str
    severity: Severity
    source: str


AUDIT_ITEMS: Final[dict[str, AuditEntry]] = {
    # ── Section A.1 — Foundational types and events ────────────────────
    "A.1.1": AuditEntry(
        "tests/audit/test_model_shape.py::test_a_1_1_on_destroy_event",
        "high",
        "docs/audits/categorization.md#section-a",
    ),
    "A.1.2": AuditEntry(
        "tests/audit/test_model_shape.py::test_a_1_2_world_info",
        "high",
        "docs/audits/categorization.md#section-a",
    ),
    "A.1.3": AuditEntry(
        "tests/audit/test_model_shape.py::test_a_1_3_processor_info",
        "high",
        "docs/audits/categorization.md#section-a",
    ),
    "A.1.4": AuditEntry(
        "tests/audit/test_model_shape.py::test_a_1_4_hook_info",
        "high",
        "docs/audits/categorization.md#section-a",
    ),
    "A.1.5": AuditEntry(
        "tests/audit/test_model_shape.py::test_a_1_5_resource_info_qualname_only",
        "high",
        "docs/audits/categorization.md#section-a",
    ),
    "A.1.6": AuditEntry(
        "tests/audit/test_model_shape.py::test_a_1_6_episode_config",
        "high",
        "docs/audits/categorization.md#section-a",
    ),
    "A.1.7": AuditEntry(
        "tests/audit/test_model_shape.py::test_a_1_7_rollout_config",
        "high",
        "docs/audits/categorization.md#section-a",
    ),
    "A.1.8": AuditEntry(
        "tests/audit/test_model_shape.py::test_a_1_8_episode_result",
        "high",
        "docs/audits/categorization.md#section-a",
    ),
    "A.1.9": AuditEntry(
        "tests/audit/test_model_shape.py::test_a_1_9_rollout_result",
        "high",
        "docs/audits/categorization.md#section-a",
    ),
    "A.1.10": AuditEntry(
        "tests/audit/test_model_shape.py::test_a_1_10_command_type_entries",
        "critical",
        "docs/audits/specification.md",
    ),
    # ── Section A.5 — Append-only invariant (load-bearing) ─────────────
    "A.5.1": AuditEntry(
        "tests/audit/test_protocol_shape.py::test_a_5_1_async_store_no_destructive_methods",
        "critical",
        "docs/audits/categorization.md#section-a",
    ),
    "A.5.2": AuditEntry(
        "tests/audit/test_protocol_shape.py::test_a_5_2_audit_log_no_destructive_methods",
        "critical",
        "docs/audits/categorization.md#section-a",
    ),
    # ── Section B.1 — Single-gate enforcement (load-bearing) ───────────
    "B.1.1": AuditEntry(
        "tests/audit/test_import_boundaries.py::test_b_1_1_runtime_no_forbidden_app_imports",
        "critical",
        "docs/guide/runtime.md",
    ),
    "B.1.6": AuditEntry(
        "tests/audit/test_import_boundaries.py::test_b_1_6_runtime_init_exports",
        "high",
        "docs/guide/runtime.md",
    ),
    # ── Section C.1 — COMMANDS_BY_ROLE ────────────────────────────────
    "C.1.1": AuditEntry(
        "tests/audit/test_commands_by_role.py::test_c_1_1_module_exports_mapping",
        "critical",
        "docs/guide/command-gate.md",
    ),
    "C.1.2": AuditEntry(
        "tests/audit/test_commands_by_role.py::test_c_1_2_dict_str_to_frozenset_command_type",
        "critical",
        "docs/guide/command-gate.md",
    ),
    "C.1.3": AuditEntry(
        "tests/audit/test_commands_by_role.py::test_c_1_3_four_role_keys_only",
        "critical",
        "docs/guide/command-gate.md",
    ),
    "C.1.4": AuditEntry(
        "tests/audit/test_commands_by_role.py::test_c_1_4_admin_is_full_command_type_set",
        "critical",
        "docs/guide/command-gate.md",
    ),
    "C.1.5": AuditEntry(
        "tests/audit/test_commands_by_role.py::test_c_1_5_operator_superset_of_player",
        "high",
        "docs/guide/command-gate.md",
    ),
    "C.1.6": AuditEntry(
        "tests/audit/test_commands_by_role.py::test_c_1_6_player_superset_of_viewer",
        "high",
        "docs/guide/command-gate.md",
    ),
    "C.1.7": AuditEntry(
        "tests/audit/test_commands_by_role.py::test_c_1_7_admin_contains_all_command_types",
        "critical",
        "docs/guide/command-gate.md",
    ),
    "C.1.8": AuditEntry(
        "tests/audit/test_commands_by_role.py::test_c_1_8_reads_in_viewer",
        "high",
        "docs/guide/command-gate.md",
    ),
    "C.1.9": AuditEntry(
        "tests/audit/test_commands_by_role.py::test_c_1_9_create_world_admin_only",
        "critical",
        "docs/guide/command-gate.md",
    ),
    "C.1.10": AuditEntry(
        "tests/audit/test_commands_by_role.py::test_c_1_10_fork_destroy_operator_and_admin_only",
        "critical",
        "docs/guide/command-gate.md",
    ),
    # ── Section C.3 — Migration cleanup ───────────────────────────────
    # C.3.1 and C.3.2 are already enforced by an existing test that uses
    # the canonical regex ["'](?:maintainer|coder)["'] and excludes its
    # own file by filename pattern. We point the manifest at it rather
    # than duplicate the scan.
    "C.3.1": AuditEntry(
        "tests/app/test_old_role_cleanup.py::TestOldRoleCleanup::test_no_maintainer_or_coder_role_strings",
        "medium",
        "docs/guide/command-gate.md",
    ),
    "C.3.2": AuditEntry(
        "tests/app/test_old_role_cleanup.py::TestOldRoleCleanup::test_no_maintainer_or_coder_role_strings",
        "medium",
        "docs/guide/command-gate.md",
    ),
    # ── Section H.2 — Forbidden imports across surfaces ───────────────
    "H.2.1": AuditEntry(
        "tests/audit/test_import_boundaries.py::test_b_1_1_runtime_no_forbidden_app_imports",
        "critical",
        "docs/guide/runtime.md",
    ),
    "H.2.7": AuditEntry(
        "tests/audit/test_import_boundaries.py::test_e_4_cli_no_forbidden_app_imports",
        "critical",
        "docs/guide/command-gate.md",
    ),
    # ── Section H.3 — No stale references ─────────────────────────────
    "H.3.1": AuditEntry(
        "tests/audit/test_no_stale_strings.py::test_h_3_1_no_remove_world_in_active_code",
        "medium",
        "docs/audits/categorization.md#section-h",
    ),
    "H.3.2": AuditEntry(
        "tests/audit/test_no_stale_strings.py::test_h_3_2_no_mutation_service_outside_app",
        "high",
        "docs/audits/categorization.md#section-h",
    ),
    "H.3.3": AuditEntry(
        "tests/audit/test_no_stale_strings.py::test_h_3_3_no_underscore_mutations_in_runtime",
        "medium",
        "docs/audits/categorization.md#section-h",
    ),
    # ── Section E.4 — CLI forbidden imports ────────────────────────────
    "E.4.1": AuditEntry(
        "tests/audit/test_import_boundaries.py::test_e_4_cli_no_forbidden_app_imports",
        "critical",
        "docs/guide/command-gate.md",
    ),
}
