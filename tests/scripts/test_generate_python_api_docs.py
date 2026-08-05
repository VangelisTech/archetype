# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the generated Python reference."""

from __future__ import annotations

from scripts.generate_python_api_docs import PAGES_DIR, main


def test_committed_python_reference_is_current() -> None:
    assert main(["--check"]) == 0


def test_artifact_context_reference_names_selected_evidence() -> None:
    reference = (PAGES_DIR / "artifacts.md").read_text(encoding="utf-8")

    assert (
        "| `artifact_ids` | `tuple[str, ...]` | `required` | "
        "UUIDv7 occurrence identities selected as evidence |"
    ) in reference
