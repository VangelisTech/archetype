# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from pathlib import Path

import pytest

from archetype.app.sandboxes.versions import load_version_inventory
from scripts.generate_version_inventory import main, render

pytestmark = pytest.mark.contract("sandboxes.environment.pinned")


def test_render_lists_every_artifact_and_the_content_digest() -> None:
    inventory = load_version_inventory()
    page = render()
    assert inventory.digest in page
    for artifact in inventory.artifacts:
        assert f"`{artifact.artifact_id}`" in page
        if artifact.status == "pinned":
            assert artifact.version in page
            assert artifact.immutable_ref in page


def test_check_mode_detects_stale_operator_page(tmp_path: Path) -> None:
    output = tmp_path / "version-inventory.md"
    assert main(["--output", str(output)]) == 0
    assert main(["--output", str(output), "--check"]) == 0
    output.write_text("stale page\n", encoding="utf-8")
    assert main(["--output", str(output), "--check"]) == 1


def test_committed_operator_page_is_current() -> None:
    assert main(["--check"]) == 0
