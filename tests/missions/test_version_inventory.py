# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Pinned coding-agent environment inventory contracts."""

from __future__ import annotations

import hashlib
import json
from importlib import resources
from typing import Any

import pytest

from archetype.missions.coding_agents.harness import CodexDriver
from archetype.missions.sandboxes.versions import (
    VersionPinError,
    load_version_inventory,
    parse_version_inventory,
)
from quality.secret_corpus import SECRET_LEAK_CORPUS

pytestmark = pytest.mark.contract("missions.environment.pinned")

_INVENTORY_BYTES = (
    resources.files("archetype.missions.sandboxes").joinpath("versions.toml").read_bytes()
)


def _mutated(old: str, new: str) -> bytes:
    text = _INVENTORY_BYTES.decode("utf-8")
    assert old in text
    return text.replace(old, new, 1).encode("utf-8")


def test_inventory_contains_only_the_v1_execution_dependencies() -> None:
    inventory = load_version_inventory()
    assert inventory.schema_version == 1
    assert inventory.digest == f"sha256:{hashlib.sha256(_INVENTORY_BYTES).hexdigest()}"
    assert {artifact.artifact_id for artifact in inventory.artifacts} == {
        "codex-cli",
        "modal-sdk",
    }
    codex = inventory.harness_pin("codex")
    assert codex.version == "0.144.6"
    assert codex.immutable_ref.startswith("sha512-")
    assert codex.harness_interface is not None
    assert codex.harness_interface.invoke == ("codex", "exec")
    assert (
        CodexDriver._session_id(json.dumps({"type": "thread.started", "thread_id": "session-1"}))
        == "session-1"
    )


def test_unknown_artifact_and_harness_fail_closed() -> None:
    inventory = load_version_inventory()
    with pytest.raises(VersionPinError, match="not in the version inventory"):
        inventory.resolve("does-not-exist")
    with pytest.raises(VersionPinError, match="exactly one pinned CLI"):
        inventory.harness_pin("unknown")


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ('version = "0.144.6"', 'version = "latest"'),
        ('version = "0.144.6"', 'version = ">=0.144.6"'),
        (
            'source = "https://registry.npmjs.org/@openai/codex/-/codex-0.144.6.tgz"',
            'source = "http://registry.npmjs.org/@openai/codex/codex.tgz"',
        ),
        ('immutable_ref = "sha256:7508a44f', 'immutable_ref = "1.5.2-'),
        ('id = "modal-sdk"', 'id = "codex-cli"'),
        ("schema_version = 1", "schema_version = 2"),
    ],
)
def test_inventory_rejects_floating_or_inconsistent_pins(old: str, new: str) -> None:
    with pytest.raises(VersionPinError):
        parse_version_inventory(_mutated(old, new))


@pytest.mark.parametrize("case", SECRET_LEAK_CORPUS, ids=lambda case: case.name)
def test_inventory_rejects_credential_shaped_values(case: Any) -> None:
    target = 'source = "https://registry.npmjs.org/@openai/codex/-/codex-0.144.6.tgz"'
    with pytest.raises(VersionPinError):
        parse_version_inventory(_mutated(target, f"source = {json.dumps(case.payload)}"))
