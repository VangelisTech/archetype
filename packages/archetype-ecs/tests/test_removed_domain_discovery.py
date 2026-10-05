# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""An upgrade must not execute stale first-party domain entry points."""

from types import SimpleNamespace

import pytest

from archetype.world_libraries.discovery import discover_world_libraries, resolve_world_libraries
from archetype.world_libraries.models import WorldLibraryManifest


@pytest.mark.parametrize("name", ["missions", "physical-ai"])
@pytest.mark.parametrize("separator", ["-", "__", ".-_"])
def test_removed_installed_distribution_is_rejected_before_import(monkeypatch, name, separator):
    def load():
        pytest.fail("removed extension code was imported")

    entry = SimpleNamespace(
        name=name,
        value=f"archetype.{name.replace('-', '_')}._extension:get_manifest",
        dist=SimpleNamespace(name=f"Archetype-{name}".replace("-", separator), version="0.6.3"),
        load=load,
    )
    monkeypatch.setattr(
        "archetype.world_libraries.discovery.metadata.entry_points", lambda **kwargs: (entry,)
    )
    with pytest.raises(ValueError, match="removed.*uninstall"):
        discover_world_libraries()


@pytest.mark.parametrize("name", ["missions", "physical-ai"])
@pytest.mark.parametrize("separator", ["-", "__", ".-_"])
def test_explicit_removed_manifest_cannot_restore_registration(name, separator):
    manifest = WorldLibraryManifest(
        name=name,
        distribution=f"Archetype-{name}".replace("-", separator),
        version="0.6.3",
        requires_framework=">=0.6,<0.7",
        operation_models=(),
        install=lambda context: pytest.fail("removed installer executed"),
    )
    with pytest.raises(ValueError, match="removed.*uninstall"):
        resolve_world_libraries((manifest,))
