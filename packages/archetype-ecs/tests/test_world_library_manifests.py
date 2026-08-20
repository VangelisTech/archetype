# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for deterministic, domain-neutral world-library discovery."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from types import SimpleNamespace
from typing import Literal

import pytest
from pydantic import BaseModel

from archetype.world_libraries import (
    InstalledWorldLibrary,
    WorldLibraryManifest,
    discover_world_libraries,
    resolve_world_libraries,
)


def test_private_root_probe_never_discovers_extensions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Private submodule imports cannot recurse through entry-point loading."""

    import archetype

    def fail_discovery(*args: object, **kwargs: object) -> object:
        raise AssertionError("private root probes must not discover world libraries")

    monkeypatch.setattr(
        "archetype.world_libraries.discover_world_libraries",
        fail_discovery,
    )
    with pytest.raises(AttributeError):
        archetype.__getattr__("_private_import_probe")


def test_framework_facades_do_not_expose_world_library_aliases() -> None:
    import archetype
    from archetype.runtime import (
        ArchetypeRuntime,
        RuntimeWorld,
        SyncArchetypeRuntime,
        SyncRuntimeWorld,
    )

    for name in (
        "AutoResearchConfig",
        "AutoResearchResult",
        "CandidateContext",
        "EvaluationResult",
        "HostedEpisodeObservation",
        "HostedEpisodeRequest",
        "ModalHostedEpisodeConfig",
        "ResearchCandidateContext",
    ):
        assert name not in dir(archetype)
        with pytest.raises(AttributeError):
            getattr(archetype, name)

    assert not hasattr(ArchetypeRuntime, "missions")
    assert "__getattr__" not in ArchetypeRuntime.__dict__
    assert "__getattr__" not in RuntimeWorld.__dict__
    assert "__getattr__" not in SyncRuntimeWorld.__dict__
    assert "library" not in SyncRuntimeWorld.__dict__
    assert "library" not in SyncArchetypeRuntime.__dict__
    for name in (
        "autoresearch",
        "run_hosted_episode",
        "ingest_claude_transcript",
        "transcript_rows",
        "query_trajectory",
        "grade_trajectory",
    ):
        assert not hasattr(RuntimeWorld, name)
        assert not hasattr(SyncRuntimeWorld, name)


class Alpha(BaseModel):
    operation: Literal["alpha"] = "alpha"


class Beta(BaseModel):
    operation: Literal["beta"] = "beta"


def _manifest(
    name: str,
    model: type[BaseModel],
    *,
    distribution: str | None = None,
    framework: str = ">=0.6,<0.7",
) -> WorldLibraryManifest:
    return WorldLibraryManifest(
        name=name,
        distribution=distribution or f"archetype-{name}",
        version="0.6.0",
        requires_framework=framework,
        operation_models=(model,),
        install=lambda _context: InstalledWorldLibrary(name=name),
    )


def test_explicit_manifests_are_immutable_and_sorted() -> None:
    beta = _manifest("beta", Beta)
    alpha = _manifest("alpha", Alpha)

    assert resolve_world_libraries((beta, alpha), framework_version="0.6.0") == (
        alpha,
        beta,
    )
    with pytest.raises(FrozenInstanceError):
        alpha.name = "changed"  # type: ignore[misc]


def test_manifest_has_no_facade_alias_authority() -> None:
    manifest = _manifest("alpha", Alpha)

    assert not hasattr(manifest, "root_exports")
    assert not hasattr(manifest, "runtime_method_aliases")
    assert not hasattr(manifest, "world_method_aliases")
    assert not hasattr(manifest, "sync_world_method_aliases")


@pytest.mark.parametrize(
    ("left", "right", "message"),
    [
        (_manifest("same", Alpha), _manifest("same", Beta), "duplicate world-library name"),
        (
            _manifest("first", Alpha),
            _manifest("second", Alpha),
            "operation name 'alpha'",
        ),
    ],
)
def test_duplicate_extension_authority_fails_before_install(
    left: WorldLibraryManifest,
    right: WorldLibraryManifest,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        resolve_world_libraries((left, right), framework_version="0.6.0")


def test_incompatible_framework_range_fails_closed() -> None:
    incompatible = _manifest("future", Alpha, framework=">=0.7,<0.8")

    with pytest.raises(ValueError, match="requires archetype-ecs"):
        resolve_world_libraries((incompatible,), framework_version="0.6.0")


def test_zero_extension_set_is_valid() -> None:
    assert resolve_world_libraries((), framework_version="0.6.0") == ()


def test_entry_point_discovery_is_stable_and_checks_distribution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    alpha = _manifest("alpha", Alpha)
    beta = _manifest("beta", Beta)

    class EntryPoint:
        def __init__(self, manifest: WorldLibraryManifest) -> None:
            self.name = manifest.name
            self.value = f"{manifest.name}._extension:get_manifest"
            self.dist = SimpleNamespace(name=manifest.distribution, version=manifest.version)
            self._manifest = manifest

        def load(self):
            return lambda: self._manifest

    monkeypatch.setattr(
        "archetype.world_libraries.discovery.metadata.entry_points",
        lambda **_kwargs: (EntryPoint(beta), EntryPoint(alpha)),
    )

    assert discover_world_libraries(framework_version="0.6.0") == (alpha, beta)


def test_entry_point_distribution_mismatch_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = _manifest("alpha", Alpha)
    entry_point = SimpleNamespace(
        name="alpha",
        value="alpha._extension:get_manifest",
        dist=SimpleNamespace(name="wrong-distribution", version="0.6.0"),
        load=lambda: manifest,
    )
    monkeypatch.setattr(
        "archetype.world_libraries.discovery.metadata.entry_points",
        lambda **_kwargs: (entry_point,),
    )

    with pytest.raises(ValueError, match="loaded from"):
        discover_world_libraries(framework_version="0.6.0")


@pytest.mark.parametrize(
    ("entry_name", "distribution_version", "message"),
    [
        ("metadata-name", "0.6.0", "entry point 'metadata-name' loaded manifest 'alpha'"),
        ("alpha", "0.6.2", "declares version '0.6.0', loaded from '0.6.2'"),
    ],
)
def test_entry_point_identity_must_match_manifest_metadata(
    monkeypatch: pytest.MonkeyPatch,
    entry_name: str,
    distribution_version: str,
    message: str,
) -> None:
    manifest = _manifest("alpha", Alpha)
    entry_point = SimpleNamespace(
        name=entry_name,
        value="alpha._extension:get_manifest",
        dist=SimpleNamespace(
            name=manifest.distribution,
            version=distribution_version,
        ),
        load=lambda: manifest,
    )
    monkeypatch.setattr(
        "archetype.world_libraries.discovery.metadata.entry_points",
        lambda **_kwargs: (entry_point,),
    )

    with pytest.raises(ValueError, match=message):
        discover_world_libraries(framework_version="0.6.0")
