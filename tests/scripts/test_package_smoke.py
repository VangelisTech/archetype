# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for wheel and source-distribution package smoke evidence."""

from __future__ import annotations

import subprocess
import zipfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from scripts import package_smoke


def _research_wheel(
    path: Path,
    *,
    project: str = "archetype-research",
    version: str = "0.6.0",
) -> Path:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("archetype/research/__init__.py", "")
        archive.writestr(
            "archetype_research-0.6.0.dist-info/METADATA",
            f"Metadata-Version: 2.4\nName: {project}\nVersion: {version}\n",
        )
        archive.writestr(
            "archetype_research-0.6.0.dist-info/licenses/LICENSE",
            "Apache License",
        )
    return path


def _smol_wheel(path: Path, *, requirement: str | None = None) -> Path:
    metadata = "Metadata-Version: 2.4\nName: archetype-smol\nVersion: 0.6.0\n"
    if requirement is not None:
        metadata += f"Requires-Dist: {requirement}\n"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("archetype/smol/__init__.py", "")
        archive.writestr("archetype/smol/py.typed", "")
        archive.writestr("archetype_smol-0.6.0.dist-info/METADATA", metadata)
        archive.writestr(
            "archetype_smol-0.6.0.dist-info/licenses/LICENSE",
            "Apache License",
        )
    return path


def _framework_wheel(path: Path, *, include_smol: bool = False) -> Path:
    requirements = [
        f'Requires-Dist: archetype-{library}<0.7,>=0.6; extra == "all"'
        for library in ("missions", "physical-ai", "research")
    ]
    if include_smol:
        requirements.append('Requires-Dist: archetype-smol<0.7,>=0.6; extra == "all"')
    metadata = (
        "Metadata-Version: 2.4\nName: archetype-ecs\nVersion: 0.6.0\n"
        + "\n".join(requirements)
        + "\n"
    )
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("archetype/__init__.py", "")
        archive.writestr("archetype_ecs-0.6.0.dist-info/METADATA", metadata)
        archive.writestr(
            "archetype_ecs-0.6.0.dist-info/licenses/LICENSE",
            "Apache License",
        )
    return path


def _sdists(tmp_path: Path) -> dict[str, Path]:
    result: dict[str, Path] = {}
    for distribution in package_smoke._DISTRIBUTIONS:
        prefix = package_smoke._PACKAGE_PREFIXES[distribution]
        path = tmp_path / f"{prefix}-0.6.0.tar.gz"
        path.touch()
        result[distribution] = path
    return result


def test_wheel_validation_binds_project_and_version_metadata(tmp_path: Path) -> None:
    wheel = _research_wheel(tmp_path / "archetype_research-0.6.0-py3-none-any.whl")

    assert (
        package_smoke._validate_wheel_contents(
            "archetype-research",
            wheel,
            expected_version="0.6.0",
        )
        == "0.6.0"
    )

    wrong_project = _research_wheel(
        tmp_path / "wrong-project.whl",
        project="not-archetype-research",
    )
    with pytest.raises(RuntimeError, match="unexpected project name"):
        package_smoke._validate_wheel_contents("archetype-research", wrong_project)

    wrong_version = _research_wheel(tmp_path / "wrong-version.whl", version="0.6.2")
    with pytest.raises(RuntimeError, match="does not match the attested wheel version"):
        package_smoke._validate_wheel_contents(
            "archetype-research",
            wrong_version,
            expected_version="0.6.0",
        )


def test_smol_wheel_validation_enforces_an_independent_typed_namespace(
    tmp_path: Path,
) -> None:
    wheel = _smol_wheel(tmp_path / "archetype_smol-0.6.0-py3-none-any.whl")

    assert package_smoke._validate_wheel_contents("archetype-smol", wheel) == "0.6.0"

    dependent = _smol_wheel(
        tmp_path / "dependent.whl",
        requirement="archetype-ecs>=0.6",
    )
    with pytest.raises(RuntimeError, match="must remain independent"):
        package_smoke._validate_wheel_contents("archetype-smol", dependent)

    missing_marker = tmp_path / "missing-marker.whl"
    with zipfile.ZipFile(wheel) as source, zipfile.ZipFile(missing_marker, "w") as target:
        for name in source.namelist():
            if name != "archetype/smol/py.typed":
                target.writestr(name, source.read(name))
    with pytest.raises(RuntimeError, match="typed-package marker"):
        package_smoke._validate_wheel_contents("archetype-smol", missing_marker)


def test_framework_all_extra_excludes_independent_smol(tmp_path: Path) -> None:
    wheel = _framework_wheel(tmp_path / "archetype_ecs-0.6.0-py3-none-any.whl")

    assert package_smoke._validate_wheel_contents("archetype-ecs", wheel) == "0.6.0"

    coupled = _framework_wheel(tmp_path / "coupled.whl", include_smol=True)
    with pytest.raises(RuntimeError, match="must not install the independent archetype-smol"):
        package_smoke._validate_wheel_contents("archetype-ecs", coupled)


def test_sdist_rebuilds_are_isolated_pep517_no_config_no_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sdists = _sdists(tmp_path)
    calls: list[tuple[list[str], dict[str, Any]]] = []
    monkeypatch.setenv("PYTHONPATH", "/checkout")
    monkeypatch.setenv("UV_CONFIG_FILE", "/checkout/uv.toml")
    monkeypatch.setenv("UV_NO_BUILD_ISOLATION", "1")
    monkeypatch.setenv("PIP_INDEX_URL", "https://wrong.invalid/simple")

    def fake_run(command: list[str], **kwargs: Any) -> SimpleNamespace:
        calls.append((command, kwargs))
        source = Path(command[-1])
        output = Path(command[command.index("--out-dir") + 1])
        prefix = source.name.removesuffix("-0.6.0.tar.gz")
        (output / f"{prefix}-0.6.0-py3-none-any.whl").touch()
        return SimpleNamespace(returncode=0, stdout="built", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    root = tmp_path / "outside-checkout"
    root.mkdir()

    rebuilt = package_smoke._rebuild_sdists(sdists=sdists, uv="uv", root=root)

    assert set(rebuilt) == set(package_smoke._DISTRIBUTIONS)
    assert len(calls) == len(package_smoke._DISTRIBUTIONS)
    for distribution, (command, kwargs) in zip(package_smoke._DISTRIBUTIONS, calls, strict=True):
        assert command[:3] == ["uv", "build", "--wheel"]
        assert "--force-pep517" in command
        assert "--no-config" in command
        assert "--no-cache" in command
        assert "--no-build-isolation" not in command
        assert command[-1] == str(sdists[distribution].resolve())
        assert kwargs["cwd"] == root
        assert kwargs["check"] is False
        assert kwargs["capture_output"] is True
        assert kwargs["text"] is True
        assert "PYTHONPATH" not in kwargs["env"]
        assert "UV_CONFIG_FILE" not in kwargs["env"]
        assert "UV_NO_BUILD_ISOLATION" not in kwargs["env"]
        assert "PIP_INDEX_URL" not in kwargs["env"]


def test_sdist_rebuild_failure_preserves_build_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(*_args: Any, **_kwargs: Any) -> SimpleNamespace:
        return SimpleNamespace(returncode=1, stdout="backend output", stderr="backend failure")

    monkeypatch.setattr(subprocess, "run", fail)
    root = tmp_path / "outside-checkout"
    root.mkdir()

    with pytest.raises(RuntimeError) as failure:
        package_smoke._rebuild_sdists(sdists=_sdists(tmp_path), uv="uv", root=root)

    message = str(failure.value)
    assert "isolated PEP 517 rebuild failed for archetype-ecs" in message
    assert "backend output" in message
    assert "backend failure" in message


def test_smoke_runs_world_stack_and_smol_probes_against_rebuilt_sdist_wheels(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wheels = {
        distribution: tmp_path / f"original-{distribution}.whl"
        for distribution in package_smoke._DISTRIBUTIONS
    }
    sdists = {
        distribution: tmp_path / f"original-{distribution}.tar.gz"
        for distribution in package_smoke._DISTRIBUTIONS
    }
    rebuilt_root = tmp_path / "rebuilt"
    rebuilt = {
        distribution: rebuilt_root / f"rebuilt-{distribution}.whl"
        for distribution in package_smoke._DISTRIBUTIONS
    }
    probes: list[dict[str, Any]] = []
    validations: list[tuple[str, Path, str | None]] = []

    monkeypatch.setattr(package_smoke, "_artifacts", lambda _dist: (wheels, sdists))
    monkeypatch.setattr(package_smoke.shutil, "which", lambda _name: "uv")
    monkeypatch.setattr(
        package_smoke,
        "_rebuild_sdists",
        lambda **_kwargs: rebuilt,
    )

    def validate(
        distribution: str,
        wheel: Path,
        *,
        expected_version: str | None = None,
    ) -> str:
        validations.append((distribution, wheel, expected_version))
        return "0.6.0"

    def run_matrix(**kwargs: Any) -> dict[str, Any]:
        probes.append(kwargs)
        return {"matrix": kwargs["matrix"], "operations": 46}

    def run_smol(**kwargs: Any) -> dict[str, Any]:
        probes.append(kwargs)
        return {"matrix": kwargs["matrix"], "tick": 2}

    monkeypatch.setattr(package_smoke, "_validate_wheel_contents", validate)
    monkeypatch.setattr(package_smoke, "_run_matrix", run_matrix)
    monkeypatch.setattr(package_smoke, "_run_smol", run_smol)

    results = package_smoke.smoke(tmp_path)

    assert [result["matrix"] for result in results] == [
        "base",
        "missions",
        "physical-ai",
        "research",
        "all",
        "smol",
        "sdist-all",
        "sdist-smol",
    ]
    assert len(probes) == 8
    assert probes[-2]["matrix"] == "all"
    assert probes[-1]["matrix"] == "sdist-smol"
    assert all(probe["version"] == "0.6.0" for probe in probes)
    assert probes[-2]["wheels"] == rebuilt
    assert probes[-2]["dist_dir"] == rebuilt_root
    assert probes[-2]["root"].name == "sdist-probe"
    assert probes[-1]["wheel"] == rebuilt["archetype-smol"]
    assert probes[-1]["dist_dir"] == rebuilt_root
    assert probes[-1]["root"].name == "sdist-probe"
    distribution_count = len(package_smoke._DISTRIBUTIONS)
    assert validations[:distribution_count] == [
        (distribution, wheels[distribution], None) for distribution in package_smoke._DISTRIBUTIONS
    ]
    assert validations[distribution_count:] == [
        (distribution, rebuilt[distribution], "0.6.0")
        for distribution in package_smoke._DISTRIBUTIONS
    ]


def test_smol_probe_checks_behavior_and_framework_absence() -> None:
    probe = package_smoke._smol_probe_source("0.6.0")

    assert 'version("archetype-smol") == release_version' in probe
    assert 'find_spec("archetype.core") is None' in probe
    assert 'version("archetype-ecs")' in probe
    assert "world.run(steps=2)" in probe
    assert 'rows[0]["counter__value"] == 3' in probe
