# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the checksum-pinned actionlint bootstrap and static gate."""

from __future__ import annotations

import hashlib
import io
import re
import tarfile
import urllib.request
from pathlib import Path

import pytest

import scripts.run_actionlint as actionlint

ROOT = Path(__file__).resolve().parents[2]


def _archive(binary: bytes = b"verified-actionlint") -> bytes:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz") as bundle:
        member = tarfile.TarInfo("actionlint")
        member.mode = 0o755
        member.size = len(binary)
        bundle.addfile(member, io.BytesIO(binary))
    return output.getvalue()


def test_static_profile_uses_the_pinned_actionlint_gate() -> None:
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    bootstrap = (ROOT / "scripts" / "run_actionlint.py").read_text(encoding="utf-8")
    config = (ROOT / ".github" / "actionlint.yaml").read_text(encoding="utf-8")
    docs_workflow = (ROOT / ".github" / "workflows" / "docs.yml").read_text(encoding="utf-8")

    static = re.search(r"^static:(?P<dependencies>[^\n]+)$", makefile, re.MULTILINE)
    assert static is not None
    assert "actionlint-audit" in static.group("dependencies").split()
    assert "uv run python scripts/run_actionlint.py" in makefile
    assert '_VERSION = "1.7.12"' in bootstrap
    for digest in (
        "5b44c3bc2255115c9b69e30efc0fecdf498fdb63c5d58e17084fd5f16324c644",
        "aba9ced2dee8d27fecca3dc7feb1a7f9a52caefa1eb46f3271ea66b6e0e6953f",
        "8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8",
        "325e971b6ba9bfa504672e29be93c24981eeb1c07576d730e9f7c8805afff0c6",
    ):
        assert digest in bootstrap
    assert "archetype-apple-container-macos-26" in config
    assert "DOCS_DEPLOY_BRANCH: ${{ github.head_ref || 'main' }}" in docs_workflow
    assert '--branch="$DOCS_DEPLOY_BRANCH"' in docs_workflow


def test_actionlint_install_accepts_only_the_pinned_archive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = _archive()
    asset = actionlint._Asset(  # noqa: SLF001 - executable bootstrap contract
        archive="actionlint_test.tar.gz",
        sha256=hashlib.sha256(archive).hexdigest(),
    )
    monkeypatch.setattr(actionlint, "_ASSETS", {("Test", "x86_64"): asset})

    def download(request: urllib.request.Request, timeout: int) -> io.BytesIO:
        assert request.full_url.endswith("/actionlint_test.tar.gz")
        assert timeout == 30
        return io.BytesIO(archive)

    monkeypatch.setattr(actionlint.urllib.request, "urlopen", download)
    binary = actionlint.install_actionlint(
        root=tmp_path,
        system="Test",
        machine="x86_64",
    )

    assert binary.read_bytes() == b"verified-actionlint"
    assert binary.stat().st_mode & 0o111

    def reject_network(*_args: object, **_kwargs: object) -> io.BytesIO:
        raise AssertionError("the verified archive should be reused")

    monkeypatch.setattr(actionlint.urllib.request, "urlopen", reject_network)
    assert (
        actionlint.install_actionlint(
            root=tmp_path,
            system="Test",
            machine="x86_64",
        ).read_bytes()
        == b"verified-actionlint"
    )


def test_actionlint_install_rejects_a_checksum_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    asset = actionlint._Asset(  # noqa: SLF001 - executable bootstrap contract
        archive="actionlint_test.tar.gz",
        sha256="0" * 64,
    )
    monkeypatch.setattr(actionlint, "_ASSETS", {("Test", "x86_64"): asset})
    monkeypatch.setattr(
        actionlint.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: io.BytesIO(_archive()),
    )

    with pytest.raises(RuntimeError, match="archive checksum mismatch"):
        actionlint.install_actionlint(
            root=tmp_path,
            system="Test",
            machine="x86_64",
        )


def test_actionlint_install_rejects_an_unpinned_platform(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="has no pinned asset"):
        actionlint.install_actionlint(root=tmp_path, system="Plan9", machine="mips")
