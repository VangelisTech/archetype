#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Install a checksum-pinned actionlint binary and lint active workflows."""

from __future__ import annotations

import hashlib
import io
import os
import platform
import subprocess
import tarfile
import tempfile
import urllib.request
from dataclasses import dataclass
from pathlib import Path

_VERSION = "1.7.12"
_RELEASE = f"https://github.com/rhysd/actionlint/releases/download/v{_VERSION}"


@dataclass(frozen=True)
class _Asset:
    archive: str
    sha256: str


_ASSETS = {
    ("Darwin", "x86_64"): _Asset(
        "actionlint_1.7.12_darwin_amd64.tar.gz",
        "5b44c3bc2255115c9b69e30efc0fecdf498fdb63c5d58e17084fd5f16324c644",
    ),
    ("Darwin", "arm64"): _Asset(
        "actionlint_1.7.12_darwin_arm64.tar.gz",
        "aba9ced2dee8d27fecca3dc7feb1a7f9a52caefa1eb46f3271ea66b6e0e6953f",
    ),
    ("Linux", "x86_64"): _Asset(
        "actionlint_1.7.12_linux_amd64.tar.gz",
        "8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8",
    ),
    ("Linux", "arm64"): _Asset(
        "actionlint_1.7.12_linux_arm64.tar.gz",
        "325e971b6ba9bfa504672e29be93c24981eeb1c07576d730e9f7c8805afff0c6",
    ),
}
_MACHINE_ALIASES = {
    "AMD64": "x86_64",
    "aarch64": "arm64",
}


def _digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _asset(*, system: str, machine: str) -> _Asset:
    normalized_machine = _MACHINE_ALIASES.get(machine, machine)
    try:
        return _ASSETS[(system, normalized_machine)]
    except KeyError as error:
        raise RuntimeError(
            f"actionlint {_VERSION} has no pinned asset for {system}/{normalized_machine}"
        ) from error


def _load_archive(*, cache: Path, asset: _Asset) -> bytes:
    archive = cache / asset.archive
    if archive.is_file():
        value = archive.read_bytes()
        if _digest(value) == asset.sha256:
            return value

    request = urllib.request.Request(
        f"{_RELEASE}/{asset.archive}",
        headers={"User-Agent": "archetype-actionlint-bootstrap"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
        value = response.read()
    observed = _digest(value)
    if observed != asset.sha256:
        raise RuntimeError(
            f"actionlint archive checksum mismatch: expected {asset.sha256}, observed {observed}"
        )

    cache.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=cache, delete=False) as stream:
        temporary = Path(stream.name)
        stream.write(value)
    os.replace(temporary, archive)
    return value


def install_actionlint(
    *,
    root: Path,
    system: str | None = None,
    machine: str | None = None,
) -> Path:
    """Materialize the verified binary for the current supported platform."""

    selected = _asset(
        system=system or platform.system(),
        machine=machine or platform.machine(),
    )
    cache = root / ".venv" / "tools" / "actionlint" / _VERSION
    archive = _load_archive(cache=cache, asset=selected)

    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as bundle:
        try:
            member = bundle.getmember("actionlint")
        except KeyError as error:
            raise RuntimeError("verified actionlint archive has no actionlint binary") from error
        if not member.isfile():
            raise RuntimeError("verified actionlint archive member is not a regular file")
        source = bundle.extractfile(member)
        if source is None:
            raise RuntimeError("could not read verified actionlint archive member")
        binary_bytes = source.read()
    if not binary_bytes:
        raise RuntimeError("verified actionlint archive contains an empty binary")

    destination = cache / selected.archive.removesuffix(".tar.gz") / "actionlint"
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as stream:
        temporary = Path(stream.name)
        stream.write(binary_bytes)
    temporary.chmod(0o755)
    os.replace(temporary, destination)
    return destination


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    binary = install_actionlint(root=root)
    print(f"Running checksum-pinned actionlint {_VERSION}")
    process = subprocess.run(
        [
            str(binary),
            "-no-color",
            # Optional host tools would make the gate machine-dependent. Ruff
            # owns Python linting; shell checks can gain their own pinned lane.
            "-pyflakes=",
            "-shellcheck=",
        ],
        cwd=root,
        check=False,
    )
    return process.returncode


if __name__ == "__main__":
    raise SystemExit(main())
