# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Sealed candidate bytes must be checked before installation can begin."""

import hashlib
from unittest.mock import patch

import pytest

from scripts import release_artifact


def candidate(tmp_path):
    directory = tmp_path / "candidate"
    directory.mkdir()
    for distribution in release_artifact.DISTRIBUTIONS:
        prefix = distribution.replace("-", "_")
        version = "0.6.3" if distribution == "archetype-smol" else "0.7.0"
        for suffix in ("-py3-none-any.whl", ".tar.gz"):
            (directory / (prefix + "-" + version + suffix)).write_bytes(b"sealed original")
    with patch.object(
        release_artifact,
        "_git",
        side_effect=lambda root, *args: "" if args[0] == "status" else "a" * 40,
    ):
        manifest = release_artifact.record(tmp_path, directory)
    return directory, manifest


def test_changed_same_name_candidate_fails_before_retained_install_inputs(tmp_path):
    directory, manifest = candidate(tmp_path)
    wheel = next(directory.glob("*.whl"))
    wheel.write_bytes(b"changed same-name wheel")
    retained = tmp_path / "installed-inputs"
    with pytest.raises(ValueError):
        release_artifact.copy_candidate(manifest, directory, retained, expected_commit="a" * 40)
    assert not retained.exists()


def test_sealed_eight_artifact_bytes_are_the_retained_install_inputs(tmp_path):
    directory, manifest = candidate(tmp_path)
    retained = tmp_path / "installed-inputs"
    release_artifact.copy_candidate(manifest, directory, retained, expected_commit="a" * 40)
    assert len(list(retained.iterdir())) == 8
    assert {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in retained.iterdir()} == {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in directory.iterdir()
    }
