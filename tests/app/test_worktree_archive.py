# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import hashlib
import io
import json
import os
import stat
import tarfile
from pathlib import Path

import pytest

from archetype.app.artifacts.worktree_archive import (
    WORKTREE_ARCHIVE_FORMAT,
    WorktreeArchiveError,
    capture_worktree_archive,
    restore_worktree_archive,
    sanitize_worktree_archive,
)
from archetype.app.redaction import RedactionService, SecretQuarantineError

pytestmark = pytest.mark.contract("artifacts.worktree_archive.portable")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fixture(tmp_path: Path) -> tuple[Path, dict[str, Path]]:
    worktree = tmp_path / "repo"
    worktree.mkdir()
    (worktree / ".git").mkdir()
    (worktree / ".git" / "config").write_text("provider internals")
    (worktree / ".gitignore").write_text("ignored.log\n")
    (worktree / "tracked.py").write_text("print('tracked')\n")
    (worktree / "untracked.txt").write_text("untracked\n")
    (worktree / "ignored.log").write_text("ignored but recoverable\n")
    (worktree / ".context").mkdir()
    (worktree / ".context" / "notes.md").write_text("review notes\n")
    (worktree / "locked").mkdir()
    (worktree / "locked" / "inside.txt").write_text("mode preserved\n")
    (worktree / "locked").chmod(0o555)
    recovery = tmp_path / "recovery"
    recovery.mkdir()
    files = {
        "git-status.txt": recovery / "status.txt",
        "worktree.patch": recovery / "worktree.patch",
        "repository.bundle": recovery / "repository.bundle",
    }
    for name, path in files.items():
        path.write_text(f"{name}\n")
    return worktree, files


def test_worktree_archive_is_deterministic_sanitized_and_restorable(tmp_path: Path) -> None:
    worktree, recovery = _fixture(tmp_path)
    raw_a = tmp_path / "raw-a.tar"
    raw_b = tmp_path / "raw-b.tar"
    capture_worktree_archive(
        worktree,
        raw_a,
        baseline_sha="a" * 40,
        head_sha="b" * 40,
        recovery_files=recovery,
    )
    capture_worktree_archive(
        worktree,
        raw_b,
        baseline_sha="a" * 40,
        head_sha="b" * 40,
        recovery_files=recovery,
    )
    assert raw_a.read_bytes() == raw_b.read_bytes()

    redaction = RedactionService()
    sanitized = tmp_path / "sanitized.tar"
    result = sanitize_worktree_archive(
        raw_a,
        sanitized,
        logical_path="recovery/worktree.tar",
        redaction_service=redaction,
    )
    restored = tmp_path / "restored"
    manifest = restore_worktree_archive(
        sanitized,
        restored,
        expected_content_hash=_sha256(sanitized),
    )

    assert result.receipt.status == "clean"
    assert manifest["archive_format"] == WORKTREE_ARCHIVE_FORMAT
    assert manifest["redaction_policy_id"] == redaction.policy_id
    assert manifest["baseline_sha"] == "a" * 40
    assert manifest["head_sha"] == "b" * 40
    assert (restored / "worktree/tracked.py").read_text() == "print('tracked')\n"
    assert (restored / "worktree/untracked.txt").read_text() == "untracked\n"
    assert (restored / "worktree/ignored.log").read_text() == "ignored but recoverable\n"
    assert (restored / "worktree/.context/notes.md").read_text() == "review notes\n"
    assert (restored / "worktree/locked/inside.txt").read_text() == "mode preserved\n"
    assert stat.S_IMODE((restored / "worktree/locked").stat().st_mode) == 0o555
    assert (restored / "recovery/repository.bundle").read_text() == "repository.bundle\n"
    assert not (restored / "worktree/.git").exists()
    assert {item["reason"] for item in manifest["exclusions"]} >= {"git-internals"}


def test_worktree_archive_redacts_text_and_excludes_credentials(tmp_path: Path) -> None:
    worktree, recovery = _fixture(tmp_path)
    secret = "sk-proj-" + "x" * 30
    (worktree / "normal.txt").write_text(f"api_key={secret}\n")
    (worktree / ".env").write_text(f"TOKEN={secret}\n")
    raw = tmp_path / "raw.tar"
    capture_worktree_archive(
        worktree,
        raw,
        baseline_sha="a" * 40,
        head_sha="b" * 40,
        recovery_files=recovery,
    )
    sanitized = tmp_path / "sanitized.tar"
    result = sanitize_worktree_archive(
        raw,
        sanitized,
        logical_path="recovery/worktree.tar",
        redaction_service=RedactionService(),
    )
    restored = tmp_path / "restored"
    manifest = restore_worktree_archive(sanitized, restored)

    assert result.receipt.status == "redacted"
    assert secret not in (restored / "worktree/normal.txt").read_text()
    assert "<redacted:" in (restored / "worktree/normal.txt").read_text()
    assert not (restored / "worktree/.env").exists()
    assert {item["path"]: item["reason"] for item in manifest["exclusions"]}[".env"] == (
        "credential-path"
    )


@pytest.mark.parametrize("unsafe", ["symlink", "hardlink"])
def test_worktree_archive_rejects_link_ambiguity(tmp_path: Path, unsafe: str) -> None:
    worktree, recovery = _fixture(tmp_path)
    target = worktree / "tracked.py"
    if unsafe == "symlink":
        (worktree / "unsafe").symlink_to(target)
    else:
        os.link(target, worktree / "unsafe")
    with pytest.raises(WorktreeArchiveError, match="links|linked"):
        capture_worktree_archive(
            worktree,
            tmp_path / "raw.tar",
            baseline_sha="a" * 40,
            head_sha="b" * 40,
            recovery_files=recovery,
        )


def test_worktree_archive_rejects_special_files(tmp_path: Path) -> None:
    worktree, recovery = _fixture(tmp_path)
    os.mkfifo(worktree / "named-pipe")
    with pytest.raises(WorktreeArchiveError, match="special files"):
        capture_worktree_archive(
            worktree,
            tmp_path / "raw.tar",
            baseline_sha="a" * 40,
            head_sha="b" * 40,
            recovery_files=recovery,
        )


def test_worktree_archive_rejects_path_race(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    worktree, recovery = _fixture(tmp_path)
    from archetype.app.artifacts import worktree_archive as archive_module

    real_copy = archive_module._copy_stable_file
    raced = False

    def mutate_then_copy(source: Path, destination: Path, expected: object) -> None:
        nonlocal raced
        if not raced and source.name == "tracked.py":
            raced = True
            source.write_text("changed after inventory\n")
        real_copy(source, destination, expected)  # type: ignore[arg-type]

    monkeypatch.setattr(archive_module, "_copy_stable_file", mutate_then_copy)
    with pytest.raises(WorktreeArchiveError, match="changed"):
        capture_worktree_archive(
            worktree,
            tmp_path / "raw.tar",
            baseline_sha="a" * 40,
            head_sha="b" * 40,
            recovery_files=recovery,
        )


def test_worktree_archive_rejects_output_inside_approved_tree(tmp_path: Path) -> None:
    worktree, recovery = _fixture(tmp_path)
    with pytest.raises(WorktreeArchiveError, match="output must be outside"):
        capture_worktree_archive(
            worktree,
            worktree / "archive.tar",
            baseline_sha="a" * 40,
            head_sha="b" * 40,
            recovery_files=recovery,
        )


def test_worktree_archive_restore_rejects_unsafe_member(tmp_path: Path) -> None:
    archive = tmp_path / "unsafe.tar"
    manifest = json.dumps(
        {
            "archive_format": "archetype-worktree-tar-v1",
            "baseline_sha": "a" * 40,
            "entries": [],
            "exclusions": [],
            "head_sha": "b" * 40,
            "redaction": {"status": "clean"},
            "redaction_policy_id": "test-policy",
            "schema_version": 1,
        },
        sort_keys=True,
    ).encode()
    with tarfile.open(archive, "w") as output:
        info = tarfile.TarInfo("archive-manifest.json")
        info.size = len(manifest)
        output.addfile(info, io.BytesIO(manifest))
        info = tarfile.TarInfo("../escape")
        info.size = 1
        output.addfile(info, io.BytesIO(b"x"))
    with pytest.raises(WorktreeArchiveError, match="members|unsafe"):
        restore_worktree_archive(archive, tmp_path / "restore")


def test_worktree_archive_restore_rejects_raw_unapproved_capture(tmp_path: Path) -> None:
    worktree, recovery = _fixture(tmp_path)
    raw = tmp_path / "raw.tar"
    capture_worktree_archive(
        worktree,
        raw,
        baseline_sha="a" * 40,
        head_sha="b" * 40,
        recovery_files=recovery,
    )
    with pytest.raises(WorktreeArchiveError, match="not redaction-approved"):
        restore_worktree_archive(raw, tmp_path / "restore")


def test_worktree_archive_quarantines_nested_container(tmp_path: Path) -> None:
    worktree, recovery = _fixture(tmp_path)
    nested = worktree / "nested.tar"
    with tarfile.open(nested, "w"):
        pass
    raw = tmp_path / "raw.tar"
    capture_worktree_archive(
        worktree,
        raw,
        baseline_sha="a" * 40,
        head_sha="b" * 40,
        recovery_files=recovery,
    )
    with pytest.raises(SecretQuarantineError, match="nested-archive-unsupported"):
        sanitize_worktree_archive(
            raw,
            tmp_path / "sanitized.tar",
            logical_path="recovery/worktree.tar",
            redaction_service=RedactionService(),
        )
