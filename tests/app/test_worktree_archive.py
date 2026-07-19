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


def _valid_manifest(*, entries: list[object] | None = None) -> dict[str, object]:
    return {
        "archive_format": WORKTREE_ARCHIVE_FORMAT,
        "baseline_sha": "a" * 40,
        "entries": [] if entries is None else entries,
        "exclusions": [],
        "head_sha": "b" * 40,
        "redaction": {"status": "clean"},
        "redaction_policy_id": "test-policy",
        "schema_version": 1,
    }


def _file_entry(
    path: str = "worktree/file.txt",
    *,
    size: int = 0,
    digest: str | None = None,
) -> dict[str, object]:
    return {
        "path": path,
        "type": "file",
        "mode": 0o600,
        "size_bytes": size,
        "sha256": hashlib.sha256(b"").hexdigest() if digest is None else digest,
    }


def _directory_entry(path: str = "worktree/directory") -> dict[str, object]:
    return {
        "path": path,
        "type": "directory",
        "mode": 0o700,
        "size_bytes": 0,
        "sha256": "",
    }


def _write_manifest_archive(
    path: Path,
    manifest: object,
    *members: tuple[tarfile.TarInfo, bytes | None],
) -> None:
    payload = json.dumps(manifest, sort_keys=True).encode()
    with tarfile.open(path, "w") as output:
        info = tarfile.TarInfo("archive-manifest.json")
        info.size = len(payload)
        output.addfile(info, io.BytesIO(payload))
        for member, content in members:
            output.addfile(member, None if content is None else io.BytesIO(content))


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


def test_worktree_archive_rejects_backslash_filename_instead_of_rewriting_it(
    tmp_path: Path,
) -> None:
    worktree, recovery = _fixture(tmp_path)
    (worktree / "folder\\file.txt").write_text("must remain one path component\n")

    with pytest.raises(WorktreeArchiveError, match="unsafe path"):
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


def test_worktree_archive_records_provider_and_cache_exclusions(tmp_path: Path) -> None:
    worktree, recovery = _fixture(tmp_path)
    (worktree / ".archetype-agent").mkdir()
    (worktree / ".archetype-agent" / "private.txt").write_text("provider state\n")
    (worktree / "__pycache__").mkdir()
    (worktree / "__pycache__" / "cached.pyc").write_bytes(b"cache")

    result = capture_worktree_archive(
        worktree,
        tmp_path / "raw.tar",
        baseline_sha="a" * 40,
        head_sha="b" * 40,
        recovery_files=recovery,
    )

    assert {item["reason"] for item in result.manifest["exclusions"]} >= {
        "provider-internals",
        "cache",
    }


def test_worktree_archive_requires_a_directory_source(tmp_path: Path) -> None:
    source = tmp_path / "not-a-directory"
    source.write_text("plain file\n")

    with pytest.raises(NotADirectoryError):
        capture_worktree_archive(
            source,
            tmp_path / "raw.tar",
            baseline_sha="a" * 40,
            head_sha="b" * 40,
        )


def test_worktree_archive_rejects_linked_recovery_material(tmp_path: Path) -> None:
    worktree, recovery = _fixture(tmp_path)
    linked = tmp_path / "linked-status"
    linked.symlink_to(recovery["git-status.txt"])

    with pytest.raises(WorktreeArchiveError, match="regular file"):
        capture_worktree_archive(
            worktree,
            tmp_path / "raw.tar",
            baseline_sha="a" * 40,
            head_sha="b" * 40,
            recovery_files={"git-status.txt": linked},
        )


@pytest.mark.parametrize(
    ("case", "message"),
    [
        ("schema", "schema"),
        ("format", "format"),
        ("identity-type", "Git identities"),
        ("collections", "collections"),
        ("policy", "policy"),
        ("identity-value", "Git identities"),
        ("entry", "entry is invalid"),
        ("entry-type", "entry type"),
        ("mode", "entry mode"),
        ("size", "entry size"),
        ("directory-integrity", "directory integrity"),
        ("digest", "file digest"),
    ],
)
def test_worktree_archive_rejects_invalid_manifest_headers(
    tmp_path: Path,
    case: str,
    message: str,
) -> None:
    manifest = _valid_manifest()
    if case == "schema":
        manifest["schema_version"] = 2
    elif case == "format":
        manifest["archive_format"] = "unknown"
    elif case == "identity-type":
        manifest["baseline_sha"] = None
    elif case == "collections":
        manifest["exclusions"] = None
    elif case == "policy":
        manifest["redaction_policy_id"] = None
    elif case == "identity-value":
        manifest["head_sha"] = "not-a-sha"
    elif case == "entry":
        manifest["entries"] = [None]
    else:
        entry = _file_entry()
        manifest["entries"] = [entry]
        if case == "entry-type":
            entry["type"] = "link"
        elif case == "mode":
            entry["mode"] = None
        elif case == "size":
            entry["size_bytes"] = -1
        elif case == "directory-integrity":
            entry.update(type="directory", size_bytes=1, sha256="")
        elif case == "digest":
            entry["sha256"] = "invalid"
    archive = tmp_path / f"{case}.tar"
    _write_manifest_archive(archive, manifest)

    with pytest.raises(WorktreeArchiveError, match=message):
        restore_worktree_archive(archive, tmp_path / f"restore-{case}")


def test_worktree_archive_rejects_non_object_manifest(tmp_path: Path) -> None:
    archive = tmp_path / "non-object.tar"
    _write_manifest_archive(archive, [])

    with pytest.raises(WorktreeArchiveError, match="manifest must be an object"):
        restore_worktree_archive(archive, tmp_path / "restore")


def test_worktree_archive_rejects_duplicate_manifest_paths(tmp_path: Path) -> None:
    archive = tmp_path / "duplicate.tar"
    entry = _directory_entry()
    _write_manifest_archive(archive, _valid_manifest(entries=[entry, dict(entry)]))

    with pytest.raises(WorktreeArchiveError, match="paths must be unique"):
        restore_worktree_archive(archive, tmp_path / "restore")


@pytest.mark.parametrize(
    ("case", "message"),
    [
        ("unexpected", "members do not match"),
        ("directory-metadata", "directory metadata"),
        ("link", "rejects links"),
        ("size", "member size"),
        ("digest", "content validation"),
        ("missing", "missing manifest members"),
    ],
)
def test_worktree_archive_rejects_tampered_members(
    tmp_path: Path,
    case: str,
    message: str,
) -> None:
    archive = tmp_path / f"tampered-{case}.tar"
    member = tarfile.TarInfo("worktree/file.txt")
    content: bytes | None = b""
    if case == "unexpected":
        manifest = _valid_manifest()
    elif case == "directory-metadata":
        manifest = _valid_manifest(entries=[_file_entry()])
        member.type = tarfile.DIRTYPE
        content = None
    elif case == "link":
        manifest = _valid_manifest(entries=[_file_entry()])
        member.type = tarfile.SYMTYPE
        member.linkname = "target"
        content = None
    elif case == "size":
        manifest = _valid_manifest(entries=[_file_entry()])
        member.size = 1
        content = b"x"
    elif case == "digest":
        manifest = _valid_manifest(entries=[_file_entry(size=1, digest="0" * 64)])
        member.size = 1
        content = b"x"
    else:
        manifest = _valid_manifest(entries=[_directory_entry()])
        _write_manifest_archive(archive, manifest)
        with pytest.raises(WorktreeArchiveError, match=message):
            restore_worktree_archive(archive, tmp_path / f"restore-{case}")
        return
    _write_manifest_archive(archive, manifest, (member, content))

    with pytest.raises(WorktreeArchiveError, match=message):
        restore_worktree_archive(archive, tmp_path / f"restore-{case}")


def test_worktree_archive_rejects_unreadable_tar(tmp_path: Path) -> None:
    archive = tmp_path / "unreadable.tar"
    archive.write_bytes(b"not a tar archive")

    with pytest.raises(WorktreeArchiveError, match="incomplete or unreadable"):
        restore_worktree_archive(archive, tmp_path / "restore")


def test_worktree_archive_sanitizer_quarantines_unsupported_source(tmp_path: Path) -> None:
    source = tmp_path / "source-directory"
    source.mkdir()

    with pytest.raises(SecretQuarantineError, match="unsupported-source-file"):
        sanitize_worktree_archive(
            source,
            tmp_path / "sanitized.tar",
            logical_path="recovery/worktree.tar",
            redaction_service=RedactionService(),
        )


def test_worktree_archive_sanitizer_quarantines_source_race(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from archetype.app.artifacts import worktree_archive as archive_module

    source = tmp_path / "raw.tar"
    source.write_bytes(b"raw")

    def fail_copy(source: Path, destination: Path, expected: object) -> None:
        raise WorktreeArchiveError("changed")

    monkeypatch.setattr(archive_module, "_copy_stable_file", fail_copy)
    with pytest.raises(SecretQuarantineError, match="source-file-race"):
        sanitize_worktree_archive(
            source,
            tmp_path / "sanitized.tar",
            logical_path="recovery/worktree.tar",
            redaction_service=RedactionService(),
        )


def test_worktree_archive_sanitizer_quarantines_invalid_archive(tmp_path: Path) -> None:
    source = tmp_path / "raw.tar"
    source.write_bytes(b"not a tar archive")

    with pytest.raises(SecretQuarantineError, match="worktree-archive-invalid"):
        sanitize_worktree_archive(
            source,
            tmp_path / "sanitized.tar",
            logical_path="recovery/worktree.tar",
            redaction_service=RedactionService(),
        )


@pytest.mark.parametrize(
    "member_name",
    [
        "outside/file.txt",
        "worktree/.git/config",
        "worktree/.archetype-agent/provider-state.json",
        "worktree/__pycache__/cached.pyc",
        "worktree/.env",
    ],
)
def test_worktree_archive_sanitizer_reapplies_capture_policy(
    tmp_path: Path,
    member_name: str,
) -> None:
    content = b"tampered raw capture\n"
    member = tarfile.TarInfo(member_name)
    member.size = len(content)
    source = tmp_path / "raw.tar"
    manifest = _valid_manifest(
        entries=[
            _file_entry(
                member_name,
                size=len(content),
                digest=hashlib.sha256(content).hexdigest(),
            )
        ]
    )
    _write_manifest_archive(source, manifest, (member, content))
    destination = tmp_path / "sanitized.tar"

    with pytest.raises(SecretQuarantineError, match="worktree-archive-policy"):
        sanitize_worktree_archive(
            source,
            destination,
            logical_path="recovery/worktree.tar",
            redaction_service=RedactionService(),
        )

    assert not destination.exists()


def test_worktree_archive_restore_checks_indexed_hash_and_clean_target(tmp_path: Path) -> None:
    archive = tmp_path / "raw.tar"
    archive.write_bytes(b"archive bytes")

    with pytest.raises(WorktreeArchiveError, match="content hash"):
        restore_worktree_archive(
            archive,
            tmp_path / "hash-restore",
            expected_content_hash="0" * 64,
        )

    dirty = tmp_path / "dirty-restore"
    dirty.mkdir()
    (dirty / "existing.txt").write_text("existing\n")
    with pytest.raises(WorktreeArchiveError, match="clean directory"):
        restore_worktree_archive(archive, dirty)
