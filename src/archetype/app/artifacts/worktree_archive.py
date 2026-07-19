# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Deterministic, sanitized, and independently restorable worktree archives."""

from __future__ import annotations

import errno
import hashlib
import json
import os
import shutil
import stat
import tarfile
import tempfile
import zipfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import IO, Any

from archetype.app.redaction.interfaces import iRedactionService
from archetype.app.redaction.models import (
    RedactedFile,
    RedactionReceipt,
    SecretQuarantineError,
)

WORKTREE_ARCHIVE_FORMAT = "archetype-worktree-tar-v1"
WORKTREE_ARCHIVE_MANIFEST_PATH = "archive-manifest.json"
WORKTREE_ARCHIVE_SCHEMA_VERSION = 1

_MAX_ARCHIVE_MEMBERS = 10_000
_MAX_ARCHIVE_MEMBER_BYTES = 1 << 30
_MAX_ARCHIVE_EXPANDED_BYTES = 4 << 30
_COPY_CHUNK_BYTES = 1 << 20
_CACHE_NAMES = frozenset(
    {
        ".cache",
        ".mypy_cache",
        ".nox",
        ".pytest_cache",
        ".ruff_cache",
        ".tox",
        ".venv",
        "__pycache__",
        "build",
        "dist",
        "node_modules",
        "venv",
    }
)
_CREDENTIAL_BASENAMES = frozenset(
    {
        ".env",
        ".env.development",
        ".env.local",
        ".env.production",
        ".git-credentials",
        ".netrc",
        ".npmrc",
        ".pypirc",
        "id_dsa",
        "id_ecdsa",
        "id_ed25519",
        "id_rsa",
    }
)
_CREDENTIAL_SUFFIXES = (
    (".codex", "auth.json"),
    (".claude", ".credentials.json"),
    (".config", "opencode", "auth.json"),
    (".local", "share", "opencode", "auth.json"),
    (".aws", "credentials"),
    (".config", "gcloud", "application_default_credentials.json"),
    (".config", "gcloud", "credentials.db"),
    (".config", "gcloud", "access_tokens.db"),
    (".config", "gh", "hosts.yml"),
    (".docker", "config.json"),
    (".kube", "config"),
    (".azure", "accesstokens.json"),
    (".cache", "huggingface", "token"),
    (".huggingface", "token"),
)


class WorktreeArchiveError(ValueError):
    """A worktree cannot be captured or restored without ambiguity."""


@dataclass(frozen=True)
class WorktreeArchiveResult:
    """One locally materialized archive and its authenticated manifest."""

    path: Path
    content_hash: str
    size_bytes: int
    manifest: dict[str, Any]


@dataclass(frozen=True)
class _InventoryEntry:
    path: str
    type: str
    mode: int
    size: int
    device: int
    inode: int
    mtime_ns: int
    ctime_ns: int


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(_COPY_CHUNK_BYTES), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _portable_path(value: str) -> str:
    if "\\" in value:
        raise WorktreeArchiveError("worktree archive contains an unsafe path")
    normalized = value.strip("/")
    path = PurePosixPath(normalized)
    if (
        not normalized
        or path.is_absolute()
        or ".." in path.parts
        or any(part in {"", "."} for part in path.parts)
    ):
        raise WorktreeArchiveError("worktree archive contains an unsafe path")
    return path.as_posix()


def _is_credential_path(parts: tuple[str, ...]) -> bool:
    lower = tuple(part.lower() for part in parts)
    return bool(lower and lower[-1] in _CREDENTIAL_BASENAMES) or any(
        len(lower) >= len(suffix) and lower[-len(suffix) :] == suffix
        for suffix in _CREDENTIAL_SUFFIXES
    )


def _exclusion_reason(parts: tuple[str, ...], *, is_dir: bool) -> str | None:
    if parts and parts[0] == ".git":
        return "git-internals"
    if parts and parts[0] == ".archetype-agent":
        return "provider-internals"
    if _is_credential_path(parts):
        return "credential-path"
    if is_dir and parts and parts[-1] in _CACHE_NAMES:
        return "cache"
    return None


def _inventory(root: Path) -> tuple[dict[str, _InventoryEntry], list[dict[str, str]]]:
    entries: dict[str, _InventoryEntry] = {}
    exclusions: list[dict[str, str]] = []
    seen_inodes: set[tuple[int, int]] = set()
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    root_descriptor = os.open(root, flags)
    root_device = os.fstat(root_descriptor).st_dev
    stack: list[tuple[int, tuple[str, ...]]] = [(root_descriptor, ())]
    try:
        while stack:
            descriptor, prefix = stack.pop()
            try:
                children = sorted(os.scandir(descriptor), key=lambda item: item.name, reverse=True)
            except OSError as exc:
                os.close(descriptor)
                raise WorktreeArchiveError("worktree directory changed during inventory") from exc
            try:
                for child in children:
                    parts = (*prefix, child.name)
                    relative = _portable_path(PurePosixPath(*parts).as_posix())
                    try:
                        info = child.stat(follow_symlinks=False)
                    except OSError as exc:
                        raise WorktreeArchiveError(
                            "worktree entry changed during inventory"
                        ) from exc
                    is_dir = stat.S_ISDIR(info.st_mode)
                    is_file = stat.S_ISREG(info.st_mode)
                    if stat.S_ISLNK(info.st_mode):
                        raise WorktreeArchiveError("worktree archive rejects symbolic links")
                    if not is_dir and not is_file:
                        raise WorktreeArchiveError("worktree archive rejects special files")
                    reason = _exclusion_reason(parts, is_dir=is_dir)
                    if reason is not None:
                        exclusions.append(
                            {
                                "path": relative,
                                "type": "directory" if is_dir else "file",
                                "reason": reason,
                            }
                        )
                        continue
                    if info.st_dev != root_device:
                        raise WorktreeArchiveError("worktree archive rejects nested filesystems")
                    if is_dir:
                        kind = "directory"
                        size = 0
                        try:
                            child_descriptor = os.open(child.name, flags, dir_fd=descriptor)
                        except OSError as exc:
                            raise WorktreeArchiveError(
                                "worktree directory changed during inventory"
                            ) from exc
                        opened = os.fstat(child_descriptor)
                        if (
                            not stat.S_ISDIR(opened.st_mode)
                            or opened.st_dev != info.st_dev
                            or opened.st_ino != info.st_ino
                        ):
                            os.close(child_descriptor)
                            raise WorktreeArchiveError(
                                "worktree directory changed during inventory"
                            )
                        stack.append((child_descriptor, parts))
                    elif is_file:
                        if info.st_nlink != 1 or (info.st_dev, info.st_ino) in seen_inodes:
                            raise WorktreeArchiveError("worktree archive rejects hard-linked files")
                        seen_inodes.add((info.st_dev, info.st_ino))
                        kind = "file"
                        size = info.st_size
                    entries[relative] = _InventoryEntry(
                        path=relative,
                        type=kind,
                        mode=stat.S_IMODE(info.st_mode),
                        size=size,
                        device=info.st_dev,
                        inode=info.st_ino,
                        mtime_ns=info.st_mtime_ns,
                        ctime_ns=info.st_ctime_ns,
                    )
            finally:
                os.close(descriptor)
    finally:
        for descriptor, _ in stack:
            os.close(descriptor)
    exclusions.sort(key=lambda item: (item["path"], item["reason"]))
    return entries, exclusions


def _copy_stable_file(source: Path, destination: Path, expected: _InventoryEntry) -> None:
    flags = os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(source, flags)
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            raise WorktreeArchiveError("worktree file became a symbolic link") from None
        raise WorktreeArchiveError("worktree file could not be opened completely") from exc
    try:
        opened = os.fstat(descriptor)
        identity = (
            opened.st_dev,
            opened.st_ino,
            opened.st_size,
            opened.st_mtime_ns,
            opened.st_ctime_ns,
        )
        expected_identity = (
            expected.device,
            expected.inode,
            expected.size,
            expected.mtime_ns,
            expected.ctime_ns,
        )
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or identity != expected_identity
        ):
            raise WorktreeArchiveError("worktree file changed before its stable read")
        destination.parent.mkdir(parents=True, exist_ok=True)
        with (
            os.fdopen(descriptor, "rb", closefd=False) as reader,
            destination.open("xb") as writer,
        ):
            shutil.copyfileobj(reader, writer, length=_COPY_CHUNK_BYTES)
        finished = os.fstat(descriptor)
        final_identity = (
            finished.st_dev,
            finished.st_ino,
            finished.st_size,
            finished.st_mtime_ns,
            finished.st_ctime_ns,
        )
        if final_identity != identity or destination.stat().st_size != expected.size:
            raise WorktreeArchiveError("worktree file changed during its stable read")
    finally:
        os.close(descriptor)


def _tar_info(name: str, *, mode: int, size: int = 0, directory: bool = False) -> tarfile.TarInfo:
    info = tarfile.TarInfo(name)
    info.mode = mode
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    info.mtime = 0
    info.size = size
    info.type = tarfile.DIRTYPE if directory else tarfile.REGTYPE
    return info


def _write_archive(
    destination: Path,
    manifest: dict[str, Any],
    files_root: Path,
) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f"{destination.name}.writing")
    temporary.unlink(missing_ok=True)
    manifest_bytes = _canonical_json(manifest).encode()
    with tarfile.open(temporary, mode="w", format=tarfile.PAX_FORMAT) as archive:
        archive.addfile(
            _tar_info(
                WORKTREE_ARCHIVE_MANIFEST_PATH,
                mode=0o644,
                size=len(manifest_bytes),
            ),
            fileobj=_BytesReader(manifest_bytes),
        )
        for entry in manifest["entries"]:
            name = str(entry["path"])
            if entry["type"] == "directory":
                archive.addfile(_tar_info(name, mode=int(entry["mode"]), directory=True))
                continue
            path = files_root / PurePosixPath(name)
            with path.open("rb") as stream:
                archive.addfile(
                    _tar_info(name, mode=int(entry["mode"]), size=int(entry["size_bytes"])),
                    fileobj=stream,
                )
    os.replace(temporary, destination)


class _BytesReader(IO[bytes]):
    """Tiny seek-free reader accepted by ``TarFile.addfile``."""

    def __init__(self, value: bytes) -> None:
        self._value = value
        self._offset = 0

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            size = len(self._value) - self._offset
        value = self._value[self._offset : self._offset + size]
        self._offset += len(value)
        return value


def _entry_payload(path: str, kind: str, mode: int, file_path: Path | None) -> dict[str, Any]:
    payload: dict[str, Any] = {"path": path, "type": kind, "mode": mode}
    if kind == "file":
        assert file_path is not None
        payload.update(
            {
                "size_bytes": file_path.stat().st_size,
                "sha256": _sha256_file(file_path),
            }
        )
    else:
        payload.update({"size_bytes": 0, "sha256": ""})
    return payload


def capture_worktree_archive(
    worktree: str | Path,
    destination: str | Path,
    *,
    baseline_sha: str,
    head_sha: str,
    recovery_files: Mapping[str, str | Path] | None = None,
) -> WorktreeArchiveResult:
    """Capture tracked, untracked, ignored, context, and recovery state.

    The source is inventoried twice. Every regular file is copied from a stable,
    no-follow handle and the archive is built only from those controlled copies.
    """

    root = Path(worktree).resolve(strict=True)
    if not root.is_dir():
        raise NotADirectoryError(root)
    output = Path(destination)
    try:
        output_relative = output.resolve(strict=False).relative_to(root)
    except ValueError:
        output_relative = None
    if output_relative is not None and (
        not output_relative.parts or output_relative.parts[0] != ".archetype-agent"
    ):
        raise WorktreeArchiveError(
            "worktree archive output must be outside the worktree or provider staging"
        )
    initial, exclusions = _inventory(root)
    with tempfile.TemporaryDirectory(prefix="archetype-worktree-") as temporary_value:
        staged = Path(temporary_value)
        for entry in sorted(initial.values(), key=lambda item: item.path):
            archive_path = f"worktree/{entry.path}"
            target = staged / PurePosixPath(archive_path)
            if entry.type == "directory":
                target.mkdir(parents=True, exist_ok=True)
                continue
            _copy_stable_file(root / PurePosixPath(entry.path), target, entry)

        recovery_entries: list[dict[str, Any]] = []
        for raw_name, raw_source in sorted((recovery_files or {}).items()):
            name = _portable_path(raw_name)
            source = Path(raw_source)
            info = source.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise WorktreeArchiveError("Git recovery material must be a regular file")
            expected = _InventoryEntry(
                path=name,
                type="file",
                mode=stat.S_IMODE(info.st_mode),
                size=info.st_size,
                device=info.st_dev,
                inode=info.st_ino,
                mtime_ns=info.st_mtime_ns,
                ctime_ns=info.st_ctime_ns,
            )
            archive_path = f"recovery/{name}"
            target = staged / PurePosixPath(archive_path)
            _copy_stable_file(source, target, expected)
            recovery_entries.append(_entry_payload(archive_path, "file", expected.mode, target))

        final, final_exclusions = _inventory(root)
        if final != initial or final_exclusions != exclusions:
            raise WorktreeArchiveError("worktree changed while the archive was captured")

        entries = [
            _entry_payload(
                f"worktree/{entry.path}",
                entry.type,
                entry.mode,
                staged / "worktree" / PurePosixPath(entry.path) if entry.type == "file" else None,
            )
            for entry in sorted(initial.values(), key=lambda item: item.path)
        ]
        entries.extend(recovery_entries)
        entries.sort(key=lambda item: str(item["path"]))
        manifest: dict[str, Any] = {
            "schema_version": WORKTREE_ARCHIVE_SCHEMA_VERSION,
            "archive_format": WORKTREE_ARCHIVE_FORMAT,
            "baseline_sha": baseline_sha,
            "head_sha": head_sha,
            "redaction_policy_id": "",
            "entries": entries,
            "exclusions": exclusions,
            "redaction": {
                "status": "unscanned",
                "files_scanned": 0,
                "bytes_scanned": 0,
                "redaction_count": 0,
                "rule_ids": [],
                "files": [],
            },
        }
        _validate_manifest_header(manifest)
        _write_archive(output, manifest, staged)
    return WorktreeArchiveResult(
        path=output,
        content_hash=_sha256_file(output),
        size_bytes=output.stat().st_size,
        manifest=manifest,
    )


def _extract_validated_archive(
    source: Path,
    destination: Path,
    *,
    require_sanitized: bool = False,
) -> dict[str, Any]:
    destination.mkdir(parents=True, exist_ok=True)
    members_seen: set[str] = set()
    expanded = 0
    manifest: dict[str, Any] | None = None
    try:
        with tarfile.open(source, mode="r:") as archive:
            members = archive.getmembers()
            if len(members) > _MAX_ARCHIVE_MEMBERS + 1:
                raise WorktreeArchiveError("worktree archive exceeds its member bound")
            manifest_members = [
                member for member in members if member.name == WORKTREE_ARCHIVE_MANIFEST_PATH
            ]
            if len(manifest_members) != 1 or not manifest_members[0].isfile():
                raise WorktreeArchiveError("worktree archive requires one regular manifest")
            if manifest_members[0].size > _MAX_ARCHIVE_MEMBER_BYTES:
                raise WorktreeArchiveError("worktree archive manifest exceeds its byte bound")
            manifest_stream = archive.extractfile(manifest_members[0])
            if manifest_stream is None:
                raise WorktreeArchiveError("worktree archive manifest is unreadable")
            with manifest_stream:
                manifest_value = json.load(manifest_stream)
            if not isinstance(manifest_value, dict):
                raise WorktreeArchiveError("worktree archive manifest must be an object")
            manifest = manifest_value
            _validate_manifest_header(manifest, require_sanitized=require_sanitized)
            expected = {str(entry["path"]): entry for entry in manifest["entries"]}
            if len(expected) != len(manifest["entries"]):
                raise WorktreeArchiveError("worktree archive manifest paths must be unique")
            for member in members:
                if member.name == WORKTREE_ARCHIVE_MANIFEST_PATH:
                    continue
                name = _portable_path(member.name)
                if name in members_seen or name not in expected:
                    raise WorktreeArchiveError("worktree archive members do not match its manifest")
                members_seen.add(name)
                entry = expected[name]
                if member.isdir():
                    if entry["type"] != "directory" or member.size != 0:
                        raise WorktreeArchiveError("worktree archive directory metadata is invalid")
                    (destination / PurePosixPath(name)).mkdir(parents=True, exist_ok=True)
                    continue
                if not member.isfile() or entry["type"] != "file":
                    raise WorktreeArchiveError("worktree archive rejects links and special members")
                declared_size = int(entry["size_bytes"])
                if member.size != declared_size or member.size > _MAX_ARCHIVE_MEMBER_BYTES:
                    raise WorktreeArchiveError("worktree archive member size is invalid")
                expanded += member.size
                if expanded > _MAX_ARCHIVE_EXPANDED_BYTES:
                    raise WorktreeArchiveError("worktree archive exceeds its expanded-byte bound")
                stream = archive.extractfile(member)
                if stream is None:
                    raise WorktreeArchiveError("worktree archive member is unreadable")
                output = destination / PurePosixPath(name)
                output.parent.mkdir(parents=True, exist_ok=True)
                digest = hashlib.sha256()
                written = 0
                with stream, output.open("xb") as target:
                    while chunk := stream.read(_COPY_CHUNK_BYTES):
                        written += len(chunk)
                        if written > declared_size:
                            raise WorktreeArchiveError("worktree archive member exceeded its size")
                        digest.update(chunk)
                        target.write(chunk)
                if written != declared_size or digest.hexdigest() != entry["sha256"]:
                    raise WorktreeArchiveError("worktree archive member failed content validation")
            if members_seen != set(expected):
                raise WorktreeArchiveError("worktree archive is missing manifest members")
    except WorktreeArchiveError:
        raise
    except (
        tarfile.TarError,
        EOFError,
        OSError,
        ValueError,
        TypeError,
        json.JSONDecodeError,
    ) as exc:
        raise WorktreeArchiveError("worktree archive is incomplete or unreadable") from exc
    assert manifest is not None
    return manifest


def _validate_manifest_header(
    manifest: dict[str, Any],
    *,
    require_sanitized: bool = False,
) -> None:
    if manifest.get("schema_version") != WORKTREE_ARCHIVE_SCHEMA_VERSION:
        raise WorktreeArchiveError("unsupported worktree archive manifest schema")
    if manifest.get("archive_format") != WORKTREE_ARCHIVE_FORMAT:
        raise WorktreeArchiveError("unsupported worktree archive format")
    if not isinstance(manifest.get("baseline_sha"), str) or not isinstance(
        manifest.get("head_sha"), str
    ):
        raise WorktreeArchiveError("worktree archive Git identities are invalid")
    entries = manifest.get("entries")
    exclusions = manifest.get("exclusions")
    redaction = manifest.get("redaction")
    if (
        not isinstance(entries, list)
        or not isinstance(exclusions, list)
        or not isinstance(redaction, dict)
    ):
        raise WorktreeArchiveError("worktree archive manifest collections are invalid")
    policy_id = manifest.get("redaction_policy_id")
    if not isinstance(policy_id, str):
        raise WorktreeArchiveError("worktree archive redaction policy is invalid")
    if require_sanitized and (
        not policy_id.strip() or redaction.get("status") not in {"clean", "redacted"}
    ):
        raise WorktreeArchiveError("worktree archive is not redaction-approved")
    for identity in (manifest["baseline_sha"], manifest["head_sha"]):
        if len(identity) not in {40, 64} or any(
            char not in "0123456789abcdef" for char in identity
        ):
            raise WorktreeArchiveError("worktree archive Git identities are invalid")
    for entry in entries:
        if not isinstance(entry, dict):
            raise WorktreeArchiveError("worktree archive entry is invalid")
        entry["path"] = _portable_path(str(entry.get("path", "")))
        if entry.get("type") not in {"file", "directory"}:
            raise WorktreeArchiveError("worktree archive entry type is invalid")
        mode = entry.get("mode")
        size = entry.get("size_bytes")
        digest = entry.get("sha256")
        if type(mode) is not int or not 0 <= mode <= 0o7777:
            raise WorktreeArchiveError("worktree archive entry mode is invalid")
        if type(size) is not int or size < 0:
            raise WorktreeArchiveError("worktree archive entry size is invalid")
        if entry["type"] == "directory" and (size != 0 or digest != ""):
            raise WorktreeArchiveError("worktree archive directory integrity is invalid")
        if entry["type"] == "file" and (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(char not in "0123456789abcdef" for char in digest)
        ):
            raise WorktreeArchiveError("worktree archive file digest is invalid")


def _import_policy_violation(entry: Mapping[str, Any]) -> str | None:
    """Re-apply capture policy to untrusted raw-archive member names."""

    parts = PurePosixPath(str(entry["path"])).parts
    if len(parts) < 2 or parts[0] not in {"worktree", "recovery"}:
        return "unexpected-namespace"
    relative = tuple(parts[1:])
    is_dir = entry["type"] == "directory"
    reason = _exclusion_reason(relative, is_dir=is_dir)
    if reason is not None:
        return reason
    for length in range(1, len(relative)):
        reason = _exclusion_reason(relative[:length], is_dir=True)
        if reason is not None:
            return reason
    return None


def sanitize_worktree_archive(
    source: Path,
    destination: Path,
    *,
    logical_path: str,
    redaction_service: iRedactionService,
) -> RedactedFile:
    """Rebuild one raw capture from per-member approved bytes.

    Text members are deterministically redacted. Credential paths, secrets in
    opaque bytes, nested containers, links, special members, and malformed
    manifests quarantine the publication before hashing or upload.
    """

    redaction_service.assert_safe_metadata(logical_path, field="artifact.logical_path")
    redaction_service.assert_safe_metadata(source.as_posix(), field="artifact.source_path")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="archetype-worktree-sanitize-") as temporary_value:
        temporary = Path(temporary_value)
        raw = temporary / "raw.tar"
        source_info = source.lstat()
        expected = _InventoryEntry(
            path=logical_path,
            type="file",
            mode=stat.S_IMODE(source_info.st_mode),
            size=source_info.st_size,
            device=source_info.st_dev,
            inode=source_info.st_ino,
            mtime_ns=source_info.st_mtime_ns,
            ctime_ns=source_info.st_ctime_ns,
        )
        if not stat.S_ISREG(source_info.st_mode) or source_info.st_nlink != 1:
            raise SecretQuarantineError(logical_path, ("unsupported-source-file",))
        try:
            _copy_stable_file(source, raw, expected)
        except WorktreeArchiveError as exc:
            raise SecretQuarantineError(logical_path, ("source-file-race",)) from exc

        extracted = temporary / "extracted"
        try:
            manifest = _extract_validated_archive(raw, extracted)
        except WorktreeArchiveError as exc:
            raise SecretQuarantineError(logical_path, ("worktree-archive-invalid",)) from exc
        redaction_service.assert_safe_metadata(
            _canonical_json(manifest),
            field="artifact.worktree_archive_manifest",
        )
        approved = temporary / "approved"
        receipts: list[tuple[str, RedactionReceipt]] = []
        sanitized_entries: list[dict[str, Any]] = []
        for entry in manifest["entries"]:
            name = str(entry["path"])
            violation = _import_policy_violation(entry)
            if violation is not None:
                raise SecretQuarantineError(
                    logical_path,
                    (f"worktree-archive-policy-{violation}",),
                )
            if entry["type"] == "directory":
                (approved / PurePosixPath(name)).mkdir(parents=True, exist_ok=True)
                sanitized_entries.append(dict(entry))
                continue
            source_member = extracted / PurePosixPath(name)
            if tarfile.is_tarfile(source_member) or zipfile.is_zipfile(source_member):
                raise SecretQuarantineError(logical_path, ("nested-archive-unsupported",))
            result = redaction_service.sanitize_file(
                source_member,
                approved / PurePosixPath(name),
                logical_path=name,
            )
            receipts.append((name, result.receipt))
            sanitized_entries.append(_entry_payload(name, "file", int(entry["mode"]), result.path))
        sanitized_entries.sort(key=lambda item: str(item["path"]))
        redacted = [receipt for _, receipt in receipts if receipt.status == "redacted"]
        manifest["entries"] = sanitized_entries
        manifest["redaction_policy_id"] = redaction_service.policy_id
        manifest["redaction"] = {
            "status": "redacted" if redacted else "clean",
            "files_scanned": len(receipts),
            "bytes_scanned": sum(receipt.scanned_bytes for _, receipt in receipts),
            "redaction_count": sum(receipt.redaction_count for _, receipt in receipts),
            "rule_ids": sorted({rule for _, receipt in receipts for rule in receipt.rule_ids}),
            "files": [
                {"path": name, **receipt.model_dump(mode="json")} for name, receipt in receipts
            ],
        }
        redaction_service.assert_safe_metadata(
            _canonical_json(manifest),
            field="artifact.worktree_archive_manifest",
        )
        _write_archive(destination, manifest, approved)
        with tempfile.TemporaryDirectory(prefix="archetype-worktree-verify-") as verify_value:
            _extract_validated_archive(
                destination,
                Path(verify_value),
                require_sanitized=True,
            )
    rule_ids = tuple(sorted({rule for _, receipt in receipts for rule in receipt.rule_ids}))
    redaction_count = sum(receipt.redaction_count for _, receipt in receipts)
    return RedactedFile(
        path=destination,
        receipt=RedactionReceipt(
            policy_id=redaction_service.policy_id,
            scope=f"artifact:{logical_path}",
            status="redacted" if redaction_count else "clean",
            scanned_bytes=sum(receipt.scanned_bytes for _, receipt in receipts),
            redaction_count=redaction_count,
            rule_ids=rule_ids,
        ),
    )


def _open_directory(parent_fd: int, name: str, *, create: bool) -> int:
    if create:
        try:
            os.mkdir(name, mode=0o700, dir_fd=parent_fd)
        except FileExistsError:
            pass
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    return os.open(name, flags, dir_fd=parent_fd)


def _parent_fd(root_fd: int, parts: tuple[str, ...]) -> int:
    current = os.dup(root_fd)
    try:
        for part in parts:
            child = _open_directory(current, part, create=True)
            os.close(current)
            current = child
        return current
    except BaseException:
        os.close(current)
        raise


def restore_worktree_archive(
    source: str | Path,
    destination: str | Path,
    *,
    expected_content_hash: str | None = None,
) -> dict[str, Any]:
    """Validate and restore an archive beneath a clean, no-follow directory."""

    archive_path = Path(source)
    content_hash = _sha256_file(archive_path)
    if expected_content_hash is not None and content_hash != expected_content_hash:
        raise WorktreeArchiveError("worktree archive content hash does not match")
    target = Path(destination)
    target.mkdir(parents=True, exist_ok=True)
    if any(target.iterdir()):
        raise WorktreeArchiveError("worktree archive restore requires a clean directory")
    with tempfile.TemporaryDirectory(prefix="archetype-worktree-restore-") as temporary_value:
        staged = Path(temporary_value)
        manifest = _extract_validated_archive(
            archive_path,
            staged,
            require_sanitized=True,
        )
        root_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        root_fd = os.open(target, root_flags)
        try:
            directory_modes: list[tuple[tuple[str, ...], int]] = []
            for entry in sorted(
                manifest["entries"],
                key=lambda value: (str(value["path"]).count("/"), str(value["path"])),
            ):
                parts = tuple(PurePosixPath(str(entry["path"])).parts)
                parent = _parent_fd(root_fd, parts[:-1])
                try:
                    if entry["type"] == "directory":
                        child = _open_directory(parent, parts[-1], create=True)
                        os.close(child)
                        directory_modes.append((parts, int(entry["mode"])))
                        continue
                    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
                    descriptor = os.open(parts[-1], flags, mode=0o600, dir_fd=parent)
                    try:
                        digest = hashlib.sha256()
                        written = 0
                        with (
                            (staged / PurePosixPath(*parts)).open("rb") as reader,
                            os.fdopen(descriptor, "wb", closefd=False) as writer,
                        ):
                            while chunk := reader.read(_COPY_CHUNK_BYTES):
                                digest.update(chunk)
                                written += len(chunk)
                                writer.write(chunk)
                        if written != entry["size_bytes"] or digest.hexdigest() != entry["sha256"]:
                            raise WorktreeArchiveError("restored worktree bytes failed validation")
                        os.fchmod(descriptor, int(entry["mode"]))
                    finally:
                        os.close(descriptor)
                finally:
                    os.close(parent)
            for parts, mode in sorted(
                directory_modes,
                key=lambda value: len(value[0]),
                reverse=True,
            ):
                parent = _parent_fd(root_fd, parts[:-1])
                try:
                    child = _open_directory(parent, parts[-1], create=False)
                    try:
                        os.fchmod(child, mode)
                    finally:
                        os.close(child)
                finally:
                    os.close(parent)
        finally:
            os.close(root_fd)
    return manifest


__all__ = [
    "WORKTREE_ARCHIVE_FORMAT",
    "WORKTREE_ARCHIVE_MANIFEST_PATH",
    "WORKTREE_ARCHIVE_SCHEMA_VERSION",
    "WorktreeArchiveError",
    "WorktreeArchiveResult",
    "capture_worktree_archive",
    "restore_worktree_archive",
    "sanitize_worktree_archive",
]
