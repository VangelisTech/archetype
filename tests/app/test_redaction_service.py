# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the shared pre-durability redaction authority."""

from __future__ import annotations

import errno
import gzip
import hashlib
import io
import stat
import tarfile
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

import archetype.redaction.service as redaction_module
from archetype.redaction import (
    RedactionPolicyConfig,
    RedactionReceipt,
    RedactionService,
    SecretQuarantineError,
)
from quality.secret_corpus import SAFE_REDACTION_CORPUS, SECRET_LEAK_CORPUS

pytestmark = pytest.mark.contract("security.redaction.pre_durability")


@pytest.mark.parametrize("case", SECRET_LEAK_CORPUS, ids=lambda case: case.name)
def test_synthetic_provider_corpus_is_redacted_without_echoing_secrets(case) -> None:
    service = RedactionService()
    result = service.redact_text(case.payload, scope=f"corpus:{case.name}")

    assert result.receipt.status == "redacted"
    assert case.rule_id in result.receipt.rule_ids
    assert case.payload not in result.text
    assert "<redacted:" in result.text

    with pytest.raises(SecretQuarantineError) as error:
        service.assert_safe_metadata(case.payload, field=f"metadata:{case.name}")
    assert case.payload not in str(error.value)
    assert case.rule_id in error.value.rule_ids


@pytest.mark.parametrize("value", SAFE_REDACTION_CORPUS)
def test_placeholders_and_nonsecret_identity_are_stable(value: str) -> None:
    service = RedactionService()
    first = service.redact_text(value, scope="safe")
    second = service.redact_text(first.text, scope="safe")
    assert first.text == value
    assert first.receipt.status == "clean"
    assert second.text == value
    assert second.receipt.status == "clean"


def test_structured_events_redact_sensitive_keys_and_nested_token_shapes() -> None:
    service = RedactionService()
    token = "opaque-oauth-value-" + "Z" * 32
    github = "ghp_" + "Y" * 36
    result = service.redact_record(
        {
            "world_id": "world-1",
            "auth": {"refresh_token": token},
            "events": [{"message": f"received {github}"}],
        },
        scope="sandbox.live_event",
    )
    encoded = str(result.value)
    assert token not in encoded
    assert github not in encoded
    assert result.receipt.status == "redacted"
    assert set(result.receipt.rule_ids) == {
        "github-token",
        "structured-sensitive-field",
    }


def test_structured_event_keys_cannot_carry_secrets() -> None:
    service = RedactionService()
    secret = "ghp_" + "K" * 36
    result = service.redact_record(
        {f"provider.{secret}": "failed"},
        scope="sandbox.live_event",
    )

    assert secret not in str(result.value)
    assert "github-token" in result.receipt.rule_ids


def test_quarantine_exception_never_retains_its_tainted_scope() -> None:
    secret = "sk-proj-" + "T" * 32
    error = SecretQuarantineError(
        f"provider diagnostic containing {secret}",
        ("openai-api-key",),
    )

    assert secret not in str(error)
    assert secret not in repr(vars(error))


def test_policy_identity_is_stable_and_binds_scan_limits() -> None:
    first = RedactionService()
    same = RedactionService()
    changed = RedactionService(RedactionPolicyConfig(max_archive_members=9))
    assert first.policy_id == same.policy_id
    assert first.policy_id != changed.policy_id
    assert first.policy_id.startswith("archetype-secret-redaction-v1:")

    with pytest.raises(ValidationError, match="expanded_bytes must be"):
        RedactionPolicyConfig(
            max_archive_member_bytes=100,
            max_archive_expanded_bytes=99,
        )


@pytest.mark.parametrize(
    "payload",
    [
        {"policy_id": "", "scope": "scope", "status": "clean", "scanned_bytes": 0},
        {"policy_id": "policy", "scope": " ", "status": "clean", "scanned_bytes": 0},
        {
            "policy_id": "policy",
            "scope": "scope",
            "status": "redacted",
            "scanned_bytes": 0,
            "redaction_count": 1,
            "rule_ids": ("",),
        },
        {
            "policy_id": "policy",
            "scope": "scope",
            "status": "clean",
            "scanned_bytes": 0,
            "redaction_count": 1,
            "rule_ids": ("rule",),
        },
        {
            "policy_id": "policy",
            "scope": "scope",
            "status": "redacted",
            "scanned_bytes": 0,
            "redaction_count": 1,
            "rule_ids": (),
        },
    ],
)
def test_redaction_receipts_reject_inconsistent_or_unsafe_evidence(payload) -> None:
    with pytest.raises(ValidationError):
        RedactionReceipt.model_validate(payload)


def test_redaction_receipt_rules_and_empty_quarantine_are_canonical() -> None:
    receipt = RedactionReceipt(
        policy_id=" policy ",
        scope=" scope ",
        status="redacted",
        scanned_bytes=1,
        redaction_count=2,
        rule_ids=("z-rule", "a-rule", "z-rule"),
    )
    assert receipt.policy_id == "policy"
    assert receipt.scope == "scope"
    assert receipt.rule_ids == ("a-rule", "z-rule")
    assert "unspecified-secret-rule" in str(SecretQuarantineError("scope", ()))


def test_structured_key_collisions_are_preserved_without_leaking_keys() -> None:
    first = "ghp_" + "A" * 36
    second = "ghp_" + "B" * 36
    result = RedactionService().redact_record(
        {first: 1, second: False, "optional": None},
        scope="sandbox.live_event",
    )

    assert first not in str(result.value)
    assert second not in str(result.value)
    assert set(result.value) == {
        "<redacted:github-token>",
        "<redacted:github-token>#2",
        "optional",
    }
    assert "structured-key-collision" in result.receipt.rule_ids


def test_text_file_is_snapshotted_and_redacted_without_mutating_source(tmp_path: Path) -> None:
    service = RedactionService()
    secret = "sk-proj-" + "Q" * 32
    source = tmp_path / "session.jsonl"
    source.write_text(f'{{"api_key":"{secret}"}}\n')
    destination = tmp_path / "controlled" / "0001"

    result = service.sanitize_file(
        source,
        destination,
        logical_path="sessions/session.jsonl",
    )
    assert secret in source.read_text()
    assert secret not in result.path.read_text()
    assert "<redacted:sensitive-assignment>" in result.path.read_text()
    assert result.receipt.status == "redacted"
    assert result.receipt.scanned_bytes == source.stat().st_size
    assert result.source_digest == hashlib.sha256(source.read_bytes()).hexdigest()


@pytest.mark.parametrize(
    "logical_path",
    [
        ".codex/auth.json",
        ".claude/.credentials.json",
        ".config/opencode/auth.json",
        ".aws/credentials",
        ".git-credentials",
        "home/agent/.netrc",
        "home/agent/.ssh/id_ed25519",
    ],
)
def test_known_credential_files_are_quarantined_by_path(tmp_path: Path, logical_path: str) -> None:
    source = tmp_path / "credential"
    source.write_text("otherwise-unrecognized-credential")
    destination = tmp_path / "output"
    with pytest.raises(SecretQuarantineError, match="credential-file-path"):
        RedactionService().sanitize_file(
            source,
            destination,
            logical_path=logical_path,
        )
    assert not destination.exists()


def test_source_symlink_is_never_followed_into_a_safe_logical_path(tmp_path: Path) -> None:
    target = tmp_path / "credential"
    target.write_text("otherwise-unrecognized-credential")
    source = tmp_path / "innocent-result.txt"
    source.symlink_to(target)
    destination = tmp_path / "approved"

    with pytest.raises(SecretQuarantineError, match="unsupported-source-file"):
        RedactionService().sanitize_file(
            source,
            destination,
            logical_path="result.txt",
        )
    assert not destination.exists()


def test_missing_source_preserves_retryable_file_not_found(tmp_path: Path) -> None:
    destination = tmp_path / "approved"
    with pytest.raises(FileNotFoundError):
        RedactionService().sanitize_file(
            tmp_path / "missing.txt",
            destination,
            logical_path="result.txt",
        )
    assert not destination.exists()


def test_snapshot_open_rejects_a_symlink_race(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "result.txt"
    source.write_text("safe")

    def raced_open(*_args, **_kwargs):
        raise OSError(errno.ELOOP, "synthetic no-follow rejection")

    monkeypatch.setattr(redaction_module.os, "open", raced_open)
    with pytest.raises(SecretQuarantineError, match="unsupported-source-file"):
        RedactionService().sanitize_file(
            source,
            tmp_path / "approved",
            logical_path="result.txt",
        )


def test_snapshot_open_preserves_non_symlink_io_errors(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "result.txt"
    source.write_text("safe")

    def denied_open(*_args, **_kwargs):
        raise PermissionError(errno.EACCES, "synthetic permission denial")

    monkeypatch.setattr(redaction_module.os, "open", denied_open)
    with pytest.raises(PermissionError, match="synthetic permission denial"):
        RedactionService().sanitize_file(
            source,
            tmp_path / "approved",
            logical_path="result.txt",
        )


def test_snapshot_rejects_inode_replacement_between_inspection_and_open(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source = tmp_path / "result.txt"
    source.write_text("safe")
    real_fstat = redaction_module.os.fstat

    def changed_inode(descriptor):
        opened = real_fstat(descriptor)
        return SimpleNamespace(
            st_mode=opened.st_mode,
            st_dev=opened.st_dev,
            st_ino=opened.st_ino + 1,
        )

    monkeypatch.setattr(redaction_module.os, "fstat", changed_inode)
    with pytest.raises(SecretQuarantineError, match="source-file-race"):
        RedactionService().sanitize_file(
            source,
            tmp_path / "approved",
            logical_path="result.txt",
        )


@pytest.mark.parametrize(
    "reference",
    [
        "file:///home/agent/.codex/auth.json",
        "modal-volume://checkpoint#home/agent/.claude/.credentials.json",
        "/home/agent/.local/share/opencode/auth.json",
        "/home/agent/.config/gh/hosts.yml",
        "/workspace/.env",
    ],
)
def test_credential_source_references_are_metadata_quarantine(reference: str) -> None:
    with pytest.raises(SecretQuarantineError, match="credential-file-path"):
        RedactionService().assert_safe_metadata(
            reference,
            field="artifact_request.artifacts[0].source_ref",
        )


def test_opaque_binary_with_secret_is_quarantined_without_echo(tmp_path: Path) -> None:
    secret = "ghp_" + "R" * 36
    source = tmp_path / "opaque.bin"
    source.write_bytes(b"\x00\xffprefix" + secret.encode() + b"suffix")
    destination = tmp_path / "output"
    with pytest.raises(SecretQuarantineError) as error:
        RedactionService().sanitize_file(
            source,
            destination,
            logical_path="opaque.bin",
        )
    assert secret not in str(error.value)
    assert error.value.rule_ids == ("github-token",)
    assert not destination.exists()


@pytest.mark.parametrize("archive_kind", ["tar", "zip"])
def test_archive_members_are_scanned_and_quarantined(tmp_path: Path, archive_kind: str) -> None:
    secret = "sk-or-v1-" + "S" * 32
    archive_path = tmp_path / f"worktree.{archive_kind}"
    payload = f"OPENROUTER_API_KEY={secret}\n".encode()
    if archive_kind == "tar":
        with tarfile.open(archive_path, "w") as archive:
            info = tarfile.TarInfo("repo/.context/session.log")
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))
    else:
        with zipfile.ZipFile(archive_path, "w") as archive:
            archive.writestr("repo/.context/session.log", payload)

    destination = tmp_path / "approved"
    with pytest.raises(SecretQuarantineError) as error:
        RedactionService().sanitize_file(
            archive_path,
            destination,
            logical_path=archive_path.name,
        )
    assert secret not in str(error.value)
    assert "sensitive-assignment" in error.value.rule_ids
    assert not destination.exists()


def test_archive_scan_limits_and_nested_archives_fail_closed(tmp_path: Path) -> None:
    oversized = tmp_path / "oversized.tar"
    with tarfile.open(oversized, "w") as archive:
        info = tarfile.TarInfo("large.txt")
        info.size = 32
        archive.addfile(info, io.BytesIO(b"x" * 32))
    service = RedactionService(
        RedactionPolicyConfig(
            max_archive_member_bytes=16,
            max_archive_expanded_bytes=16,
        )
    )
    with pytest.raises(SecretQuarantineError, match="archive-scan-limit"):
        service.sanitize_file(
            oversized,
            tmp_path / "oversized-approved",
            logical_path="oversized.tar",
        )

    nested = tmp_path / "nested.zip"
    with zipfile.ZipFile(nested, "w") as archive:
        archive.writestr("inner.tar.gz", b"not-even-a-real-archive")
    with pytest.raises(SecretQuarantineError, match="nested-archive-unsupported"):
        RedactionService().sanitize_file(
            nested,
            tmp_path / "nested-approved",
            logical_path="nested.zip",
        )


@pytest.mark.parametrize("archive_kind", ["tar", "zip"])
def test_unsafe_or_secret_archive_member_names_are_quarantined(
    tmp_path: Path,
    archive_kind: str,
) -> None:
    path = tmp_path / f"unsafe.{archive_kind}"
    secret = "ghp_" + "C" * 36
    names = ("../escape.txt", f"repo/{secret}.txt")
    for index, name in enumerate(names):
        candidate = path.with_name(f"{path.stem}-{index}.{archive_kind}")
        if archive_kind == "tar":
            with tarfile.open(candidate, "w") as archive:
                info = tarfile.TarInfo(name)
                info.size = 1
                archive.addfile(info, io.BytesIO(b"x"))
        else:
            with zipfile.ZipFile(candidate, "w") as archive:
                archive.writestr(name, b"x")
        with pytest.raises(SecretQuarantineError):
            RedactionService().sanitize_file(
                candidate,
                tmp_path / f"approved-{archive_kind}-{index}",
                logical_path=candidate.name,
            )


def test_encrypted_zip_metadata_fails_before_member_open(tmp_path: Path) -> None:
    path = tmp_path / "encrypted.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("result.txt", b"safe")
    payload = bytearray(path.read_bytes())
    payload[6] |= 0x01
    central = payload.index(b"PK\x01\x02")
    payload[central + 8] |= 0x01
    path.write_bytes(payload)

    with pytest.raises(SecretQuarantineError, match="encrypted-archive"):
        RedactionService().sanitize_file(
            path,
            tmp_path / "approved",
            logical_path="encrypted.zip",
        )


@pytest.mark.parametrize(
    "metadata_field",
    ["archive_comment", "member_comment", "member_extra"],
)
def test_zip_metadata_is_scanned_before_archive_approval(
    tmp_path: Path,
    metadata_field: str,
) -> None:
    path = tmp_path / f"secret-{metadata_field}.zip"
    secret = ("ghp_" + "M" * 36).encode()
    with zipfile.ZipFile(path, "w") as archive:
        member = zipfile.ZipInfo("result.txt")
        if metadata_field == "archive_comment":
            archive.comment = secret
        elif metadata_field == "member_comment":
            member.comment = secret
        else:
            member.extra = b"\xfe\xca" + len(secret).to_bytes(2, "little") + secret
        archive.writestr(member, b"safe")

    destination = tmp_path / "approved"
    with pytest.raises(SecretQuarantineError) as error:
        RedactionService().sanitize_file(
            path,
            destination,
            logical_path=path.name,
        )
    assert error.value.rule_ids == ("github-token",)
    assert secret.decode() not in str(error.value)
    assert not destination.exists()


@pytest.mark.parametrize("archive_kind", ["tar", "zip"])
def test_archive_stream_size_must_match_member_metadata(
    tmp_path: Path,
    monkeypatch,
    archive_kind: str,
) -> None:
    path = tmp_path / f"mismatch.{archive_kind}"
    if archive_kind == "tar":
        with tarfile.open(path, "w") as archive:
            info = tarfile.TarInfo("result.txt")
            info.size = 4
            archive.addfile(info, io.BytesIO(b"safe"))
    else:
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("result.txt", b"safe")
    service = RedactionService()
    monkeypatch.setattr(service, "_scan_binary_stream", lambda *_args, **_kwargs: 0)

    with pytest.raises(SecretQuarantineError, match="archive-size-mismatch"):
        if archive_kind == "tar":
            service._scan_tar(path, scope="archive")
        else:
            service._scan_zip(path, scope="archive")


def test_unreadable_tar_member_returns_safe_quarantine(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "unreadable.tar"
    with tarfile.open(path, "w") as archive:
        info = tarfile.TarInfo("result.txt")
        info.size = 4
        archive.addfile(info, io.BytesIO(b"safe"))
    monkeypatch.setattr(tarfile.TarFile, "extractfile", lambda *_args, **_kwargs: None)

    with pytest.raises(SecretQuarantineError, match="archive-member-unreadable"):
        RedactionService()._scan_tar(path, scope="archive")


@pytest.mark.parametrize("archive_kind", ["tar", "zip"])
def test_corrupt_archive_readers_return_safe_quarantine(
    tmp_path: Path,
    archive_kind: str,
) -> None:
    path = tmp_path / f"corrupt.{archive_kind}"
    path.write_bytes(b"not an archive")
    service = RedactionService()
    with pytest.raises(SecretQuarantineError, match="archive-unreadable"):
        if archive_kind == "tar":
            service._scan_tar(path, scope="archive")
        else:
            service._scan_zip(path, scope="archive")


@pytest.mark.parametrize("archive_kind", ["tar", "zip"])
def test_archive_links_are_not_treated_as_scanned_regular_files(
    tmp_path: Path,
    archive_kind: str,
) -> None:
    path = tmp_path / f"links.{archive_kind}"
    if archive_kind == "tar":
        with tarfile.open(path, "w") as archive:
            member = tarfile.TarInfo("repo/session-link")
            member.type = tarfile.SYMTYPE
            member.linkname = ".codex/auth.json"
            archive.addfile(member)
    else:
        with zipfile.ZipFile(path, "w") as archive:
            member = zipfile.ZipInfo("repo/session-link")
            member.create_system = 3
            member.external_attr = (stat.S_IFLNK | 0o777) << 16
            archive.writestr(member, ".codex/auth.json")

    with pytest.raises(SecretQuarantineError, match="unsupported-archive-member"):
        RedactionService().sanitize_file(
            path,
            tmp_path / "approved",
            logical_path=path.name,
        )


def test_disguised_nested_and_unsupported_containers_fail_closed(tmp_path: Path) -> None:
    inner = io.BytesIO()
    with zipfile.ZipFile(inner, "w") as archive:
        archive.writestr("secret.txt", b"compressed opaque content")
    outer = tmp_path / "outer.zip"
    with zipfile.ZipFile(outer, "w") as archive:
        archive.writestr("renamed.bin", inner.getvalue())

    with pytest.raises(SecretQuarantineError, match="nested-archive-unsupported"):
        RedactionService().sanitize_file(
            outer,
            tmp_path / "outer-approved",
            logical_path="outer.zip",
        )

    compressed = tmp_path / "opaque.bin"
    compressed.write_bytes(gzip.compress(b"not semantically inspected"))
    with pytest.raises(SecretQuarantineError, match="nested-archive-unsupported"):
        RedactionService().sanitize_file(
            compressed,
            tmp_path / "compressed-approved",
            logical_path="opaque.bin",
        )


def test_streamed_archive_limit_uses_observed_bytes_not_only_headers() -> None:
    service = RedactionService()
    with pytest.raises(SecretQuarantineError, match="archive-scan-limit"):
        service._scan_binary_stream(
            io.BytesIO(b"x" * 17),
            scope="archive-member",
            max_bytes=16,
        )


def test_safe_archive_and_private_key_text_have_explicit_dispositions(tmp_path: Path) -> None:
    safe = tmp_path / "safe.tar"
    with tarfile.open(safe, "w") as archive:
        directory = tarfile.TarInfo("repo")
        directory.type = tarfile.DIRTYPE
        archive.addfile(directory)
        payload = b"validator passed\n"
        info = tarfile.TarInfo("repo/result.txt")
        info.size = len(payload)
        archive.addfile(info, io.BytesIO(payload))
    clean = RedactionService().sanitize_file(
        safe,
        tmp_path / "safe-approved",
        logical_path="safe.tar",
    )
    assert clean.receipt.status == "clean"
    assert clean.path.read_bytes() == safe.read_bytes()

    key = tmp_path / "debug.log"
    key.write_text(
        "before\n-----BEGIN " + "PRIVATE KEY-----\nabc123\n-----END " + "PRIVATE KEY-----\nafter\n"
    )
    redacted = RedactionService().sanitize_file(
        key,
        tmp_path / "key-approved",
        logical_path="debug.log",
    )
    assert redacted.receipt.rule_ids == ("private-key",)
    assert redacted.path.read_text() == "before\n<redacted:private-key>\nafter\n"

    safe_zip = tmp_path / "safe.zip"
    with zipfile.ZipFile(safe_zip, "w") as archive:
        archive.mkdir("repo/")
        archive.writestr("repo/result.txt", b"validator passed\n")
    clean_zip = RedactionService().sanitize_file(
        safe_zip,
        tmp_path / "safe-zip-approved",
        logical_path="safe.zip",
    )
    assert clean_zip.receipt.status == "clean"


def test_private_key_redaction_handles_same_line_and_trailing_end_content(tmp_path: Path) -> None:
    same_line = tmp_path / "same-line.log"
    same_line.write_text(
        "before -----BEGIN " + "PRIVATE KEY-----body-----END " + "PRIVATE KEY----- after\n"
    )
    same_result = RedactionService().sanitize_file(
        same_line,
        tmp_path / "same-line-approved",
        logical_path="same-line.log",
    )
    assert same_result.path.read_text() == "before <redacted:private-key> after\n"

    trailing = tmp_path / "trailing.log"
    trailing.write_text(
        "-----BEGIN " + "PRIVATE KEY-----\r\nbody\r\n-----END " + "PRIVATE KEY----- after\n"
    )
    trailing_result = RedactionService().sanitize_file(
        trailing,
        tmp_path / "trailing-approved",
        logical_path="trailing.log",
    )
    assert trailing_result.path.read_bytes() == b"<redacted:private-key>\r\n after\n"

    unclosed = tmp_path / "unclosed.log"
    unclosed.write_text("before -----BEGIN " + "PRIVATE KEY-----body")
    unclosed_result = RedactionService().sanitize_file(
        unclosed,
        tmp_path / "unclosed-approved",
        logical_path="unclosed.log",
    )
    assert unclosed_result.path.read_text() == "before <redacted:private-key>"


def test_empty_and_non_utf8_binary_files_have_explicit_clean_disposition(tmp_path: Path) -> None:
    for name, payload in (("empty.bin", b""), ("opaque.bin", b"\xff\xfe")):
        source = tmp_path / name
        source.write_bytes(payload)
        result = RedactionService().sanitize_file(
            source,
            tmp_path / f"{name}.approved",
            logical_path=name,
        )
        assert result.receipt.status == "clean"
        assert result.path.read_bytes() == payload


def test_uri_metadata_and_empty_scope_branches_fail_closed() -> None:
    service = RedactionService()
    with pytest.raises(SecretQuarantineError, match="uri-userinfo"):
        service.assert_safe_metadata(
            "https://agent@provider.invalid/result",
            field="artifact.source_ref",
        )
    for value in (
        "https://provider.invalid/result?page=1",
        "https://provider.invalid/result?token=",
        "https://provider.invalid/result?token={env:PROVIDER_TOKEN}",
    ):
        assert service.assert_safe_metadata(value, field="artifact.source_ref").status == "clean"
    assert not service._is_credential_path("/")
    assert service._has_container_magic(b"x" * 257 + b"ustar" + b"x")
    with pytest.raises(ValueError, match="scope must not be empty"):
        service.redact_text("safe", scope=" ")


def test_unbounded_text_line_is_quarantined(tmp_path: Path) -> None:
    service = RedactionService(RedactionPolicyConfig(max_text_line_bytes=4096))
    source = tmp_path / "one-line.json"
    source.write_text("x" * 4097)
    with pytest.raises(SecretQuarantineError, match="text-line-scan-limit"):
        service.sanitize_file(
            source,
            tmp_path / "approved",
            logical_path="one-line.json",
        )


def test_text_that_becomes_invalid_after_sampling_is_safely_quarantined(tmp_path: Path) -> None:
    source = tmp_path / "late-invalid.log"
    source.write_bytes(b"a" * (64 << 10) + b"\xff")

    with pytest.raises(SecretQuarantineError, match="text-decode-failed"):
        RedactionService().sanitize_file(
            source,
            tmp_path / "approved",
            logical_path="late-invalid.log",
        )
