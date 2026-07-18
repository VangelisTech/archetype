# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the shared pre-durability redaction authority."""

from __future__ import annotations

import gzip
import io
import stat
import tarfile
import zipfile
from pathlib import Path

import pytest
from pydantic import ValidationError

from archetype.app.redaction import (
    RedactionPolicyConfig,
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
