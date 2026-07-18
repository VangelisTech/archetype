# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Deterministic secret scanning and redaction before durable I/O."""

from __future__ import annotations

import errno
import hashlib
import json
import os
import re
import shutil
import stat
import tarfile
import zipfile
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import IO, cast
from urllib.parse import parse_qsl, unquote, urlparse

from pydantic import JsonValue

from archetype.app.redaction.models import (
    RedactedFile,
    RedactedRecord,
    RedactedText,
    RedactionPolicyConfig,
    RedactionReceipt,
    SecretQuarantineError,
)


@dataclass(frozen=True)
class _Rule:
    rule_id: str
    pattern: re.Pattern[str]


def _compile(pattern: str, *, verbose: bool = False) -> re.Pattern[str]:
    flags = re.IGNORECASE | (re.VERBOSE if verbose else 0)
    return re.compile(pattern, flags)


_RULES = (
    _Rule(
        "authorization-header",
        _compile(
            r"(?P<prefix>\b(?:authorization|proxy-authorization)\s*[:=]\s*"
            r"(?:bearer|basic)\s+)(?P<secret>[A-Za-z0-9._~+/=-]{8,2048})"
        ),
    ),
    _Rule(
        "signed-url-query",
        _compile(
            r"(?P<prefix>[?&](?:access[_-]?token|refresh[_-]?token|api[_-]?key|"
            r"x-amz-signature|x-goog-signature|signature|sig|token|accountkey)=)"
            r"(?P<secret>[^&#\s]{4,2048})"
        ),
    ),
    _Rule(
        "cli-secret-argument",
        _compile(
            r"(?P<prefix>--(?:api[-_]?key|access[-_]?token|refresh[-_]?token|"
            r"client[-_]?secret|token|secret|password)(?:=|\s+))"
            r"(?P<secret>[^\s\"'`;]{6,2048})"
        ),
    ),
    _Rule(
        "sensitive-assignment",
        _compile(
            r"""
            (?P<prefix>
                [\"']?
                (?:
                    access[_-]?token|refresh[_-]?token|id[_-]?token|
                    api[_-]?key|client[_-]?secret|secret[_-]?access[_-]?key|
                    secret[_-]?key|password|passwd|private[_-]?key|credential|
                    accountkey|modal(?:[_-][a-z0-9]+)*[_-]token[_-](?:id|secret)
                )
                [\"']?\s*(?:=|:)\s*[\"']?
            )
            (?P<secret>[^\"'\\\s,;}\]\[{]{6,2048})
            """,
            verbose=True,
        ),
    ),
    _Rule(
        "anthropic-api-key",
        _compile(r"(?P<secret>sk-ant-[A-Za-z0-9_-]{20,256})"),
    ),
    _Rule(
        "openrouter-api-key",
        _compile(r"(?P<secret>sk-or-v1-[A-Za-z0-9_-]{20,256})"),
    ),
    _Rule(
        "github-token",
        _compile(
            r"(?P<secret>(?:gh[pousr]_[A-Za-z0-9]{20,255}|"
            r"github_pat_[A-Za-z0-9_]{20,255}))"
        ),
    ),
    _Rule(
        "openai-api-key",
        _compile(r"(?P<secret>sk-(?:proj-|svcacct-)?[A-Za-z0-9_-]{20,256})"),
    ),
    _Rule(
        "modal-token",
        _compile(r"(?P<secret>(?:ak|as)-[A-Za-z0-9_-]{20,128})"),
    ),
    _Rule(
        "aws-access-key-id",
        _compile(r"(?P<secret>(?:AKIA|ASIA)[A-Z0-9]{16})"),
    ),
    _Rule(
        "google-api-key",
        _compile(r"(?P<secret>AIza[0-9A-Za-z_-]{35})"),
    ),
    _Rule(
        "jwt",
        _compile(
            r"(?P<secret>\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\."
            r"[A-Za-z0-9_-]{8,}\b)"
        ),
    ),
)

_PRIVATE_BLOCK = re.compile(
    r"-----BEGIN (?:RSA |EC |DSA |OPENSSH |PGP )?PRIVATE KEY(?: BLOCK)?-----"
    r".*?"
    r"-----END (?:RSA |EC |DSA |OPENSSH |PGP )?PRIVATE KEY(?: BLOCK)?-----",
    re.IGNORECASE | re.DOTALL,
)
_PRIVATE_BEGIN_TO_EOF = re.compile(
    r"-----BEGIN (?:RSA |EC |DSA |OPENSSH |PGP )?PRIVATE KEY(?: BLOCK)?-----.*\Z",
    re.IGNORECASE | re.DOTALL,
)
_PRIVATE_BEGIN = re.compile(
    r"-----BEGIN (?:RSA |EC |DSA |OPENSSH |PGP )?PRIVATE KEY(?: BLOCK)?-----",
    re.IGNORECASE,
)
_PRIVATE_END = re.compile(
    r"-----END (?:RSA |EC |DSA |OPENSSH |PGP )?PRIVATE KEY(?: BLOCK)?-----",
    re.IGNORECASE,
)

_SENSITIVE_RECORD_KEYS = frozenset(
    {
        "access_token",
        "refresh_token",
        "id_token",
        "api_key",
        "client_secret",
        "secret",
        "secret_key",
        "secret_access_key",
        "password",
        "passwd",
        "private_key",
        "credential",
        "credentials",
        "authorization",
        "proxy_authorization",
        "accountkey",
        "token",
    }
)
_SENSITIVE_QUERY_KEYS = _SENSITIVE_RECORD_KEYS | frozenset(
    {"x_amz_signature", "x_goog_signature", "signature", "sig"}
)
_CREDENTIAL_PATH_SUFFIXES = (
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
_CREDENTIAL_BASENAMES = frozenset(
    {
        ".env",
        ".env.local",
        ".env.production",
        ".env.development",
        ".git-credentials",
        ".netrc",
        ".npmrc",
        ".pypirc",
        "id_rsa",
        "id_ed25519",
        "id_ecdsa",
        "id_dsa",
    }
)
_CONTAINER_SUFFIXES = (
    ".tar",
    ".tar.gz",
    ".tgz",
    ".tar.bz2",
    ".tbz2",
    ".tar.xz",
    ".txz",
    ".zip",
    ".7z",
    ".rar",
    ".gz",
    ".bz2",
    ".xz",
    ".zst",
    ".lz4",
    ".cab",
    ".iso",
    ".cpio",
    ".ar",
    ".deb",
    ".rpm",
    ".dmg",
    ".wim",
)
_PLACEHOLDER_PREFIXES = (
    "<redacted:",
    "[redacted",
    "{env:",
    "${",
    "$env:",
    "***",
)
_CONTAINER_MAGIC_PREFIXES = (
    b"PK\x03\x04",
    b"PK\x05\x06",
    b"PK\x07\x08",
    b"\x1f\x8b",
    b"BZh",
    b"\xfd7zXZ\x00",
    b"\x28\xb5\x2f\xfd",
    b"7z\xbc\xaf\x27\x1c",
    b"Rar!\x1a\x07",
    b"\x04\x22\x4d\x18",
    b"070701",
    b"070702",
    b"070707",
    b"!<arch>\n",
    b"MSCF",
    b"MSWIM\x00\x00\x00",
)


class RedactionService:
    """One deterministic scanner shared by artifact and telemetry adapters."""

    def __init__(self, config: RedactionPolicyConfig | None = None) -> None:
        self._config = config or RedactionPolicyConfig()
        policy_material = {
            "schema_version": 1,
            "implementation_revision": 1,
            "config": self._config.model_dump(mode="json"),
            "rules": [(rule.rule_id, rule.pattern.pattern, rule.pattern.flags) for rule in _RULES],
            "private_key_rules": (
                _PRIVATE_BLOCK.pattern,
                _PRIVATE_BEGIN_TO_EOF.pattern,
                _PRIVATE_BEGIN.pattern,
                _PRIVATE_END.pattern,
            ),
            "sensitive_record_keys": sorted(_SENSITIVE_RECORD_KEYS),
            "sensitive_query_keys": sorted(_SENSITIVE_QUERY_KEYS),
            "credential_path_suffixes": _CREDENTIAL_PATH_SUFFIXES,
            "credential_basenames": sorted(_CREDENTIAL_BASENAMES),
            "container_suffixes": _CONTAINER_SUFFIXES,
            "container_magic_prefixes": [value.hex() for value in _CONTAINER_MAGIC_PREFIXES],
            "placeholder_prefixes": _PLACEHOLDER_PREFIXES,
            "nested_archive_behavior": "quarantine",
            "archive_link_behavior": "quarantine",
            "source_snapshot_behavior": "regular-file-no-follow",
        }
        digest = hashlib.sha256(
            json.dumps(
                policy_material,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode()
        ).hexdigest()
        self._policy_id = f"archetype-secret-redaction-v1:{digest}"

    @property
    def policy_id(self) -> str:
        return self._policy_id

    def redact_text(self, value: str, *, scope: str) -> RedactedText:
        self._require_scope(scope)
        sanitized, findings = self._redact_string(value)
        return RedactedText(
            text=sanitized,
            receipt=self._receipt(
                scope,
                scanned_bytes=len(value.encode("utf-8")),
                findings=findings,
            ),
        )

    def redact_record(
        self,
        value: Mapping[str, JsonValue],
        *,
        scope: str,
    ) -> RedactedRecord:
        self._require_scope(scope)
        findings: Counter[str] = Counter()

        def visit_mapping(value: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
            sanitized: dict[str, JsonValue] = {}
            for raw_key, item in value.items():
                key, nested = self._redact_string(str(raw_key))
                findings.update(nested)
                if key in sanitized:
                    findings["structured-key-collision"] += 1
                    base = key
                    index = 2
                    while key in sanitized:
                        key = f"{base}#{index}"
                        index += 1
                sanitized[key] = visit(item, str(raw_key))
            return sanitized

        def visit(item: JsonValue, key: str | None = None) -> JsonValue:
            normalized_key = self._normalized_key(key) if key is not None else ""
            if key is not None and self._is_sensitive_key(normalized_key) and item is not None:
                findings["structured-sensitive-field"] += 1
                return "<redacted:structured-sensitive-field>"
            if isinstance(item, str):
                sanitized, nested = self._redact_string(item)
                findings.update(nested)
                return sanitized
            if isinstance(item, list):
                return [visit(child) for child in item]
            if isinstance(item, dict):
                return visit_mapping(item)
            return item

        sanitized = visit_mapping(value)
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        return RedactedRecord(
            value=cast(dict[str, JsonValue], sanitized),
            receipt=self._receipt(
                scope,
                scanned_bytes=len(encoded.encode()),
                findings=findings,
            ),
        )

    def assert_safe_metadata(self, value: str, *, field: str) -> RedactionReceipt:
        self._require_scope(field)
        _, findings = self._redact_string(value)
        findings.update(self._uri_findings(value))
        if any(
            marker in field.lower()
            for marker in ("logical_path", "source_path", "source_ref", "member_name")
        ) and self._is_credential_reference(value):
            findings["credential-file-path"] += 1
        if findings:
            raise SecretQuarantineError(field, tuple(findings))
        return self._receipt(
            field,
            scanned_bytes=len(value.encode("utf-8")),
            findings=findings,
        )

    def sanitize_file(
        self,
        source: Path,
        destination: Path,
        *,
        logical_path: str,
    ) -> RedactedFile:
        scope = f"artifact:{logical_path}"
        self.assert_safe_metadata(logical_path, field="artifact.logical_path")
        self.assert_safe_metadata(source.as_posix(), field="artifact.source_path")

        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(f"{destination.name}.redacting")
        findings: Counter[str] = Counter()
        snapshot_bytes = 0
        try:
            snapshot_bytes = self._copy_snapshot(source, destination, scope=scope)
            if zipfile.is_zipfile(destination):
                self._scan_zip(destination, scope=scope)
            elif tarfile.is_tarfile(destination):
                self._scan_tar(destination, scope=scope)
            elif self._is_text_file(destination):
                try:
                    findings = self._redact_text_file(destination, temporary)
                except UnicodeError:
                    raise SecretQuarantineError(
                        scope,
                        ("text-decode-failed",),
                    ) from None
                if findings:
                    os.replace(temporary, destination)
                else:
                    temporary.unlink(missing_ok=True)
            else:
                with destination.open("rb") as stream:
                    self._scan_binary_stream(
                        stream,
                        scope=scope,
                        reject_container=True,
                    )
        except BaseException:
            temporary.unlink(missing_ok=True)
            destination.unlink(missing_ok=True)
            raise

        return RedactedFile(
            path=destination,
            receipt=self._receipt(
                scope,
                scanned_bytes=snapshot_bytes,
                findings=findings,
            ),
        )

    @staticmethod
    def _copy_snapshot(source: Path, destination: Path, *, scope: str) -> int:
        """Copy one stable regular-file handle without following a symlink."""
        try:
            original = source.lstat()
        except FileNotFoundError:
            raise
        if not stat.S_ISREG(original.st_mode):
            raise SecretQuarantineError(scope, ("unsupported-source-file",))

        flags = os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(source, flags)
        except OSError as exc:
            if exc.errno == errno.ELOOP:
                raise SecretQuarantineError(scope, ("unsupported-source-file",)) from None
            raise
        try:
            opened = os.fstat(descriptor)
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_dev != original.st_dev
                or opened.st_ino != original.st_ino
            ):
                raise SecretQuarantineError(scope, ("source-file-race",))
            with (
                os.fdopen(descriptor, "rb", closefd=False) as reader,
                destination.open("wb") as writer,
            ):
                shutil.copyfileobj(reader, writer)
        finally:
            os.close(descriptor)
        return destination.stat().st_size

    def _redact_string(self, value: str) -> tuple[str, Counter[str]]:
        findings: Counter[str] = Counter()

        def private_replacement(match: re.Match[str]) -> str:
            findings["private-key"] += 1
            return "<redacted:private-key>"

        value = _PRIVATE_BLOCK.sub(private_replacement, value)
        value = _PRIVATE_BEGIN_TO_EOF.sub(private_replacement, value)
        for rule in _RULES:

            def replacement(match: re.Match[str], *, current: _Rule = rule) -> str:
                secret = match.group("secret")
                if self._is_placeholder(secret):
                    return match.group(0)
                findings[current.rule_id] += 1
                prefix = match.groupdict().get("prefix") or ""
                return f"{prefix}<redacted:{current.rule_id}>"

            value = rule.pattern.sub(replacement, value)
        return value, findings

    def _redact_text_file(self, source: Path, destination: Path) -> Counter[str]:
        findings: Counter[str] = Counter()
        inside_private_key = False
        with (
            source.open("r", encoding="utf-8", newline="") as reader,
            destination.open("w", encoding="utf-8", newline="") as writer,
        ):
            while True:
                line = reader.readline(self._config.max_text_line_bytes + 1)
                if not line:
                    break
                if len(line.encode("utf-8")) > self._config.max_text_line_bytes:
                    raise SecretQuarantineError(
                        "artifact:text-line",
                        ("text-line-scan-limit",),
                    )

                if inside_private_key:
                    end = _PRIVATE_END.search(line)
                    if end is None:
                        continue
                    inside_private_key = False
                    remainder = line[end.end() :]
                    if remainder in {"\n", "\r", "\r\n"}:
                        continue
                    sanitized, nested = self._redact_string(remainder)
                    findings.update(nested)
                    writer.write(sanitized)
                    continue

                begin = _PRIVATE_BEGIN.search(line)
                if begin is not None:
                    before, remainder = line[: begin.start()], line[begin.end() :]
                    sanitized, nested = self._redact_string(before)
                    findings.update(nested)
                    findings["private-key"] += 1
                    writer.write(sanitized)
                    writer.write("<redacted:private-key>")
                    end = _PRIVATE_END.search(remainder)
                    if end is None:
                        inside_private_key = True
                        if line.endswith("\r\n"):
                            writer.write("\r\n")
                        elif line.endswith(("\n", "\r")):
                            writer.write(line[-1])
                    else:
                        trailing, nested = self._redact_string(remainder[end.end() :])
                        findings.update(nested)
                        writer.write(trailing)
                    continue

                sanitized, nested = self._redact_string(line)
                findings.update(nested)
                writer.write(sanitized)
        return findings

    def _scan_tar(self, path: Path, *, scope: str) -> None:
        members = expanded = 0
        try:
            with tarfile.open(path, mode="r:*") as archive:
                for member in archive:
                    members += 1
                    self._assert_archive_limits(members, 0, expanded, scope)
                    member_scope = self._archive_member_scope(scope, member.name)
                    if member.isdir():
                        continue
                    if not member.isfile():
                        raise SecretQuarantineError(
                            scope,
                            ("unsupported-archive-member",),
                        )
                    self._assert_archive_limits(
                        members,
                        member.size,
                        expanded + member.size,
                        scope,
                    )
                    stream = archive.extractfile(member)
                    if stream is None:
                        raise SecretQuarantineError(scope, ("archive-member-unreadable",))
                    remaining = self._config.max_archive_expanded_bytes - expanded
                    with stream:
                        actual = self._scan_binary_stream(
                            stream,
                            scope=member_scope,
                            max_bytes=min(self._config.max_archive_member_bytes, remaining),
                            reject_container=True,
                        )
                    if actual != member.size:
                        raise SecretQuarantineError(scope, ("archive-size-mismatch",))
                    expanded += actual
        except SecretQuarantineError:
            raise
        except (tarfile.TarError, EOFError, OSError):
            raise SecretQuarantineError(scope, ("archive-unreadable",)) from None

    def _scan_zip(self, path: Path, *, scope: str) -> None:
        members = expanded = 0
        try:
            with zipfile.ZipFile(path) as archive:
                for member in archive.infolist():
                    members += 1
                    self._assert_archive_limits(members, 0, expanded, scope)
                    member_scope = self._archive_member_scope(scope, member.filename)
                    if member.is_dir():
                        continue
                    mode = member.external_attr >> 16
                    if stat.S_ISLNK(mode) or (stat.S_IFMT(mode) and not stat.S_ISREG(mode)):
                        raise SecretQuarantineError(
                            scope,
                            ("unsupported-archive-member",),
                        )
                    if member.flag_bits & 0x1:
                        raise SecretQuarantineError(scope, ("encrypted-archive",))
                    self._assert_archive_limits(
                        members,
                        member.file_size,
                        expanded + member.file_size,
                        scope,
                    )
                    remaining = self._config.max_archive_expanded_bytes - expanded
                    with archive.open(member) as stream:
                        actual = self._scan_binary_stream(
                            stream,
                            scope=member_scope,
                            max_bytes=min(self._config.max_archive_member_bytes, remaining),
                            reject_container=True,
                        )
                    if actual != member.file_size:
                        raise SecretQuarantineError(scope, ("archive-size-mismatch",))
                    expanded += actual
        except SecretQuarantineError:
            raise
        except (
            zipfile.BadZipFile,
            zipfile.LargeZipFile,
            EOFError,
            NotImplementedError,
            OSError,
            RuntimeError,
        ):
            raise SecretQuarantineError(scope, ("archive-unreadable",)) from None

    def _archive_member_scope(self, scope: str, member_name: str) -> str:
        path = PurePosixPath(member_name.replace("\\", "/"))
        if path.is_absolute() or not path.parts or ".." in path.parts:
            raise SecretQuarantineError(scope, ("unsafe-archive-member",))
        normalized = path.as_posix()
        try:
            self.assert_safe_metadata(normalized, field="archive.member_name")
        except SecretQuarantineError as exc:
            raise SecretQuarantineError(scope, exc.rule_ids) from None
        if self._is_credential_path(normalized):
            raise SecretQuarantineError(scope, ("credential-file-path",))
        if normalized.lower().endswith(_CONTAINER_SUFFIXES):
            raise SecretQuarantineError(scope, ("nested-archive-unsupported",))
        return f"{scope}:archive-member"

    def _assert_archive_limits(
        self,
        members: int,
        member_bytes: int,
        expanded_bytes: int,
        scope: str,
    ) -> None:
        if (
            members > self._config.max_archive_members
            or member_bytes > self._config.max_archive_member_bytes
            or expanded_bytes > self._config.max_archive_expanded_bytes
        ):
            raise SecretQuarantineError(scope, ("archive-scan-limit",))

    def _scan_binary_stream(
        self,
        stream: IO[bytes],
        *,
        scope: str,
        max_bytes: int | None = None,
        reject_container: bool = False,
    ) -> int:
        overlap = ""
        overlap_bytes = 4096
        scanned = 0
        while chunk := stream.read(self._config.scan_chunk_bytes):
            scanned += len(chunk)
            if max_bytes is not None and scanned > max_bytes:
                raise SecretQuarantineError(scope, ("archive-scan-limit",))
            if reject_container and scanned == len(chunk) and self._has_container_magic(chunk):
                raise SecretQuarantineError(scope, ("nested-archive-unsupported",))
            text = overlap + chunk.decode("latin-1")
            _, findings = self._redact_string(text)
            if findings:
                raise SecretQuarantineError(scope, tuple(findings))
            overlap = text[-overlap_bytes:]
        return scanned

    @staticmethod
    def _has_container_magic(value: bytes) -> bool:
        return value.startswith(_CONTAINER_MAGIC_PREFIXES) or (
            len(value) >= 262 and value[257:262] == b"ustar"
        )

    @staticmethod
    def _is_text_file(path: Path) -> bool:
        with path.open("rb") as stream:
            sample = stream.read(64 << 10)
        if b"\x00" in sample:
            return False
        try:
            text = sample.decode("utf-8")
        except UnicodeDecodeError:
            return False
        if not text:
            return True
        controls = sum(ord(char) < 32 and char not in "\t\r\n\f\b" for char in text)
        return controls / len(text) <= 0.01

    def _uri_findings(self, value: str) -> Counter[str]:
        findings: Counter[str] = Counter()
        parsed = urlparse(value)
        if parsed.username is not None or parsed.password is not None:
            findings["uri-userinfo"] += 1
        for key, item in parse_qsl(parsed.query, keep_blank_values=True):
            if item and self._is_sensitive_key(self._normalized_key(key), query=True):
                if not self._is_placeholder(item):
                    findings["signed-url-query"] += 1
        return findings

    @staticmethod
    def _normalized_key(value: str | None) -> str:
        return re.sub(r"[^a-z0-9]+", "_", (value or "").strip().lower()).strip("_")

    @staticmethod
    def _is_sensitive_key(value: str, *, query: bool = False) -> bool:
        keys = _SENSITIVE_QUERY_KEYS if query else _SENSITIVE_RECORD_KEYS
        return value in keys or value.endswith(("_access_token", "_refresh_token", "_api_key"))

    @staticmethod
    def _is_credential_path(value: str) -> bool:
        path = PurePosixPath(value.replace("\\", "/").strip("/").lower())
        if not path.parts:
            return False
        if path.name in _CREDENTIAL_BASENAMES:
            return True
        return any(
            len(path.parts) >= len(suffix) and tuple(path.parts[-len(suffix) :]) == suffix
            for suffix in _CREDENTIAL_PATH_SUFFIXES
        )

    @classmethod
    def _is_credential_reference(cls, value: str) -> bool:
        parsed = urlparse(value)
        candidates = (value, unquote(parsed.path), unquote(parsed.fragment))
        return any(cls._is_credential_path(candidate) for candidate in candidates if candidate)

    @staticmethod
    def _is_placeholder(value: str) -> bool:
        normalized = value.strip().lower()
        return (
            not normalized
            or normalized.startswith(_PLACEHOLDER_PREFIXES)
            or normalized in {"redacted", "none", "null", "changeme"}
            or ("..." in normalized and len(normalized) <= 64)
        )

    def _receipt(
        self,
        scope: str,
        *,
        scanned_bytes: int,
        findings: Counter[str],
    ) -> RedactionReceipt:
        return RedactionReceipt(
            policy_id=self.policy_id,
            scope=scope,
            status="redacted" if findings else "clean",
            scanned_bytes=scanned_bytes,
            redaction_count=sum(findings.values()),
            rule_ids=tuple(findings),
        )

    @staticmethod
    def _require_scope(scope: str) -> None:
        if not scope.strip():
            raise ValueError("redaction scope must not be empty")
