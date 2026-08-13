# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Fail-closed access to the pinned coding-agent environment inventory.

``versions.toml`` is the one machine-readable inventory of executable
dependencies that can affect execution: agent CLIs, sandbox SDKs and
runtimes, collector and proxy images, and evaluation packages. Loading
validates every value against strict shape whitelists and credential deny
patterns, so a required immutable version either resolves exactly or raises
:class:`VersionPinError`; nothing ever degrades to ``latest``.
"""

from __future__ import annotations

import hashlib
import re
import tomllib
from dataclasses import dataclass
from functools import cache
from importlib import resources
from typing import Any

_RESOURCE_NAME = "versions.toml"
_SUPPORTED_SCHEMA_VERSION = 2
_SUPPORTED_HARNESSES = ("codex",)

_STATUSES = frozenset({"pinned", "planned"})
_ROLES = frozenset(
    {"agent-harness", "sandbox-sdk", "sandbox-runtime", "collector", "proxy", "evaluation"}
)
_KINDS = frozenset(
    {
        "npm-package",
        "python-package",
        "macos-installer",
        "container-image",
        "release-binary",
    }
)

_ID_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
_VERSION_RE = re.compile(r"^[0-9][0-9A-Za-z.+-]*$")
_CONSUMER_RE = re.compile(r"^#[0-9]+$")
_SOURCE_RE = re.compile(r"^https://[a-z0-9.-]+/[A-Za-z0-9._/@+-]*$")
_INTERFACE_TOKEN_RE = re.compile(r"^[A-Za-z0-9._-]+$")
_INTERFACE_EVENT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9._-]*(?:/[A-Za-z][A-Za-z0-9._-]*)*$")
_NAME_RES: dict[str, re.Pattern[str]] = {
    "npm-package": re.compile(r"^(?:@[a-z0-9][a-z0-9._-]*/)?[a-z0-9][a-z0-9._-]*$"),
    "python-package": re.compile(r"^[a-z0-9](?:[a-z0-9._-]*[a-z0-9])?$"),
    "macos-installer": re.compile(r"^[a-z0-9][a-z0-9._-]*(?:/[a-z0-9][a-z0-9._-]*)?$"),
    "container-image": re.compile(r"^[a-z0-9][a-z0-9._-]*(?:/[a-z0-9][a-z0-9._-]*)+$"),
    "release-binary": re.compile(r"^[a-z0-9][a-z0-9._-]*$"),
}
_IMMUTABLE_REF_RES: dict[str, re.Pattern[str]] = {
    "npm-package": re.compile(r"^sha512-[A-Za-z0-9+/]{86}={0,2}$"),
    "python-package": re.compile(r"^sha256:[0-9a-f]{64}$"),
    "macos-installer": re.compile(r"^sha256:[0-9a-f]{64}$"),
    "container-image": re.compile(r"^[a-z0-9.-]+(?:/[a-z0-9._-]+)+@sha256:[0-9a-f]{64}$"),
    "release-binary": re.compile(r"^sha256:[0-9a-f]{64}$"),
}

# Deny patterns mirror the redaction authority's credential rule set; the
# architecture boundary keeps app.redaction out of this family, so the
# inventory carries its own scan and tests prove corpus parity.
_CREDENTIAL_RES: tuple[re.Pattern[str], ...] = (
    re.compile(r"://[^/\s]*@"),
    re.compile(r"[?&]"),
    re.compile(r"\s"),
    re.compile(r"sk-[A-Za-z0-9_-]{8,}"),
    re.compile(r"(?:gh[pousr]_|github_pat_)[A-Za-z0-9_]{8,}"),
    re.compile(r"(?<![A-Za-z0-9])(?:ak|as)-[A-Za-z0-9_-]{20,}"),
    re.compile(r"(?:AKIA|ASIA)[A-Z0-9]{16}"),
    re.compile(r"AIza[0-9A-Za-z_-]{35}"),
    re.compile(r"xox[a-z]-[A-Za-z0-9-]{8,}"),
    re.compile(r"eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"),
    re.compile(r"-----BEGIN", re.IGNORECASE),
    re.compile(r"(?i)(?:bearer|basic)\s"),
    re.compile(r"(?i)(?:token|secret|password|passwd|credential|apikey|api_key|accountkey)="),
)

_PINNED_KEYS = frozenset(
    {"id", "status", "role", "kind", "name", "version", "source", "immutable_ref", "consumers"}
)
_HARNESS_KEYS = _PINNED_KEYS | {"harness", "harness_interfaces"}
_PLANNED_KEYS = frozenset({"id", "status", "role", "consumers"})
_INTERFACE_KEYS = frozenset(
    {"id", "invoke", "output_flags", "resume", "session_event", "session_fields"}
)


class VersionPinError(ValueError):
    """A required immutable version cannot be resolved; callers must fail closed."""


@dataclass(frozen=True)
class HarnessInterface:
    """Command, machine-output, and session contract of one pinned agent CLI."""

    interface_id: str
    invoke: tuple[str, ...]
    output_flags: tuple[str, ...]
    resume: tuple[str, ...]
    session_event: str
    session_fields: tuple[str, ...]


@dataclass(frozen=True)
class PinnedArtifact:
    """One inventory row; planned rows carry empty pin fields until resolved."""

    artifact_id: str
    status: str
    role: str
    kind: str
    name: str
    version: str
    source: str
    immutable_ref: str
    consumers: tuple[str, ...]
    harness: str = ""
    harness_interfaces: tuple[HarnessInterface, ...] = ()

    @property
    def harness_interface(self) -> HarnessInterface | None:
        """Return the supported primary interface for compatibility."""

        return self.harness_interfaces[0] if self.harness_interfaces else None


@dataclass(frozen=True)
class VersionInventory:
    """Validated inventory bound to the exact content digest it was read from."""

    schema_version: int
    digest: str
    artifacts: tuple[PinnedArtifact, ...]

    def resolve(self, artifact_id: str) -> PinnedArtifact:
        """Return one pinned artifact or raise instead of falling back."""

        for artifact in self.artifacts:
            if artifact.artifact_id == artifact_id:
                if artifact.status != "pinned":
                    raise VersionPinError(
                        f"artifact {artifact_id!r} is declared but not pinned; "
                        "refusing to resolve a floating version"
                    )
                return artifact
        raise VersionPinError(f"artifact {artifact_id!r} is not in the version inventory")

    def harness_pin(self, harness: str) -> PinnedArtifact:
        """Return the pinned CLI for one agent harness or raise."""

        matches = [artifact for artifact in self.artifacts if artifact.harness == harness]
        if len(matches) != 1:
            raise VersionPinError(
                f"agent harness {harness!r} requires exactly one pinned CLI artifact, "
                f"found {len(matches)}"
            )
        return self.resolve(matches[0].artifact_id)


def _scan_value(value: str, field: str) -> str:
    for pattern in _CREDENTIAL_RES:
        if pattern.search(value):
            raise VersionPinError(
                f"{field} matches credential deny pattern {pattern.pattern!r}; "
                "version evidence must never carry secrets"
            )
    return value


def _string(row: dict[str, Any], key: str, label: str) -> str:
    value = row.get(key)
    if not isinstance(value, str) or not value:
        raise VersionPinError(f"{label}: {key} must be a non-empty string")
    return _scan_value(value, f"{label}.{key}")


def _string_tuple(
    value: Any, label: str, pattern: re.Pattern[str], *, minimum: int = 1
) -> tuple[str, ...]:
    if not isinstance(value, list) or len(value) < minimum:
        raise VersionPinError(f"{label} must be an array with at least {minimum} item(s)")
    items: list[str] = []
    for item in value:
        if not isinstance(item, str) or not pattern.fullmatch(item):
            raise VersionPinError(f"{label} item {item!r} does not match {pattern.pattern!r}")
        items.append(_scan_value(item, label))
    return tuple(items)


def _harness_interface(value: Any, label: str) -> HarnessInterface:
    if not isinstance(value, dict) or set(value) != _INTERFACE_KEYS:
        raise VersionPinError(f"{label} requires exactly the keys {sorted(_INTERFACE_KEYS)}")
    interface_id = value["id"]
    if not isinstance(interface_id, str) or not _INTERFACE_TOKEN_RE.fullmatch(interface_id):
        raise VersionPinError(f"{label}.id must be an interface token")
    session_event = value["session_event"]
    if not isinstance(session_event, str) or (
        session_event and not _INTERFACE_EVENT_RE.fullmatch(session_event)
    ):
        raise VersionPinError(
            f"{label}.session_event must be a JSON-RPC method/event token or empty"
        )
    token = _INTERFACE_TOKEN_RE
    return HarnessInterface(
        interface_id=_scan_value(interface_id, f"{label}.id"),
        invoke=_string_tuple(value["invoke"], f"{label}.invoke", token),
        output_flags=_string_tuple(value["output_flags"], f"{label}.output_flags", token),
        resume=_string_tuple(value["resume"], f"{label}.resume", _INTERFACE_EVENT_RE),
        session_event=_scan_value(session_event, f"{label}.session_event"),
        session_fields=_string_tuple(value["session_fields"], f"{label}.session_fields", token),
    )


def _parse_artifact(row: Any, index: int) -> PinnedArtifact:
    label = f"artifact[{index}]"
    if not isinstance(row, dict):
        raise VersionPinError(f"{label} must be a table")
    artifact_id = _string(row, "id", label)
    if not _ID_RE.fullmatch(artifact_id):
        raise VersionPinError(f"{label}: invalid artifact id {artifact_id!r}")
    label = artifact_id
    status = _string(row, "status", label)
    if status not in _STATUSES:
        raise VersionPinError(f"{label}: status must be one of {sorted(_STATUSES)}")
    role = _string(row, "role", label)
    if role not in _ROLES:
        raise VersionPinError(f"{label}: role must be one of {sorted(_ROLES)}")
    consumers = _string_tuple(row.get("consumers"), f"{label}.consumers", _CONSUMER_RE)

    if status == "planned":
        if set(row) != _PLANNED_KEYS:
            raise VersionPinError(
                f"{label}: planned artifacts declare only {sorted(_PLANNED_KEYS)}"
            )
        return PinnedArtifact(
            artifact_id=artifact_id,
            status=status,
            role=role,
            kind="",
            name="",
            version="",
            source="",
            immutable_ref="",
            consumers=consumers,
        )

    expected_keys = _HARNESS_KEYS if role == "agent-harness" else _PINNED_KEYS
    if set(row) != expected_keys:
        raise VersionPinError(f"{label}: pinned artifacts declare exactly {sorted(expected_keys)}")

    kind = _string(row, "kind", label)
    if kind not in _KINDS:
        raise VersionPinError(f"{label}: kind must be one of {sorted(_KINDS)}")
    name = _string(row, "name", label)
    if not _NAME_RES[kind].fullmatch(name):
        raise VersionPinError(f"{label}: name {name!r} is not a valid {kind} name")
    version = _string(row, "version", label)
    if not _VERSION_RE.fullmatch(version):
        raise VersionPinError(
            f"{label}: version {version!r} must be exact; ranges and floating tags are refused"
        )
    source = _string(row, "source", label)
    if not _SOURCE_RE.fullmatch(source):
        raise VersionPinError(f"{label}: source {source!r} must be a credential-free https URL")
    immutable_ref = _string(row, "immutable_ref", label)
    if not _IMMUTABLE_REF_RES[kind].fullmatch(immutable_ref):
        raise VersionPinError(
            f"{label}: immutable_ref {immutable_ref!r} is not an immutable {kind} digest"
        )

    harness = ""
    interfaces: tuple[HarnessInterface, ...] = ()
    if role == "agent-harness":
        harness = _string(row, "harness", label)
        if harness not in _SUPPORTED_HARNESSES:
            raise VersionPinError(f"{label}: unknown agent harness {harness!r}")
        raw_interfaces = row["harness_interfaces"]
        if not isinstance(raw_interfaces, list) or not raw_interfaces:
            raise VersionPinError(f"{label}.harness_interfaces requires at least one row")
        interfaces = tuple(
            _harness_interface(value, f"{label}.harness_interfaces[{interface_index}]")
            for interface_index, value in enumerate(raw_interfaces)
        )
        interface_ids = [interface.interface_id for interface in interfaces]
        if len(interface_ids) != len(set(interface_ids)):
            raise VersionPinError(f"{label}.harness_interfaces contains duplicate ids")

    return PinnedArtifact(
        artifact_id=artifact_id,
        status=status,
        role=role,
        kind=kind,
        name=name,
        version=version,
        source=source,
        immutable_ref=immutable_ref,
        consumers=consumers,
        harness=harness,
        harness_interfaces=interfaces,
    )


def parse_version_inventory(data: bytes) -> VersionInventory:
    """Validate raw inventory bytes and bind them to their content digest."""

    try:
        payload = tomllib.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise VersionPinError(f"version inventory is not valid TOML: {exc}") from exc
    if set(payload) != {"schema_version", "artifact"}:
        raise VersionPinError(
            "version inventory requires exactly the schema_version and artifact keys"
        )
    if payload["schema_version"] != _SUPPORTED_SCHEMA_VERSION:
        raise VersionPinError(
            f"unsupported version inventory schema: {payload['schema_version']!r}"
        )
    rows = payload["artifact"]
    if not isinstance(rows, list) or not rows:
        raise VersionPinError("version inventory requires at least one [[artifact]] row")

    artifacts = tuple(_parse_artifact(row, index) for index, row in enumerate(rows))
    seen: set[str] = set()
    for artifact in artifacts:
        if artifact.artifact_id in seen:
            raise VersionPinError(f"duplicate artifact id {artifact.artifact_id!r}")
        seen.add(artifact.artifact_id)
    for harness in _SUPPORTED_HARNESSES:
        pins = [artifact for artifact in artifacts if artifact.harness == harness]
        if len(pins) != 1:
            raise VersionPinError(
                f"agent harness {harness!r} requires exactly one pinned CLI, found {len(pins)}"
            )
    return VersionInventory(
        schema_version=_SUPPORTED_SCHEMA_VERSION,
        digest=f"sha256:{hashlib.sha256(data).hexdigest()}",
        artifacts=artifacts,
    )


@cache
def load_version_inventory() -> VersionInventory:
    """Load and validate the packaged inventory; missing data fails closed."""

    resource = resources.files(__package__).joinpath(_RESOURCE_NAME)
    try:
        data = resource.read_bytes()
    except (FileNotFoundError, OSError) as exc:
        raise VersionPinError(
            f"packaged version inventory {_RESOURCE_NAME!r} is unavailable: {exc}"
        ) from exc
    return parse_version_inventory(data)
