# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Shared coding-agent image recipe used by local sandbox adapters."""

from __future__ import annotations

import base64
import hashlib
from pathlib import PurePosixPath

from archetype.missions.sandboxes.contracts import ProcessRequest, SandboxSession, SandboxSpec
from archetype.missions.sandboxes.versions import load_version_inventory

AGENT_USER = "agent"
AGENT_HOME = "/home/agent"
CODEX_HOME = f"{AGENT_HOME}/.codex"
WORKSPACE_ROOT = "/workspace"
BASE_IMAGE_REF = load_version_inventory().resolve("coding-agent-base-image").immutable_ref
_APT_PACKAGES = "ca-certificates curl git make nodejs npm openssh-client"


def codex_package() -> str:
    """Return the exact npm package reference admitted by the inventory."""

    pin = load_version_inventory().harness_pin("codex")
    return f"{pin.name}@{pin.version}"


def coding_agent_environment() -> str:
    """Return the content identity attested by the shared runtime image."""

    pin = load_version_inventory().harness_pin("codex")
    material = "\n".join(
        (
            BASE_IMAGE_REF,
            _APT_PACKAGES,
            pin.name,
            pin.version,
            pin.source,
            pin.immutable_ref,
            f"user={AGENT_USER}",
            f"home={AGENT_HOME}",
            f"workdir={WORKSPACE_ROOT}",
        )
    )
    return f"archetype-agent://sha256:{hashlib.sha256(material.encode()).hexdigest()}"


def _codex_sha512_hex() -> str:
    integrity = load_version_inventory().harness_pin("codex").immutable_ref
    prefix = "sha512-"
    if not integrity.startswith(prefix):
        raise ValueError("Codex inventory integrity must use sha512")
    return base64.b64decode(integrity.removeprefix(prefix), validate=True).hex()


def codex_install_command() -> str:
    """Return a shell command that verifies and installs the pinned CLI artifact."""

    pin = load_version_inventory().harness_pin("codex")
    return (
        f"curl --fail --location --output /tmp/codex.tgz {pin.source} "
        f"&& printf '%s  %s\\n' {_codex_sha512_hex()} /tmp/codex.tgz "
        "| sha512sum --check --strict "
        "&& npm install --global /tmp/codex.tgz "
        "&& rm -f /tmp/codex.tgz"
    )


def coding_agent_containerfile() -> str:
    """Build one provider-neutral Linux userspace for Codex missions."""

    environment = coding_agent_environment()
    return f"""\
FROM {BASE_IMAGE_REF}
RUN apt-get update \\
    && apt-get install -y --no-install-recommends {_APT_PACKAGES} \\
    && rm -rf /var/lib/apt/lists/*
# {codex_package()}
RUN {codex_install_command()}
RUN useradd --create-home --uid 1000 {AGENT_USER} \\
    && mkdir -p {WORKSPACE_ROOT} \\
    && chown {AGENT_USER}:{AGENT_USER} {WORKSPACE_ROOT}
ENV HOME={AGENT_HOME}
ENV ARCHETYPE_SANDBOX_ENVIRONMENT={environment}
USER {AGENT_USER}
WORKDIR {WORKSPACE_ROOT}
"""


def local_image_name(prefix: str) -> str:
    """Return a cache key bound to the complete local image recipe."""

    digest = coding_agent_environment().rsplit(":", 1)[-1][:16]
    return f"{prefix}:codex-{digest}"


async def verify_coding_agent_environment(
    session: SandboxSession,
    spec: SandboxSpec,
    *,
    expected_user: str,
    verify_environment: bool = True,
) -> None:
    """Fail closed when the running container differs from its declared recipe."""

    if session.identity.environment != spec.environment:
        raise RuntimeError("sandbox reported a different environment identity")
    expected = {
        "user": expected_user,
        "home": session.capabilities.home_directory,
        "workdir": str(PurePosixPath(spec.workdir).parent),
        "environment": spec.environment,
    }
    requests = {
        "user": ProcessRequest(("id", "-un"), timeout_seconds=60),
        "home": ProcessRequest(("sh", "-c", 'printf %s "$HOME"'), timeout_seconds=60),
        "workdir": ProcessRequest(
            ("pwd",),
            workdir=expected["workdir"],
            timeout_seconds=60,
        ),
        "codex": ProcessRequest(("codex", "--version"), timeout_seconds=60),
    }
    if verify_environment:
        requests["environment"] = ProcessRequest(
            ("sh", "-c", 'printf %s "$ARCHETYPE_SANDBOX_ENVIRONMENT"'),
            timeout_seconds=60,
        )
    observed: dict[str, str] = {}
    for name, request in requests.items():
        result = await session.exec(request)
        if result.returncode != 0:
            detail = (result.stderr or result.stdout)[-1000:]
            raise RuntimeError(
                f"sandbox environment probe {name} failed with "
                f"exit code {result.returncode}: {detail}"
            )
        observed[name] = result.stdout.strip()
    for name in ("user", "home", "workdir", *(("environment",) if verify_environment else ())):
        if observed[name] != expected[name]:
            raise RuntimeError(
                f"sandbox {name} mismatch: expected {expected[name]!r}, observed {observed[name]!r}"
            )
    expected_codex = load_version_inventory().harness_pin("codex").version
    if expected_codex not in observed["codex"].split():
        raise RuntimeError(
            f"sandbox Codex version mismatch: expected {expected_codex!r}, "
            f"observed {observed['codex']!r}"
        )


__all__ = [
    "AGENT_HOME",
    "AGENT_USER",
    "BASE_IMAGE_REF",
    "CODEX_HOME",
    "WORKSPACE_ROOT",
    "coding_agent_environment",
    "coding_agent_containerfile",
    "codex_install_command",
    "codex_package",
    "local_image_name",
    "verify_coding_agent_environment",
]
