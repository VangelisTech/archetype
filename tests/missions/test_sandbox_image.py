# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Executable contracts for the shared coding-agent image attestation."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from archetype.missions.sandboxes import (
    ProcessRequest,
    ProcessResult,
    SandboxCapabilities,
    SandboxIdentity,
    SandboxSpec,
)
from archetype.missions.sandboxes._image import (
    AGENT_HOME,
    coding_agent_environment,
    verify_coding_agent_environment,
)


@dataclass
class _ProbeSession:
    spec: SandboxSpec
    overrides: dict[str, ProcessResult]

    @property
    def identity(self) -> SandboxIdentity:
        return SandboxIdentity("fake", "probe", self.spec.environment)

    @property
    def capabilities(self) -> SandboxCapabilities:
        return SandboxCapabilities(home_directory=AGENT_HOME)

    async def exec(self, request: ProcessRequest) -> ProcessResult:
        key = request.argv[0] if request.argv[0] != "sh" else request.argv[-1]
        defaults = {
            "id": ProcessResult(request.argv, 0, stdout="agent\n"),
            'printf %s "$HOME"': ProcessResult(request.argv, 0, stdout=f"{AGENT_HOME}\n"),
            "pwd": ProcessResult(request.argv, 0, stdout="/workspace\n"),
            "codex": ProcessResult(request.argv, 0, stdout="codex-cli 0.144.6\n"),
            "tmux": ProcessResult(request.argv, 0, stdout="tmux 3.3a\n"),
            "ttyd": ProcessResult(request.argv, 0, stdout="ttyd version 1.7.7-40e79c7\n"),
            'printf %s "$ARCHETYPE_SANDBOX_ENVIRONMENT"': ProcessResult(
                request.argv,
                0,
                stdout=f"{self.spec.environment}\n",
            ),
        }
        return self.overrides.get(key, defaults[key])


def _spec() -> SandboxSpec:
    return SandboxSpec("fake", coding_agent_environment(), "/workspace/repo")


@pytest.mark.asyncio
async def test_environment_attestation_accepts_the_exact_runtime() -> None:
    spec = _spec()
    await verify_coding_agent_environment(_ProbeSession(spec, {}), spec, expected_user="agent")


@pytest.mark.asyncio
async def test_environment_attestation_reports_probe_and_identity_mismatches() -> None:
    spec = _spec()
    session = _ProbeSession(
        spec,
        {"pwd": ProcessResult(("pwd",), 7, stderr="workspace missing")},
    )
    with pytest.raises(RuntimeError, match="probe workdir failed"):
        await verify_coding_agent_environment(session, spec, expected_user="agent")

    session = _ProbeSession(
        spec,
        {"pwd": ProcessResult(("pwd",), 0, stdout="/wrong\n")},
    )
    with pytest.raises(RuntimeError, match="workdir mismatch"):
        await verify_coding_agent_environment(session, spec, expected_user="agent")

    session = _ProbeSession(
        spec,
        {"codex": ProcessResult(("codex",), 0, stdout="codex-cli 0.0.1\n")},
    )
    with pytest.raises(RuntimeError, match="Codex version mismatch"):
        await verify_coding_agent_environment(
            session,
            spec,
            expected_user="agent",
            verify_environment=False,
        )

    other = SandboxSpec("fake", "other-environment", "/workspace/repo")
    with pytest.raises(RuntimeError, match="different environment identity"):
        await verify_coding_agent_environment(
            _ProbeSession(other, {}),
            spec,
            expected_user="agent",
        )
