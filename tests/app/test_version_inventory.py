# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Pinned execution-environment inventory: fail-closed loading, harness
command/JSON/session compatibility, and secret-free effective-version
evidence. Bumping a pinned CLI must re-affirm its recorded interface here."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from importlib import resources
from typing import Any, get_args

import pytest

from archetype.app.sandboxes import CodingAgentSandboxClient
from archetype.app.sandboxes.models import AgentHarness, CommandResult
from archetype.app.sandboxes.versions import (
    VersionPinError,
    load_version_inventory,
    parse_version_inventory,
)
from quality.secret_corpus import SECRET_LEAK_CORPUS

pytestmark = pytest.mark.contract("sandboxes.environment.pinned")

_INVENTORY_BYTES = resources.files("archetype.app.sandboxes").joinpath("versions.toml").read_bytes()
_EVIDENCE_KEYS = {
    "schema_version",
    "inventory_digest",
    "harness",
    "model",
    "provider",
    "configuration_digest",
    "runtime",
}
_HARNESS_EVIDENCE_KEYS = {
    "artifact_id",
    "name",
    "version",
    "immutable_ref",
    "observed_version",
    "observed_error",
}


@dataclass(frozen=True)
class _ProbeSpec:
    repo_url: str = "https://example.test/repo.git"
    branch: str = "agent/fix"
    base_ref: str = "main"
    harness: str = "codex"
    model: str = "test-model"
    opencode_base_url: str = "https://endpoint.example/v1"
    opencode_provider_id: str = "test-endpoint"
    opencode_wire_api: str = "chat-completions"
    opencode_header_env: Mapping[str, str] = field(
        default_factory=lambda: {"Modal-Key": "MODAL_ENDPOINT_TOKEN_ID"}
    )
    workspace: str = "/workspace/repo"
    agent_timeout_seconds: int = 60
    snapshot_timeout_seconds: int = 30
    snapshot_ttl_seconds: int | None = None
    snapshot_after_attempt: bool = False
    capture_filesystem_manifests: bool = False
    push: bool = False
    git_author_name: str = "Agent"
    git_author_email: str = "agent@example.test"


@dataclass
class _ProbeClient(CodingAgentSandboxClient[_ProbeSpec]):
    spec: _ProbeSpec
    _sandbox: object = field(default_factory=object)
    _agent_secret: object | None = None
    exec_calls: list[tuple[tuple[str, ...], dict[str, Any]]] = field(default_factory=list)
    agent_argv: tuple[str, ...] = ()
    files: dict[str, str] = field(default_factory=dict)
    version_output: str = ""
    version_returncode: int = 0

    @property
    def sandbox_id(self) -> str:
        return "probe-sandbox"

    async def close(self) -> None:
        self._closed = True

    async def _exec(
        self,
        *args: str,
        workdir: str | None = None,
        timeout: int | None = None,
        secrets: Sequence[Any] = (),
        env: dict[str, str] | None = None,
    ) -> CommandResult:
        self.exec_calls.append(
            (args, {"workdir": workdir, "timeout": timeout, "secrets": tuple(secrets), "env": env})
        )
        if args[1:] == ("--version",):
            return CommandResult(
                args,
                self.version_returncode,
                self.version_output,
                "probe failed" if self.version_returncode else "",
            )
        return CommandResult(args, 0, "", "")

    async def _exec_agent(
        self,
        *args: str,
        workdir: str | None = None,
        timeout: int | None = None,
        secrets: Sequence[Any] = (),
        env: dict[str, str] | None = None,
    ) -> CommandResult:
        self.agent_argv = args
        return CommandResult(args, 0, "", "")

    async def _write_text(self, path: str, value: str) -> None:
        self.files[path] = value

    async def _snapshot_if_configured(self, checkpoint_key: str = "") -> str:
        return ""

    def _checkpoint_provider(self) -> str:
        return "probe"

    def _sandbox_uri(self, path: str) -> str:
        return f"probe://{path}"


def _mutated(old: str, new: str) -> bytes:
    text = _INVENTORY_BYTES.decode("utf-8")
    assert old in text, f"inventory no longer contains {old!r}; update this test"
    return text.replace(old, new, 1).encode("utf-8")


def test_packaged_inventory_loads_and_binds_its_content_digest() -> None:
    inventory = load_version_inventory()
    assert inventory.schema_version == 1
    assert inventory.digest == f"sha256:{hashlib.sha256(_INVENTORY_BYTES).hexdigest()}"
    assert inventory is load_version_inventory()


def test_every_supported_harness_has_exactly_one_pinned_cli() -> None:
    inventory = load_version_inventory()
    for harness in get_args(AgentHarness):
        pin = inventory.harness_pin(harness)
        assert pin.status == "pinned"
        assert pin.kind == "npm-package"
        assert pin.version and pin.immutable_ref.startswith("sha512-")
        assert pin.harness_interface is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("harness", sorted(get_args(AgentHarness)))
async def test_harness_command_json_and_session_compatibility(harness: str) -> None:
    """The kernel's invocation and session parsing must match the pin's
    recorded interface; a version bump re-affirms this record explicitly."""

    interface = load_version_inventory().harness_pin(harness).harness_interface
    assert interface is not None
    client = _ProbeClient(_ProbeSpec(harness=harness))

    await client._run_agent("complete the task gate", session_id="")
    fresh = client.agent_argv
    assert fresh[: len(interface.invoke)] == interface.invoke
    assert " ".join(interface.output_flags) in " ".join(fresh)
    assert fresh[-1] == "complete the task gate"

    await client._run_agent("complete the task gate", session_id="session-9")
    resumed = client.agent_argv
    assert " ".join(interface.resume) in " ".join(resumed)
    assert "session-9" in resumed

    if interface.session_event:
        line = {"type": interface.session_event, interface.session_fields[0]: "session-1"}
        assert client._session_id(json.dumps(line)) == "session-1"
    else:
        for session_field in interface.session_fields:
            assert client._session_id(json.dumps({session_field: "session-1"})) == "session-1"


@pytest.mark.asyncio
async def test_environment_evidence_is_pinned_secret_free_and_probe_backed() -> None:
    inventory = load_version_inventory()
    client = _ProbeClient(_ProbeSpec(), version_output="codex-cli 0.144.6\n")

    evidence = await client._environment_evidence()

    pin = inventory.harness_pin("codex")
    assert set(evidence) == _EVIDENCE_KEYS
    assert set(evidence["harness"]) == _HARNESS_EVIDENCE_KEYS
    assert evidence["inventory_digest"] == inventory.digest
    assert evidence["harness"]["artifact_id"] == pin.artifact_id
    assert evidence["harness"]["version"] == pin.version
    assert evidence["harness"]["immutable_ref"] == pin.immutable_ref
    assert evidence["harness"]["observed_version"] == "codex-cli 0.144.6"
    assert evidence["harness"]["observed_error"] == ""
    assert evidence["provider"] == "probe"
    assert (
        evidence["configuration_digest"]
        == client.provider_execution_capabilities.request_fingerprint
    )
    assert evidence["runtime"] == {}

    probe_call = next(call for call in client.exec_calls if call[0][1:] == ("--version",))
    assert probe_call[0][0] == "codex"
    assert probe_call[1]["secrets"] == ()

    serialized = json.dumps(evidence)
    for case in SECRET_LEAK_CORPUS:
        assert case.payload not in serialized, case.name


@pytest.mark.asyncio
async def test_environment_probe_failure_is_recorded_not_fatal() -> None:
    client = _ProbeClient(_ProbeSpec(), version_returncode=1)

    evidence = await client._environment_evidence()

    assert evidence["harness"]["observed_version"] == ""
    assert evidence["harness"]["observed_error"] == "probe failed"


def test_unknown_and_planned_artifacts_fail_closed() -> None:
    inventory = load_version_inventory()
    with pytest.raises(VersionPinError, match="not in the version inventory"):
        inventory.resolve("does-not-exist")
    planned = [artifact for artifact in inventory.artifacts if artifact.status == "planned"]
    assert planned, "the inventory declares at least one planned obligation"
    for artifact in planned:
        assert not artifact.version and not artifact.immutable_ref
        with pytest.raises(VersionPinError, match="not pinned"):
            inventory.resolve(artifact.artifact_id)


def test_unpinned_harness_fails_closed() -> None:
    inventory = load_version_inventory()
    with pytest.raises(VersionPinError, match="exactly one pinned CLI"):
        inventory.harness_pin("unpinned-harness")


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ('version = "0.144.6"', 'version = "latest"'),
        ('version = "0.144.6"', 'version = ">=0.144.6"'),
        ('version = "0.144.6"', 'version = "^0.144.6"'),
        (
            'source = "https://registry.npmjs.org/opencode-ai/-/opencode-ai-1.18.3.tgz"',
            'source = "http://registry.npmjs.org/opencode-ai/-/opencode-ai-1.18.3.tgz"',
        ),
        (
            'source = "https://registry.npmjs.org/opencode-ai/-/opencode-ai-1.18.3.tgz"',
            'source = "https://registry.npmjs.org/opencode-ai.tgz?signature=abcdef123456"',
        ),
        ('immutable_ref = "sha256:7508a44f', 'immutable_ref = "1.5.2-'),
        ('harness = "opencode"', 'harness = "codex"'),
        ('id = "opencode-cli"', 'id = "codex-cli"'),
        ("schema_version = 1", "schema_version = 2"),
    ],
)
def test_inventory_rejects_floating_or_inconsistent_pins(old: str, new: str) -> None:
    with pytest.raises(VersionPinError):
        parse_version_inventory(_mutated(old, new))


@pytest.mark.parametrize("case", SECRET_LEAK_CORPUS, ids=lambda case: case.name)
def test_inventory_rejects_credential_shaped_values(case: Any) -> None:
    """No secret or private registry credential can enter version evidence:
    a corpus payload in any pin field must fail the load outright."""

    target = 'source = "https://registry.npmjs.org/@openai/codex/-/codex-0.144.6.tgz"'
    with pytest.raises(VersionPinError):
        parse_version_inventory(_mutated(target, f"source = {json.dumps(case.payload)}"))
    with pytest.raises(VersionPinError):
        parse_version_inventory(
            _mutated('name = "@openai/codex"', f"name = {json.dumps(case.payload)}")
        )
