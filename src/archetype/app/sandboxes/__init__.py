# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Isolated execution and provider-checkpoint lifecycle."""

from archetype.app.sandboxes.apple_container import (
    AppleContainerSandboxBackend,
    AppleContainerSandboxClient,
    AppleContainerSandboxSpec,
)
from archetype.app.sandboxes.interfaces import iSandboxBackend, iSandboxService, iSandboxSession
from archetype.app.sandboxes.modal import (
    AgentAuthMode,
    AgentHarness,
    CodingAgentSandboxClient,
    ModalArtifactSourceResolver,
    ModalSandboxBackend,
    ModalSandboxClient,
    ModalSandboxSpec,
    OpenCodeWireAPI,
    ValidatorSpec,
)
from archetype.app.sandboxes.service import SandboxService

__all__ = [
    "AgentAuthMode",
    "AgentHarness",
    "AppleContainerSandboxBackend",
    "AppleContainerSandboxClient",
    "AppleContainerSandboxSpec",
    "CodingAgentSandboxClient",
    "ModalArtifactSourceResolver",
    "ModalSandboxBackend",
    "ModalSandboxClient",
    "ModalSandboxSpec",
    "OpenCodeWireAPI",
    "SandboxService",
    "ValidatorSpec",
    "iSandboxBackend",
    "iSandboxService",
    "iSandboxSession",
]
