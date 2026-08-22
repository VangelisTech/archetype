#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""One-time Codex device login persisted into the broker Volume.

Runs ``codex login --device-auth`` in a temporary Modal login sandbox with a
one-hour window (the backend default of fifteen minutes races the operator to
a browser) and persists only ``auth.json`` into the configured auth Volume.
Configuration mirrors ``examples/11_coding_agent_mission.py``.
"""

from __future__ import annotations

import asyncio
import os

from archetype.missions.sandboxes.modal import (
    MODAL_ACTIVITY_PROTOCOL_EPOCH,
    ModalSandboxBackend,
    ModalSandboxConfig,
)

LOGIN_WINDOW_SECONDS = 3600


async def main() -> None:
    backend = ModalSandboxBackend(
        ModalSandboxConfig(
            app_name=os.environ.get("CODING_AGENT_MODAL_APP", "archetype-agent-missions"),
            image_id=os.environ.get("CODING_AGENT_MODAL_IMAGE_ID", ""),
            auth_volume_name=os.environ.get("CODEX_AUTH_VOLUME", "archetype-codex-auth"),
            github_secret_name=os.environ.get("CODING_AGENT_GITHUB_SECRET", "archetype-github"),
            workspace_name=os.environ.get("CODING_AGENT_MODAL_WORKSPACE") or None,
            environment_name=os.environ.get("CODING_AGENT_MODAL_ENVIRONMENT") or None,
            operation_protocol_epoch=MODAL_ACTIVITY_PROTOCOL_EPOCH,
            login_timeout_seconds=LOGIN_WINDOW_SECONDS,
        )
    )
    await backend.login_codex()
    print("Codex device auth persisted to the broker Volume.")


if __name__ == "__main__":
    asyncio.run(main())
