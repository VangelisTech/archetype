# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Archetype's native agent-facing Mission MCP server (issue #810).

A thin typed stdio adapter over the supported MissionRun REST contract
(issue #809). Archetype remains mission and policy authority; this process
is replaceable transport. Base URL, credential, TLS policy, and limits come
exclusively from trusted host configuration (:mod:`archetype.missions.mcp.config`);
tool arguments carry domain inputs and opaque ids only.
"""

from archetype.missions.mcp.config import McpHostConfig, McpHostConfigError
from archetype.missions.mcp.server import MissionMcpServer, main

__all__ = [
    "McpHostConfig",
    "McpHostConfigError",
    "MissionMcpServer",
    "main",
]
