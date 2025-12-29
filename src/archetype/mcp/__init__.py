# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Archetype MCP Server
====================

Model Context Protocol server exposing the Archetype service layer.

Tools:
- create_world: Create a new simulation world
- list_worlds: List all managed worlds
- run_world: Run a world for N steps
- run_parallel_worlds: Run multiple worlds in parallel
- run_monte_carlo: Run Monte Carlo simulation
- submit_command: Submit a command to a world
- get_pending_commands: Get pending commands for a world
- get_world_status: Get status of a world

Usage:
    # As a standalone server
    python -m archetype.mcp

    # Or import and run
    from archetype.mcp import create_server
    server = create_server()
"""

from archetype.mcp.server import ArchetypeMCP, create_server

__all__ = ["create_server", "ArchetypeMCP"]
