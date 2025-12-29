# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
