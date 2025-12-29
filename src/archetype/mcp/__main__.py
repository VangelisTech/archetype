# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Entry point for running the Archetype MCP server.

Usage:
    python -m archetype.mcp
"""

import asyncio

from archetype.mcp.server import main

if __name__ == "__main__":
    asyncio.run(main())
