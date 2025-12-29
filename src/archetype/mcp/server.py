# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Archetype MCP Server Implementation
===================================

Exposes the Archetype service layer via Model Context Protocol.
"""

import asyncio
import json
from dataclasses import dataclass
from typing import Any

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

from archetype.app import (
    ServiceContainer,
)
from archetype.core.aio import AsyncSystem
from archetype.core.config import CacheConfig, RunConfig, StorageConfig, WorldConfig


@dataclass
class ArchetypeMCP:
    """
    MCP server state container.

    Holds the service container and provides access to all services.
    """

    container: ServiceContainer

    @property
    def simulation(self):
        return self.container.simulation_service

    @property
    def command(self):
        return self.container.command_service

    @property
    def world(self):
        return self.container.world_service

    @property
    def broker(self):
        return self.container.broker

    @property
    def orchestrator(self):
        return self.container.orchestrator


def create_server(
    storage_uri: str = ".archetype_mcp_data",
    namespace: str = "mcp_simulations",
) -> Server:
    """
    Create and configure the MCP server with all Archetype tools.

    Args:
        storage_uri: Path for storage backend
        namespace: Storage namespace

    Returns:
        Configured MCP Server instance
    """
    server = Server("archetype")

    # Initialize state (lazy - created on first tool call)
    state: ArchetypeMCP | None = None

    def get_state() -> ArchetypeMCP:
        nonlocal state
        if state is None:
            storage_config = StorageConfig(uri=storage_uri, namespace=namespace)
            cache_config = CacheConfig()
            container = ServiceContainer(
                storage_config=storage_config,
                cache_config=cache_config,
            )
            state = ArchetypeMCP(container=container)
        return state

    # =========================================================================
    # Tool Definitions
    # =========================================================================

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        return [
            Tool(
                name="create_world",
                description="Create a new simulation world",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "description": "Human-readable name for the world",
                        }
                    },
                    "required": ["name"],
                },
            ),
            Tool(
                name="list_worlds",
                description="List all managed simulation worlds",
                inputSchema={"type": "object", "properties": {}},
            ),
            Tool(
                name="get_world_status",
                description="Get the current status of a world",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "world_name": {"type": "string", "description": "Name of the world"}
                    },
                    "required": ["world_name"],
                },
            ),
            Tool(
                name="run_world",
                description="Run a world for a specified number of steps",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "world_name": {"type": "string", "description": "Name of the world to run"},
                        "steps": {
                            "type": "integer",
                            "description": "Number of simulation steps",
                            "default": 1,
                        },
                    },
                    "required": ["world_name"],
                },
            ),
            Tool(
                name="run_parallel_worlds",
                description="Create and run multiple worlds in parallel",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "num_worlds": {
                            "type": "integer",
                            "description": "Number of worlds to create and run",
                        },
                        "steps": {
                            "type": "integer",
                            "description": "Steps per world",
                            "default": 100,
                        },
                    },
                    "required": ["num_worlds"],
                },
            ),
            Tool(
                name="run_monte_carlo",
                description="Run Monte Carlo simulation with multiple seeded trials",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "num_trials": {"type": "integer", "description": "Number of trials to run"},
                        "steps": {
                            "type": "integer",
                            "description": "Steps per trial",
                            "default": 100,
                        },
                        "seed": {
                            "type": "integer",
                            "description": "Master random seed for reproducibility",
                        },
                    },
                    "required": ["num_trials"],
                },
            ),
            Tool(
                name="submit_command",
                description="Submit a command to a world (spawn entity, add component, etc.)",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "world_name": {"type": "string", "description": "Target world name"},
                        "operation": {
                            "type": "string",
                            "description": "Operation type",
                            "enum": [
                                "create_entity",
                                "delete_entity",
                                "add_component",
                                "remove_component",
                                "create_world",
                                "run_rollout",
                                "query_world",
                            ],
                        },
                        "payload": {
                            "type": "object",
                            "description": "Command payload (operation-specific)",
                        },
                        "priority": {
                            "type": "integer",
                            "description": "Command priority (lower = higher priority)",
                            "default": 0,
                        },
                    },
                    "required": ["world_name", "operation", "payload"],
                },
            ),
            Tool(
                name="get_pending_commands",
                description="Get pending commands for a world",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "world_name": {"type": "string", "description": "World name"},
                        "limit": {
                            "type": "integer",
                            "description": "Max commands to return",
                            "default": 10,
                        },
                    },
                    "required": ["world_name"],
                },
            ),
            Tool(
                name="get_command_history",
                description="Get command history for a world",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "world_name": {"type": "string", "description": "World name"},
                        "limit": {
                            "type": "integer",
                            "description": "Max commands to return",
                            "default": 50,
                        },
                    },
                    "required": ["world_name"],
                },
            ),
        ]

    # =========================================================================
    # Tool Implementations
    # =========================================================================

    @server.call_tool()
    async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
        s = get_state()

        try:
            if name == "create_world":
                world = await _create_world(s, arguments["name"])
                return [
                    TextContent(
                        type="text",
                        text=json.dumps(
                            {
                                "status": "created",
                                "world_id": str(world.world_id),
                                "name": arguments["name"],
                            },
                            indent=2,
                        ),
                    )
                ]

            elif name == "list_worlds":
                world_ids = s.orchestrator.list_worlds()
                return [
                    TextContent(
                        type="text",
                        text=json.dumps(
                            {
                                "count": len(world_ids),
                                "world_ids": [str(wid) for wid in world_ids],
                            },
                            indent=2,
                        ),
                    )
                ]

            elif name == "get_world_status":
                status = await _get_world_status(s, arguments["world_name"])
                return [TextContent(type="text", text=json.dumps(status, indent=2))]

            elif name == "run_world":
                result = await _run_world(s, arguments["world_name"], arguments.get("steps", 1))
                return [TextContent(type="text", text=json.dumps(result, indent=2))]

            elif name == "run_parallel_worlds":
                result = await _run_parallel_worlds(
                    s, arguments["num_worlds"], arguments.get("steps", 100)
                )
                return [TextContent(type="text", text=json.dumps(result, indent=2))]

            elif name == "run_monte_carlo":
                result = await _run_monte_carlo(
                    s,
                    arguments["num_trials"],
                    arguments.get("steps", 100),
                    arguments.get("seed"),
                )
                return [TextContent(type="text", text=json.dumps(result, indent=2))]

            elif name == "submit_command":
                result = await _submit_command(
                    s,
                    arguments["world_name"],
                    arguments["operation"],
                    arguments["payload"],
                    arguments.get("priority", 0),
                )
                return [TextContent(type="text", text=json.dumps(result, indent=2))]

            elif name == "get_pending_commands":
                result = await _get_pending_commands(
                    s,
                    arguments["world_name"],
                    arguments.get("limit", 10),
                )
                return [TextContent(type="text", text=json.dumps(result, indent=2))]

            elif name == "get_command_history":
                result = await _get_command_history(
                    s,
                    arguments["world_name"],
                    arguments.get("limit", 50),
                )
                return [TextContent(type="text", text=json.dumps(result, indent=2))]

            else:
                return [
                    TextContent(type="text", text=json.dumps({"error": f"Unknown tool: {name}"}))
                ]

        except Exception as e:
            return [
                TextContent(type="text", text=json.dumps({"error": str(e), "tool": name}, indent=2))
            ]

    return server


# =============================================================================
# Tool Implementation Helpers
# =============================================================================


async def _create_world(s: ArchetypeMCP, name: str):
    """Create a new world."""
    config = WorldConfig(name=name)
    system = AsyncSystem()

    world = await s.orchestrator.create_world(
        config=config,
        system=system,
        storage_config=s.container.storage_config,
        cache_config=s.container.cache_config,
    )
    return world


async def _get_world_status(s: ArchetypeMCP, world_name: str) -> dict[str, Any]:
    """Get world status."""
    world = s.orchestrator.get_world_by_name(world_name)
    return {
        "world_id": str(world.world_id),
        "name": world_name,
        "tick": getattr(world, "tick", 0),
        "entity_count": len(getattr(world, "_entity2sig", {})),
        "active_signatures": len(getattr(world, "active_signatures", set())),
    }


async def _run_world(s: ArchetypeMCP, world_name: str, steps: int) -> dict[str, Any]:
    """Run a world for N steps."""
    run_config = RunConfig(num_steps=steps)
    await s.orchestrator.run_world_by_name(world_name, run_config)

    world = s.orchestrator.get_world_by_name(world_name)
    return {
        "status": "completed",
        "world_name": world_name,
        "steps_run": steps,
        "final_tick": getattr(world, "tick", 0),
    }


async def _run_parallel_worlds(s: ArchetypeMCP, num_worlds: int, steps: int) -> dict[str, Any]:
    """Run parallel worlds."""
    run_config = RunConfig(num_steps=steps)
    world_ids = await s.simulation.run_parallel_worlds(
        num_worlds=num_worlds,
        run_config=run_config,
    )
    return {
        "status": "completed",
        "num_worlds": num_worlds,
        "steps_per_world": steps,
        "world_ids": [str(wid) for wid in world_ids],
    }


async def _run_monte_carlo(
    s: ArchetypeMCP, num_trials: int, steps: int, seed: int | None
) -> dict[str, Any]:
    """Run Monte Carlo simulation."""
    run_config = RunConfig(num_steps=steps)

    # Simple system factory (no custom behavior)
    def system_factory(trial_seed: int):
        return AsyncSystem()

    result = await s.simulation.run_monte_carlo(
        num_trials=num_trials,
        system_factory=system_factory,
        run_config=run_config,
        seed=seed,
    )

    return {
        "status": "completed",
        "num_trials": result["num_trials"],
        "master_seed": result["master_seed"],
        "world_ids": [str(wid) for wid in result["world_ids"]],
    }


async def _submit_command(
    s: ArchetypeMCP,
    world_name: str,
    operation: str,
    payload: dict[str, Any],
    priority: int,
) -> dict[str, Any]:
    """Submit a command to a world."""
    world = s.orchestrator.get_world_by_name(world_name)

    cmd_id = await s.command.enqueue_command(
        world_id=world.world_id,
        op=operation,
        payload=payload,
        priority=priority,
    )

    return {
        "status": "enqueued",
        "command_id": str(cmd_id),
        "world_name": world_name,
        "operation": operation,
    }


async def _get_pending_commands(
    s: ArchetypeMCP,
    world_name: str,
    limit: int,
) -> dict[str, Any]:
    """Get pending commands."""
    world = s.orchestrator.get_world_by_name(world_name)
    commands = await s.command.peek_commands(world.world_id, limit)

    return {
        "world_name": world_name,
        "count": len(commands),
        "commands": [
            {
                "id": str(cmd.id),
                "op": cmd.op,
                "priority": cmd.priority,
                "tick": cmd.tick,
            }
            for cmd in commands
        ],
    }


async def _get_command_history(
    s: ArchetypeMCP,
    world_name: str,
    limit: int,
) -> dict[str, Any]:
    """Get command history."""
    world = s.orchestrator.get_world_by_name(world_name)
    commands = await s.command.get_command_history(world.world_id, limit)

    return {
        "world_name": world_name,
        "count": len(commands),
        "commands": [
            {
                "id": str(cmd.id),
                "op": cmd.op,
                "priority": cmd.priority,
                "tick": cmd.tick,
            }
            for cmd in commands
        ],
    }


# =============================================================================
# Entry Point
# =============================================================================


async def main():
    """Run the MCP server."""
    server = create_server()
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream)


if __name__ == "__main__":
    asyncio.run(main())
