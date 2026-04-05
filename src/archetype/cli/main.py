# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Archetype CLI — Typer application."""

from __future__ import annotations

import asyncio
import json

import typer

from archetype.app.container import ServiceContainer
from archetype.app.registry import default_registry_path

app = typer.Typer(name="archetype", help="Archetype ECS — AI-Native Simulation Engine")
world_app = typer.Typer(help="World management commands")
app.add_typer(world_app, name="world")


def _run(coro):
    """Run an async coroutine synchronously."""
    return asyncio.run(coro)


async def _make_container() -> ServiceContainer:
    """Create a ServiceContainer with the persistent world registry enabled and
    rehydrate any worlds recorded by previous CLI invocations."""
    container = ServiceContainer(registry_path=default_registry_path())
    await container.world_service.discover_worlds()
    return container


@app.command()
def serve(
    host: str = typer.Option("0.0.0.0", help="Bind host"),
    port: int = typer.Option(8000, help="Bind port"),
    reload: bool = typer.Option(False, help="Enable auto-reload"),
):
    """Start the FastAPI server."""
    import uvicorn

    uvicorn.run("archetype.api.app:create_app", host=host, port=port, reload=reload, factory=True)


@app.command()
def status():
    """Show all worlds and their state."""

    async def _status():
        container = await _make_container()
        try:
            worlds = container.world_service.list_worlds()
            if not worlds:
                typer.echo("No worlds found.")
                return
            for w in worlds:
                typer.echo(
                    f"  {w.world_id}  name={w.name}  tick={w.tick}  entities={w.entity_count}"
                )
        finally:
            await container.shutdown()

    _run(_status())


# ── World subcommands ──


@world_app.command("create")
def world_create(
    name: str = typer.Argument(..., help="World name"),
    uri: str = typer.Option("./archetype_data", help="Storage URI"),
    namespace: str = typer.Option("archetypes", help="Storage namespace"),
):
    """Create a new world."""

    async def _create():
        from archetype.core.config import StorageConfig, WorldConfig

        container = await _make_container()
        try:
            config = WorldConfig(name=name)
            storage_config = StorageConfig(uri=uri, namespace=namespace)
            world = await container.world_service.create_world(config, storage_config)
            typer.echo(f"Created world: {world.world_id} (name={name})")
        finally:
            await container.shutdown()

    _run(_create())


@world_app.command("list")
def world_list():
    """List all worlds."""

    async def _list():
        container = await _make_container()
        try:
            worlds = container.world_service.list_worlds()
            if not worlds:
                typer.echo("No worlds found.")
                return
            for w in worlds:
                typer.echo(f"  {w.world_id}  name={w.name}  tick={w.tick}")
        finally:
            await container.shutdown()

    _run(_list())


@world_app.command("inspect")
def world_inspect(world_id: str = typer.Argument(..., help="World ID")):
    """Show world details."""

    async def _inspect():
        from uuid_utils import UUID

        container = await _make_container()
        try:
            world = container.world_service.get_world(UUID(world_id))
            typer.echo(f"World ID: {world.world_id}")
            typer.echo(f"Name: {getattr(world, 'name', 'N/A')}")
            typer.echo(f"Tick: {getattr(world, 'tick', 0)}")
        finally:
            await container.shutdown()

    _run(_inspect())


@world_app.command("remove")
def world_remove(world_id: str = typer.Argument(..., help="World ID")):
    """Remove a world."""

    async def _remove():
        from uuid_utils import UUID

        container = await _make_container()
        try:
            container.world_service.remove_world(UUID(world_id))
            typer.echo(f"Removed world: {world_id}")
        finally:
            await container.shutdown()

    _run(_remove())


# ── Simulation commands ──


@app.command()
def run(
    world_id: str = typer.Argument(..., help="World ID"),
    steps: int = typer.Option(1, "--steps", "-n", help="Number of steps"),
):
    """Run simulation for N steps."""

    async def _run_sim():
        from uuid_utils import UUID

        from archetype.core.config import RunConfig

        container = await _make_container()
        try:
            run_config = RunConfig(num_steps=steps)
            result = await container.simulation_service.run(UUID(world_id), run_config)
            typer.echo(
                f"Run complete: {result.ticks_completed} ticks, "
                f"{result.commands_applied} commands applied"
            )
        finally:
            await container.shutdown()

    _run(_run_sim())


@app.command()
def step(world_id: str = typer.Argument(..., help="World ID")):
    """Execute a single tick."""

    async def _step():
        from uuid_utils import UUID

        container = await _make_container()
        try:
            cmds = await container.simulation_service.step(UUID(world_id))
            typer.echo(f"Step complete: {cmds} commands applied")
        finally:
            await container.shutdown()

    _run(_step())


# ── Query commands ──


@app.command()
def query(
    world_id: str = typer.Argument(..., help="World ID"),
    tick: int | None = typer.Option(None, "--tick", "-t", help="Tick to query"),
):
    """Query world state at a tick."""

    async def _query():
        from uuid_utils import UUID

        container = await _make_container()
        try:
            snapshot = await container.query_service.get_world_state(UUID(world_id), tick)
            typer.echo(json.dumps(snapshot.model_dump(mode="json"), indent=2))
        finally:
            await container.shutdown()

    _run(_query())


@app.command()
def history(
    world_id: str = typer.Argument(..., help="World ID"),
    limit: int = typer.Option(50, "--limit", "-n", help="Max commands to show"),
):
    """Show command history for a world."""

    async def _history():
        from uuid_utils import UUID

        container = await _make_container()
        try:
            cmds = await container.query_service.get_command_history(UUID(world_id), limit)
            if not cmds:
                typer.echo("No command history.")
                return
            for cmd in cmds:
                typer.echo(f"  [{cmd.tick}] {cmd.type.value} (priority={cmd.priority})")
        finally:
            await container.shutdown()

    _run(_history())


if __name__ == "__main__":
    app()
