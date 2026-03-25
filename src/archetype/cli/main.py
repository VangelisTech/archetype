# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Archetype CLI — Typer application."""

from __future__ import annotations

import asyncio
import json

import typer

app = typer.Typer(name="archetype", help="Archetype ECS — AI-Native Simulation Engine")
world_app = typer.Typer(help="World management commands")
memory_app = typer.Typer(help="Agent memory — ingest, query, list")
app.add_typer(world_app, name="world")
app.add_typer(memory_app, name="memory")


def _run(coro):
    """Run an async coroutine synchronously."""
    return asyncio.run(coro)


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
        from archetype.app.container import ServiceContainer

        container = ServiceContainer()
        try:
            worlds = container.world_service.list_worlds()
            if not worlds:
                typer.echo("No worlds found.")
                return
            for w in worlds:
                typer.echo(f"  {w.world_id}  name={w.name}  tick={w.tick}  entities={w.entity_count}")
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
        from archetype.app.container import ServiceContainer
        from archetype.core.config import StorageConfig, WorldConfig

        container = ServiceContainer()
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
        from archetype.app.container import ServiceContainer

        container = ServiceContainer()
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

        from archetype.app.container import ServiceContainer

        container = ServiceContainer()
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

        from archetype.app.container import ServiceContainer

        container = ServiceContainer()
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

        from archetype.app.container import ServiceContainer
        from archetype.core.config import RunConfig

        container = ServiceContainer()
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

        from archetype.app.container import ServiceContainer

        container = ServiceContainer()
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

        from archetype.app.container import ServiceContainer

        container = ServiceContainer()
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

        from archetype.app.container import ServiceContainer

        container = ServiceContainer()
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


# ── Memory commands ──
# Memory commands hit the running HTTP server (archetype serve).
# Set ARCHETYPE_SERVER or pass --server to override the base URL.

_DEFAULT_SERVER = "http://localhost:8000"


def _server_url(server: str) -> str:
    import os
    return os.environ.get("ARCHETYPE_SERVER", server).rstrip("/")


def _http(server: str):
    try:
        import httpx
        return httpx.Client(base_url=_server_url(server), timeout=30)
    except ImportError:
        typer.echo("httpx is required for memory commands. Install with: pip install httpx", err=True)
        raise typer.Exit(1)


@memory_app.command("ingest")
def memory_ingest(
    session_id: str = typer.Argument(..., help="Session identifier"),
    text: str = typer.Argument(..., help="Text to ingest"),
    source: str = typer.Option("unknown", "--source", "-s", help="Source label"),
    server: str = typer.Option(_DEFAULT_SERVER, "--server", envvar="ARCHETYPE_SERVER"),
):
    """Ingest a text chunk. Requires `archetype serve` to be running."""
    with _http(server) as client:
        resp = client.post("/memory/ingest", json={
            "session_id": session_id, "text": text, "source": source,
        })
        resp.raise_for_status()
        r = resp.json()
        if r.get("is_duplicate"):
            typer.echo(
                f"[duplicate] chunk_id={r['chunk_id']}  "
                f"canonical={r.get('canonical_id', '')}  "
                f"jaccard={r.get('similarity_score', 0):.3f}"
            )
        else:
            typer.echo(f"[stored]    chunk_id={r['chunk_id']}")


@memory_app.command("query")
def memory_query(
    session_id: str = typer.Argument(..., help="Session identifier"),
    q: str = typer.Argument(..., help="Query text"),
    top_k: int = typer.Option(5, "--top-k", "-k", help="Number of results"),
    server: str = typer.Option(_DEFAULT_SERVER, "--server", envvar="ARCHETYPE_SERVER"),
):
    """Query the top-K most relevant memory chunks. Requires `archetype serve`."""
    with _http(server) as client:
        resp = client.post("/memory/query", json={
            "session_id": session_id, "q": q, "top_k": top_k,
        })
        resp.raise_for_status()
        results = resp.json()
        if not results:
            typer.echo("No results found.")
            return
        for i, r in enumerate(results, 1):
            typer.echo(
                f"[{i}] sim={r.get('query_similarity', 0):.3f}  "
                f"source={r.get('source', '?')}  "
                f"{r.get('content', '')[:120]}"
            )


@memory_app.command("list")
def memory_list(
    session_id: str = typer.Argument(..., help="Session identifier"),
    server: str = typer.Option(_DEFAULT_SERVER, "--server", envvar="ARCHETYPE_SERVER"),
):
    """List non-duplicate chunks for a session. Requires `archetype serve`."""
    with _http(server) as client:
        resp = client.get(f"/memory/sessions/{session_id}/chunks")
        resp.raise_for_status()
        chunks = resp.json()
        if not chunks:
            typer.echo("No chunks found.")
            return
        typer.echo(f"{len(chunks)} chunk(s) in session '{session_id}':")
        for c in chunks:
            typer.echo(
                f"  {c['chunk_id']}  source={c.get('source', '?')}  "
                f"{c.get('content', '')[:80]}"
            )


if __name__ == "__main__":
    app()
