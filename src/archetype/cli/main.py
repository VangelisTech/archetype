# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Archetype CLI — Typer application.

The CLI is a thin HTTP client that delegates to a running ``archetype serve``
instance.  Every command (except ``serve`` itself) makes REST calls against
the server, so all worlds live in a single event-loop and benefit from the
parallelism model built into ``AsyncWorld`` and ``SimulationService.run_all``.
"""

from __future__ import annotations

import json
import os

import httpx
import typer

DEFAULT_BASE_URL = "http://localhost:8000"
ENV_BASE_URL = "ARCHETYPE_URL"

app = typer.Typer(
    name="archetype",
    help="Archetype ECS — dataframe-first ECS runtime for simulations and AI agents",
)
world_app = typer.Typer(help="World management commands")
app.add_typer(world_app, name="world")
chronicle_app = typer.Typer(help="Chronicle — personal archive (local, no server required)")
app.add_typer(chronicle_app, name="chronicle")


def _base_url() -> str:
    return os.environ.get(ENV_BASE_URL, DEFAULT_BASE_URL).rstrip("/")


def _client() -> httpx.Client:
    return httpx.Client(base_url=_base_url(), timeout=120.0)


def _check_server(client: httpx.Client) -> None:
    """Verify the server is reachable, give a clear error if not."""
    try:
        client.get("/")
    except httpx.RequestError as exc:
        typer.echo(
            f"Error: Cannot reach Archetype server at {_base_url()}.\n"
            "Start the server first:  archetype serve\n"
            f"Or set {ENV_BASE_URL} to point at a running instance.",
            err=True,
        )
        raise typer.Exit(code=1) from exc


def _handle_response(resp: httpx.Response) -> dict | list:
    """Return JSON on success, print error and exit on failure."""
    if resp.is_success:
        return resp.json()
    try:
        detail = resp.json().get("detail", resp.text)
    except Exception:
        detail = resp.text
    typer.echo(f"Error ({resp.status_code}): {detail}", err=True)
    raise typer.Exit(code=1)


def _request(method: str, path: str, **kwargs) -> dict | list:
    """Make an HTTP request against the Archetype server.

    Creates a fresh client per call to keep each CLI invocation stateless.
    """
    client = _client()
    try:
        _check_server(client)
        resp = getattr(client, method)(path, **kwargs)
        return _handle_response(resp)
    finally:
        client.close()


# ── Server ──


@app.command()
def serve(
    host: str = typer.Option("0.0.0.0", help="Bind host"),
    port: int = typer.Option(8000, help="Bind port"),
    reload: bool = typer.Option(False, help="Enable auto-reload"),
):
    """Start the FastAPI server."""
    import uvicorn

    uvicorn.run("archetype.api.app:create_app", host=host, port=port, reload=reload, factory=True)


# ── Status ──


@app.command()
def status():
    """Show all worlds and their state."""
    worlds = _request("get", "/worlds")
    if not worlds:
        typer.echo("No worlds found.")
        return
    for w in worlds:
        typer.echo(
            f"  {w['world_id']}  name={w.get('name')}  "
            f"tick={w.get('tick', 0)}  entities={w.get('entity_count', 0)}"
        )


# ── World subcommands ──


@world_app.command("create")
def world_create(
    name: str = typer.Argument(..., help="World name"),
    uri: str = typer.Option("./archetype_data", help="Storage URI"),
    namespace: str = typer.Option("archetypes", help="Storage namespace"),
):
    """Create a new world."""
    body = {"name": name, "storage_uri": uri, "namespace": namespace}
    data = _request("post", "/worlds", json=body)
    typer.echo(f"Created world: {data['world_id']} (name={data.get('name')})")


@world_app.command("list")
def world_list():
    """List all worlds."""
    worlds = _request("get", "/worlds")
    if not worlds:
        typer.echo("No worlds found.")
        return
    for w in worlds:
        typer.echo(f"  {w['world_id']}  name={w.get('name')}  tick={w.get('tick', 0)}")


@world_app.command("inspect")
def world_inspect(world_id: str = typer.Argument(..., help="World ID")):
    """Show world details."""
    data = _request("get", f"/worlds/{world_id}")
    typer.echo(f"World ID: {data['world_id']}")
    typer.echo(f"Name: {data.get('name', 'N/A')}")
    typer.echo(f"Tick: {data.get('tick', 0)}")


@world_app.command("fork")
def world_fork(
    world_id: str = typer.Argument(..., help="Source world ID"),
    name: str = typer.Option(None, "--name", "-n", help="Name for the fork"),
):
    """Fork a world (branch from current state)."""
    body: dict = {}
    if name:
        body["name"] = name
    data = _request("post", f"/worlds/{world_id}/fork", json=body)
    typer.echo(f"Forked: {data['world_id']} (from {world_id})")


@world_app.command("remove")
def world_remove(world_id: str = typer.Argument(..., help="World ID")):
    """Remove a world."""
    _request("delete", f"/worlds/{world_id}")
    typer.echo(f"Removed world: {world_id}")


# ── Simulation commands ──


@app.command()
def run(
    world_id: str = typer.Argument(..., help="World ID"),
    steps: int = typer.Option(1, "--steps", "-n", help="Number of steps"),
):
    """Run simulation for N steps."""
    data = _request("post", f"/worlds/{world_id}/run", json={"num_steps": steps})
    typer.echo(
        f"Run complete: {data['ticks_completed']} ticks, "
        f"{data['commands_applied']} commands applied"
    )


@app.command()
def step(world_id: str = typer.Argument(..., help="World ID")):
    """Execute a single tick."""
    data = _request("post", f"/worlds/{world_id}/step", json={})
    typer.echo(f"Step complete: {data['commands_applied']} commands applied")


# ── Query commands ──


@app.command()
def query(
    world_id: str = typer.Argument(..., help="World ID"),
    tick: int | None = typer.Option(None, "--tick", "-t", help="Tick to query"),
):
    """Query world state at a tick."""
    params = {}
    if tick is not None:
        params["tick"] = tick
    data = _request("get", f"/worlds/{world_id}/state", params=params)
    typer.echo(json.dumps(data, indent=2))


@app.command()
def history(
    world_id: str = typer.Argument(..., help="World ID"),
    limit: int = typer.Option(50, "--limit", "-n", help="Max commands to show"),
):
    """Show command history for a world."""
    data = _request("get", f"/worlds/{world_id}/history", params={"limit": limit})
    if not data:
        typer.echo("No command history.")
        return
    for cmd in data:
        typer.echo(f"  [{cmd['tick']}] {cmd['type']} (priority={cmd['priority']})")


# ── Chronicle commands (local, no server needed) ──


@chronicle_app.command("ingest")
def chronicle_ingest(
    world_name: str = typer.Option("chronicle", help="World name for the chronicle"),
    storage_uri: str = typer.Option("./archetype_data", help="Storage URI"),
    namespace: str = typer.Option("chronicle", help="Storage namespace"),
    claude_code_dir: str | None = typer.Option(
        None, help="Claude Code projects dir (default: ~/.claude/projects)"
    ),
    claude_ai_export: str | None = typer.Option(
        None, help="Path to claude.ai data export (dir or conversations.json)"
    ),
    chatgpt_export: str | None = typer.Option(
        None, help="Path to ChatGPT data export (dir or conversations.json)"
    ),
    apple_photos: bool = typer.Option(
        False, help="Ingest Apple Photos library (requires osxphotos + FDA)"
    ),
    apple_photos_dir: str | None = typer.Option(
        None, help="Path to exported photos/videos folder (fallback)"
    ),
):
    """Ingest chat histories, photos, and videos into a chronicle world.

    Runs locally — does NOT require ``archetype serve``.
    Each source is attempted independently; missing sources are skipped.
    """
    import asyncio

    async def _run():
        from archetype.app.container import ServiceContainer
        from archetype.chronicle.loaders import ingest_all
        from archetype.core.config import StorageConfig, WorldConfig

        container = ServiceContainer()
        try:
            world = await container.world_service.create_world(
                WorldConfig(name=world_name),
                StorageConfig(uri=storage_uri, namespace=namespace),
            )

            counts = await ingest_all(
                world,
                claude_code_dir=claude_code_dir,
                claude_ai_export=claude_ai_export,
                chatgpt_export=chatgpt_export,
                apple_photos_library=str(apple_photos_dir or "") if apple_photos else None,
                apple_photos_dir=apple_photos_dir if not apple_photos else None,
            )

            # Materialize to storage
            if any(counts.values()):
                from archetype.core.config import RunConfig

                await container.simulation_service.step(
                    world.world_id,
                    RunConfig(num_steps=1),
                )

            total = sum(counts.values())
            typer.echo(f"\nChronicle ingest complete: {total} items")
            for source, count in sorted(counts.items()):
                typer.echo(f"  {source}: {count}")
            typer.echo(f"\nWorld: {world.world_id} (name={world_name})")
        finally:
            await container.shutdown()

    asyncio.run(_run())


if __name__ == "__main__":
    app()
