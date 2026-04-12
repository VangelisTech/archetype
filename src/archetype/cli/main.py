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

app = typer.Typer(name="archetype", help="Archetype ECS — AI-Native Simulation Engine")
world_app = typer.Typer(help="World management commands")
app.add_typer(world_app, name="world")


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


def _print_table(columns: list[str], rows: list[dict]) -> None:
    """Print rows as a simple aligned table."""
    if not rows:
        return
    # Compute column widths
    widths = {c: len(c) for c in columns}
    str_rows = []
    for row in rows:
        str_row = {c: str(row.get(c, "")) for c in columns}
        for c in columns:
            widths[c] = max(widths[c], len(str_row[c]))
        str_rows.append(str_row)
    # Header
    header = "  ".join(c.ljust(widths[c]) for c in columns)
    typer.echo(header)
    typer.echo("  ".join("-" * widths[c] for c in columns))
    # Rows
    for sr in str_rows:
        typer.echo("  ".join(sr[c].ljust(widths[c]) for c in columns))


# ── Query commands ──


@app.command()
def query(
    world_id: str = typer.Argument(..., help="World ID"),
    components: str = typer.Argument(
        None, help="Comma-separated component types (e.g. Agent,Score)"
    ),
    tick: int | None = typer.Option(None, "--tick", "-t", help="Tick to query"),
    where: str | None = typer.Option(None, "--where", "-w", help="SQL filter expression"),
    show: int = typer.Option(10, "--show", "-s", help="Number of rows to show"),
    count: bool = typer.Option(False, "--count", "-c", help="Return count only"),
    select: str | None = typer.Option(None, "--select", help="Comma-separated columns"),
    sort: str | None = typer.Option(None, "--sort", help="Column to sort by"),
    desc: bool = typer.Option(False, "--desc", help="Sort descending"),
    offset: int = typer.Option(0, "--offset", help="Skip N rows"),
):
    """Query world components as a DataFrame.

    Without component types, shows world state snapshot.
    With component types, builds a lazy DataFrame query and materializes at
    the terminal operation (--show or --count).

    Examples:

        archetype query <id> Agent,Score --show 5

        archetype query <id> Agent,Score --where "score__val > 0.5" --show 10

        archetype query <id> Trajectory,Label --count
    """
    if not components:
        # Fallback: world state snapshot (original behavior)
        params: dict = {}
        if tick is not None:
            params["tick"] = tick
        data = _request("get", f"/worlds/{world_id}/state", params=params)
        typer.echo(json.dumps(data, indent=2))
        return

    # Build DataFrame query body
    body: dict = {
        "components": [c.strip() for c in components.split(",") if c.strip()],
        "limit": show,
        "offset": offset,
        "desc": desc,
        "count": count,
    }
    if tick is not None:
        body["tick"] = tick
    if where:
        body["where"] = where
    if select:
        body["select"] = [s.strip() for s in select.split(",") if s.strip()]
    if sort:
        body["sort"] = sort

    data = _request("post", f"/worlds/{world_id}/query", json=body)

    if count:
        typer.echo(f"count: {data.get('count', 0)}")
    else:
        rows = data.get("rows", [])
        columns = data.get("columns", [])
        if not rows:
            typer.echo("No results.")
            return
        # Tabular output
        _print_table(columns, rows)


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


if __name__ == "__main__":
    app()
