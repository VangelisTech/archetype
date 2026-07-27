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

"""Archetype CLI.

Except for ``serve``, commands are thin HTTP calls to a running
``archetype serve`` process. The API owns validation, authorization, and
service-layer dispatch.
"""

from __future__ import annotations

import json
import os
from enum import StrEnum
from typing import Annotated, Any

import httpx
import typer
from rich.console import Console
from rich.table import Table

from archetype._logging import configure_host_observability

DEFAULT_BASE_URL = "http://localhost:8000"
ENV_BASE_URL = "ARCHETYPE_URL"

console = Console()

app = typer.Typer(
    name="archetype",
    help="Archetype ECS - dataframe-first ECS runtime for simulations and AI agents",
)
world_app = typer.Typer(help="World management commands")
entity_app = typer.Typer(help="Entity mutation commands")
processors_app = typer.Typer(help="Processor introspection commands")
hooks_app = typer.Typer(help="Hook introspection commands")
resources_app = typer.Typer(help="Resource introspection commands")

app.add_typer(world_app, name="world")
app.add_typer(entity_app, name="entity")
app.add_typer(processors_app, name="processors")
app.add_typer(hooks_app, name="hooks")
app.add_typer(resources_app, name="resources")


class Role(StrEnum):
    ADMIN = "admin"
    ADMIN_SHORT = "a"
    OPERATOR = "operator"
    PLAYER = "player"
    VIEWER = "viewer"


ROLE_OPTION = typer.Option(
    None,
    "--role",
    "-r",
    help="Role shortcut for a server started with --dev-auth",
)


def _base_url(url: str | None = None) -> str:
    return (url or os.environ.get(ENV_BASE_URL, DEFAULT_BASE_URL)).rstrip("/")


def _headers(role: Role | None = None, token: str | None = None) -> dict[str, str]:
    if token:
        return {"Authorization": f"Bearer {token}"}
    if role:
        role_value = "admin" if role == Role.ADMIN_SHORT else role.value
        return {"Authorization": f"Bearer {role_value}"}
    return {}


def _client(url: str | None = None) -> httpx.Client:
    return httpx.Client(base_url=_base_url(url), timeout=120.0)


def _check_server(client: httpx.Client, url: str | None = None) -> None:
    """Verify the server is reachable, give a clear error if not."""
    try:
        client.get("/healthz")
    except httpx.RequestError as exc:
        typer.echo(
            f"Error: Cannot reach Archetype server at {_base_url(url)}.\n"
            "Start the server first:  archetype serve\n"
            f"Or set {ENV_BASE_URL} to point at a running instance.",
            err=True,
        )
        raise typer.Exit(code=1) from exc


def _handle_response(resp: httpx.Response, *, role: Role | None = None) -> dict | list:
    """Return JSON on success, print mapped API errors, and exit."""
    if resp.is_success:
        if resp.status_code == 204 or not resp.content:
            return {}
        return resp.json()

    try:
        detail = resp.json().get("detail", resp.text)
    except Exception:
        detail = resp.text

    if resp.status_code == 403:
        role_text = role.value if role else "api-default"
        typer.secho(
            f"permission denied for role {role_text}: {detail}", fg=typer.colors.RED, err=True
        )
        raise typer.Exit(code=1)
    if resp.status_code == 404:
        typer.echo(f"world / entity not found: {detail}", err=True)
        raise typer.Exit(code=1)
    if resp.status_code == 400:
        typer.echo(f"validation error: {detail}", err=True)
        raise typer.Exit(code=1)
    if resp.status_code >= 500:
        typer.echo("server error: see logs", err=True)
        raise typer.Exit(code=2)

    typer.echo(f"Error ({resp.status_code}): {detail}", err=True)
    raise typer.Exit(code=1)


def _request(
    method: str,
    path: str,
    *,
    url: str | None = None,
    role: Role | None = None,
    token: str | None = None,
    **kwargs,
) -> Any:
    """Make one HTTP request against the Archetype server; returns parsed JSON."""
    client = _client(url)
    try:
        _check_server(client, url)
        headers = kwargs.pop("headers", {})
        headers = {**headers, **_headers(role, token)}
        if headers:
            kwargs["headers"] = headers
        resp = getattr(client, method)(path, **kwargs)
        return _handle_response(resp, role=role)
    finally:
        client.close()


def _request_obj(method: str, path: str, **kwargs) -> dict[str, Any]:
    """_request for endpoints that return a single JSON object."""
    result = _request(method, path, **kwargs)
    if not isinstance(result, dict):
        typer.echo(
            f"unexpected response: expected an object, got {type(result).__name__}", err=True
        )
        raise typer.Exit(code=1)
    return result


def _request_list(method: str, path: str, **kwargs) -> list[dict[str, Any]]:
    """_request for endpoints that return a JSON array of objects."""
    result = _request(method, path, **kwargs)
    if not isinstance(result, list):
        typer.echo(f"unexpected response: expected an array, got {type(result).__name__}", err=True)
        raise typer.Exit(code=1)
    return result


def _parse_json_array(value: str) -> list[dict[str, Any]]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        typer.echo(f"validation error: invalid JSON: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    if not isinstance(parsed, list):
        typer.echo("validation error: expected a JSON array", err=True)
        raise typer.Exit(code=1)
    return parsed


def _csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _print_json(data: Any) -> None:
    typer.echo(json.dumps(data, indent=2))


def _print_world(info: dict[str, Any], *, prefix: str = "World") -> None:
    typer.echo(f"{prefix} ID: {info.get('world_id')}")
    typer.echo(f"Name: {info.get('name')}")
    typer.echo(f"Tick: {info.get('tick', 0)}")
    if info.get("run_id") is not None:
        typer.echo(f"Run ID: {info.get('run_id')}")


def _print_world_table(worlds: list[dict[str, Any]]) -> None:
    if not worlds:
        typer.echo("No worlds found.")
        return
    table = Table("world_id", "name", "tick")
    for world in worlds:
        table.add_row(str(world.get("world_id")), str(world.get("name")), str(world.get("tick", 0)))
    console.print(table)


def _print_rows(rows: list[dict[str, Any]], *, empty: str = "No rows found.") -> None:
    if not rows:
        typer.echo(empty)
        return
    columns = list(dict.fromkeys(key for row in rows for key in row))
    table = Table(*columns)
    for row in rows:
        table.add_row(*(str(row.get(column, "")) for column in columns))
    console.print(table)


def _common_request_kwargs(
    url: str | None,
    role: Role | None,
    token: str | None,
) -> dict[str, Any]:
    return {"url": url, "role": role, "token": token}


# ── Server ───────────────────────────────────────────────────────────────


@app.command()
def serve(
    host: str = typer.Option("127.0.0.1", help="Bind host"),
    port: int = typer.Option(8000, help="Bind port"),
    reload: bool = typer.Option(False, help="Enable auto-reload"),
    dev_auth: Annotated[
        bool,
        typer.Option(
            "--dev-auth",
            help="Enable role-based development auth (loopback only)",
        ),
    ] = False,
):
    """Start the FastAPI server."""
    from archetype.api.app import (
        _SERVE_DEV_AUTH_ENV,
        _SERVE_HOST_ENV,
        _is_loopback_host,
    )

    if dev_auth and not _is_loopback_host(host):
        typer.echo("Error: --dev-auth requires a loopback --host", err=True)
        raise typer.Exit(code=2)

    import uvicorn

    configure_host_observability(service_name="archetype-api")
    os.environ[_SERVE_HOST_ENV] = host
    os.environ[_SERVE_DEV_AUTH_ENV] = "1" if dev_auth else "0"
    uvicorn.run(
        "archetype.api.app:_create_cli_app",
        host=host,
        port=port,
        reload=reload,
        factory=True,
    )


# ── Status ───────────────────────────────────────────────────────────────


@app.command()
def status(
    url: str | None = typer.Option(None, "--url", help="Override ARCHETYPE_URL for this command"),
    role: Role | None = ROLE_OPTION,
    token: str | None = typer.Option(None, "--token", help="Bearer token to send verbatim"),
    json_output: bool = typer.Option(False, "--json", help="Emit raw JSON"),
):
    """Show all worlds and their state."""
    worlds = _request_list("get", "/worlds", **_common_request_kwargs(url, role, token))
    if json_output:
        _print_json(worlds)
        return
    _print_world_table(worlds)


# ── World subcommands ────────────────────────────────────────────────────


@world_app.command("create")
def world_create(
    name: str = typer.Argument(..., help="World name"),
    storage: str = typer.Option("./archetype_data", "--storage", "--uri", help="Storage URI"),
    namespace: str = typer.Option("archetypes", help="Storage namespace"),
    cache: bool = typer.Option(False, "--cache", help="Use default cache config"),
    url: str | None = typer.Option(None, "--url", help="Override ARCHETYPE_URL for this command"),
    role: Role | None = ROLE_OPTION,
    token: str | None = typer.Option(None, "--token", help="Bearer token to send verbatim"),
):
    """Create a world."""
    body: dict[str, Any] = {
        "config": {"name": name},
        "storage_config": {"uri": storage, "namespace": namespace},
    }
    if cache:
        body["cache_config"] = {}
    data = _request_obj("post", "/worlds", json=body, **_common_request_kwargs(url, role, token))
    typer.echo(f"Created world: {data['world_id']} (name={data.get('name')})")


@world_app.command("list")
def world_list(
    url: str | None = typer.Option(None, "--url", help="Override ARCHETYPE_URL for this command"),
    role: Role | None = ROLE_OPTION,
    token: str | None = typer.Option(None, "--token", help="Bearer token to send verbatim"),
    json_output: bool = typer.Option(False, "--json", help="Emit raw JSON"),
):
    """List live worlds."""
    worlds = _request_list("get", "/worlds", **_common_request_kwargs(url, role, token))
    if json_output:
        _print_json(worlds)
        return
    _print_world_table(worlds)


@world_app.command("inspect")
def world_inspect(
    world_id: str = typer.Argument(..., help="World ID"),
    url: str | None = typer.Option(None, "--url", help="Override ARCHETYPE_URL for this command"),
    role: Role | None = ROLE_OPTION,
    token: str | None = typer.Option(None, "--token", help="Bearer token to send verbatim"),
    json_output: bool = typer.Option(False, "--json", help="Emit raw JSON"),
):
    """Show world details."""
    data = _request_obj("get", f"/worlds/{world_id}", **_common_request_kwargs(url, role, token))
    if json_output:
        _print_json(data)
        return
    _print_world(data)


@world_app.command("fork")
def world_fork(
    world_id: str = typer.Argument(..., help="Source world ID"),
    name: str | None = typer.Option(None, "--name", "-n", help="Name for the fork"),
    storage: str | None = typer.Option(None, "--storage", "--uri", help="Storage URI"),
    namespace: str = typer.Option("archetypes", help="Storage namespace"),
    cache: bool = typer.Option(False, "--cache", help="Use default cache config"),
    url: str | None = typer.Option(None, "--url", help="Override ARCHETYPE_URL for this command"),
    role: Role | None = ROLE_OPTION,
    token: str | None = typer.Option(None, "--token", help="Bearer token to send verbatim"),
):
    """Fork a world."""
    body: dict[str, Any] = {"name": name}
    if storage:
        body["storage_config"] = {"uri": storage, "namespace": namespace}
    if cache:
        body["cache_config"] = {}
    data = _request_obj(
        "post",
        f"/worlds/{world_id}/fork",
        json=body,
        **_common_request_kwargs(url, role, token),
    )
    typer.echo(f"Forked world: {data['world_id']} (from {world_id})")


@world_app.command("destroy")
def world_destroy(
    world_id: str = typer.Argument(..., help="World ID"),
    url: str | None = typer.Option(None, "--url", help="Override ARCHETYPE_URL for this command"),
    role: Role | None = ROLE_OPTION,
    token: str | None = typer.Option(None, "--token", help="Bearer token to send verbatim"),
):
    """Drop the in-memory world. Storage and audit rows are retained."""
    _request("delete", f"/worlds/{world_id}", **_common_request_kwargs(url, role, token))
    typer.echo(f"Destroyed world: {world_id} (storage retained)")


@world_app.command("remove", hidden=True)
def world_remove_alias(
    world_id: str = typer.Argument(..., help="World ID"),
    url: str | None = typer.Option(None, "--url", help="Override ARCHETYPE_URL for this command"),
    role: Role | None = ROLE_OPTION,
    token: str | None = typer.Option(None, "--token"),
):
    """Deprecated alias for world destroy."""
    world_destroy(world_id, url=url, role=role, token=token)


# ── Entity subcommands ───────────────────────────────────────────────────


@entity_app.command("spawn")
def entity_spawn(
    world_id: str = typer.Argument(..., help="World ID"),
    components: str = typer.Option(..., "--components", help="JSON array of component payloads"),
    url: str | None = typer.Option(None, "--url", help="Override ARCHETYPE_URL for this command"),
    role: Role | None = ROLE_OPTION,
    token: str | None = typer.Option(None, "--token", help="Bearer token to send verbatim"),
):
    """Spawn an entity from component payloads."""
    data = _request_obj(
        "post",
        f"/worlds/{world_id}/entities",
        json={"components": _parse_json_array(components)},
        **_common_request_kwargs(url, role, token),
    )
    typer.echo(f"Spawned entity: {data['entity_id']}")


@entity_app.command("despawn")
def entity_despawn(
    world_id: str = typer.Argument(..., help="World ID"),
    entity_id: int = typer.Argument(..., help="Entity ID"),
    url: str | None = typer.Option(None, "--url", help="Override ARCHETYPE_URL for this command"),
    role: Role | None = ROLE_OPTION,
    token: str | None = typer.Option(None, "--token", help="Bearer token to send verbatim"),
):
    """Despawn an entity."""
    _request(
        "delete",
        f"/worlds/{world_id}/entities/{entity_id}",
        **_common_request_kwargs(url, role, token),
    )
    typer.echo(f"Despawned entity: {entity_id}")


@entity_app.command("update")
def entity_update(
    world_id: str = typer.Argument(..., help="World ID"),
    entity_id: int = typer.Argument(..., help="Entity ID"),
    components: str = typer.Option(..., "--components", help="JSON array of component payloads"),
    url: str | None = typer.Option(None, "--url", help="Override ARCHETYPE_URL for this command"),
    role: Role | None = ROLE_OPTION,
    token: str | None = typer.Option(None, "--token", help="Bearer token to send verbatim"),
):
    """Overlay component values on an entity."""
    _request(
        "patch",
        f"/worlds/{world_id}/entities/{entity_id}",
        json={"components": _parse_json_array(components)},
        **_common_request_kwargs(url, role, token),
    )
    typer.echo(f"Updated entity: {entity_id}")


@entity_app.command("add-components")
def entity_add_components(
    world_id: str = typer.Argument(..., help="World ID"),
    entity_id: int = typer.Argument(..., help="Entity ID"),
    components: str = typer.Option(..., "--components", help="JSON array of component payloads"),
    url: str | None = typer.Option(None, "--url", help="Override ARCHETYPE_URL for this command"),
    role: Role | None = ROLE_OPTION,
    token: str | None = typer.Option(None, "--token", help="Bearer token to send verbatim"),
):
    """Add components to an entity."""
    _request(
        "post",
        f"/worlds/{world_id}/entities/{entity_id}/components",
        json={"components": _parse_json_array(components)},
        **_common_request_kwargs(url, role, token),
    )
    typer.echo(f"Added components to entity: {entity_id}")


@entity_app.command("remove-components")
def entity_remove_components(
    world_id: str = typer.Argument(..., help="World ID"),
    entity_id: int = typer.Argument(..., help="Entity ID"),
    types: str = typer.Option(..., "--types", "--typ", help="Comma-separated component type names"),
    url: str | None = typer.Option(None, "--url", help="Override ARCHETYPE_URL for this command"),
    role: Role | None = ROLE_OPTION,
    token: str | None = typer.Option(None, "--token", help="Bearer token to send verbatim"),
):
    """Remove component types from an entity."""
    _request(
        "delete",
        f"/worlds/{world_id}/entities/{entity_id}/components",
        json={"component_types": _csv(types)},
        **_common_request_kwargs(url, role, token),
    )
    typer.echo(f"Removed components from entity: {entity_id}")


# ── Simulation commands ─────────────────────────────────────────────────


@app.command()
def run(
    world_id: str = typer.Argument(..., help="World ID"),
    steps: int = typer.Option(1, "--steps", "-n", help="Number of steps"),
    url: str | None = typer.Option(None, "--url", help="Override ARCHETYPE_URL for this command"),
    role: Role | None = ROLE_OPTION,
    token: str | None = typer.Option(None, "--token", help="Bearer token to send verbatim"),
):
    """Run simulation for N steps."""
    data = _request_obj(
        "post",
        f"/worlds/{world_id}/run",
        json={"num_steps": steps},
        **_common_request_kwargs(url, role, token),
    )
    typer.echo(
        f"Run complete: {data['ticks_completed']} ticks, "
        f"{data['commands_applied']} commands applied"
    )


@app.command()
def step(
    world_id: str = typer.Argument(..., help="World ID"),
    url: str | None = typer.Option(None, "--url", help="Override ARCHETYPE_URL for this command"),
    role: Role | None = ROLE_OPTION,
    token: str | None = typer.Option(None, "--token", help="Bearer token to send verbatim"),
):
    """Execute a single tick."""
    data = _request_obj(
        "post",
        f"/worlds/{world_id}/step",
        json={},
        **_common_request_kwargs(url, role, token),
    )
    typer.echo(f"Step complete: {data['commands_applied']} commands applied")


@app.command()
def episode(
    world_id: str = typer.Argument(..., help="World ID"),
    max_steps: int = typer.Option(1000, "--max-steps", "-n", help="Maximum episode steps"),
    terminal_component: str | None = typer.Option(
        None,
        "--terminal-component",
        help="Terminal component type name",
    ),
    url: str | None = typer.Option(None, "--url", help="Override ARCHETYPE_URL for this command"),
    role: Role | None = ROLE_OPTION,
    token: str | None = typer.Option(None, "--token", help="Bearer token to send verbatim"),
    json_output: bool = typer.Option(False, "--json", help="Emit raw JSON"),
):
    """Run one episode."""
    body: dict[str, Any] = {"max_steps": max_steps}
    if terminal_component:
        body["terminal_component"] = terminal_component
    data = _request_obj(
        "post",
        f"/worlds/{world_id}/episode",
        json=body,
        **_common_request_kwargs(url, role, token),
    )
    if json_output:
        _print_json(data)
        return
    typer.echo(f"Episode ID: {data.get('episode_id')}")
    typer.echo(f"Final tick: {data.get('final_tick')}")
    typer.echo(f"Terminated: {data.get('terminated')}")
    typer.echo(f"Duration steps: {data.get('duration_steps')}")


@app.command()
def rollout(
    world_id: str = typer.Argument(..., help="World ID"),
    num_episodes: int = typer.Option(1, "--num-episodes", "-e", help="Number of episodes"),
    max_steps: int = typer.Option(1000, "--max-steps", "-n", help="Maximum steps per episode"),
    terminal_component: str | None = typer.Option(
        None,
        "--terminal-component",
        help="Terminal component type name",
    ),
    parallel: bool = typer.Option(False, "--parallel", help="Run episodes concurrently"),
    destroy_forks_on_complete: bool = typer.Option(
        False,
        "--destroy-forks-on-complete",
        help="Destroy forked worlds after each episode; storage retained",
    ),
    url: str | None = typer.Option(None, "--url", help="Override ARCHETYPE_URL for this command"),
    role: Role | None = ROLE_OPTION,
    token: str | None = typer.Option(None, "--token", help="Bearer token to send verbatim"),
    json_output: bool = typer.Option(False, "--json", help="Emit raw JSON"),
):
    """Run a rollout from a base world."""
    episode_config: dict[str, Any] = {"max_steps": max_steps}
    if terminal_component:
        episode_config["terminal_component"] = terminal_component
    body = {
        "num_episodes": num_episodes,
        "parallel": parallel,
        "destroy_forks_on_complete": destroy_forks_on_complete,
        "episode_config": episode_config,
    }
    data = _request_obj(
        "post",
        f"/worlds/{world_id}/rollout",
        json=body,
        **_common_request_kwargs(url, role, token),
    )
    if json_output:
        _print_json(data)
        return
    typer.echo(f"Rollout ID: {data.get('rollout_id')}")
    typer.echo(f"Base world ID: {data.get('base_world_id')}")
    typer.echo(f"Episodes: {len(data.get('episodes', []))}")
    typer.echo(f"Total duration steps: {data.get('total_duration_steps', 0)}")
    _print_rows(data.get("episodes", []), empty="No episodes.")


# ── Query commands ──────────────────────────────────────────────────────


@app.command()
def query(
    world_id: str = typer.Argument(..., help="World ID"),
    component_types: str | None = typer.Argument(
        None,
        help="Comma-separated component types, for example Agent,Score",
    ),
    types: str | None = typer.Option(None, "--types", help="Comma-separated component types"),
    tick: int | None = typer.Option(None, "--tick", "-t", help="Tick to query"),
    ticks: str | None = typer.Option(None, "--ticks", help="Comma-separated ticks; first is used"),
    entity_ids: str | None = typer.Option(None, "--entity-ids", help="Comma-separated entity IDs"),
    show: int | None = typer.Option(None, "--show", "-s", min=1, help="Limit to N rows"),
    count: bool = typer.Option(False, "--count", "-c", help="Return the row count only"),
    where: str | None = typer.Option(
        None,
        "--where",
        "-w",
        help='Filter with one comparison, for example "score__value > 0.5"',
    ),
    url: str | None = typer.Option(None, "--url", help="Override ARCHETYPE_URL for this command"),
    role: Role | None = ROLE_OPTION,
    token: str | None = typer.Option(None, "--token", help="Bearer token to send verbatim"),
    json_output: bool = typer.Option(False, "--json", help="Emit raw JSON"),
):
    """Query world state or lazily filter matching component rows."""
    if component_types and types:
        typer.echo(
            "validation error: pass component types positionally or with --types, not both",
            err=True,
        )
        raise typer.Exit(code=1)
    selected_types = component_types or types
    if count and show is not None:
        typer.echo("validation error: --count and --show are mutually exclusive", err=True)
        raise typer.Exit(code=1)
    if (show is not None or count or where) and not selected_types:
        typer.echo(
            "validation error: --show, --count, and --where require component types",
            err=True,
        )
        raise typer.Exit(code=1)

    params: dict[str, Any] = {}
    if entity_ids:
        params["entity_ids"] = entity_ids
    if tick is not None:
        params["tick"] = tick
    elif ticks:
        params["tick"] = _csv(ticks)[0]

    path = f"/worlds/{world_id}/state"
    if selected_types:
        path = f"/worlds/{world_id}/components"
        params["types"] = selected_types
        if show is not None:
            params["show"] = show
        if count:
            params["count"] = True
        if where:
            params["where"] = where

    request_kwargs = {
        "params": params,
        **_common_request_kwargs(url, role, token),
    }
    if count:
        result = _request_obj("get", path, **request_kwargs)
        if json_output:
            _print_json(result)
        else:
            typer.echo(f"Count: {result['count']}")
        return

    data = _request_list("get", path, **request_kwargs)
    if json_output:
        _print_json(data)
        return
    _print_rows(data, empty="No state rows found.")


@app.command()
def history(
    world_id: str = typer.Argument(..., help="World ID"),
    limit: int = typer.Option(50, "--limit", "-n", help="Max audit rows to show"),
    actor_id: str | None = typer.Option(None, "--actor-id", help="Actor ID filter"),
    idempotency_key: str | None = typer.Option(None, "--idempotency-key", help="Idempotency key"),
    tick_from: int | None = typer.Option(None, "--tick-from", help="Reserved for API v2"),
    tick_to: int | None = typer.Option(None, "--tick-to", help="Reserved for API v2"),
    url: str | None = typer.Option(None, "--url", help="Override ARCHETYPE_URL for this command"),
    role: Role | None = ROLE_OPTION,
    token: str | None = typer.Option(None, "--token", help="Bearer token to send verbatim"),
    json_output: bool = typer.Option(False, "--json", help="Emit raw JSON"),
):
    """Show audit history for a world."""
    params = {
        "limit": limit,
        "actor_id": actor_id,
        "idempotency_key": idempotency_key,
        "tick_from": tick_from,
        "tick_to": tick_to,
    }
    data = _request_list(
        "get",
        f"/worlds/{world_id}/history",
        params={key: value for key, value in params.items() if value is not None},
        **_common_request_kwargs(url, role, token),
    )
    if json_output:
        _print_json(data)
        return
    _print_rows(data, empty="No audit history.")


# ── Introspection groups ─────────────────────────────────────────────────


@processors_app.command("list")
def processors_list(
    world_id: str = typer.Argument(..., help="World ID"),
    url: str | None = typer.Option(None, "--url", help="Override ARCHETYPE_URL for this command"),
    role: Role | None = ROLE_OPTION,
    token: str | None = typer.Option(None, "--token", help="Bearer token to send verbatim"),
    json_output: bool = typer.Option(False, "--json", help="Emit raw JSON"),
):
    """List deployment-configured processors."""
    data = _request_list(
        "get",
        f"/worlds/{world_id}/processors",
        **_common_request_kwargs(url, role, token),
    )
    if json_output:
        _print_json(data)
        return
    _print_rows(data, empty="No processors.")


@hooks_app.command("list")
def hooks_list(
    world_id: str = typer.Argument(..., help="World ID"),
    url: str | None = typer.Option(None, "--url", help="Override ARCHETYPE_URL for this command"),
    role: Role | None = ROLE_OPTION,
    token: str | None = typer.Option(None, "--token", help="Bearer token to send verbatim"),
    json_output: bool = typer.Option(False, "--json", help="Emit raw JSON"),
):
    """List deployment-configured hooks."""
    data = _request_list(
        "get", f"/worlds/{world_id}/hooks", **_common_request_kwargs(url, role, token)
    )
    if json_output:
        _print_json(data)
        return
    _print_rows(data, empty="No hooks.")


@resources_app.command("list")
def resources_list(
    world_id: str = typer.Argument(..., help="World ID"),
    url: str | None = typer.Option(None, "--url", help="Override ARCHETYPE_URL for this command"),
    role: Role | None = ROLE_OPTION,
    token: str | None = typer.Option(None, "--token", help="Bearer token to send verbatim"),
    json_output: bool = typer.Option(False, "--json", help="Emit raw JSON"),
):
    """List deployment-configured resources."""
    data = _request_list(
        "get",
        f"/worlds/{world_id}/resources",
        **_common_request_kwargs(url, role, token),
    )
    if json_output:
        _print_json(data)
        return
    _print_rows(data, empty="No resources.")


if __name__ == "__main__":
    app()
