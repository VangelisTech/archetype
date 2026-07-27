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

"""FastAPI application factory."""

import ipaddress
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from archetype import __version__
from archetype._logging import configure_host_observability
from archetype.api.deps import Authenticator
from archetype.api.routes import commands, entities, missions, query, simulation, worlds

_SERVE_HOST_ENV = "ARCHETYPE_SERVE_HOST"
_SERVE_DEV_AUTH_ENV = "ARCHETYPE_SERVE_DEV_AUTH"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Own one process resource graph for the exact FastAPI lifespan."""
    if hasattr(app.state, "resources"):
        retained = app.state.resources
        await retained.aclose()
        del app.state.resources

    # Composition stays lazy: importing the ASGI module and calling
    # ``create_app`` must not construct providers, catalogs, or tasks.
    from archetype.wiring import RuntimeBootstrapConfig, build_runtime_resources

    configure_host_observability(service_name="archetype-api")
    resources = build_runtime_resources(RuntimeBootstrapConfig.from_env())
    app.state.resources = resources
    try:
        yield
    finally:
        await resources.aclose()
        del app.state.resources


def _is_loopback_host(host: str) -> bool:
    candidate = host.strip().strip("[]")
    if candidate.casefold() == "localhost":
        return True
    try:
        return ipaddress.ip_address(candidate).is_loopback
    except ValueError:
        return False


def create_app(
    *,
    authenticator: Authenticator | None = None,
    dev_auth: bool = False,
    bind_host: str | None = None,
) -> FastAPI:
    """Create the FastAPI application with explicit authentication."""
    if dev_auth and authenticator is not None:
        raise ValueError("development auth and an injected authenticator are mutually exclusive")
    if dev_auth and (bind_host is None or not _is_loopback_host(bind_host)):
        raise ValueError("development auth requires an explicit loopback bind host")

    app = FastAPI(
        title="Archetype ECS",
        description="Dataframe-first ECS runtime for simulations and AI agents.",
        version=__version__,
        lifespan=lifespan,
    )
    app.state.authenticator = authenticator
    app.state.development_auth = dev_auth
    app.include_router(worlds.router)
    app.include_router(entities.router)
    app.include_router(commands.router)
    app.include_router(simulation.router)
    app.include_router(query.router)
    app.include_router(missions.router)

    @app.get("/")
    async def root():
        return {"name": "archetype-ecs", "version": __version__}

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    return app


def _create_cli_app() -> FastAPI:
    """Rebuild the CLI-selected app inside Uvicorn reload/worker processes."""
    host = os.environ.get(_SERVE_HOST_ENV)
    dev_auth = os.environ.get(_SERVE_DEV_AUTH_ENV) == "1"
    return create_app(dev_auth=dev_auth, bind_host=host)
