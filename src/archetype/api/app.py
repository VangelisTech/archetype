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

from contextlib import asynccontextmanager

from fastapi import FastAPI

from archetype import __version__
from archetype._logging import configure_host_observability
from archetype.api.deps import get_container, set_container
from archetype.api.routes import commands, entities, query, simulation, worlds


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: initialize and shutdown ServiceContainer."""
    configure_host_observability(service_name="archetype-api")
    container = get_container()
    app.state.container = container
    try:
        yield
    finally:
        await container.shutdown()
        app.state.container = None
        set_container(None)


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="Archetype ECS",
        description="Dataframe-first ECS runtime for simulations and AI agents.",
        version=__version__,
        lifespan=lifespan,
    )
    app.include_router(worlds.router)
    app.include_router(entities.router)
    app.include_router(commands.router)
    app.include_router(simulation.router)
    app.include_router(query.router)

    @app.get("/")
    async def root():
        return {"name": "archetype-ecs", "version": __version__}

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    return app
