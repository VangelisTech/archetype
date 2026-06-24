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
from logging import basicConfig

import logfire
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from logfire.exceptions import LogfireConfigError

from archetype.api.deps import get_container, set_container
from archetype.api.routes import commands, entities, inspector, query, simulation, worlds

try:
    logfire.configure(service_name="archetype-ecs", send_to_logfire=False)
except LogfireConfigError:
    # No Logfire credentials on this machine — degrade to local-only
    # instrumentation instead of refusing to import.
    logfire.configure(service_name="archetype-ecs", send_to_logfire=False)
basicConfig(handlers=[logfire.LogfireLoggingHandler()])


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: initialize and shutdown ServiceContainer."""
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
        version="0.1.1",
        lifespan=lifespan,
    )
    logfire.instrument_fastapi(app)

    app.include_router(worlds.router)
    app.include_router(entities.router)
    app.include_router(commands.router)
    app.include_router(simulation.router)
    app.include_router(query.router)
    app.include_router(inspector.router)

    inspector_output = inspector._OUTPUT_DIR
    inspector_output.mkdir(parents=True, exist_ok=True)
    app.mount(
        "/inspector/live-agent/files",
        StaticFiles(directory=str(inspector_output), html=True),
        name="live-agent-inspector-files",
    )

    @app.get("/")
    async def root():
        return {"name": "archetype-ecs", "version": "0.1.1"}

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    return app
