# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""FastAPI application factory."""

from contextlib import asynccontextmanager
from logging import basicConfig

import logfire
from fastapi import FastAPI

from archetype.api.deps import get_container, set_container
from archetype.api.routes import commands, entities, query, simulation, worlds

logfire.configure(service_name="archetype-ecs")
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

    @app.get("/")
    async def root():
        return {"name": "archetype-ecs", "version": "0.1.1"}

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    return app
