# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""FastAPI application factory."""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from archetype.api.deps import get_container, set_container
from archetype.api.routes import chat, commands, query, simulation, worlds


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: initialize and shutdown ServiceContainer."""
    container = get_container()
    try:
        yield
    finally:
        await container.shutdown()
        set_container(None)


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="Archetype ECS",
        description="AI-Native Simulation Engine API",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.include_router(worlds.router)
    app.include_router(commands.router)
    app.include_router(simulation.router)
    app.include_router(query.router)
    app.include_router(chat.router)

    @app.get("/")
    async def root():
        return {"name": "archetype-ecs", "version": "0.1.0"}

    return app
