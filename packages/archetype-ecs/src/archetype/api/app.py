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

from collections.abc import Mapping
from contextlib import asynccontextmanager
from types import MappingProxyType

from fastapi import APIRouter, FastAPI

from archetype import __version__
from archetype._logging import configure_host_observability
from archetype.api.routes import FRAMEWORK_ROUTERS
from archetype.world_libraries import WorldLibraryManifest, resolve_world_libraries


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Own one process resource graph for the exact FastAPI lifespan."""
    if hasattr(app.state, "resources"):
        retained = app.state.resources
        await retained.aclose()
        del app.state.resources

    # Composition stays lazy: importing the ASGI module and calling
    # ``create_app`` must not construct providers, catalogs, or tasks.
    from archetype.api.principals import (
        MissionPrincipalDirectory,
        bind_host_from_env,
    )
    from archetype.wiring import RuntimeBootstrapConfig, build_runtime_resources

    configure_host_observability(service_name="archetype-api")
    missions_installed = any(
        manifest.name == "missions" for manifest in app.state.world_library_manifests
    )
    mission_principals = (
        MissionPrincipalDirectory.from_env()
        if missions_installed
        else MissionPrincipalDirectory.empty()
    )
    if missions_installed:
        mission_principals.require_non_loopback_configuration(bind_host_from_env())
    resources = build_runtime_resources(
        RuntimeBootstrapConfig.from_env(
            world_libraries=app.state.world_library_manifests,
            world_library_configs=app.state.world_library_configs,
        )
    )
    app.state.resources = resources
    app.state.mission_principals = mission_principals
    try:
        yield
    finally:
        await resources.aclose()
        del app.state.resources
        del app.state.mission_principals


def create_app(
    *,
    world_libraries: tuple[WorldLibraryManifest, ...] | None = None,
    world_library_configs: Mapping[str, object] | None = None,
) -> FastAPI:
    """Create and configure the FastAPI application."""
    manifests = resolve_world_libraries(
        world_libraries,
        framework_version=__version__,
    )
    configs = dict(world_library_configs or {})
    unknown_configs = set(configs) - {manifest.name for manifest in manifests}
    if unknown_configs:
        raise ValueError(
            "world-library configs have no resolved manifest: " + ", ".join(sorted(unknown_configs))
        )
    app = FastAPI(
        title="Archetype ECS",
        description="Dataframe-first ECS runtime for simulations and AI agents.",
        version=__version__,
        lifespan=lifespan,
    )
    app.state.world_library_manifests = manifests
    app.state.world_library_configs = MappingProxyType(configs)
    for router in FRAMEWORK_ROUTERS:
        app.include_router(router)
    for manifest in manifests:
        for factory in manifest.api_router_factories:
            router = factory()
            if not isinstance(router, APIRouter):
                raise TypeError(
                    f"world library {manifest.name!r} API factory did not return APIRouter"
                )
            app.include_router(router)

    @app.get("/")
    async def root():
        return {"name": "archetype-ecs", "version": __version__}

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    return app
