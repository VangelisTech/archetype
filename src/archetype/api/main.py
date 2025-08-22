from fastapi import FastAPI, Depends, Request
from .routes import command, world, admin
from .deps import DependencyContainer, get_broker
from archetype.app.auth.request_context import current_actor_ctx
from archetype.app.auth.middleware import AuthMiddleware
from archetype.app.auth.models import AuthConfig
import ray

app = FastAPI(title="Archetype Broker API")

# Install auth middleware with defaults (non-strict by default)
app.add_middleware(AuthMiddleware, config=AuthConfig())

@app.on_event("startup")
async def _startup():
    # Initialize dependencies
    DependencyContainer.get_instance()
    # Optionally initialize Ray for distributed execution
    # ray.init(address="auto", ignore_reinit_error=True)

@app.on_event("shutdown")
async def _shutdown():
    container = DependencyContainer.get_instance()
    await container.container.shutdown()
    # ray.shutdown()

app.include_router(command.router, prefix="/v1", dependencies=[Depends(get_broker)])
app.include_router(world.router,   prefix="/v1")
app.include_router(admin.router,   prefix="/ops")

# Request context population for non-middleware consumers (e.g., services)
@app.middleware("http")
async def _set_context(request: Request, call_next):
    # If middleware attached an ActorCtx, stash it in a ContextVar
    ctx = getattr(request.state, "actor_ctx", None)
    token = current_actor_ctx.set(ctx)
    try:
        response = await call_next(request)
    finally:
        current_actor_ctx.reset(token)
    return response
