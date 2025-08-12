from fastapi import FastAPI, Depends
from .routes import command, world, admin
from .deps import DependencyContainer, get_broker
import ray

app = FastAPI(title="Archetype Broker API")

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
