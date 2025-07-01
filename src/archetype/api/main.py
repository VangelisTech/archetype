from fastapi import FastAPI
from .routers import command, world, admin
from archetype.core.aio.async_command import InMemoryBroker
import ray

broker = InMemoryBroker()  # swap for RayBroker in prod
app = FastAPI(title="Archetype Broker API")

@app.on_event("startup")
async def _startup():
    ray.init(address="auto", ignore_reinit_error=True)

def get_broker():
    return broker

app.include_router(command.router, prefix="/v1", dependencies=[Depends(get_broker)])
app.include_router(world.router,   prefix="/v1")
app.include_router(admin.router,   prefix="/ops")
