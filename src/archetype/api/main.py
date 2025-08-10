from fastapi import FastAPI
from .routes import command, world, admin
from archetype.app.broker import CommandBroker
import ray

broker = CommandBroker()  # swap for RayBroker in prod
app = FastAPI(title="Archetype Broker API")

@app.on_event("startup")
async def _startup():
    ray.init(address="auto", ignore_reinit_error=True)

@app.on_event("shutdown")
async def _shutdown():
    await app.shutdown()
    ray.shutdown()

def get_broker():
    return broker

app.include_router(command.router, prefix="/v1", dependencies=[Depends(get_broker)])
app.include_router(world.router,   prefix="/v1")
app.include_router(admin.router,   prefix="/ops")
