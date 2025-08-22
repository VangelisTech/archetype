import ray
from typing import List

from archetype.core.aio.async_world import AsyncWorld
from archetype.app.models import Command
from archetype.app.auth.models import ActorCtx

@ray.remote
class WorldActor:
    def __init__(self, 
        world_config: WorldConfig, 
        actor_ctx: ActorCtx,
        querier: iAsyncQueryManager,
        updater: iAsyncUpdateManager,
        system: iAsyncSystem,
        ):
        self.world = AsyncWorld(world_config, querier, updater, system)
        self.ctx = actor_ctx

    async def apply_commands(self ):
        #check policies
        pass
        

    async def step(self, commands: List[Command], **input_kwargs):
        await self.world.apply_commands(commands)
        await self.world.step(**input_kwargs)

ray.remote
class SimulationSupervisor:
    """
    Ok so for a ray based simulation orchestrator we use the supervisor pattern over an actor pool. 
    Given an actor ctx (or world ctx if we subclass actor_ctx), we run a given world with its actor ctx id as an 
    actor and then supply a new UUID7 for the run id each time its run. 

    Not sure how this ends up getting implemented with an actor pool but the worlds are short lived processes.
    They are created and destroyed after every run as needed, hydrated by the actor ctx and a fresh run_id which is also a uuid7. 
     """
    def __init__(self):
        self.world_ctxs: Dict[UUID, ActorCtx] = {} 
        self.actor_pool: ActorPool = []

    def spawn_world(self, actor_ctx: ActorCtx = None):
        #world id is locked as a uuid7 

    async def run_simulations(self, world_ids: List[UUID], steps: int = 1):

        futures = [actor.step.remote() for actor in self.]

    async def new_sim

