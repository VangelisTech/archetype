import asyncio, json, ssl
import daft, nats, ulid
from daft import DataFrame
from nats.aio.client import Client as NATS
from nats.js.api import StreamConfig
from components.chat import ChatPrompt, ChatCompletion
from ..core.system import System     # sequential base you already have

class NATSTransportSystem(System):
    def __init__(self, world, url="tls://nats.local:4222", ssl_ctx=None):
        super().__init__()
        self.world = world
        self.loop  = asyncio.get_event_loop()
        self.nc    = self.loop.run_until_complete(
            NATS().connect(servers=[url], tls=ssl_ctx or ssl.create_default_context())
        )
        self.js = self.loop.run_until_complete(self.nc.jetstream())
        self.responses: list[dict] = []

        # One durable stream for all chat I/O
        self.loop.run_until_complete(
            self.js.add_stream(StreamConfig(name="CHAT", subjects=["chat.*.*.*"]))
        )
        # Async listener for completions
        self.loop.run_until_complete(
            self.nc.subscribe(
                f"chat.{world.id}.*.resp",
                cb=self._on_resp,
                pending_msgs_limit=512,
            )
        )

    async def _on_resp(self, msg):
        data = json.loads(msg.data)
        self.responses.append(data)

    # --------------- ECS hook -----------------
    def execute(self, dt=0.0) -> DataFrame | None:
        step = self.world.current_step
        df   = self.world.store.get_components(ChatPrompt, steps=step)
        if df and len(df) > 0:
            self.loop.run_until_complete(self._send(df))
        if not self.responses:
            return None
        df_out = daft.from_pylist(self.responses)
        self.responses = []
        return df_out.select(
            "entity_id",
            "episode_id",
            # map into component-prefixed columns
            daft.col("text").alias("chatcompletion__text"),
            daft.col("episode_id").alias("chatcompletion__episode_id"),
        )

    async def _send(self, prompt_df: DataFrame):
        for row in prompt_df.to_pydict():
            subj = f"chat.{self.world.id}.{row['episode_id']}.req"
            await self.js.publish(subj, json.dumps(row).encode())

    async def interact(self, typer: import Typer):
        await self.nc.publish(f"chat.{self.world.id}.{row['episode_id']}.req", json.dumps(row).encode())
    


class SimpleNatsMessagingSystem(System):
    async def __init__(self, ssl_ctx = ssl.create_default_context(cafile="ca.pem")):

        self.nc = NATS()
        await self.nc.connect(
            servers=[f"tls://nats.default_world.local:4222"],
            tls=ssl_ctx,
            user_credentials="creds/agent42.creds",  # JWT + NKey
            name="world-manager",
            reconnect_time_wait=2,
        )

        self.js = self.nc.jetstream()
        # One durable stream for **all** sim traffic
        await self.js.add_stream(
            StreamConfig(name="SIM", subjects=["sim.>"], max_age=3600_000_000)
        )

        async def _inbox_cb(msg):
            subj, data = msg.subject, json.loads(msg.data)
            entity_id = subj.split(".")[3]
            # append message to Inbox component…

        async def sub(self, zone, entity_id):
            pass
            

        async def pub(self, df: daft.DataFrame):
            for row in df.to_pydict():
                subj = f"sim.worldA.zone1.{row['id']}.out"
                await self.nc.publish(subj, json.dumps(row["packet"]).encode())