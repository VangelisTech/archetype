#!/usr/bin/env python3
"""
Hello World: Two agents exchange greetings across ticks.

Demonstrates:
  - Components (entity data as DataFrame columns)
  - Processors (pure DataFrame -> DataFrame transforms)
  - Tick boundaries (Outbox at tick N -> Inbox at tick N+1)
  - Resources (shared config via DI)

Run: uv run python examples/hello_world.py
"""

import asyncio
import json
from dataclasses import dataclass

import daft
import pyarrow as pa
from daft import DataFrame, col

from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.aio.async_system import AsyncSystem
from archetype.core.aio.async_world import AsyncWorld
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import RunConfig, WorldConfig
from archetype.core.resources import Resources


# -- Components ---------------------------------------------------------------


class Agent(Component):
    """Who this entity is."""

    name: str = ""
    role: str = ""


class Inbox(Component):
    """Messages received this tick (JSON strings)."""

    messages: list[str] = []


class Outbox(Component):
    """Messages to send (delivered next tick)."""

    messages: list[str] = []


# -- Resource -----------------------------------------------------------------


@dataclass
class MessageBus:
    """Collects outbox messages and delivers them next tick."""

    pending: dict[int, list[str]] = None  # receiver_id -> [json messages]

    def __post_init__(self):
        self.pending = self.pending or {}

    def enqueue(self, receiver_id: int, message: str):
        self.pending.setdefault(receiver_id, []).append(message)

    def drain(self, receiver_id: int) -> list[str]:
        return self.pending.pop(receiver_id, [])

    def drain_all(self) -> dict[int, list[str]]:
        msgs = dict(self.pending)
        self.pending.clear()
        return msgs


# -- Processors ---------------------------------------------------------------


class DeliverMessages(AsyncProcessor):
    """Moves pending messages from the bus into each agent's Inbox. Runs first."""

    components = (Agent, Inbox)
    priority = -100  # before everything else

    async def process(self, df: DataFrame, resources: Resources, **kw) -> DataFrame:
        bus = resources.require(MessageBus)
        pending = bus.drain_all()

        if not pending:
            # Clear inboxes from last tick
            return df.with_column("inbox__messages", daft.lit([]).cast(daft.DataType.list(daft.DataType.string())))

        # Row-wise UDF: look up this entity's messages
        @daft.func.batch(return_dtype=daft.DataType.list(daft.DataType.string()))
        def deliver(entity_ids: daft.Series) -> list:
            return [pending.get(eid, []) for eid in entity_ids.to_pylist()]

        return df.with_column("inbox__messages", deliver(col("entity_id")))


class SayHello(AsyncProcessor):
    """Each agent reads its inbox and writes a reply to the other's outbox."""

    components = (Agent, Inbox, Outbox)
    priority = 10

    async def process(self, df: DataFrame, resources: Resources, tick: int, **kw) -> DataFrame:
        bus = resources.require(MessageBus)

        # Need cross-row context: who are all the agents?
        # This is one of the justified .collect() cases.
        rows = df.select("entity_id", "agent__name", "inbox__messages").collect().to_pylist()

        all_ids = {r["entity_id"] for r in rows}

        for row in rows:
            eid = row["entity_id"]
            name = row["agent__name"]
            inbox = row["inbox__messages"] or []

            if tick == 0:
                # First tick: say hello to everyone else
                for other_id in all_ids - {eid}:
                    msg = json.dumps({"from": name, "text": f"Hello! I'm {name}."})
                    bus.enqueue(other_id, msg)
            else:
                # Reply to each received message
                for raw in inbox:
                    parsed = json.loads(raw)
                    reply = json.dumps({
                        "from": name,
                        "text": f"Hey {parsed['from']}, nice to meet you!",
                    })
                    # Send back to whoever sent this
                    sender_id = next(
                        r["entity_id"] for r in rows if r["agent__name"] == parsed["from"]
                    )
                    bus.enqueue(sender_id, reply)

        # Clear outbox (messages are already in the bus)
        return df.with_column(
            "outbox__messages",
            daft.lit([]).cast(daft.DataType.list(daft.DataType.string())),
        )


# -- In-memory storage (no persistence needed for toy demo) ------------------


class MemQuerier:
    def __init__(self):
        self._store: dict = {}

    async def query_archetype(self, **kwargs):
        sig = kwargs["sig"]
        ticks = kwargs.get("ticks", [0])
        key = (sig, ticks[0] if ticks else 0)
        if key in self._store:
            return self._store[key]
        schema = Archetype.get_archetype_schema(sig)
        return daft.from_arrow(pa.Table.from_batches([], schema=schema))

    def store(self, sig, tick, df):
        self._store[(sig, tick)] = df


class MemUpdater:
    def __init__(self, querier: MemQuerier):
        self._querier = querier

    async def update(self, df, sig, tick, world_id, run_id):
        df = (
            df.with_column("tick", daft.lit(tick))
            .with_column("world_id", daft.lit(str(world_id)))
            .with_column("run_id", daft.lit(str(run_id)))
        )
        df_mat = df.collect()
        self._querier.store(sig, tick, df_mat)
        return df_mat


# -- Main ---------------------------------------------------------------------


async def main():
    print("Archetype Hello World")
    print("=" * 40)

    # Wire up the world
    querier = MemQuerier()
    world = AsyncWorld(
        world_config=WorldConfig(name="hello"),
        querier=querier,
        updater=MemUpdater(querier),
        system=AsyncSystem(),
    )

    # Shared message bus (Resource)
    bus = MessageBus()
    world.resources.insert(bus)

    # Register processors
    await world.add_processor(DeliverMessages())
    await world.add_processor(SayHello())

    # Spawn two agents
    await world.create_entity([Agent(name="Alice", role="greeter"), Inbox(), Outbox()])
    await world.create_entity([Agent(name="Bob", role="greeter"), Inbox(), Outbox()])

    print("Agents: Alice, Bob")
    print()

    # Run 3 ticks with a hook to print what happens
    async def log_tick(world, tick, **kw):
        results = kw.get("results", [])
        for df in results:
            if not isinstance(df, DataFrame):
                continue
            rows = df.select("agent__name", "inbox__messages").collect().to_pylist()
            for r in rows:
                name = r["agent__name"]
                msgs = r["inbox__messages"] or []
                if msgs:
                    for raw in msgs:
                        parsed = json.loads(raw)
                        print(f"  tick {tick}: {name} received: \"{parsed['text']}\"")
                else:
                    print(f"  tick {tick}: {name} — inbox empty")

    world.add_hook("post_tick", log_tick)

    await world.run(RunConfig(num_steps=3, prefer_live_reads=True))

    print()
    print("Done! Messages flowed across tick boundaries:")
    print("  tick 0: agents write greetings -> bus")
    print("  tick 1: bus delivers -> inboxes, agents reply")
    print("  tick 2: replies delivered -> inboxes")


if __name__ == "__main__":
    asyncio.run(main())
