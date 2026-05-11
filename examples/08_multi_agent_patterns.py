#!/usr/bin/env python3
# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Multi-Agent Coordination Patterns
==================================

Five canonical multi-agent coordination patterns, each implemented as a small
self-contained simulation on top of Archetype primitives (Components +
AsyncProcessors + Resources). Source taxonomy: Factory.ai's "Five multi-agent
strategies" (AI Engineer 2026).

| # | Pattern              | Shape                                                  |
|---|----------------------|--------------------------------------------------------|
| 1 | Delegation           | A files a task -> B claims and completes it            |
| 2 | Creator-Verifier     | A produces a draft -> B independently approves/rejects |
| 3 | Direct Communication | Peers talk to each other; no central coordinator       |
| 4 | Negotiation          | Two agents iterate offers over a shared resource       |
| 5 | Broadcast            | One agent emits status; many subscribers receive       |

No external dependencies. Pure simulation; no LLM credentials required.

Run:
    uv run python examples/08_multi_agent_patterns.py
"""

import asyncio
import json
from dataclasses import dataclass, field

import daft
from daft import DataFrame, col

from archetype import ArchetypeRuntime
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.core.resources import Resources

# =============================================================================
# Shared building blocks
# =============================================================================


class Inbox(Component):
    """JSON-encoded incoming messages."""

    messages: list[str] = []


class Outbox(Component):
    """JSON-encoded outgoing messages staged by the agent."""

    messages: list[str] = []


@dataclass
class Mailbox:
    """Shared message spool. Producers append envelopes to ``pending``; the
    delivery processor drains them into per-entity Inboxes at the next tick."""

    pending: list[dict] = field(default_factory=list)
    delivered: int = 0


class MessageDeliveryProcessor(AsyncProcessor):
    """Tick-boundary message router: Mailbox.pending -> Inbox.messages.

    Runs at priority -100 so all later processors observe a populated inbox.
    Messages enqueued at tick N become visible at tick N+1 (causal ordering).
    """

    components = (Inbox,)
    priority = -100

    async def process(
        self,
        df: DataFrame,
        resources: Resources,
        tick: int,
        **kwargs,
    ) -> DataFrame:
        mailbox = resources.require(Mailbox)
        if not mailbox.pending:
            return df

        envelopes = mailbox.pending[:]
        mailbox.pending.clear()
        mailbox.delivered += len(envelopes)

        by_receiver: dict[int, list[str]] = {}
        for env in envelopes:
            by_receiver.setdefault(env["receiver_id"], []).append(json.dumps(env))

        @daft.func.batch(return_dtype=daft.DataType.list(daft.DataType.string()))
        def route(eids: daft.Series, current: daft.Series) -> list:
            out = []
            for eid, inbox in zip(eids.to_pylist(), current.to_pylist(), strict=False):
                inbox = list(inbox) if inbox else []
                out.append(inbox + by_receiver.get(eid, []))
            return out

        return df.with_column("inbox__messages", route(col("entity_id"), col("inbox__messages")))


def _banner(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


async def _latest(world, *components: type[Component]) -> DataFrame:
    """Filter a component query to the most-recent tick (current state).

    ``world.query()`` returns the full append-only history; for a 'snapshot'
    view we keep only the rows from the last tick that ran.
    """
    info = await world.info()
    df = await world.query(*components)
    return df.where(col("tick") == info.tick - 1)


# =============================================================================
# Pattern 1 — DELEGATION
# =============================================================================
# "One agent spawns another for a subtask. The simplest multi-agent pattern."
#
# The Manager files tasks onto a shared TaskBoard each tick. Workers claim the
# oldest pending task on their turn and mark it complete. The board is the
# coordination surface: producers and consumers never reference each other.


class Manager(Component):
    name: str = ""
    delegated: int = 0


class Worker(Component):
    name: str = ""
    completed: int = 0


@dataclass
class TaskBoard:
    pending: list[dict] = field(default_factory=list)
    done: list[dict] = field(default_factory=list)
    next_id: int = 0


class ManagerProcessor(AsyncProcessor):
    components = (Manager,)
    priority = 10

    async def process(
        self,
        df: DataFrame,
        resources: Resources,
        tick: int,
        **kwargs,
    ) -> DataFrame:
        board = resources.require(TaskBoard)
        managers = df.select("entity_id", "manager__name").collect().to_pylist()
        # File two tasks per manager per tick so multiple workers stay engaged.
        filed = 0
        for m in managers:
            for _ in range(2):
                board.pending.append(
                    {
                        "id": board.next_id,
                        "title": f"task-{board.next_id}",
                        "manager_id": m["entity_id"],
                        "manager_name": m["manager__name"],
                        "filed_at": tick,
                    }
                )
                board.next_id += 1
                filed += 1
        return df.with_column(
            "manager__delegated", col("manager__delegated") + (filed // max(len(managers), 1))
        )


class WorkerProcessor(AsyncProcessor):
    components = (Worker,)
    priority = 20

    async def process(
        self,
        df: DataFrame,
        resources: Resources,
        tick: int,
        **kwargs,
    ) -> DataFrame:
        board = resources.require(TaskBoard)
        workers = df.select("entity_id", "worker__name").collect().to_pylist()

        completed_by_eid: dict[int, int] = {}
        for w in workers:
            if not board.pending:
                break
            task = board.pending.pop(0)
            board.done.append(
                {
                    **task,
                    "worker_id": w["entity_id"],
                    "worker_name": w["worker__name"],
                    "done_at": tick,
                }
            )
            completed_by_eid[w["entity_id"]] = completed_by_eid.get(w["entity_id"], 0) + 1

        @daft.func
        def bump(eid: int, current: int) -> int:
            return current + completed_by_eid.get(eid, 0)

        return df.with_column("worker__completed", bump(col("entity_id"), col("worker__completed")))


async def demo_delegation() -> None:
    _banner("Pattern 1 — DELEGATION  (manager files tasks; workers claim)")

    board = TaskBoard()
    async with ArchetypeRuntime() as runtime:
        world = runtime.world(
            "delegation",
            storage=StorageConfig(uri="./archetype_data", namespace="p1_delegation"),
            processors=[ManagerProcessor(), WorkerProcessor()],
            resources=[board],
        )
        await world.spawn(Manager(name="boss"))
        for name in ("worker-1", "worker-2"):
            await world.spawn(Worker(name=name))

        await world.run(steps=5)

        print(f"  filed={board.next_id}  completed={len(board.done)}")
        (await _latest(world, Manager)).select("manager__name", "manager__delegated").show()
        (await _latest(world, Worker)).select("worker__name", "worker__completed").show()


# =============================================================================
# Pattern 2 — CREATOR-VERIFIER
# =============================================================================
# "One agent builds, a different agent checks. Separation of concerns removes
#  sunk-cost bias."
#
# Creator writes one draft per tick onto the DraftBoard. Verifier reads every
# pending draft and applies a verification rule. The rule is independent of
# the Creator's process, so its judgement is not biased by authorship.


class Creator(Component):
    name: str = ""
    drafts_written: int = 0


class Verifier(Component):
    name: str = ""
    approved: int = 0
    rejected: int = 0


@dataclass
class DraftBoard:
    pending: list[dict] = field(default_factory=list)
    decisions: list[dict] = field(default_factory=list)
    next_id: int = 0


class CreatorProcessor(AsyncProcessor):
    components = (Creator,)
    priority = 10

    async def process(
        self,
        df: DataFrame,
        resources: Resources,
        tick: int,
        **kwargs,
    ) -> DataFrame:
        board = resources.require(DraftBoard)
        creators = df.select("entity_id", "creator__name").collect().to_pylist()
        for c in creators:
            content = (
                f"def f(): return {tick * 2}"
                if tick % 2 == 0
                else f"def f(): retrn {tick}"  # intentional typo on odd ticks
            )
            board.pending.append(
                {
                    "id": board.next_id,
                    "author_id": c["entity_id"],
                    "author": c["creator__name"],
                    "content": content,
                    "tick": tick,
                }
            )
            board.next_id += 1
        return df.with_column("creator__drafts_written", col("creator__drafts_written") + 1)


class VerifierProcessor(AsyncProcessor):
    # Negative priority guarantees the Verifier drains drafts from the
    # PREVIOUS tick. Without this, Creator and Verifier (different archetypes)
    # would execute concurrently and race on DraftBoard.pending.
    components = (Verifier,)
    priority = -100

    async def process(
        self,
        df: DataFrame,
        resources: Resources,
        tick: int,
        **kwargs,
    ) -> DataFrame:
        board = resources.require(DraftBoard)
        if not board.pending:
            return df

        drafts = board.pending[:]
        board.pending.clear()

        approved = rejected = 0
        for d in drafts:
            ok = "retrn" not in d["content"]  # independent verification rule
            board.decisions.append({**d, "approved": ok, "verified_at": tick})
            if ok:
                approved += 1
            else:
                rejected += 1

        return df.with_columns(
            {
                "verifier__approved": col("verifier__approved") + approved,
                "verifier__rejected": col("verifier__rejected") + rejected,
            }
        )


async def demo_creator_verifier() -> None:
    _banner("Pattern 2 — CREATOR-VERIFIER  (build vs. check separated)")

    board = DraftBoard()
    async with ArchetypeRuntime() as runtime:
        world = runtime.world(
            "creator-verifier",
            storage=StorageConfig(uri="./archetype_data", namespace="p2_cv"),
            processors=[CreatorProcessor(), VerifierProcessor()],
            resources=[board],
        )
        await world.spawn(Creator(name="coder"))
        await world.spawn(Verifier(name="reviewer"))

        # Run one extra tick beyond the number of drafts so the
        # tick-deferred Verifier processes the final draft.
        await world.run(steps=7)

        print(f"  drafts={board.next_id}  decisions={len(board.decisions)}")
        for d in board.decisions:
            mark = "OK" if d["approved"] else "NO"
            print(f"  [{mark}] tick={d['tick']:>2}  {d['content']}")


# =============================================================================
# Pattern 3 — DIRECT COMMUNICATION
# =============================================================================
# "Agents talking peer-to-peer without a coordinator. Hard to get right:
#  state fragments."
#
# Each peer addresses one specific other peer per tick (round-robin offset by
# tick). Routing happens through MessageDeliveryProcessor, but no peer reads
# global state to decide who to talk to — the addressing is local.


class Peer(Component):
    name: str = ""
    sent: int = 0
    received: int = 0


class PeerOutboundProcessor(AsyncProcessor):
    """Each peer addresses one other peer per tick (deterministic rotation)."""

    components = (Peer, Outbox)
    priority = 10

    async def process(
        self,
        df: DataFrame,
        resources: Resources,
        tick: int,
        **kwargs,
    ) -> DataFrame:
        mailbox = resources.require(Mailbox)
        peers = df.select("entity_id", "peer__name").collect().to_pylist()
        n = len(peers)
        if n < 2:
            return df

        for i, sender in enumerate(peers):
            target = peers[(i + 1 + tick) % n]
            if target["entity_id"] == sender["entity_id"]:
                continue
            mailbox.pending.append(
                {
                    "sender_id": sender["entity_id"],
                    "receiver_id": target["entity_id"],
                    "content": (f"hi {target['peer__name']} from {sender['peer__name']} @t{tick}"),
                    "tick": tick,
                }
            )
        return df.with_column("peer__sent", col("peer__sent") + 1)


class PeerInboundProcessor(AsyncProcessor):
    """Reflect inbox size into the agent's ``received`` counter."""

    components = (Peer, Inbox)
    priority = 20

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        @daft.func
        def count(inbox: list[str]) -> int:
            return len(inbox) if inbox else 0

        return df.with_column("peer__received", count(col("inbox__messages")))


async def demo_direct_communication() -> None:
    _banner("Pattern 3 — DIRECT COMMUNICATION  (peer-to-peer, no coordinator)")

    mailbox = Mailbox()
    async with ArchetypeRuntime() as runtime:
        world = runtime.world(
            "direct",
            storage=StorageConfig(uri="./archetype_data", namespace="p3_direct"),
            processors=[
                MessageDeliveryProcessor(),
                PeerOutboundProcessor(),
                PeerInboundProcessor(),
            ],
            resources=[mailbox],
        )
        for name in ("Alice", "Bob", "Carol", "Dave"):
            await world.spawn(Peer(name=name), Inbox(), Outbox())

        await world.run(steps=4)

        print(f"  messages routed via mailbox: {mailbox.delivered}")
        (await _latest(world, Peer)).select("peer__name", "peer__sent", "peer__received").show()


# =============================================================================
# Pattern 4 — NEGOTIATION
# =============================================================================
# "Two agents coordinate over shared resources. Best when there's a possible
#  win-win."
#
# Buyer wants price <= max_acceptable; Seller wants price >= min_acceptable.
# They iterate offers on a shared NegotiationTable. If the bid/ask cross,
# the deal closes. Each side keeps their bottom line private.


class Negotiator(Component):
    name: str = ""
    role: str = ""  # "buyer" or "seller"
    bottom_line: float = 0.0  # buyer max; seller min
    last_offer: float = 0.0
    rounds: int = 0
    accepted: bool = False


@dataclass
class NegotiationTable:
    buyer_bid: float = 0.0  # 0 means no bid yet
    seller_ask: float = 0.0  # 0 means no ask yet
    deal_price: float = 0.0  # 0 while open
    history: list[dict] = field(default_factory=list)

    @property
    def closed(self) -> bool:
        return self.deal_price > 0.0


class NegotiationProcessor(AsyncProcessor):
    components = (Negotiator,)
    priority = 10

    async def process(
        self,
        df: DataFrame,
        resources: Resources,
        tick: int,
        **kwargs,
    ) -> DataFrame:
        table = resources.require(NegotiationTable)
        rows = (
            df.select(
                "entity_id",
                "negotiator__role",
                "negotiator__bottom_line",
                "negotiator__last_offer",
            )
            .collect()
            .to_pylist()
        )

        new_offers: dict[int, float] = {}
        accepted_eids: set[int] = set()

        if not table.closed:
            for r in rows:
                role = r["negotiator__role"]
                bottom = r["negotiator__bottom_line"]
                last = r["negotiator__last_offer"]

                if role == "buyer":
                    if table.seller_ask and table.seller_ask <= bottom:
                        table.deal_price = table.seller_ask
                        accepted_eids.add(r["entity_id"])
                        break
                    bid = (last + 5.0) if last else 50.0
                    bid = min(bid, bottom)
                    table.buyer_bid = bid
                    new_offers[r["entity_id"]] = bid
                elif role == "seller":
                    if table.buyer_bid and table.buyer_bid >= bottom:
                        table.deal_price = table.buyer_bid
                        accepted_eids.add(r["entity_id"])
                        break
                    ask = (last - 5.0) if last else 100.0
                    ask = max(ask, bottom)
                    table.seller_ask = ask
                    new_offers[r["entity_id"]] = ask

        table.history.append(
            {
                "tick": tick,
                "bid": table.buyer_bid,
                "ask": table.seller_ask,
                "deal": table.deal_price,
            }
        )

        deal_closed = table.closed

        @daft.func
        def update_offer(eid: int, current: float) -> float:
            return new_offers.get(eid, current)

        @daft.func
        def update_accepted(eid: int, current: bool) -> bool:
            return current or deal_closed

        return df.with_columns(
            {
                "negotiator__last_offer": update_offer(
                    col("entity_id"), col("negotiator__last_offer")
                ),
                "negotiator__rounds": col("negotiator__rounds") + 1,
                "negotiator__accepted": update_accepted(
                    col("entity_id"), col("negotiator__accepted")
                ),
            }
        )


async def demo_negotiation() -> None:
    _banner("Pattern 4 — NEGOTIATION  (buyer/seller iterate on shared table)")

    table = NegotiationTable()
    async with ArchetypeRuntime() as runtime:
        world = runtime.world(
            "negotiation",
            storage=StorageConfig(uri="./archetype_data", namespace="p4_negotiation"),
            processors=[NegotiationProcessor()],
            resources=[table],
        )
        # Buyer is willing to pay up to 80; seller will accept down to 70.
        # Overlap exists -> deal should close.
        await world.spawn(Negotiator(name="buyer", role="buyer", bottom_line=80.0))
        await world.spawn(Negotiator(name="seller", role="seller", bottom_line=70.0))

        await world.run(steps=10)

        print(f"  closed={table.closed}  deal_price={table.deal_price}")
        print("  round-by-round (bid / ask / deal):")
        for h in table.history:
            print(f"    t={h['tick']:>2}  bid={h['bid']:>5}  ask={h['ask']:>5}  deal={h['deal']}")
        (await _latest(world, Negotiator)).select(
            "negotiator__name",
            "negotiator__role",
            "negotiator__last_offer",
            "negotiator__rounds",
            "negotiator__accepted",
        ).show()


# =============================================================================
# Pattern 5 — BROADCAST
# =============================================================================
# "One agent sends status updates and shared context to many. Critical for
#  coherence."
#
# A Coordinator publishes one bulletin per tick to the BroadcastBus. Every
# Subscriber receives every bulletin in the bus on the same tick (fan-out).


class Coordinator(Component):
    name: str = ""
    bulletins_sent: int = 0


class Subscriber(Component):
    name: str = ""
    bulletins_received: int = 0
    last_bulletin: str = ""


@dataclass
class BroadcastBus:
    pending: list[str] = field(default_factory=list)
    delivered: int = 0


class BroadcastPublishProcessor(AsyncProcessor):
    components = (Coordinator,)
    priority = 10

    async def process(
        self,
        df: DataFrame,
        resources: Resources,
        tick: int,
        **kwargs,
    ) -> DataFrame:
        bus = resources.require(BroadcastBus)
        coords = df.select("entity_id", "coordinator__name").collect().to_pylist()
        for c in coords:
            bus.pending.append(
                json.dumps(
                    {
                        "from": c["coordinator__name"],
                        "tick": tick,
                        "status": f"all systems nominal @ tick {tick}",
                    }
                )
            )
        return df.with_column("coordinator__bulletins_sent", col("coordinator__bulletins_sent") + 1)


class BroadcastDeliveryProcessor(AsyncProcessor):
    """Fan-out: every subscriber gets every bulletin published in the
    previous tick. Negative priority guarantees this drains the bus before
    any other processor — and decouples publisher and subscriber archetypes,
    which would otherwise execute concurrently and race on the bus."""

    components = (Subscriber,)
    priority = -100

    async def process(
        self,
        df: DataFrame,
        resources: Resources,
        **kwargs,
    ) -> DataFrame:
        bus = resources.require(BroadcastBus)
        if not bus.pending:
            return df

        bulletins = bus.pending[:]
        bus.pending.clear()
        bus.delivered += len(bulletins)
        last = bulletins[-1]
        n = len(bulletins)

        return df.with_columns(
            {
                "subscriber__bulletins_received": (col("subscriber__bulletins_received") + n),
                "subscriber__last_bulletin": daft.lit(last),
            }
        )


async def demo_broadcast() -> None:
    _banner("Pattern 5 — BROADCAST  (one publisher, many subscribers)")

    bus = BroadcastBus()
    async with ArchetypeRuntime() as runtime:
        world = runtime.world(
            "broadcast",
            storage=StorageConfig(uri="./archetype_data", namespace="p5_broadcast"),
            processors=[BroadcastPublishProcessor(), BroadcastDeliveryProcessor()],
            resources=[bus],
        )
        await world.spawn(Coordinator(name="ops"))
        for name in ("svc-a", "svc-b", "svc-c", "svc-d"):
            await world.spawn(Subscriber(name=name))

        await world.run(steps=4)

        print(f"  bulletins delivered: {bus.delivered}")
        (await _latest(world, Subscriber)).select(
            "subscriber__name",
            "subscriber__bulletins_received",
            "subscriber__last_bulletin",
        ).show()


# =============================================================================
# Main — run every pattern in sequence
# =============================================================================


async def main() -> None:
    print("=" * 72)
    print("Archetype: Five Multi-Agent Coordination Patterns")
    print("=" * 72)

    await demo_delegation()
    await demo_creator_verifier()
    await demo_direct_communication()
    await demo_negotiation()
    await demo_broadcast()

    print("\n" + "=" * 72)
    print("Done. Each pattern composed from Components + Processors + Resources.")
    print("=" * 72)


if __name__ == "__main__":
    asyncio.run(main())
