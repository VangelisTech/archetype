"""Tests for the archetype DSL."""

import pytest
from daft import DataFrame, col

from archetype.core.component import Component
from archetype.dsl import World, behavior
from archetype.dsl.proxy import AgentProxy


# ── Test components ──────────────────────────────────────────────────────────


class Position(Component):
    x: float = 0.0
    y: float = 0.0


class Velocity(Component):
    vx: float = 1.0
    vy: float = 0.0


class Agent(Component):
    name: str = ""
    score: float = 0.0


# ── Test behaviors ───────────────────────────────────────────────────────────


@behavior
class Move:
    requires = [Position, Velocity]
    priority = 10

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return (
            df.with_column("position__x", col("position__x") + col("velocity__vx"))
            .with_column("position__y", col("position__y") + col("velocity__vy"))
        )


@behavior
class ScoreUp:
    requires = [Agent]
    priority = 20

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.with_column("agent__score", col("agent__score") + 1.0)


# ── Tests ────────────────────────────────────────────────────────────────────


class TestBehaviorDecorator:
    def test_creates_processor_class(self):
        assert hasattr(Move, "components")
        assert hasattr(Move, "priority")
        assert Move.components == (Position, Velocity)
        assert Move.priority == 10

    def test_raises_without_requires(self):
        with pytest.raises(TypeError, match="must define 'requires'"):

            @behavior
            class Bad:
                async def process(self, df, **kwargs):
                    return df

    def test_raises_without_process(self):
        with pytest.raises(TypeError, match="must define 'async def process"):

            @behavior
            class Bad:
                requires = [Agent]


class TestAgentProxy:
    def test_attribute_access(self):
        row = {"entity_id": 1, "agent__name": "Alice", "agent__score": 42.0}
        proxy = AgentProxy(row, ["agent"])
        assert proxy.entity_id == 1
        assert proxy.name == "Alice"
        assert proxy.score == 42.0

    def test_missing_attribute(self):
        row = {"entity_id": 1, "agent__name": "Alice"}
        proxy = AgentProxy(row, ["agent"])
        with pytest.raises(AttributeError):
            _ = proxy.nonexistent

    def test_repr(self):
        row = {"entity_id": 1, "agent__name": "Alice", "agent__score": 5.0}
        proxy = AgentProxy(row, ["agent"])
        r = repr(proxy)
        assert "entity_id=1" in r
        assert "name='Alice'" in r


@pytest.mark.asyncio
class TestWorld:
    async def test_spawn_and_run(self):
        async with World("test") as world:
            await world.spawn(Agent(name="Alice", score=0.0))
            world.add_behavior(ScoreUp)
            await world.run(ticks=5)

            agents = world.agents
            assert len(agents) == 1
            assert agents[0].name == "Alice"
            assert agents[0].score == 5.0

    async def test_multiple_entities(self):
        async with World("test") as world:
            world.add_behavior(ScoreUp)
            await world.spawn(Agent(name="Alice"))
            await world.spawn(Agent(name="Bob"))
            await world.run(ticks=3)

            agents = world.agents
            assert len(agents) == 2
            names = {a.name for a in agents}
            assert names == {"Alice", "Bob"}
            for a in agents:
                assert a.score == 3.0

    async def test_movement(self):
        async with World("test") as world:
            world.add_behavior(Move)
            await world.spawn(Position(x=0, y=0), Velocity(vx=2, vy=3))
            await world.run(ticks=4)

            agents = world.agents
            assert len(agents) == 1
            assert agents[0].x == 8.0
            assert agents[0].y == 12.0

    async def test_tick_counter(self):
        async with World("test") as world:
            await world.spawn(Agent(name="A"))
            assert world.tick == 0
            await world.run(ticks=7)
            assert world.tick == 7

    async def test_context_manager_required(self):
        world = World("test")
        with pytest.raises(RuntimeError, match="must be used as"):
            await world.spawn(Agent(name="A"))
