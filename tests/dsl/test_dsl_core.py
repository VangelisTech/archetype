# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Tests for archetype.dsl core functionality.

Tests cover:
- AgentProxy and ComponentProxy
- @behavior decorator
- World context manager
- spawn_world() for inner simulations
- Auto message realization
"""

import pytest

from archetype.core.component import Component
from archetype.dsl import Inbox, World, behavior, spawn_world
from archetype.dsl.core import (
    AgentProxy,
    BehaviorSpec,
    ComponentProxy,
    component_column,
    component_prefix,
)

# =============================================================================
# Test Components (prefixed with Sample to avoid pytest collection)
# =============================================================================


class SamplePerspective(Component):
    name: str = ""
    type: str = ""


class SampleState(Component):
    value: int = 0
    history_json: str = "[]"


# =============================================================================
# Unit Tests: Component Utilities
# =============================================================================


class TestComponentUtilities:
    def test_component_prefix(self):
        assert component_prefix(SamplePerspective) == "sampleperspective__"
        assert component_prefix(SampleState) == "samplestate__"

    def test_component_column(self):
        assert component_column(SamplePerspective, "name") == "sampleperspective__name"
        assert component_column(SampleState, "value") == "samplestate__value"


# =============================================================================
# Unit Tests: ComponentProxy
# =============================================================================


class TestComponentProxy:
    def test_getattr_returns_value(self):
        row_data = {"samplestate__value": 42, "samplestate__history_json": "[]"}
        mutations = {}
        proxy = ComponentProxy(SampleState, row_data, mutations)

        assert proxy.value == 42
        assert proxy.history_json == []  # Auto-deserialized

    def test_setattr_tracks_mutation(self):
        row_data = {"samplestate__value": 0}
        mutations = {}
        proxy = ComponentProxy(SampleState, row_data, mutations)

        proxy.value = 100

        assert mutations["samplestate__value"] == 100

    def test_setattr_serializes_list(self):
        row_data = {"samplestate__history_json": "[]"}
        mutations = {}
        proxy = ComponentProxy(SampleState, row_data, mutations)

        proxy.history_json = [{"a": 1}]

        assert mutations["samplestate__history_json"] == '[{"a": 1}]'

    def test_getattr_prefers_mutations(self):
        row_data = {"samplestate__value": 0}
        mutations = {"samplestate__value": 99}
        proxy = ComponentProxy(SampleState, row_data, mutations)

        assert proxy.value == 99

    def test_getattr_raises_for_unknown_field(self):
        row_data = {}
        mutations = {}
        proxy = ComponentProxy(SampleState, row_data, mutations)

        with pytest.raises(AttributeError, match="no field 'unknown'"):
            _ = proxy.unknown


# =============================================================================
# Unit Tests: AgentProxy
# =============================================================================


class TestAgentProxy:
    def test_component_access_via_snake_case(self):
        row_data = {
            "entity_id": 1,
            "sampleperspective__name": "Alice",
            "sampleperspective__type": "objective",
        }
        proxy = AgentProxy(
            entity_id=1,
            row_data=row_data,
            component_classes=[SamplePerspective],
            world=None,
        )

        assert proxy.sample_perspective.name == "Alice"
        assert proxy.sample_perspective.type == "objective"

    def test_mutations_tracked_across_components(self):
        row_data = {
            "entity_id": 1,
            "sampleperspective__name": "Alice",
            "samplestate__value": 0,
        }
        proxy = AgentProxy(
            entity_id=1,
            row_data=row_data,
            component_classes=[SamplePerspective, SampleState],
            world=None,
        )

        proxy.sample_state.value = 42
        proxy.sample_perspective.name = "Bob"

        mutations = proxy.get_mutations()
        assert mutations["samplestate__value"] == 42
        assert mutations["sampleperspective__name"] == "Bob"

    def test_clear_mutations(self):
        row_data = {"entity_id": 1, "samplestate__value": 0}
        proxy = AgentProxy(
            entity_id=1,
            row_data=row_data,
            component_classes=[SampleState],
            world=None,
        )

        proxy.sample_state.value = 100
        assert len(proxy.get_mutations()) == 1

        proxy.clear_mutations()
        assert len(proxy.get_mutations()) == 0


# =============================================================================
# Unit Tests: @behavior decorator
# =============================================================================


class TestBehaviorDecorator:
    def test_creates_behavior_spec(self):
        @behavior
        class MyBehavior:
            requires = [SamplePerspective, SampleState]
            priority = 5
            runs_on = "final_tick"

            async def act(self, agent, world, tick):
                pass

        assert isinstance(MyBehavior, BehaviorSpec)
        assert MyBehavior.name == "MyBehavior"
        assert MyBehavior.requires == [SamplePerspective, SampleState]
        assert MyBehavior.priority == 5
        assert MyBehavior.runs_on == "final_tick"

    def test_default_values(self):
        @behavior
        class MinimalBehavior:
            async def act(self, agent, world, tick):
                pass

        assert MinimalBehavior.requires == []
        assert MinimalBehavior.priority == 10
        assert MinimalBehavior.runs_on is None


# =============================================================================
# Integration Tests: World
# =============================================================================


class TestWorld:
    @pytest.mark.asyncio
    async def test_world_context_manager(self, tmp_path):
        async with World("test", storage=tmp_path / "data") as world:
            assert world.name == "test"
            assert world._orchestrator is not None
            assert world._broker is not None

    @pytest.mark.asyncio
    async def test_spawn_creates_entity(self, tmp_path):
        async with World("test", storage=tmp_path / "data") as world:
            await world.spawn(
                SamplePerspective(name="Alice", type="objective"),
                SampleState(value=42),
            )
            await world.run(ticks=1)

            assert len(world.agents) == 1
            agent = world.agents[0]
            assert agent.sample_perspective.name == "Alice"
            assert agent.sample_state.value == 42

    @pytest.mark.asyncio
    async def test_find_one(self, tmp_path):
        async with World("test", storage=tmp_path / "data") as world:
            await world.spawn(SamplePerspective(name="Alice", type="a"))
            await world.spawn(SamplePerspective(name="Bob", type="b"))
            await world.run(ticks=1)

            bob = await world.find_one(lambda a: a.sample_perspective.name == "Bob")
            assert bob is not None
            assert bob.sample_perspective.type == "b"

    @pytest.mark.asyncio
    async def test_find(self, tmp_path):
        async with World("test", storage=tmp_path / "data") as world:
            await world.spawn(SamplePerspective(name="A", type="x"))
            await world.spawn(SamplePerspective(name="B", type="x"))
            await world.spawn(SamplePerspective(name="C", type="y"))
            await world.run(ticks=1)

            x_agents = await world.find(lambda a: a.sample_perspective.type == "x")
            assert len(x_agents) == 2

    @pytest.mark.asyncio
    async def test_broadcast(self, tmp_path):
        async with World("test", storage=tmp_path / "data") as world:
            await world.spawn(
                SamplePerspective(name="Alice"),
                Inbox(messages_json="[]"),
            )
            await world.spawn(
                SamplePerspective(name="Bob"),
                Inbox(messages_json="[]"),
            )
            await world.run(ticks=1)

            alice = await world.find_one(lambda a: a.sample_perspective.name == "Alice")
            await world.broadcast("Hello!", sender=alice, exclude=[alice])

            # Check broker has pending messages
            pending = await world.broker.get_pending_count(world.name)
            assert pending == 1  # One message to Bob


# =============================================================================
# Integration Tests: spawn_world
# =============================================================================


class TestSpawnWorld:
    @pytest.mark.asyncio
    async def test_spawn_world_creates_child(self, tmp_path):
        async with World("parent", storage=tmp_path / "parent") as parent:
            await parent.spawn(SamplePerspective(name="ParentAgent"))
            await parent.run(ticks=1)

            async with spawn_world("child", parent=parent) as child:
                await child.spawn(SamplePerspective(name="ChildAgent"))
                await child.run(ticks=1)

                assert len(child.agents) == 1
                assert child.agents[0].sample_perspective.name == "ChildAgent"

            # Parent still has its agent
            assert len(parent.agents) == 1
            assert parent.agents[0].sample_perspective.name == "ParentAgent"

    @pytest.mark.asyncio
    async def test_spawn_world_inherits_broker(self, tmp_path):
        async with World("parent", storage=tmp_path / "parent") as parent:
            async with spawn_world("child", parent=parent) as child:
                assert child.broker is parent.broker

    @pytest.mark.asyncio
    async def test_spawn_world_fork_state(self, tmp_path):
        async with World("parent", storage=tmp_path / "parent") as parent:
            await parent.spawn(SamplePerspective(name="Original", type="test"))
            await parent.run(ticks=1)

            async with spawn_world("child", parent=parent, fork_state=True) as child:
                await child.run(ticks=1)

                # Child should have copy of parent's agent
                assert len(child.agents) == 1
                assert child.agents[0].sample_perspective.name == "Original"


# =============================================================================
# Integration Tests: Behavior Execution
# =============================================================================


class TestBehaviorExecution:
    @pytest.mark.asyncio
    async def test_behavior_modifies_state(self, tmp_path):
        @behavior
        class Incrementer:
            requires = [SampleState]

            async def act(self, agent, world, tick):
                agent.sample_state.value = agent.sample_state.value + 10

        async with World("test", storage=tmp_path / "data") as world:
            world.add_behavior(Incrementer)
            await world.spawn(SampleState(value=0))
            await world.run(ticks=3)

            assert world.agents[0].sample_state.value == 30

    @pytest.mark.asyncio
    async def test_behavior_runs_on_final_tick(self, tmp_path):
        call_count = {"n": 0}

        @behavior
        class FinalOnly:
            requires = [SampleState]
            runs_on = "final_tick"

            async def act(self, agent, world, tick):
                call_count["n"] += 1

        async with World("test", storage=tmp_path / "data") as world:
            world.add_behavior(FinalOnly)
            await world.spawn(SampleState())
            await world.run(ticks=3)

            assert call_count["n"] == 1

    @pytest.mark.asyncio
    async def test_behavior_filter(self, tmp_path):
        modified = []

        @behavior
        class SelectiveModifier:
            requires = [SamplePerspective, SampleState]
            filter = lambda a: a.sample_perspective.type == "special"

            async def act(self, agent, world, tick):
                modified.append(agent.sample_perspective.name)

        async with World("test", storage=tmp_path / "data") as world:
            world.add_behavior(SelectiveModifier)
            await world.spawn(SamplePerspective(name="A", type="normal"), SampleState())
            await world.spawn(SamplePerspective(name="B", type="special"), SampleState())
            await world.spawn(SamplePerspective(name="C", type="normal"), SampleState())
            await world.run(ticks=1)

            assert modified == ["B"]
