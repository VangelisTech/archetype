# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Tests for the synchronous API (archetype.init / ArchetypeSim)

Tests cover:
- ArchetypeSim initialization
- World creation and management
- Entity spawning
- Processor execution
- World stepping
"""

import pytest
from daft import DataFrame, col

import archetype
from archetype import Component, Processor


# =============================================================================
# Test Components
# =============================================================================


class Position(Component):
    x: float = 0.0
    y: float = 0.0


class Velocity(Component):
    vx: float = 0.0
    vy: float = 0.0


class Health(Component):
    current: int = 100
    max: int = 100


# =============================================================================
# Test Processors
# =============================================================================


class MovementProcessor(Processor):
    components = (Position, Velocity)
    priority = 10

    def process(self, df: DataFrame, dt: float = 1.0, **kwargs) -> DataFrame:
        return df.with_columns(
            {
                "position__x": col("position__x") + col("velocity__vx") * dt,
                "position__y": col("position__y") + col("velocity__vy") * dt,
            }
        )


class DamageProcessor(Processor):
    components = (Health,)
    priority = 20

    def process(self, df: DataFrame, damage: int = 10, **kwargs) -> DataFrame:
        return df.with_column(
            "health__current",
            col("health__current") - damage,
        )


# =============================================================================
# Tests: ArchetypeSim Initialization
# =============================================================================


class TestArchetypeSimInit:
    def test_init_creates_sim(self, tmp_path):
        sim = archetype.init(str(tmp_path / "data"))
        assert sim is not None
        assert sim.uri.exists()

    def test_init_with_namespace(self, tmp_path):
        sim = archetype.init(str(tmp_path / "data"), namespace="custom")
        assert sim.namespace == "custom"

    def test_init_with_debug(self, tmp_path):
        sim = archetype.init(str(tmp_path / "data"), debug=True)
        assert sim.debug is True


# =============================================================================
# Tests: World Management
# =============================================================================


class TestWorldManagement:
    def test_spawn_world(self, tmp_path):
        sim = archetype.init(str(tmp_path / "data"))
        world_id = sim.spawn_world("test_world")

        assert world_id is not None
        assert len(world_id) > 0

    def test_get_world(self, tmp_path):
        sim = archetype.init(str(tmp_path / "data"))
        world_id = sim.spawn_world("test")
        world = sim.get_world(world_id)

        assert world is not None

    def test_multiple_worlds(self, tmp_path):
        sim = archetype.init(str(tmp_path / "data"))
        world_id_1 = sim.spawn_world("world_1")
        world_id_2 = sim.spawn_world("world_2")

        assert world_id_1 != world_id_2

        world_1 = sim.get_world(world_id_1)
        world_2 = sim.get_world(world_id_2)

        assert world_1 is not world_2


# =============================================================================
# Tests: Entity Spawning
# =============================================================================


class TestEntitySpawning:
    def test_spawn_entity(self, tmp_path):
        sim = archetype.init(str(tmp_path / "data"))
        world_id = sim.spawn_world("test")

        entity_id = sim.spawn_entity(
            world_id,
            Position(x=10.0, y=20.0),
            Velocity(vx=1.0, vy=2.0),
        )

        assert entity_id is not None

    def test_spawn_multiple_entities(self, tmp_path):
        sim = archetype.init(str(tmp_path / "data"))
        world_id = sim.spawn_world("test")

        ids = []
        for i in range(5):
            eid = sim.spawn_entity(world_id, Position(x=float(i), y=0.0))
            ids.append(eid)

        assert len(set(ids)) == 5  # All unique


# =============================================================================
# Tests: Processor Execution
# =============================================================================


class TestProcessorExecution:
    def test_add_processor(self, tmp_path):
        sim = archetype.init(str(tmp_path / "data"))
        world_id = sim.spawn_world("test")

        processor = MovementProcessor()
        sim.add_processor_to_world(world_id, processor)

        # Should not raise
        world = sim.get_world(world_id)
        assert len(world.system.processors) >= 1

    def test_step_world_runs_processor(self, tmp_path):
        sim = archetype.init(str(tmp_path / "data"))
        world_id = sim.spawn_world("physics")

        sim.add_processor_to_world(world_id, MovementProcessor())
        sim.spawn_entity(
            world_id,
            Position(x=0.0, y=0.0),
            Velocity(vx=10.0, vy=5.0),
        )

        sim.step_world(world_id, dt=1.0)

        # Verify position updated
        world = sim.get_world(world_id)
        # Access live state
        for sig, df in world._live.items():
            rows = df.collect().to_pylist()
            if rows:
                assert rows[0]["position__x"] == 10.0
                assert rows[0]["position__y"] == 5.0

    def test_multiple_steps(self, tmp_path):
        sim = archetype.init(str(tmp_path / "data"))
        world_id = sim.spawn_world("physics")

        sim.add_processor_to_world(world_id, MovementProcessor())
        sim.spawn_entity(
            world_id,
            Position(x=0.0, y=0.0),
            Velocity(vx=1.0, vy=1.0),
        )

        # Step 3 times
        for _ in range(3):
            sim.step_world(world_id, dt=1.0)

        world = sim.get_world(world_id)
        for sig, df in world._live.items():
            rows = df.collect().to_pylist()
            if rows:
                assert rows[0]["position__x"] == 3.0
                assert rows[0]["position__y"] == 3.0


# =============================================================================
# Tests: Processor Priority
# =============================================================================


class TestProcessorPriority:
    def test_processors_run_in_priority_order(self, tmp_path):
        execution_order = []

        class FirstProcessor(Processor):
            components = (Position,)
            priority = 1

            def process(self, df: DataFrame, **kwargs) -> DataFrame:
                execution_order.append("first")
                return df

        class SecondProcessor(Processor):
            components = (Position,)
            priority = 10

            def process(self, df: DataFrame, **kwargs) -> DataFrame:
                execution_order.append("second")
                return df

        sim = archetype.init(str(tmp_path / "data"))
        world_id = sim.spawn_world("test")

        # Add in reverse order
        sim.add_processor_to_world(world_id, SecondProcessor())
        sim.add_processor_to_world(world_id, FirstProcessor())
        sim.spawn_entity(world_id, Position())

        sim.step_world(world_id)

        assert execution_order == ["first", "second"]
