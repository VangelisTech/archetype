# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Integration tests: DSL + Storage

Tests the full stack from DSL down to storage persistence.
"""

import json
import pytest

from archetype.core.component import Component
from archetype.dsl import World, behavior, spawn_world


# =============================================================================
# Test Components
# =============================================================================


class Agent(Component):
    name: str = ""
    role: str = "worker"


class Position(Component):
    x: float = 0.0
    y: float = 0.0


class Velocity(Component):
    vx: float = 0.0
    vy: float = 0.0


class Memory(Component):
    history_json: str = "[]"


# =============================================================================
# Integration Tests: Multi-tick Simulation
# =============================================================================


class TestMultiTickSimulation:
    @pytest.mark.asyncio
    async def test_physics_simulation(self, tmp_path):
        """Test position updates over multiple ticks."""
        
        @behavior
        class Movement:
            requires = [Position, Velocity]
            
            async def act(self, agent, world, tick):
                agent.position.x = agent.position.x + agent.velocity.vx
                agent.position.y = agent.position.y + agent.velocity.vy
        
        async with World("physics", storage=tmp_path / "data") as world:
            world.add_behavior(Movement)
            await world.spawn(
                Position(x=0, y=0),
                Velocity(vx=1.0, vy=0.5),
            )
            await world.run(ticks=10)
            
            agent = world.agents[0]
            # After 10 ticks: x = 0 + 10*1 = 10, y = 0 + 10*0.5 = 5
            assert agent.position.x == 10.0
            assert agent.position.y == 5.0

    @pytest.mark.asyncio
    async def test_multi_agent_simulation(self, tmp_path):
        """Test multiple agents evolving independently."""
        
        @behavior
        class Counter:
            requires = [Agent, Memory]
            
            async def act(self, agent, world, tick):
                # Access history_json which auto-deserializes
                history = agent.memory.history_json
                if isinstance(history, str):
                    history = json.loads(history)
                history.append({"tick": tick, "name": agent.agent.name})
                agent.memory.history_json = json.dumps(history)
        
        async with World("multi", storage=tmp_path / "data") as world:
            world.add_behavior(Counter)
            await world.spawn(Agent(name="Alice"), Memory())
            await world.spawn(Agent(name="Bob"), Memory())
            await world.spawn(Agent(name="Charlie"), Memory())
            await world.run(ticks=5)
            
            assert len(world.agents) == 3
            for agent in world.agents:
                history = agent.memory.history_json
                if isinstance(history, str):
                    history = json.loads(history)
                assert len(history) == 5
                assert all(h["name"] == agent.agent.name for h in history)


# =============================================================================
# Integration Tests: MCTS / Counterfactual
# =============================================================================


class TestMCTSPatterns:
    @pytest.mark.asyncio
    async def test_spawn_world_isolation(self, tmp_path):
        """Test that child worlds are isolated from parent."""
        
        @behavior
        class Modifier:
            requires = [Agent]
            
            async def act(self, agent, world, tick):
                agent.agent.role = "modified"
        
        async with World("parent", storage=tmp_path / "parent") as parent:
            await parent.spawn(Agent(name="Original", role="worker"))
            await parent.run(ticks=1)
            
            # Spawn child and modify
            async with spawn_world("child", parent=parent, fork_state=True) as child:
                child.add_behavior(Modifier)
                await child.run(ticks=1)
                
                # Child should be modified
                assert child.agents[0].agent.role == "modified"
            
            # Parent should be unchanged
            assert parent.agents[0].agent.role == "worker"

    @pytest.mark.asyncio
    async def test_parallel_branches(self, tmp_path):
        """Test running multiple branches from same parent state."""
        
        async with World("parent", storage=tmp_path / "parent") as parent:
            await parent.spawn(Position(x=0, y=0), Velocity(vx=1, vy=0))
            await parent.run(ticks=1)
            
            results = []
            
            # Branch A: continue moving right
            async with spawn_world("branch_a", parent=parent, fork_state=True) as branch_a:
                @behavior
                class MoveRight:
                    requires = [Position, Velocity]
                    
                    async def act(self, agent, world, tick):
                        agent.position.x = agent.position.x + agent.velocity.vx
                
                branch_a.add_behavior(MoveRight)
                await branch_a.run(ticks=5)
                results.append(("right", branch_a.agents[0].position.x))
            
            # Branch B: move up instead
            async with spawn_world("branch_b", parent=parent, fork_state=True) as branch_b:
                @behavior
                class MoveUp:
                    requires = [Position, Velocity]
                    
                    async def act(self, agent, world, tick):
                        agent.position.y = agent.position.y + 1.0
                
                branch_b.add_behavior(MoveUp)
                await branch_b.run(ticks=5)
                results.append(("up", branch_b.agents[0].position.y))
            
            # Verify different outcomes
            right_result = next(r for r in results if r[0] == "right")
            up_result = next(r for r in results if r[0] == "up")
            
            assert right_result[1] > 1.0  # Moved right
            assert up_result[1] >= 5.0  # Moved up

    @pytest.mark.asyncio
    async def test_nested_spawn_worlds(self, tmp_path):
        """Test spawning worlds within spawned worlds (for deep MCTS)."""
        
        async with World("root", storage=tmp_path / "root") as root:
            await root.spawn(Agent(name="Root"))
            await root.run(ticks=1)
            
            async with spawn_world("level1", parent=root, fork_state=True) as level1:
                await level1.spawn(Agent(name="Level1"))
                await level1.run(ticks=1)
                
                async with spawn_world("level2", parent=level1, fork_state=True) as level2:
                    await level2.spawn(Agent(name="Level2"))
                    await level2.run(ticks=1)
                    
                    # Level2 should have agents from all levels
                    names = {a.agent.name for a in level2.agents}
                    assert "Root" in names
                    assert "Level1" in names
                    assert "Level2" in names


# =============================================================================
# Integration Tests: Behavior Composition
# =============================================================================


class TestBehaviorComposition:
    @pytest.mark.asyncio
    async def test_priority_ordering(self, tmp_path):
        """Test that behaviors run in priority order."""
        
        execution_order = []
        
        @behavior
        class First:
            requires = [Agent]
            priority = 1
            
            async def act(self, agent, world, tick):
                execution_order.append("first")
        
        @behavior
        class Second:
            requires = [Agent]
            priority = 10
            
            async def act(self, agent, world, tick):
                execution_order.append("second")
        
        @behavior
        class Third:
            requires = [Agent]
            priority = 100
            
            async def act(self, agent, world, tick):
                execution_order.append("third")
        
        async with World("test", storage=tmp_path / "data") as world:
            # Add in reverse order to test sorting
            world.add_behavior(Third)
            world.add_behavior(First)
            world.add_behavior(Second)
            
            await world.spawn(Agent(name="Test"))
            await world.run(ticks=1)
            
            assert execution_order == ["first", "second", "third"]

    @pytest.mark.asyncio
    async def test_conditional_behaviors(self, tmp_path):
        """Test behaviors with filter conditions."""
        
        workers_modified = []
        managers_modified = []
        
        @behavior
        class WorkerBehavior:
            requires = [Agent]
            filter = lambda a: a.agent.role == "worker"
            
            async def act(self, agent, world, tick):
                workers_modified.append(agent.agent.name)
        
        @behavior
        class ManagerBehavior:
            requires = [Agent]
            filter = lambda a: a.agent.role == "manager"
            
            async def act(self, agent, world, tick):
                managers_modified.append(agent.agent.name)
        
        async with World("test", storage=tmp_path / "data") as world:
            world.add_behavior(WorkerBehavior)
            world.add_behavior(ManagerBehavior)
            
            await world.spawn(Agent(name="Alice", role="worker"))
            await world.spawn(Agent(name="Bob", role="manager"))
            await world.spawn(Agent(name="Charlie", role="worker"))
            
            await world.run(ticks=1)
            
            assert set(workers_modified) == {"Alice", "Charlie"}
            assert managers_modified == ["Bob"]


# =============================================================================
# Integration Tests: Complex State
# =============================================================================


class TestComplexState:
    @pytest.mark.asyncio
    async def test_json_state_accumulation(self, tmp_path):
        """Test accumulating complex state across ticks."""
        
        @behavior
        class EventLogger:
            requires = [Agent, Memory]
            
            async def act(self, agent, world, tick):
                history = agent.memory.history_json
                if isinstance(history, str):
                    history = json.loads(history)
                history.append({
                    "tick": tick,
                    "event": f"{agent.agent.name} acted",
                    "data": {"random": tick * 2},
                })
                agent.memory.history_json = json.dumps(history)
        
        async with World("test", storage=tmp_path / "data") as world:
            world.add_behavior(EventLogger)
            await world.spawn(Agent(name="Logger"), Memory())
            await world.run(ticks=5)
            
            agent = world.agents[0]
            history = agent.memory.history_json
            if isinstance(history, str):
                history = json.loads(history)
            
            assert len(history) == 5
            assert history[0]["tick"] == 0
            assert history[4]["tick"] == 4
            assert history[2]["data"]["random"] == 4

    @pytest.mark.asyncio
    async def test_multiple_behaviors_same_agent(self, tmp_path):
        """Test multiple behaviors modifying same agent in one tick."""
        
        @behavior
        class AddX:
            requires = [Position]
            priority = 1
            
            async def act(self, agent, world, tick):
                agent.position.x = agent.position.x + 10
        
        @behavior
        class AddY:
            requires = [Position]
            priority = 2
            
            async def act(self, agent, world, tick):
                agent.position.y = agent.position.y + 5
        
        async with World("test", storage=tmp_path / "data") as world:
            world.add_behavior(AddX)
            world.add_behavior(AddY)
            await world.spawn(Position(x=0, y=0))
            await world.run(ticks=3)
            
            agent = world.agents[0]
            # 3 ticks * 10 = 30, 3 ticks * 5 = 15
            assert agent.position.x == 30.0
            assert agent.position.y == 15.0
