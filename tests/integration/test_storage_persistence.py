# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Integration tests: Storage Persistence

Tests data persistence via the DSL World.
"""

import pytest

from archetype.core.component import Component
from archetype.dsl import World, behavior


# =============================================================================
# Test Components
# =============================================================================


class Counter(Component):
    value: int = 0


class Entity(Component):
    name: str = ""


# =============================================================================
# Integration Tests: Persistence via World
# =============================================================================


class TestWorldPersistence:
    @pytest.mark.asyncio
    async def test_state_persists_across_run(self, tmp_path):
        """Test that state changes persist across a full run."""
        
        @behavior
        class Increment:
            requires = [Counter]
            
            async def act(self, agent, world, tick):
                agent.counter.value = agent.counter.value + 1
        
        async with World("persist", storage=tmp_path / "data") as world:
            world.add_behavior(Increment)
            await world.spawn(Counter(value=0))
            
            # Run 5 ticks - each tick increments by 1
            await world.run(ticks=5)
            assert world.agents[0].counter.value == 5

    @pytest.mark.asyncio
    async def test_multiple_entities_persist(self, tmp_path):
        """Test multiple entities maintain separate state."""
        
        @behavior
        class NamedIncrement:
            requires = [Entity, Counter]
            
            async def act(self, agent, world, tick):
                # Different increment based on name
                if agent.entity.name == "fast":
                    agent.counter.value = agent.counter.value + 10
                else:
                    agent.counter.value = agent.counter.value + 1
        
        async with World("multi", storage=tmp_path / "data") as world:
            world.add_behavior(NamedIncrement)
            await world.spawn(Entity(name="fast"), Counter(value=0))
            await world.spawn(Entity(name="slow"), Counter(value=0))
            
            await world.run(ticks=5)
            
            fast = await world.find_one(lambda a: a.entity.name == "fast")
            slow = await world.find_one(lambda a: a.entity.name == "slow")
            
            assert fast.counter.value == 50  # 5 * 10
            assert slow.counter.value == 5   # 5 * 1


# =============================================================================
# Integration Tests: Time Travel Patterns
# =============================================================================


class TestTimeTravelPatterns:
    @pytest.mark.asyncio
    async def test_fork_preserves_parent_state(self, tmp_path):
        """Test forking preserves parent state while child diverges."""
        from archetype.dsl import spawn_world
        
        @behavior
        class Doubler:
            requires = [Counter]
            
            async def act(self, agent, world, tick):
                agent.counter.value = agent.counter.value * 2
        
        async with World("parent", storage=tmp_path / "parent") as parent:
            parent.add_behavior(Doubler)
            await parent.spawn(Counter(value=1))
            await parent.run(ticks=3)  # 1 -> 2 -> 4 -> 8
            
            parent_value_before_fork = parent.agents[0].counter.value
            assert parent_value_before_fork == 8
            
            # Fork and continue
            async with spawn_world("child", parent=parent, fork_state=True) as child:
                child.add_behavior(Doubler)
                await child.run(ticks=2)  # 8 -> 16 -> 32
                
                child_value = child.agents[0].counter.value
                assert child_value == 32
            
            # Parent unchanged
            assert parent.agents[0].counter.value == 8

    @pytest.mark.asyncio
    async def test_branch_and_compare(self, tmp_path):
        """Test branching for comparison (A/B testing pattern)."""
        from archetype.dsl import spawn_world
        
        async with World("root", storage=tmp_path / "root") as root:
            await root.spawn(Counter(value=100))
            await root.run(ticks=1)
            
            results = {}
            
            # Strategy A: Aggressive (multiply by 2)
            async with spawn_world("strategy_a", parent=root, fork_state=True) as branch_a:
                @behavior
                class Aggressive:
                    requires = [Counter]
                    async def act(self, agent, world, tick):
                        agent.counter.value = agent.counter.value * 2
                
                branch_a.add_behavior(Aggressive)
                await branch_a.run(ticks=3)
                results["aggressive"] = branch_a.agents[0].counter.value
            
            # Strategy B: Conservative (add 10)
            async with spawn_world("strategy_b", parent=root, fork_state=True) as branch_b:
                @behavior
                class Conservative:
                    requires = [Counter]
                    async def act(self, agent, world, tick):
                        agent.counter.value = agent.counter.value + 10
                
                branch_b.add_behavior(Conservative)
                await branch_b.run(ticks=3)
                results["conservative"] = branch_b.agents[0].counter.value
            
            # Compare outcomes
            assert results["aggressive"] == 800  # 100 * 2 * 2 * 2
            assert results["conservative"] == 130  # 100 + 10 + 10 + 10
            assert results["aggressive"] > results["conservative"]

    @pytest.mark.asyncio
    async def test_independent_branch_outcomes(self, tmp_path):
        """Test that branches don't affect each other."""
        from archetype.dsl import spawn_world
        
        async with World("root", storage=tmp_path / "root") as root:
            await root.spawn(Counter(value=10))
            await root.run(ticks=1)
            
            # Branch 1: Add 100
            async with spawn_world("b1", parent=root, fork_state=True) as b1:
                @behavior
                class Add100:
                    requires = [Counter]
                    async def act(self, agent, world, tick):
                        agent.counter.value = agent.counter.value + 100
                
                b1.add_behavior(Add100)
                await b1.run(ticks=1)
                b1_value = b1.agents[0].counter.value
            
            # Branch 2: Subtract 5 (should still start from 10, not 110)
            async with spawn_world("b2", parent=root, fork_state=True) as b2:
                @behavior
                class Sub5:
                    requires = [Counter]
                    async def act(self, agent, world, tick):
                        agent.counter.value = agent.counter.value - 5
                
                b2.add_behavior(Sub5)
                await b2.run(ticks=1)
                b2_value = b2.agents[0].counter.value
            
            assert b1_value == 110  # 10 + 100
            assert b2_value == 5    # 10 - 5
