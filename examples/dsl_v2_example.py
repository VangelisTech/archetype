#!/usr/bin/env python3
"""
DSL v2 Example - DataFrame-First Design
========================================

This example demonstrates the improved DSL that honors the core engine by:
1. Compiling behaviors to pure DataFrame transforms
2. Separating query API from behavior definition
3. Making archetypes explicit
4. Preserving batch operations (no collect-and-loop)

Run: python examples/dsl_v2_example.py
"""

import asyncio

from archetype.core.component import Component
from archetype.dsl.v2 import WorldV2, processor


# =============================================================================
# Components
# =============================================================================


class Position(Component):
    """Spatial position."""
    x: float = 0.0
    y: float = 0.0


class Velocity(Component):
    """Movement velocity."""
    vx: float = 0.0
    vy: float = 0.0


class Health(Component):
    """Entity health."""
    current: int = 100
    max: int = 100


class Energy(Component):
    """Energy for movement."""
    current: float = 100.0
    max: float = 100.0


# =============================================================================
# Processors - DataFrame-First Behaviors
# =============================================================================


@processor
class Movement:
    """Update positions based on velocity."""
    
    requires = [Position, Velocity]
    priority = 10
    
    def transform(self, arch, tick, dt):
        """
        Pure DataFrame transform.
        arch.position.x compiles to col("position__x")
        """
        return {
            "position__x": arch.position.x + arch.velocity.vx * dt,
            "position__y": arch.position.y + arch.velocity.vy * dt,
        }


@processor
class EnergyDrain:
    """Drain energy when moving."""
    
    requires = [Energy, Velocity]
    priority = 15  # After movement
    
    @staticmethod
    def filter(arch):
        """Only drain energy if moving (compiles to df.where())."""
        return (arch.velocity.vx != 0) | (arch.velocity.vy != 0)
    
    def transform(self, arch, tick, dt):
        """Drain energy proportional to velocity."""
        speed = (arch.velocity.vx ** 2 + arch.velocity.vy ** 2) ** 0.5
        drain = speed * 2.0 * dt
        return {
            "energy__current": arch.energy.current - drain,
        }


@processor
class Friction:
    """Apply friction to slow down entities."""
    
    requires = [Velocity, Energy]
    priority = 20  # After energy drain
    
    def transform(self, arch, tick, dt):
        """Reduce velocity, more when low energy."""
        friction_factor = 0.95
        
        # Higher friction when low energy
        energy_ratio = arch.energy.current / arch.energy.max
        adjusted_friction = friction_factor - (1.0 - energy_ratio) * 0.2
        
        return {
            "velocity__vx": arch.velocity.vx * adjusted_friction,
            "velocity__vy": arch.velocity.vy * adjusted_friction,
        }


@processor
class HealthRegen:
    """Regenerate health when stationary."""
    
    requires = [Health, Velocity]
    priority = 25
    
    @staticmethod
    def filter(arch):
        """Only regen when stationary."""
        return (arch.velocity.vx == 0) & (arch.velocity.vy == 0)
    
    def transform(self, arch, tick, dt):
        """Regenerate 5 health per tick, capped at max."""
        new_health = arch.health.current + 5
        # Note: We'd need a min() function for proper capping
        return {
            "health__current": new_health,
        }


@processor
class CollisionDamage:
    """Damage entities that collide (simple boundary check)."""
    
    requires = [Health, Position]
    priority = 30
    
    @staticmethod
    def filter(arch):
        """Check if out of bounds."""
        return (arch.position.x < 0) | (arch.position.x > 100) | \
               (arch.position.y < 0) | (arch.position.y > 100)
    
    def transform(self, arch, tick, dt):
        """Take damage when out of bounds."""
        return {
            "health__current": arch.health.current - 20,
        }


# =============================================================================
# Main Simulation
# =============================================================================


async def main():
    print("=" * 60)
    print("DSL v2 Example - DataFrame-First Design")
    print("=" * 60)
    
    async with WorldV2("dsl_v2_demo") as world:
        # Register processors (order doesn't matter, priority does)
        world.register(Movement)
        world.register(EnergyDrain)
        world.register(Friction)
        world.register(HealthRegen)
        world.register(CollisionDamage)
        
        print("\n✓ Registered 5 processors")
        print("  - Movement (priority 10)")
        print("  - EnergyDrain (priority 15)")
        print("  - Friction (priority 20)")
        print("  - HealthRegen (priority 25)")
        print("  - CollisionDamage (priority 30)")
        
        # Spawn entities with different configurations
        print("\n✓ Spawning entities...")
        
        # Fast moving entity
        await world.spawn(
            Position(x=10.0, y=10.0),
            Velocity(vx=5.0, vy=3.0),
            Health(current=100, max=100),
            Energy(current=100.0, max=100.0),
        )
        print("  - Fast mover at (10, 10)")
        
        # Slow moving entity
        await world.spawn(
            Position(x=50.0, y=50.0),
            Velocity(vx=1.0, vy=1.0),
            Health(current=100, max=100),
            Energy(current=100.0, max=100.0),
        )
        print("  - Slow mover at (50, 50)")
        
        # Stationary entity
        await world.spawn(
            Position(x=25.0, y=25.0),
            Velocity(vx=0.0, vy=0.0),
            Health(current=80, max=100),
            Energy(current=50.0, max=100.0),
        )
        print("  - Stationary at (25, 25)")
        
        # Entity heading out of bounds
        await world.spawn(
            Position(x=95.0, y=50.0),
            Velocity(vx=3.0, vy=0.0),
            Health(current=100, max=100),
            Energy(current=100.0, max=100.0),
        )
        print("  - Boundary tester at (95, 50)")
        
        # Run simulation
        print("\n" + "-" * 60)
        print("Running 10 ticks...")
        print("-" * 60)
        
        await world.run(ticks=10, dt=1.0)
        
        # Query and display results
        print("\n" + "=" * 60)
        print("Final State")
        print("=" * 60)
        
        agents = world.query(Position, Velocity, Health, Energy)
        
        for i, agent in enumerate(agents, 1):
            print(f"\nEntity {i}:")
            print(f"  Position: ({agent.position.x:.2f}, {agent.position.y:.2f})")
            print(f"  Velocity: ({agent.velocity.vx:.2f}, {agent.velocity.vy:.2f})")
            print(f"  Health: {agent.health.current}/{agent.health.max}")
            print(f"  Energy: {agent.energy.current:.2f}/{agent.energy.max:.2f}")
        
        # Analysis
        print("\n" + "=" * 60)
        print("Analysis")
        print("=" * 60)
        
        print("\nKey observations:")
        print("1. Fast mover drained energy quickly due to high velocity")
        print("2. Slow mover maintained energy better")
        print("3. Stationary entity regenerated health (if implemented)")
        print("4. Boundary tester took collision damage")
        print("\nAll updates happened via pure DataFrame transforms!")
        print("No Python loops in the hot path. ✓")


if __name__ == "__main__":
    asyncio.run(main())
