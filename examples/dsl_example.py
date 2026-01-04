#!/usr/bin/env python3
"""
DataFrame-First DSL Example
============================

Demonstrates the simplified DSL that honors the core engine by compiling
agent-centric syntax to pure DataFrame transforms.

Features:
- Field expressions compile to col() operations
- when/otherwise for conditional logic
- Pure DataFrame transforms (no collect-and-loop)

Run: python examples/dsl_example.py
"""

import asyncio

from archetype.core.component import Component
from archetype.dsl import World, behavior, when


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
# Behaviors - DataFrame-First
# =============================================================================


@behavior
class Movement:
    """Update positions based on velocity."""
    
    requires = [Position, Velocity]
    priority = 10
    
    def transform(self, arch, tick, dt):
        """Pure DataFrame transform."""
        return {
            "position__x": arch.position.x + arch.velocity.vx * dt,
            "position__y": arch.position.y + arch.velocity.vy * dt,
        }


@behavior
class EnergyDrain:
    """Drain energy when moving."""
    
    requires = [Energy, Velocity]
    priority = 15
    
    @staticmethod
    def filter(arch):
        """Only drain energy if moving (compiles to df.where())."""
        return (arch.velocity.vx != 0) | (arch.velocity.vy != 0)
    
    def transform(self, arch, tick, dt):
        """Drain energy proportional to velocity."""
        # Use power for speed calculation
        speed_squared = arch.velocity.vx ** 2 + arch.velocity.vy ** 2
        drain = speed_squared ** 0.5 * 2.0 * dt
        return {
            "energy__current": arch.energy.current - drain,
        }


@behavior
class ConditionalHealthRegen:
    """Regenerate health when stationary, using when/otherwise."""
    
    requires = [Health, Velocity]
    priority = 20
    
    def transform(self, arch, tick, dt):
        """
        Use Daft's when/otherwise for conditional logic.
        Regenerate 5 health per tick when stationary, otherwise damage by 2.
        """
        is_stationary = (arch.velocity.vx == 0) & (arch.velocity.vy == 0)
        
        return {
            "health__current": when(is_stationary)
                .then(arch.health.current + 5)
                .otherwise(arch.health.current - 2)
        }


@behavior
class EnergyBasedVelocity:
    """Reduce velocity when low on energy using when/otherwise."""
    
    requires = [Velocity, Energy]
    priority = 25
    
    def transform(self, arch, tick, dt):
        """Apply friction based on energy levels."""
        energy_ratio = arch.energy.current / arch.energy.max
        
        # When energy is low (<30%), reduce velocity more aggressively
        friction = when(energy_ratio < 0.3).then(0.5).otherwise(0.95)
        
        return {
            "velocity__vx": arch.velocity.vx * friction,
            "velocity__vy": arch.velocity.vy * friction,
        }


# =============================================================================
# Main Simulation
# =============================================================================


async def main():
    print("=" * 60)
    print("DataFrame-First DSL Example")
    print("=" * 60)
    
    async with World("dsl_demo") as world:
        # Register behaviors
        world.register(Movement)
        world.register(EnergyDrain)
        world.register(ConditionalHealthRegen)
        world.register(EnergyBasedVelocity)
        
        print("\n✓ Registered 4 behaviors")
        print("  - Movement (priority 10)")
        print("  - EnergyDrain (priority 15)")
        print("  - ConditionalHealthRegen (priority 20) - uses when/otherwise")
        print("  - EnergyBasedVelocity (priority 25) - uses when/otherwise")
        
        # Spawn entities
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
        print("1. All updates happened via pure DataFrame transforms")
        print("2. when/otherwise used for conditional logic")
        print("3. No Python loops in execution path")
        print("4. Fast mover drained energy and slowed down")
        print("5. Stationary entity regenerated health (when/otherwise)")
        print("\n✓ Honors the core engine's DataFrame-first architecture!")


if __name__ == "__main__":
    asyncio.run(main())
