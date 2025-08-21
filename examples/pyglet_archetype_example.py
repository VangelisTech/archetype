#!/usr/bin/env python3
"""
Minimal Pyglet wall-collision example using Archetype ECS (async).

Parallels Esper's pyglet example:
- Arrow keys move the player by setting velocity
- Movement keeps the sprite within the window bounds (clamp)
"""

import asyncio
import time
import random

try:
    import pyglet
    from pyglet import shapes
    from pyglet.window import key
except ImportError:  # pragma: no cover
    raise SystemExit("Please install pyglet: pip install pyglet")

from archetype.core import (
    Component,
    AsyncProcessor,
    RunConfig,
    WorldConfig,
    StorageConfig,
    CacheConfig, 
)
from archetype.core.aio import AsyncWorld, AsyncSystem
from archetype.core.orchestrator import WorldOrchestrator
from daft import DataFrame, col, lit


# ================= Components =================

class Position(Component):
    x: float
    y: float


class Velocity(Component):
    vx: float
    vy: float


class Sprite(Component):
    color: str
    size: float


class Player(Component):
    player_id: int


# ================= Processors =================

class PlayerInputProcessor(AsyncProcessor):
    components = [Position, Velocity, Player]
    priority = 0

    async def process(self, df: DataFrame, keys_pressed: set | None = None, **kwargs) -> DataFrame:
        speed = 200.0
        pressed = keys_pressed or set()
        vx = (
            -speed if key.LEFT in pressed else (speed if key.RIGHT in pressed else 0.0)
        )
        vy = (
            -speed if key.DOWN in pressed else (speed if key.UP in pressed else 0.0)
        )

        return df.with_columns({
            "velocity__vx": lit(vx),
            "velocity__vy": lit(vy),
        })


class MovementProcessor(AsyncProcessor):
    components = [Position, Velocity]
    priority = 2

    async def process(self, df: DataFrame, dt: float = 1.0 / 60.0, **kwargs) -> DataFrame:
        return df.with_columns({
            "position__x": col("position__x") + col("velocity__vx") * lit(dt),
            "position__y": col("position__y") + col("velocity__vy") * lit(dt),
        })


class Bouncer(Component):
    enabled: bool = True


class BounceAndSpawnProcessor(AsyncProcessor):
    components = [Position, Velocity, Sprite, Bouncer]
    priority = 1

    async def process(
        self,
        df: DataFrame,
        *,
        width: int = 800,
        height: int = 600,
        dt: float = 1.0 / 60.0,
        spawn=None,
        **kwargs,
    ) -> DataFrame:
        # Predict next position to detect this-frame wall collisions
        radius = col("sprite__size")
        next_x = col("position__x") + col("velocity__vx") * lit(dt)
        next_y = col("position__y") + col("velocity__vy") * lit(dt)
        will_hit_x = (next_x <= radius) | (next_x >= (lit(width) - radius))
        will_hit_y = (next_y <= radius) | (next_y >= (lit(height) - radius))
        collided = will_hit_x | will_hit_y

        # Reflect velocity on collision and clamp position within bounds
        df2 = df.with_columns({
            "velocity__vx": will_hit_x.if_else(col("velocity__vx") * lit(-1), col("velocity__vx")),
            "velocity__vy": will_hit_y.if_else(col("velocity__vy") * lit(-1), col("velocity__vy")),
            "position__x": col("position__x").clip(radius, (lit(width) - radius)),
            "position__y": col("position__y").clip(radius, (lit(height) - radius)),
        })

        # Spawn a new entity for each collision with a random direction
        if spawn is not None:
            try:
                hits = df.where(collided).select("position__x", "position__y", "sprite__color", "sprite__size").collect()
                data = hits.to_pydict()
                center_x = float(width) / 2.0
                center_y = float(height) / 2.0
                for i in range(hits.count_rows()):
                    # Always spawn at window center
                    x = center_x
                    y = center_y
                    color = data["sprite__color"][i]
                    size = data["sprite__size"][i]

                    # Random direction and speed
                    import math
                    angle = random.uniform(0, 2 * math.pi)
                    speed = random.uniform(80.0, 180.0)
                    vx = math.cos(angle) * speed
                    vy = math.sin(angle) * speed

                    # Fire-and-forget spawn; this processor is async so we await it
                    await spawn([
                        Position(x=x, y=y),
                        Velocity(vx=vx, vy=vy),
                        Sprite(color=color, size=size),
                        Bouncer(enabled=True),
                    ])
            except Exception as e:  # pragma: no cover
                print(f"Spawn failed: {e}")

        return df2


# ================ Async world runner =================

class AsyncWorldRunner:
    """Runs the async world in a side loop and exposes state for rendering."""

    def __init__(self, world: AsyncWorld, width: int = 800, height: int = 600):
        self.world = world
        self.width = width
        self.height = height
        self.run_config = RunConfig(num_steps=1)
        self.loop = asyncio.new_event_loop()
        self._keys: set[int] = set()
        self._state: list[dict] = []
        self._running = False
        self._stats: dict[str, float] = {"step_ms": 0.0, "query_ms": 0.0}

    def start(self):
        import threading

        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False
        if hasattr(self, "_thread"):
            self._thread.join(timeout=1.0)

    def update_keys(self, keys: set[int]):
        # Copy to avoid cross-thread mutation hazards
        self._keys = set(keys)

    def get_state(self) -> list[dict]:
        return list(self._state)

    def _run_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self._async_main())

    async def _async_main(self):
        while self._running:
            try:
                # Advance simulation one tick, passing inputs
                t0 = time.perf_counter()
                await self.world.step(
                    self.run_config,
                    dt=1.0 / 60.0,
                    width=self.width,
                    height=self.height,
                    keys_pressed=self._keys,
                    spawn=self.world.create_entity,
                )
                t1 = time.perf_counter()

                # Query lightweight render state
                q0 = time.perf_counter()
                df = await self.world.get_components([Position, Sprite])
                if df is not None:
                    tbl = df.select("entity_id", "position__x", "position__y", "sprite__color", "sprite__size").collect()
                    data = tbl.to_pydict()
                    self._state = [
                        {
                            "id": data["entity_id"][i],
                            "x": data["position__x"][i],
                            "y": data["position__y"][i],
                            "color": data["sprite__color"][i],
                            "size": data["sprite__size"][i],
                        }
                        for i in range(tbl.count_rows())
                    ]
                q1 = time.perf_counter()

                # Update stats in ms
                self._stats["step_ms"] = (t1 - t0) * 1000.0
                self._stats["query_ms"] = (q1 - q0) * 1000.0

                await asyncio.sleep(1.0 / 60.0)
            except Exception as e:  # pragma: no cover
                print(f"Async loop error: {e}")

    def get_stats(self) -> dict:
        return dict(self._stats)


# ================= Pyglet window =================

class GameWindow(pyglet.window.Window):
    def __init__(self, runner: AsyncWorldRunner, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.runner = runner
        self.keys_pressed: set[int] = set()
        self.batch = pyglet.graphics.Batch()
        self.shapes: dict[int, shapes.Circle] = {}
        pyglet.clock.schedule_interval(self.update, 1.0 / 60.0)
        self.fps_display = pyglet.window.FPSDisplay(self)
        self.hud_label = pyglet.text.Label(
            '',
            font_size=12,
            x=10,
            y=self.height - 20,
            anchor_x='left',
            anchor_y='top',
            color=(255, 255, 255, 255),
        )
        self._fps_ema = 0.0

    def on_key_press(self, symbol, modifiers):
        self.keys_pressed.add(symbol)
        self.runner.update_keys(self.keys_pressed)
        if symbol == key.ESCAPE:
            self.close()

    def on_key_release(self, symbol, modifiers):
        self.keys_pressed.discard(symbol)
        self.runner.update_keys(self.keys_pressed)

    def update(self, dt):
        state = self.runner.get_state()
        self._sync_shapes(state)
        stats = self.runner.get_stats()
        fps_inst = (1.0 / dt) if dt > 0 else 0.0
        self._fps_ema = fps_inst if self._fps_ema == 0.0 else (0.9 * self._fps_ema + 0.1 * fps_inst)
        text = (
            f"FPS: {self._fps_ema:.1f} | step_ms: {stats.get('step_ms', 0.0):.2f} | "
            f"query_ms: {stats.get('query_ms', 0.0):.2f} | entities: {len(self.shapes)}"
        )
        self.hud_label.text = text

    def _sync_shapes(self, entities: list[dict]):
        seen = set()
        for ent in entities:
            ent_id = ent["id"]
            seen.add(ent_id)
            color_map = {
                "red": (255, 0, 0),
                "blue": (0, 0, 255),
                "green": (0, 255, 0),
                "yellow": (255, 255, 0),
                "white": (255, 255, 255),
            }
            rgb = color_map.get(ent["color"], (255, 255, 255))
            if ent_id in self.shapes:
                c = self.shapes[ent_id]
                c.x, c.y = ent["x"], ent["y"]
            else:
                self.shapes[ent_id] = shapes.Circle(
                    ent["x"], ent["y"], ent["size"], color=rgb, batch=self.batch
                )

        # Remove stale shapes
        for ent_id in list(self.shapes.keys() - seen):
            self.shapes[ent_id].delete()
            del self.shapes[ent_id]

    def on_draw(self):
        self.clear()
        self.batch.draw()
        self.fps_display.draw()
        self.hud_label.draw()

# ================= Bootstrap =================

async def setup_world(uri: str, namespace: str, width: int, height: int) -> AsyncWorld:
    orchestrator = WorldOrchestrator()
    system = AsyncSystem()
    world = await orchestrator.create_world(
        config=WorldConfig(name="pyglet_minimal"),
        system=system,
        storage_config=StorageConfig(uri=uri, namespace=namespace, backend= "lancedb"),
        cache_config=CacheConfig(enabled=True),
    )

    await world.add_processor(PlayerInputProcessor())
    await world.add_processor(MovementProcessor())
    await world.add_processor(BounceAndSpawnProcessor())

    # Player
    await world.create_entity([
        Position(x=width / 2, y=height / 2),
        Velocity(vx=0.0, vy=0.0),
        Sprite(color="blue", size=15.0),
        Player(player_id=1),
    ])

    # A second entity that drifts and bounces/spawns on collisions
    await world.create_entity([
        Position(x=random.uniform(50, width - 50), y=random.uniform(50, height - 50)),
        Velocity(vx=120.0, vy=-90.0),
        Sprite(color="red", size=10.0),
        Bouncer(enabled=True),
    ])

    return world


def main():  # pragma: no cover
    import pathlib
    uri = "./archetype_data"
    namespace = "examples"
    width, height = 800, 600

    # Ensure uri exists
    resolved_uri = pathlib.Path(uri).resolve()
    resolved_uri.mkdir(parents=True, exist_ok=True)

    loop = asyncio.new_event_loop()
    world = loop.run_until_complete(setup_world(resolved_uri, namespace, width, height))
    runner = AsyncWorldRunner(world, width=width, height=height)
    runner.start()

    _ = GameWindow(runner, width=width, height=height, caption="Archetype ECS - Minimal Pyglet")
    pyglet.app.run()
    runner.stop()


if __name__ == "__main__":  # pragma: no cover
    main()


