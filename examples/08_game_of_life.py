# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Conway's Game of Life
======================

Game of Life as a single DataFrame transform. The next-generation rule
is a self-join (every live cell votes for its 8 neighbors) plus a
group-by, plus one boolean expression for B/S.

Showcases:
- A real Daft pipeline as the processor body (not a UDF).
- Time-travel: every generation is preserved, so we can replay any
  earlier tick from the same world.
- Forking: the same initial seed runs forward under Conway (B3/S23)
  and HighLife (B36/S23), and we compare populations side-by-side.

No external dependencies — runs entirely in-process.

Usage:
    uv run python examples/08_game_of_life.py
"""

import asyncio
from dataclasses import dataclass

import daft
from daft import DataFrame, col
from uuid_utils import uuid7

from archetype import ArchetypeRuntime
from archetype.app.auth.models import ActorCtx
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.core.resources import Resources

WIDTH, HEIGHT = 22, 12
TICKS = 8


def _admin() -> ActorCtx:
    """Fresh admin actor — keeps each world under the per-actor command quota."""
    return ActorCtx(id=uuid7(), roles={"admin"})


class Cell(Component):
    x: int = 0
    y: int = 0
    alive: bool = False


@dataclass
class Rule:
    """Birth/Survival rule. Conway = B3/S23, HighLife = B36/S23."""

    name: str = "Conway"
    birth: tuple[int, ...] = (3,)
    survive: tuple[int, ...] = (2, 3)


_OFFSETS = daft.from_pylist(
    [{"dx": dx, "dy": dy} for dx in (-1, 0, 1) for dy in (-1, 0, 1) if (dx, dy) != (0, 0)]
)


class LifeProcessor(AsyncProcessor):
    components = (Cell,)
    priority = 10

    async def process(self, df: DataFrame, resources: Resources, **kwargs) -> DataFrame:
        rule = resources.require(Rule)

        # Each live cell contributes one vote to each of its 8 neighbour positions.
        neighbour_counts = (
            df.where(col("cell__alive"))
            .join(_OFFSETS, how="cross")
            .select(
                (col("cell__x") + col("dx")).alias("cell__x"),
                (col("cell__y") + col("dy")).alias("cell__y"),
            )
            .groupby("cell__x", "cell__y")
            .agg(col("cell__x").count().alias("neighbours"))
        )

        n = col("neighbours").fill_null(0)
        next_alive = (col("cell__alive") & n.is_in(list(rule.survive))) | (
            ~col("cell__alive") & n.is_in(list(rule.birth))
        )

        return (
            df.join(neighbour_counts, on=["cell__x", "cell__y"], how="left")
            .with_column("cell__alive", next_alive)
            .exclude("neighbours")
        )


# ─── Visualisation helpers ──────────────────────────────────────────────────


def render_cells(alive: set[tuple[int, int]]) -> str:
    grid = [[" "] * WIDTH for _ in range(HEIGHT)]
    for x, y in alive:
        if 0 <= x < WIDTH and 0 <= y < HEIGHT:
            grid[y][x] = "█"
    border = "┌" + "─" * WIDTH + "┐"
    bottom = "└" + "─" * WIDTH + "┘"
    body = "\n".join("│" + "".join(row) + "│" for row in grid)
    return f"{border}\n{body}\n{bottom}"


def render(rows: list[dict]) -> str:
    return render_cells({(r["cell__x"], r["cell__y"]) for r in rows if r["cell__alive"]})


async def latest_rows(world, tick: int) -> list[dict]:
    df = await world.query(Cell)
    return df.where(col("tick") == tick).select("cell__x", "cell__y", "cell__alive").to_pylist()


# ─── Seeds ──────────────────────────────────────────────────────────────────


def glider(x0: int, y0: int) -> set[tuple[int, int]]:
    return {(x0 + 1, y0), (x0 + 2, y0 + 1), (x0, y0 + 2), (x0 + 1, y0 + 2), (x0 + 2, y0 + 2)}


def block(x0: int, y0: int) -> set[tuple[int, int]]:
    return {(x0, y0), (x0 + 1, y0), (x0, y0 + 1), (x0 + 1, y0 + 1)}


def r_pentomino(x0: int, y0: int) -> set[tuple[int, int]]:
    """The famous chaotic methuselah — quickly creates dense neighbourhoods,
    so Conway and HighLife rules diverge within a few ticks."""
    return {(x0 + 1, y0), (x0 + 2, y0), (x0, y0 + 1), (x0 + 1, y0 + 1), (x0 + 1, y0 + 2)}


SEED = glider(1, 1) | r_pentomino(11, 4) | block(18, 9)


# ─── Demo ───────────────────────────────────────────────────────────────────


async def seed_world(world, seed: set[tuple[int, int]]) -> None:
    for y in range(HEIGHT):
        for x in range(WIDTH):
            await world.spawn(Cell(x=x, y=y, alive=(x, y) in seed))


async def population(world, tick: int) -> int:
    rows = await latest_rows(world, tick)
    return sum(1 for r in rows if r["cell__alive"])


async def main() -> None:
    storage = StorageConfig(uri="./archetype_data", namespace="game_of_life")

    async with ArchetypeRuntime() as runtime:
        # Each world gets its own admin actor — the per-actor command quota
        # is not refilled during a process, and seeding a grid is hundreds
        # of spawn commands.
        conway = runtime.world(
            "conway",
            storage=storage,
            processors=[LifeProcessor()],
            resources=[Rule(name="Conway", birth=(3,), survive=(2, 3))],
        ).as_actor(_admin())
        highlife = runtime.world(
            "highlife",
            storage=storage,
            processors=[LifeProcessor()],
            resources=[Rule(name="HighLife", birth=(3, 6), survive=(2, 3))],
        ).as_actor(_admin())

        await seed_world(conway, SEED)
        print(f"Initial seed ({len(SEED)} live cells):")
        print(render_cells(SEED))

        await conway.run(steps=TICKS)
        info = await conway.info()
        final_tick = info.tick - 1
        print(f"\nConway after {TICKS} ticks (tick={final_tick}):")
        print(render(await latest_rows(conway, tick=final_tick)))

        # ── Time-travel: replay an earlier generation from the same world ───
        replay = TICKS // 2
        print(f"\nTime-travel — same world, generation {replay}:")
        print(render(await latest_rows(conway, tick=replay)))

        # ── HighLife on the same seed (fresh world; rule lives in resources) ─
        await seed_world(highlife, SEED)
        await highlife.run(steps=TICKS)
        info = await highlife.info()
        print(f"\nHighLife after {TICKS} ticks (tick={info.tick - 1}):")
        print(render(await latest_rows(highlife, tick=info.tick - 1)))

        # ── Population trajectories side-by-side ────────────────────────────
        print("\nPopulation per generation:")
        print(f"  {'tick':>4}  {'Conway':>7}  {'HighLife':>9}")
        for t in range(TICKS):
            c = await population(conway, t)
            h = await population(highlife, t)
            marker = "  ←" if c != h else ""
            print(f"  {t:>4}  {c:>7}  {h:>9}{marker}")


if __name__ == "__main__":
    asyncio.run(main())
