"""Two immutable DDlog programs through the supported Archetype 0.7 facade.

Configure the five ARCHETYPE_* native paths and use a fresh store. This script
requires the real DDlog driver. It contains no simulated compiler fallback.
"""

from __future__ import annotations

import asyncio
import time

from archetype import (
    ArchetypeRuntime,
    Change,
    ComponentProjection,
    Composition,
    Connection,
    Endpoint,
    InputPort,
    LeafProgram,
    OutputPort,
    ProgramNode,
    Relation,
)


async def until(probe, predicate, *, timeout=600):
    deadline = time.monotonic() + timeout
    while True:
        value = await probe()
        if predicate(value):
            return value
        if time.monotonic() >= deadline:
            raise TimeoutError(
                "Native work did not reach the requested state; inspect retained evidence"
            )
        await asyncio.sleep(0.05)


async def main():
    definition = LeafProgram(
        "out(E,B,D) :- seed(E,B,D).",
        (
            Relation("seed", True, ("int64", "bool", "float64")),
            Relation("out", False, ("int64", "bool", "float64")),
        ),
        ("seed",),
        ("out",),
    )
    async with ArchetypeRuntime() as runtime:
        first = await runtime.program("first_program").publish(definition, request_key="first")
        second = await runtime.program("second_program").publish(definition, request_key="second")
        composition = Composition(
            (ProgramNode("first", first), ProgramNode("second", second)),
            (InputPort("seed", ("int64", "bool", "float64"), (Endpoint("first", "seed"),)),),
            (Connection(Endpoint("first", "out"), Endpoint("second", "seed")),),
            (OutputPort("out", Endpoint("second", "out")),),
        )
        program = await runtime.program("pipeline").publish(composition, request_key="pipeline")
        world = runtime.world(
            "experiment",
            components=(ComponentProjection("live", "out", ("entity_id", "enabled", "value"), 0),),
            inputs=(("seed", ("int64", "bool", "float64")),),
        )
        await world.create(program, request_key="experiment")
        await world.start()
        status = await until(world.status, lambda value: value.state == "running")
        await world.admit(
            (Change("seed", (9007199254741109, True, 1.25)),),
            generation=status.generation,
            revision=status.revision,
            admission_key="first_cut",
            expected_head=None,
        )
        admission = await until(
            lambda: world.admission_status(status.generation, "first_cut"),
            lambda value: value.state == "frozen",
        )
        cut = await world.publish(admission.boundary)
        await world.confirm(admission.boundary, cut)
        page = await cut.read("live")
        assert page.rows == ((9007199254741109, True, 1.25),)
        print("Complete cut:", cut.world, cut.run, cut.tick, cut.cut_id)
        print("Typed rows:", page.rows)


if __name__ == "__main__":
    asyncio.run(main())
