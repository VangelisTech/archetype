# Two-program quickstart

Use matching candidate wheels and the native build/driver configuration in the
[README](../index.md). Run `python examples/native_simulation.py` after setting
`ARCHETYPE_NATIVE_LIBRARY`, `ARCHETYPE_STORE`, `ARCHETYPE_REGISTRY`,
`ARCHETYPE_BUILDS` and `ARCHETYPE_NATIVE_DRIVER`.

The example creates two immutable identity programs, composes their ports,
declares an Int64 entity key with Bool and Float64 component fields, creates a
world, starts it, admits changes, publishes a complete cut and reads exact values.
It retains the admission key and confirms the exact cut before leaving the runtime.
Construction and world creation do not compile; start activates live work.

```python
from archetype import ArchetypeRuntime, Change

async with ArchetypeRuntime() as runtime:
    # Program definitions, composition and projections are in the example.
    world = runtime.world("experiment", components=projections, inputs=inputs)
    await world.create(program, request_key="experiment")
    await world.start()
    # Wait for running status, then admit using that generation/revision.
    await world.admit((Change("seed", (9007199254741109, True, 1.25)),),
                      generation=status.generation, revision=status.revision,
                      admission_key="first", expected_head=None)
    # Wait for the exact admission to freeze, publish and confirm its boundary.
    cut = await world.publish(admission.boundary)
    await world.confirm(admission.boundary, cut)
    rows = await cut.read("live")
```

The complete file includes bounded polling and exact state checks. Reuse a
request key only for identical creation; use a fresh store for the standalone
quickstart. See [durability](durability.md) before implementing recovery or retries.
