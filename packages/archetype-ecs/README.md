# Archetype ECS

Archetype provides typed simulation state, world history, forks, and
world-context artifacts. The existing Python 0.6 runtime uses Daft for execution.

A Rust adapter over DDlog Runtime is under development in the repository.
The live DDlog path executes no Daft work; the Python bridge and generic
simulation API/MCP migration are still planned. See the
[execution contract](https://github.com/VangelisTech/archetype/blob/main/docs/guide/ddlog-runtime.md)
for the implemented boundary and remaining work.

Install the framework with `pip install archetype-ecs`. Research is available
separately as `archetype-research`. Missions and Physical AI evaluation products
are removed from the intended Archetype surface. Existing release artifacts and
stored histories remain separate from this source migration.

```python
import asyncio
from archetype import ArchetypeRuntime

async def main():
    async with ArchetypeRuntime() as runtime:
        world = runtime.world("example")
        await world.spawn()
        print(await world.run(steps=1))

asyncio.run(main())
```

This example uses the existing Python runtime. It is not a DDlog example.
See the repository README for the native preview and its explicit tests.
