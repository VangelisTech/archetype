# Runtime contract

`ArchetypeRuntime` is the supported 0.7 entry point. Construction, handles and
context entry are inert. The first operation loads the matched C ABI library,
checks ABI 1 and contract 3, then opens one private owner. The library reuses the
pinned DDlog WorldManager and CutStore. Public handles expose logical resources.

```python
from archetype import ArchetypeRuntime

async with ArchetypeRuntime() as runtime:
    world = runtime.world("experiment")
    history = await world.history()
```

For live operations configure `ARCHETYPE_NATIVE_LIBRARY`, `ARCHETYPE_STORE`,
`ARCHETYPE_REGISTRY`, `ARCHETYPE_BUILDS` and `ARCHETYPE_NATIVE_DRIVER`. Explicit
constructor keywords `library`, `store`, `registry`, `builds`, `driver` override
the corresponding environment values. For a cold reader omit the last three.
These are operator configuration paths; they are never wire resource identities.

Async ownership belongs to one loop and creating PID. `with ArchetypeRuntime.sync()`
provides the same methods without `await` and owns one retained Runner. Sync calls
inside an active event loop are rejected. Failed close retains the owner for retry.
`entrypoint(**configuration)` injects either facade and guarantees teardown.

World shutdown is local. Process shutdown drains all retained calls. A cancelled
wait does not cancel native work or free concurrency early. Incompatible libraries
are rejected before native open effects. Loader failures are bounded; private
paths and native diagnostics do not appear in public exceptions.

Declare programs through `runtime.program(name).publish(...)`; declare component
mappings and input types when configuring a live world. Use `RuntimeCut.read`
for bounded factual rows and `.analyze` for a lazy Daft frame after reading.
See [quickstart](quickstart.md) and [durability](durability.md).
