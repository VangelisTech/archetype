# Querier

`AsyncQueryManager` is the read facade to the store. It provides filtered access to archetype tables by tick, entity ID, and component projections.

```python
class AsyncQueryManager(iAsyncQueryManager):
    def __init__(self, store: iAsyncStore):
        self._store = store

    async def get_archetype(self, sig: ArchetypeSignature, world_id: str, run_id: str) -> DataFrame:
        return await self._store.get_archetype_df(sig=sig, world_id=world_id, run_id=run_id)

    async def query_archetype(
        self,
        sig: ArchetypeSignature,
        world_id: str,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        components: list["Component"] | None = None,
        run_id: str = None,
    ) -> DataFrame:
        df = await self.get_archetype(sig=sig, world_id=world_id, run_id=run_id)
        df = df.where(df["is_active"])

        if ticks:
            df = df.where(df["tick"].is_in(ticks))

        if entity_ids:
            df = df.where(df["entity_id"].is_in(entity_ids))

        if components:
            a = Archetype(components)
            df = df.select(*a.schema.names)

        return df
```

## How It Works

The querier sits between the world and the store:

```text
AsyncWorld.query_archetype()
       |
AsyncQueryManager.query_archetype()
       |
AsyncStore.get_archetype_df()
```

All queries start with the raw archetype DataFrame from the store, then apply filters in sequence.

## API

### get_archetype

Fetch the raw DataFrame for a signature, world, and run:

```python
df = await querier.get_archetype(sig, world_id="abc", run_id="run-1")
```

Delegates directly to `store.get_archetype_df()` without applying any filters.

### query_archetype

The primary query method with full filtering:

```python
df = await querier.query_archetype(
    sig=sig,
    world_id="abc",
    run_id="run-1",
    ticks=[5, 6],           # only these ticks
    entity_ids=[1, 2, 3],   # only these entities
    components=[Position()], # project to these columns
)
```

Filters are applied in order:

1. **is_active** -- only active (alive) entities
2. **ticks** -- restrict to specific tick numbers (if provided)
3. **entity_ids** -- restrict to specific entities (if provided)
4. **components** -- project to the schema of the given components (if provided)

All filters are optional. With no filters, you get all active entities for the given world and run.

### Component Projection

When `components` is provided, the result DataFrame is projected to only the columns defined by those component types (plus base columns). This is useful when you only need a subset of an archetype's fields:

```python
# Archetype has (Health, Position, Velocity)
# Only fetch position columns + base columns
df = await querier.query_archetype(
    sig=sig,
    world_id="abc",
    components=[Position()],
)
# Columns: world_id, run_id, entity_id, tick, is_active, position__x, position__y
```

## World Facade

Most code uses the world's facade methods rather than the querier directly:

```python
# Delegates to querier with world_id and run_id filled in
df = await world.query_archetype(sig, ticks=[5])

# Multi-archetype union query
df = await world.get_components([Position, Health])
```

`world.query_archetype()` automatically fills in `world_id` and `run_id` from the world's current state and defaults to the current tick if no ticks are specified.

## Source Reference

The querier is defined in `src/archetype/core/aio/async_querier.py`.
