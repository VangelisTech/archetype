# Updater

`AsyncUpdateManager` is the write facade to the store. It stamps housekeeping columns onto processed DataFrames and delegates the append to the store.

```python
class AsyncUpdateManager(iAsyncUpdateManager):
    def __init__(self, store: iAsyncStore, validate_flag: bool = False):
        self.store = store
        self.validate_flag = validate_flag

    async def update(
        self, df: DataFrame, sig: ArchetypeSignature, tick: int, world_id: str, run_id: str
    ) -> DataFrame:
        df = df.with_columns(
            {
                "tick": lit(tick).cast(daft.DataType.int32()),
                "world_id": lit(str(world_id)),
                "run_id": lit(str(run_id)),
                "entity_id": col("entity_id").cast(daft.DataType.int32()),
            }
        )

        await self.store.append(sig, df)
        return df
```

## How It Works

The updater sits between the world and the store on the write path:

```text
AsyncWorld.update()
       |
AsyncUpdateManager.update()
       |
AsyncStore.append()
```

Every DataFrame returned by processor execution passes through the updater before being appended to the archetype table.

## What It Does

The `update()` method applies four housekeeping mutations before appending:

```python
df = await updater.update(df, sig, tick=5, world_id="abc", run_id="run-1")
```

1. **Stamp `tick`** -- overwrite with the current tick as `int32`
2. **Stamp `world_id`** -- overwrite with the world's ID as `string`
3. **Stamp `run_id`** -- overwrite with the current run's ID as `string`
4. **Cast `entity_id`** -- ensure `int32` type for schema consistency

These stamps ensure every row in storage has correct, consistent metadata regardless of what processors may have done to the DataFrame.

After stamping, the updater calls `store.append(sig, df)` and logs the duration.

## Why Stamping Matters

Processors receive DataFrames and return DataFrames. They can add columns, modify values, and filter rows -- but they should not modify housekeeping columns. The updater is the single point that enforces correct metadata before persistence:

- **Spawned entities** arrive with placeholder `run_id=""` from the spawn cache. The updater stamps the real `run_id`.
- **Forked worlds** re-stamp `world_id` so cloned rows are attributed to the new world.
- **Type safety** -- `entity_id` is cast to `int32` to match the base schema, preventing schema mismatches in union operations.

## World Facade

Most code goes through the world:

```python
# Internally calls updater.update() with world_id and run_config.run_id
df = await world.update(df, sig, run_config)
```

## Further Reading

- [Data Flow](data-flow.md) -- how the updater fits into the write path and command pipeline
- [Querier](querier.md) -- the read counterpart to the updater
- [Stores](stores.md) -- the storage backends the updater appends to

## Source Reference

The updater is defined in `src/archetype/core/aio/async_updater.py`.
