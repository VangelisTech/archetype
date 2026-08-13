# Updater

`AsyncUpdateManager` is the write facade to the store. It stamps housekeeping
and commit-identity columns onto processed DataFrames and delegates the append
to the store.

```python
class AsyncUpdateManager(iAsyncUpdateManager):
    def __init__(self, store: iAsyncStore, validate_flag: bool = False):
        self.store = store
        self.validate_flag = validate_flag

    async def update(
        self,
        df: DataFrame,
        sig: ArchetypeSignature,
        tick: int,
        world_id: str,
        run_id: str,
        commit: CommitContext | None = None,
    ) -> DataFrame:
        df = df.with_columns(
            {
                "tick": lit(tick).cast(daft.DataType.int32()),
                "world_id": lit(str(world_id)),
                "run_id": lit(str(run_id)),
                "entity_id": col("entity_id").cast(daft.DataType.int32()),
                "commit_token": lit(commit.commit_token if commit else ""),
                "writer_epoch": lit(commit.writer_epoch if commit else 0).cast(
                    daft.DataType.int64()
                ),
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

The `update()` method applies six metadata mutations before appending:

```python
df = await updater.update(
    df,
    sig,
    tick=5,
    world_id="abc",
    run_id="run-1",
    commit=commit_context,
)
```

1. **Stamp `tick`** -- overwrite with the current tick as `int32`
2. **Stamp `world_id`** -- overwrite with the world's ID as `string`
3. **Stamp `run_id`** -- overwrite with the current run's ID as `string`
4. **Cast `entity_id`** -- ensure `int32` type for schema consistency
5. **Stamp `commit_token`** -- identify the coordinated tick attempt
6. **Stamp `writer_epoch`** -- identify the fenced writer as `int64`

These stamps ensure every row in storage has correct, consistent metadata regardless of what processors may have done to the DataFrame.

After stamping, the updater calls `store.append(sig, df)` and logs the duration.
Without a commit context, it stamps `""` and `0`, the implicit epoch-0 identity
used by uncoordinated core worlds.

## Why Stamping Matters

Processors receive DataFrames and return DataFrames. They can add columns, modify values, and filter rows -- but they should not modify housekeeping columns. The updater is the single point that enforces correct metadata before persistence:

- **Spawned entities** arrive with placeholder `run_id=""` from the spawn cache. The updater stamps the real `run_id`.
- **Forked worlds** re-stamp `world_id` so cloned rows are attributed to the new world.
- **Type safety** -- `entity_id` is cast to `int32` to match the base schema, preventing schema mismatches in union operations.
- **Coordinated visibility** -- every archetype written for one tick receives the same commit token and writer epoch.

## Append Is Not Visibility

For an uncoordinated core world, a successful append retains the legacy
epoch-0 behavior and is immediately readable. For a world created through the
service layer, the updater's successful append is only one phase of the tick
commit:

```text
compute every archetype
    -> append every stamped frame
    -> flush staged rows
    -> publish one tick manifest
    -> consume mutation caches and advance the tick
```

Readers admit current-generation rows only when their commit token is published
for that tick. If append, flush, or manifest publication fails, the tick does
not advance and its staged mutations remain available for retry. Physical rows
from an unpublished attempt may remain in storage, but they are invisible.
See [Atomic Tick Visibility](atomic-visibility.md) for the normative protocol.

## World Facade

Most code goes through the world:

```python
# Internally passes the world's pinned run_id and current tick commit context
df = await world.update(df, sig, run_config)
```

## Further Reading

- [Data Flow](data-flow.md) -- how the updater fits into the write path and command pipeline
- [Querier](querier.md) -- the read counterpart to the updater
- [Stores](stores.md) -- the storage backends the updater appends to
- [Atomic Tick Visibility](atomic-visibility.md) -- the coordinated visibility boundary

## Source Reference

The updater is defined in `packages/archetype-ecs/src/archetype/core/aio/async_updater.py`.
