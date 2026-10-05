# Complete history and explicit forks

`await world.history(offset=0, limit=32)` returns complete cut handles. Read one
cut using `await cut.read("component", offset=0, limit=32)`. Rows preserve exact
Int64, Bool and finite Float64 values. Empty cuts are complete results, not an
instruction to resurrect older entity rows. `.analyze(...)` is explicit Daft
analysis of that bounded result outside live execution.

```python
child = runtime.world("child", components=projections, inputs=inputs)
status = await child.fork(world, source_cut, request_key="child_from_a",
                          expected_generation=0)
```

Fork selects an explicit source cut, including a historical cut before retractions.
The request key and source identity must stay identical for retry. A pending or
uncertain result is not permission to change those inputs. Inspect the returned
`lineage_ready` state and retry the exact request when needed. Destination storage
and hosted context are isolated. Inherited history remains readable through the
child's authorized logical scope. Artifacts retain exact source occurrence identity
and attribution; the fork does not invent duplicate occurrences.
