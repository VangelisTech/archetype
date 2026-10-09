# GiT: query other Archetype episodes

This offline dogfood query uses the supported 0.7 `ArchetypeRuntime` cold reader.
It verifies complete cut and artifact occurrence identities through the native
store, then lets Daft join evidence **across** episodes. It does not add a second
tick engine, read Iceberg tables by physical path, or put Daft in live execution.

Install the matched candidate native/ECS wheels with `archetype-ecs[analysis]`.
Set `ARCHETYPE_NATIVE_LIBRARY` and `ARCHETYPE_STORE` to the matching installed
native library and store. From the checkout:

```sh
python examples/episode_queries/query.py episodes.jsonl --limit 32
```

Each JSONL row is a curator-supplied episode reference with exactly these keys:

```json
{"episode_id":"ep-a","pair_id":"pair-1","split":"test","task":"H3","instruction":"Put away the bowl used first","required_target":"bowl-left","history_event":"left-first","world":"episode-a","run":"main","context":"episode-a-media","tick":2,"cut_id":"CUT_FROM_HISTORY","history_artifact_id":"UUIDV7_FROM_OCCURRENCES","current_artifact_id":"UUIDV7_FROM_OCCURRENCES"}
```

Supply one row for each A/B episode. `world` and `run` select the historical
world. `context` is its hosted artifact context resource. The history occurrence
must be cutless or bound to an earlier cut; the current image must be bound to
the declared decision cut. Publish the decision cut before attaching that image.
The manifest's `pair_id`, instruction, event and correct target are annotations,
not facts inferred from file metadata. Use unique context resource names for
different logical worlds within a reader process.

The query returns a pair only when it has the same declared pair ID, task,
instruction, split and **byte-identical current image**, plus different history
content, history event and required target. These checks catch accidental
single-frame shortcuts and some mislabeled pairs. They do not establish that the
past event caused the correct target, nor do they evaluate a policy. Human or
environment verification must establish the target labels. To reproduce GiT's
policy result, run both histories with a fixed model and record target selection
and full task success separately, using the returned cut and artifact IDs as
evidence anchors. Keep train/test partitions and pair mates together when
sampling; never split a counterfactual pair across partitions.

The same verified episode index can support other paper probes: failure discovery
joins outcomes to cut/occurrence IDs; scenario search filters task and event
annotations; model-output verification joins predictions to the exact observation
and evaluation receipt. Those probes need outcome and prediction records that the
0.7 artifact index does not invent. The GiT project page currently has no public
dataset download, so populate this manifest from existing Archetype episodes
until its data is published.
