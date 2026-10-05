# Archetype

Archetype manages typed simulation state, world history, forks, and artifacts.
It remains an active project, with limited current investment.

The [DDlog execution migration](guide/ddlog-runtime.md) moves live execution
to the existing Rust DDlog Runtime. Persistent public relations become typed
ECS components. A complete cut manifest controls which Iceberg snapshots form
one visible tick. Daft will serve queries and analysis outside that loop.

The Rust adapter is a tested preview. The existing Python 0.6 runtime and
REST/CLI still use the earlier Daft execution loop. The Python bridge and
generic DDlog simulation API/MCP are planned. The documentation marks that
boundary rather than presenting the migration as complete.

Start with the [current Python quickstart](guide/quickstart.md), the
[runtime contract](guide/runtime.md), or the
[native preview](guide/ddlog-runtime.md#run-the-evidence).
Missions and Physical AI evaluation products are outside the retained scope.
Generic inference, Research, world-context artifacts, and engine verification
remain. Historical release records remain available under History.
