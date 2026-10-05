# Archetype DDlog adapter

This Rust preview maps declared outputs of a composed DDlog Runtime program to
typed ECS components and publishes complete ticks through Iceberg.

It does not evaluate rules or schedule native operators. DDlog Runtime owns
execution. The adapter owns component schemas, world/run/tick attribution,
immutable cut publication, and reads of complete historical cuts.

See [the migration contract](../../docs/guide/ddlog-runtime.md) for schema
mapping, recovery limits, commands, and remaining Python/API/MCP work.
