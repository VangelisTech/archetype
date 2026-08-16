# Framework

| | |
|---|---|
| Distribution | `archetype-ecs` |
| Import package | `archetype` |
| Primary entry point | `ArchetypeRuntime` |
| Depends on a world library | No |

The framework owns the generic ECS and process host: Components, Processors,
worlds, append-only ticks, storage, commands, Activities, artifacts,
evaluation, runtime lifetime, REST hosting, and the CLI.

Missions, Physical AI, and Research are separately installed libraries. Their
domain values and workflows do not belong to the framework root API.

For a deliberately bounded synchronous teaching loop without production
runtime or storage semantics, see [Smol](../smol/index.md).

## Learn the engine

- [Quickstart](../guide/quickstart.md)
- [Components](../guide/components.md)
- [Processors](../guide/processors.md)
- [Working with worlds](../guide/working-with-worlds.md)
- [History and forks](../guide/history-and-forks.md)
- [Core architecture](../guide/core-architecture.md)

## Build and extend

- [Building simulations](../guide/building-simulations.md)
- [Resources](../guide/resources.md)
- [Lifecycle hooks](../guide/hooks.md)
- [Prefab libraries](../guide/prefab-libraries.md)
- [Custom commands](../guide/custom-commands.md)

## Run and operate

- [Runtime contract](../guide/runtime.md)
- [Activities](../guide/activities.md)
- [Storage](../guide/stores.md)
- [Artifacts](../guide/artifacts.md)
- [Access control and audit](../guide/command-gate.md)
- [Running a server](../guide/api-layer.md)

## Reference

- [Python API](../reference/python-api.md)
- [Framework evaluation](../reference/python/evaluation.md)
- [REST API](../reference/rest-api.md)
- [CLI](../reference/cli.md)

For framework internals, cross-package architecture, and executable contract
evidence, continue to [Maintainers](../maintainers/index.md).
