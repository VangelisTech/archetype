# Python API Reference

Auto-generated from source docstrings via
[mkdocstrings](https://mkdocstrings.github.io/).

---

## Core

::: archetype.core.component.Component
    options:
      show_bases: false
      members:
        - get_type_by_name
        - from_dict
        - get_prefix
        - to_pyarrow_schema
        - get_prefixed_schema
        - to_row_dict

::: archetype.core.aio.async_processor.AsyncProcessor
    options:
      members:
        - process

::: archetype.core.aio.async_world.AsyncWorld
    options:
      members:
        - add_hook
        - step

::: archetype.core.resources.Resources
    options:
      members:
        - insert
        - get
        - require
        - remove

---

## Configuration

::: archetype.core.config.WorldConfig

::: archetype.core.config.StorageConfig

::: archetype.core.config.RunConfig
    options:
      members:
        - dev
        - benchmark
        - validate

---

## App

::: archetype.app.container.ServiceContainer
    options:
      members:
        - shutdown

::: archetype.app.models.Command

::: archetype.app.models.CommandType

::: archetype.app.auth.models.ActorCtx
