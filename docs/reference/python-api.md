# Python API Reference

This reference is auto-generated from source docstrings using
[mkdocstrings](https://mkdocstrings.github.io/).

---

## Core

### Component

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

### AsyncProcessor

::: archetype.core.aio.async_processor.AsyncProcessor
    options:
      members:
        - process

### AsyncWorld

::: archetype.core.aio.async_world.AsyncWorld
    options:
      members:
        - add_hook
        - step

### Resources

::: archetype.core.resources.Resources
    options:
      members:
        - insert
        - get
        - require
        - remove

---

## Configuration

### WorldConfig

::: archetype.core.config.WorldConfig

### StorageConfig

::: archetype.core.config.StorageConfig

### RunConfig

::: archetype.core.config.RunConfig
    options:
      members:
        - dev
        - benchmark
        - validate

---

## App

### ServiceContainer

::: archetype.app.container.ServiceContainer
    options:
      members:
        - shutdown

### Command

::: archetype.app.models.Command

### CommandType

::: archetype.app.models.CommandType

### ActorCtx

::: archetype.app.auth.models.ActorCtx
