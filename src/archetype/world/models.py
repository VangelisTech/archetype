# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""World-owned values and exact operation models.

Operation models deliberately contain only family behavior inputs.  Durable
scheduling metadata (target tick, priority, leases, principal evidence, and
settlement) belongs to the commands family.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any, ClassVar, Literal, cast

import uuid_utils as uuid
from pydantic import BaseModel, Field, field_validator
from pydantic.json_schema import SkipJsonSchema

from archetype.core.component import Component
from archetype.core.config import (
    CacheConfig,
    JsonUUID,
    RunConfig,
    StorageConfig,
    WorldConfig,
)
from archetype.core.hooks import FireMode, HookEvent, HookHandle
from archetype.storage.catalog import schema_fingerprint


class _FrozenModel(BaseModel):
    model_config = dict(frozen=True, arbitrary_types_allowed=True, extra="forbid")


def _component_candidates(type_name: str) -> list[type[Component]]:
    """Return every imported component class with ``type_name``."""
    candidates: list[type[Component]] = []
    stack = list(Component.__subclasses__())
    seen: set[type[Component]] = set()
    while stack:
        component_type = stack.pop()
        if component_type in seen:
            continue
        seen.add(component_type)
        stack.extend(component_type.__subclasses__())
        if component_type.__name__ == type_name:
            candidates.append(component_type)
    return sorted(candidates, key=lambda cls: (cls.__module__, cls.__qualname__))


class ComponentTypeRef(_FrozenModel):
    """Immutable, schema-bound identity for one component type."""

    type_name: str
    schema_fingerprint: str = Field(min_length=64, max_length=64)
    # Direct dispatch retains exact class affinity; durable JSON deliberately
    # omits it and resolves only from the two portable fields above.
    local_type_affinity: SkipJsonSchema[type[Component] | None] = Field(
        default=None,
        exclude=True,
        repr=False,
    )

    @classmethod
    def from_type(cls, component_type: type[Component]) -> ComponentTypeRef:
        return cls(
            type_name=component_type.__name__,
            schema_fingerprint=schema_fingerprint(component_type.get_prefixed_schema()),
            local_type_affinity=component_type,
        )

    def resolve(self) -> type[Component]:
        if self.local_type_affinity is not None:
            local_fingerprint = schema_fingerprint(self.local_type_affinity.get_prefixed_schema())
            if (
                self.local_type_affinity.__name__ != self.type_name
                or local_fingerprint != self.schema_fingerprint
            ):
                raise ValueError("local component type affinity no longer matches wire identity")
            return self.local_type_affinity

        matches = [
            component_type
            for component_type in _component_candidates(self.type_name)
            if schema_fingerprint(component_type.get_prefixed_schema()) == self.schema_fingerprint
        ]
        if not matches:
            raise ValueError(
                f"Component type {self.type_name!r} with schema fingerprint "
                f"{self.schema_fingerprint} is not imported"
            )
        return matches[0]


class ComponentValue(ComponentTypeRef):
    """Immutable component value snapshot suitable for canonical JSON.

    A live Pydantic component instance is intentionally not retained: freezing
    an outer model or tuple would not freeze nested component state.  Canonical
    field JSON makes the operation value-based and gives deferred admission a
    stable serialization boundary.
    """

    fields_json: str

    @field_validator("fields_json", mode="before")
    @classmethod
    def _canonicalize_fields_json(cls, value: object) -> str:
        if not isinstance(value, str):
            raise TypeError("fields_json must be a JSON string")
        decoded = json.loads(value)
        if not isinstance(decoded, dict):
            raise ValueError("fields_json must decode to an object")
        return json.dumps(
            decoded,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )

    @classmethod
    def from_component(cls, component: Component) -> ComponentValue:
        fields_json = json.dumps(
            component.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        return cls(
            type_name=type(component).__name__,
            schema_fingerprint=schema_fingerprint(type(component).get_prefixed_schema()),
            fields_json=fields_json,
            local_type_affinity=type(component),
        )

    def materialize(self) -> Component:
        fields = json.loads(self.fields_json)
        if not isinstance(fields, dict):
            raise ValueError("component fields_json must decode to an object")
        return self.resolve()(**fields)


class WorldInfo(_FrozenModel):
    """Immutable snapshot of a world's identity and current position."""

    world_id: str | JsonUUID
    name: str | None = None
    tick: int = 0
    run_id: str | JsonUUID


class RunResult(_FrozenModel):
    """Summary of a completed managed run."""

    run_id: str | JsonUUID
    world_id: str | JsonUUID
    ticks_completed: int = 0
    commands_applied: int = 0
    final_tick: int = 0


class EpisodeConfig(_FrozenModel):
    """Configure a bounded episode on one live world."""

    episode_id: str | JsonUUID = Field(default_factory=uuid.uuid7)
    run_config: RunConfig = Field(default_factory=RunConfig)
    max_steps: int = Field(default=1000, ge=0)
    terminal_component: SkipJsonSchema[type[Component] | None] = None
    terminal_field: str | None = None
    terminal_all: bool = True
    termination: SkipJsonSchema[Callable[[Any], bool] | None] = None


class RolloutConfig(_FrozenModel):
    """Configure episodes run on fresh forks of a base world."""

    rollout_id: str | JsonUUID = Field(default_factory=uuid.uuid7)
    episode_config: EpisodeConfig = Field(default_factory=EpisodeConfig)
    num_episodes: int = Field(default=1, ge=0)
    parallel: bool = False
    name_prefix: str = "ep"
    destroy_forks_on_complete: bool = False


class EpisodeResult(_FrozenModel):
    """Result of one bounded episode."""

    episode_id: str | JsonUUID
    world_id: str | JsonUUID
    run_id: str | JsonUUID
    start_tick: int = 0
    final_tick: int = 0
    terminated: bool = False
    duration_steps: int = 0


class RolloutResult(_FrozenModel):
    """Ordered aggregate of rollout episode results."""

    rollout_id: str | JsonUUID
    base_world_id: str | JsonUUID
    episodes: tuple[EpisodeResult, ...] = ()
    num_episodes: int = 0
    total_duration_steps: int = 0


class ProcessorInfo(_FrozenModel):
    qualname: str
    priority: int = 0
    components: tuple[str, ...] = ()


class HookInfo(_FrozenModel):
    event_type: str
    handler_qualname: str
    mode: str = "blocking"
    handle_id: int


class ResourceInfo(_FrozenModel):
    qualname: str


class WorldOperation(_FrozenModel):
    """Base for exact world operation inputs.

    ``direct_only`` is classification metadata for registration validation,
    not a claim that the outer frozen model makes a nested live capability
    immutable.
    """

    direct_only: ClassVar[bool] = True
    operation: str


class Spawn(WorldOperation):
    direct_only: ClassVar[bool] = False
    operation: Literal["spawn"] = "spawn"
    world_id: str | JsonUUID
    components: tuple[ComponentValue, ...] = ()

    @classmethod
    def from_components(
        cls,
        *,
        world_id: str | JsonUUID,
        components: list[Component] | tuple[Component, ...],
    ) -> Spawn:
        return cls(
            world_id=world_id,
            components=tuple(ComponentValue.from_component(value) for value in components),
        )


class CreateEntities(WorldOperation):
    operation: Literal["create_entities"] = "create_entities"
    world_id: str | JsonUUID
    entities: tuple[tuple[ComponentValue, ...], ...] = ()

    @classmethod
    def from_entities(
        cls,
        *,
        world_id: str | JsonUUID,
        entities: list[list[Component]],
    ) -> CreateEntities:
        return cls(
            world_id=world_id,
            entities=tuple(
                tuple(ComponentValue.from_component(value) for value in components)
                for components in entities
            ),
        )


class ReserveEntityIds(WorldOperation):
    operation: Literal["reserve_entity_ids"] = "reserve_entity_ids"
    world_id: str | JsonUUID
    count: int = Field(ge=1)


class SpawnReserved(WorldOperation):
    direct_only: ClassVar[bool] = False
    operation: Literal["spawn_reserved"] = "spawn_reserved"
    world_id: str | JsonUUID
    entity_id: int
    components: tuple[ComponentValue, ...] = ()


class Despawn(WorldOperation):
    direct_only: ClassVar[bool] = False
    operation: Literal["despawn"] = "despawn"
    world_id: str | JsonUUID
    entity_id: int


class Update(WorldOperation):
    direct_only: ClassVar[bool] = False
    operation: Literal["update"] = "update"
    world_id: str | JsonUUID
    entity_id: int
    components: tuple[ComponentValue, ...] = ()


class AddComponents(WorldOperation):
    direct_only: ClassVar[bool] = False
    operation: Literal["add_components"] = "add_components"
    world_id: str | JsonUUID
    entity_id: int
    components: tuple[ComponentValue, ...] = ()


class RemoveComponents(WorldOperation):
    direct_only: ClassVar[bool] = False
    operation: Literal["remove_components"] = "remove_components"
    world_id: str | JsonUUID
    entity_id: int
    component_types: tuple[ComponentTypeRef, ...] = ()


class AddProcessor(WorldOperation):
    operation: Literal["add_processor"] = "add_processor"
    world_id: str | JsonUUID
    processor: Any


class RemoveProcessor(WorldOperation):
    operation: Literal["remove_processor"] = "remove_processor"
    world_id: str | JsonUUID
    processor_type: type[Any]


class CreateWorld(WorldOperation):
    operation: Literal["create_world"] = "create_world"
    config: WorldConfig = Field(default_factory=WorldConfig)
    storage_config: StorageConfig | None = None
    cache_config: CacheConfig | None = None


class ForkWorld(WorldOperation):
    operation: Literal["fork_world"] = "fork_world"
    source_world_id: str | JsonUUID
    name: str | None = None
    storage_config: StorageConfig | None = None
    cache_config: CacheConfig | None = None


class DestroyWorld(WorldOperation):
    operation: Literal["destroy_world"] = "destroy_world"
    world_id: str | JsonUUID


class GetWorldInfo(WorldOperation):
    operation: Literal["get_world_info"] = "get_world_info"
    world_id: str | JsonUUID


class ListWorlds(WorldOperation):
    operation: Literal["list_worlds"] = "list_worlds"


class DiscoverWorlds(WorldOperation):
    operation: Literal["discover_worlds"] = "discover_worlds"
    storage_config: StorageConfig


class OpenWorldReadonly(WorldOperation):
    operation: Literal["open_world_readonly"] = "open_world_readonly"
    storage_config: StorageConfig
    world_id: str | JsonUUID


class ResumeWorld(WorldOperation):
    operation: Literal["resume_world"] = "resume_world"
    storage_config: StorageConfig
    world_id: str | JsonUUID


class Step(WorldOperation):
    operation: Literal["step"] = "step"
    world_id: str | JsonUUID
    run_config: RunConfig = Field(default_factory=RunConfig)
    input_kwargs: dict[str, Any] = Field(default_factory=dict)


class Run(WorldOperation):
    operation: Literal["run"] = "run"
    world_id: str | JsonUUID
    run_config: RunConfig = Field(default_factory=RunConfig)
    input_kwargs: dict[str, Any] = Field(default_factory=dict)


class RunEpisode(WorldOperation):
    operation: Literal["run_episode"] = "run_episode"
    world_id: str | JsonUUID
    config: EpisodeConfig = Field(default_factory=EpisodeConfig)
    input_kwargs: dict[str, Any] = Field(default_factory=dict)


class RunRollout(WorldOperation):
    operation: Literal["run_rollout"] = "run_rollout"
    world_id: str | JsonUUID
    config: RolloutConfig = Field(default_factory=RolloutConfig)
    input_kwargs: dict[str, Any] = Field(default_factory=dict)


class QueryComponents(WorldOperation):
    operation: Literal["query_components"] = "query_components"
    components: tuple[ComponentTypeRef, ...]
    world_id: str | JsonUUID
    run_id: str | JsonUUID
    storage_config: StorageConfig | None = None
    ticks: tuple[int, ...] | None = None
    entity_ids: tuple[int, ...] | None = None
    lineage: tuple[tuple[str, str, int], ...] | None = None
    visibility_tokens: tuple[str, ...] | None = None


class QueryArchetype(WorldOperation):
    operation: Literal["query_archetype"] = "query_archetype"
    signature: tuple[ComponentTypeRef, ...]
    world_id: str | JsonUUID
    run_id: str | JsonUUID
    storage_config: StorageConfig | None = None
    ticks: tuple[int, ...] | None = None
    entity_ids: tuple[int, ...] | None = None
    components: tuple[ComponentTypeRef, ...] | None = None
    lineage: tuple[tuple[str, str, int], ...] | None = None


class ListSignatures(WorldOperation):
    operation: Literal["list_signatures"] = "list_signatures"
    storage_config: StorageConfig | None = None


class ListWorldSignatures(WorldOperation):
    operation: Literal["list_world_signatures"] = "list_world_signatures"
    world_id: str | JsonUUID
    storage_config: StorageConfig | None = None


class AddResource(WorldOperation):
    operation: Literal["add_resource"] = "add_resource"
    world_id: str | JsonUUID
    resource: object


class AddHook(WorldOperation):
    operation: Literal["add_hook"] = "add_hook"
    world_id: str | JsonUUID
    event_type: type[HookEvent]
    handler: Callable[..., Any]
    mode: FireMode = "blocking"


class RemoveHook(WorldOperation):
    operation: Literal["remove_hook"] = "remove_hook"
    world_id: str | JsonUUID
    handle: HookHandle


class ListProcessors(WorldOperation):
    operation: Literal["list_processors"] = "list_processors"
    world_id: str | JsonUUID


class ListHooks(WorldOperation):
    operation: Literal["list_hooks"] = "list_hooks"
    world_id: str | JsonUUID


class ListResources(WorldOperation):
    operation: Literal["list_resources"] = "list_resources"
    world_id: str | JsonUUID


type PortableTickOperation = (
    Spawn | SpawnReserved | Despawn | Update | AddComponents | RemoveComponents
)
PORTABLE_TICK_OPERATION_TYPES = frozenset(
    {Spawn, SpawnReserved, Despawn, Update, AddComponents, RemoveComponents}
)


def require_portable_tick_operation(
    operation: WorldOperation,
) -> PortableTickOperation:
    """Fail closed unless ``operation`` is an exact portable mutation type."""
    if type(operation) not in PORTABLE_TICK_OPERATION_TYPES:
        raise TypeError(
            f"{type(operation).__name__} is a direct-only world operation and "
            "cannot enter portable tick admission"
        )
    return cast("PortableTickOperation", operation)


WORLD_OPERATION_TYPES = (
    Spawn,
    CreateEntities,
    ReserveEntityIds,
    SpawnReserved,
    Despawn,
    Update,
    AddComponents,
    RemoveComponents,
    AddProcessor,
    RemoveProcessor,
    CreateWorld,
    ForkWorld,
    DestroyWorld,
    GetWorldInfo,
    ListWorlds,
    DiscoverWorlds,
    OpenWorldReadonly,
    ResumeWorld,
    Step,
    Run,
    RunEpisode,
    RunRollout,
    QueryComponents,
    QueryArchetype,
    ListSignatures,
    ListWorldSignatures,
    AddResource,
    AddHook,
    RemoveHook,
    ListProcessors,
    ListHooks,
    ListResources,
)
