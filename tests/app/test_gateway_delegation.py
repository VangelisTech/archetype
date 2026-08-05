# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Exact operation-value evidence for dispatcher consumers."""

from archetype.core.component import Component
from archetype.core.config import RunConfig, StorageConfig
from archetype.world.models import (
    AddComponents,
    AddProcessor,
    ComponentTypeRef,
    ComponentValue,
    DiscoverWorlds,
    EpisodeConfig,
    OpenWorldReadonly,
    RemoveComponents,
    RemoveProcessor,
    ResumeWorld,
    RolloutConfig,
    Run,
    RunEpisode,
    RunRollout,
    Step,
    Update,
)


class Pos(Component):
    x: int = 0


def test_direct_mutations_construct_exact_operation_values():
    components = [Pos(x=3)]
    component_types = [Pos]
    processor = object()
    expected_value = (ComponentValue.from_component(components[0]),)
    operations = (
        Update(world_id="world-1", entity_id=7, components=expected_value),
        AddComponents(world_id="world-1", entity_id=7, components=expected_value),
        RemoveComponents(
            world_id="world-1",
            entity_id=7,
            component_types=tuple(
                ComponentTypeRef.from_type(component_type) for component_type in component_types
            ),
        ),
        AddProcessor(world_id="world-1", processor=processor),
        RemoveProcessor(world_id="world-1", processor_type=type(processor)),
    )

    assert operations[0].components == expected_value
    assert operations[1].components == expected_value
    assert operations[2].component_types == (ComponentTypeRef.from_type(Pos),)
    assert operations[3].processor is processor
    assert operations[4].processor_type is type(processor)


def test_discovery_and_resume_construct_exact_operations():
    storage = StorageConfig()
    operations = (
        DiscoverWorlds(storage_config=storage),
        OpenWorldReadonly(storage_config=storage, world_id="world-1"),
        ResumeWorld(storage_config=storage, world_id="world-1"),
    )

    assert [operation.operation for operation in operations] == [
        "discover_worlds",
        "open_world_readonly",
        "resume_world",
    ]
    assert all(operation.storage_config is storage for operation in operations)


def test_simulation_operations_preserve_live_inputs():
    capability = object()
    coordinates = ("outer", ("inner", 3))
    input_kwargs = {
        "capability": capability,
        "coordinates": coordinates,
    }

    operations = [
        Step(world_id="world-1", run_config=RunConfig(), input_kwargs=input_kwargs),
        Run(world_id="world-1", run_config=RunConfig(), input_kwargs=input_kwargs),
        RunEpisode(world_id="world-1", config=EpisodeConfig(), input_kwargs=input_kwargs),
        RunRollout(world_id="world-1", config=RolloutConfig(), input_kwargs=input_kwargs),
    ]
    assert [type(operation) for operation in operations] == [
        Step,
        Run,
        RunEpisode,
        RunRollout,
    ]
    for operation in operations:
        assert operation.input_kwargs["capability"] is capability
        assert operation.input_kwargs["coordinates"] is coordinates
        assert operation.input_kwargs["coordinates"] == ("outer", ("inner", 3))
