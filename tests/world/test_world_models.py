# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""World value and operation-model contracts."""

from __future__ import annotations

from typing import Literal, get_args, get_origin

import pytest
from pydantic import Field, ValidationError

from archetype.core.component import Component
from archetype.world.handlers import WORLD_OPERATION_HANDLERS
from archetype.world.models import (
    PORTABLE_TICK_OPERATION_TYPES,
    WORLD_OPERATION_TYPES,
    AddProcessor,
    ComponentTypeRef,
    ComponentValue,
    Despawn,
    RunResult,
    Spawn,
    Step,
    WorldOperation,
    require_portable_tick_operation,
)


class MutableMarker(Component):
    value: int = 0
    labels: list[str] = Field(default_factory=list)


def test_portable_component_value_is_an_immutable_snapshot() -> None:
    marker = MutableMarker(value=3, labels=["before"])
    value = ComponentValue.from_component(marker)
    marker.value = 99
    marker.labels.append("after")

    restored = value.materialize()

    assert isinstance(restored, MutableMarker)
    assert restored.value == 3
    assert restored.labels == ["before"]
    with pytest.raises(ValidationError):
        value.fields_json = "{}"


def test_component_type_reference_is_schema_bound() -> None:
    reference = ComponentTypeRef.from_type(MutableMarker)

    assert reference.type_name == "MutableMarker"
    assert len(reference.schema_fingerprint) == 64
    assert reference.resolve() is MutableMarker


@pytest.mark.parametrize(
    ("model", "expected"),
    [
        (Spawn(world_id="world", components=()), "spawn"),
        (Despawn(world_id="world", entity_id=7), "despawn"),
        (Step(world_id="world"), "step"),
    ],
)
def test_operation_discriminator_is_literal_and_model_is_frozen(
    model: WorldOperation, expected: str
) -> None:
    field = type(model).model_fields["operation"]
    assert get_origin(field.annotation) is Literal
    assert get_args(field.annotation) == (expected,)
    assert model.operation == expected

    with pytest.raises(ValidationError):
        model.operation = "other"


def test_live_capability_is_explicit_and_identity_preserving() -> None:
    processor = object()
    operation = AddProcessor(world_id="world", processor=processor)

    assert operation.processor is processor
    assert type(operation).direct_only is True
    with pytest.raises(TypeError, match="direct-only"):
        require_portable_tick_operation(operation)


def test_portable_admission_and_handler_inventories_are_exact() -> None:
    spawn = Spawn(world_id="world", components=())

    assert require_portable_tick_operation(spawn) is spawn
    assert type(spawn) in PORTABLE_TICK_OPERATION_TYPES
    assert frozenset(WORLD_OPERATION_HANDLERS) == frozenset(WORLD_OPERATION_TYPES)


def test_results_are_frozen_and_use_tuple_aggregation() -> None:
    result = RunResult(
        run_id="00000000-0000-7000-8000-000000000003",
        world_id="00000000-0000-7000-8000-000000000002",
        ticks_completed=1,
        final_tick=1,
    )

    with pytest.raises(ValidationError):
        result.final_tick = 2
