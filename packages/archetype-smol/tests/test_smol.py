# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import tomllib
from enum import StrEnum
from pathlib import Path
from typing import Any, Self

import pytest
from daft import col, lit
from pydantic import (
    BaseModel,
    Field,
    ValidationError,
    ValidationInfo,
    computed_field,
    field_serializer,
    field_validator,
    model_serializer,
    model_validator,
)
from pydantic_core import SchemaValidator

import archetype.smol as smol
from archetype.smol import Component, Processor, World


class Position(Component):
    x: float = 0.0


class Velocity(Component):
    dx: float = 0.0


class Move(Processor):
    components = (Position, Velocity)

    def process(self, df, *, tick):
        del tick
        return df.with_column("position__x", col("position__x") + col("velocity__dx"))


def test_public_surface_is_intentionally_small() -> None:
    assert smol.__all__ == ["Component", "Processor", "RunResult", "World"]


def test_component_values_are_typed_frozen_and_strict() -> None:
    position = Position(x=2)

    assert position.x == 2.0
    with pytest.raises(ValidationError):
        Position(x=2, unknown=True)
    with pytest.raises(ValidationError):
        position.x = 3


def test_step_runs_matching_processors_and_preserves_initial_snapshot() -> None:
    world = World(processors=[Move()])
    moving = world.spawn(Position(x=2), Velocity(dx=0.5))
    stationary = world.spawn(Position(x=7))

    world.step()

    assert world.query(Position).to_pylist() == [
        {"entity_id": moving, "tick": 1, "is_active": True, "position__x": 2.5},
        {"entity_id": stationary, "tick": 1, "is_active": True, "position__x": 7.0},
    ]
    assert world.history(Position).to_pylist() == [
        {"entity_id": moving, "tick": 0, "is_active": True, "position__x": 2.0},
        {"entity_id": stationary, "tick": 0, "is_active": True, "position__x": 7.0},
        {"entity_id": moving, "tick": 1, "is_active": True, "position__x": 2.5},
        {"entity_id": stationary, "tick": 1, "is_active": True, "position__x": 7.0},
    ]


def test_updates_and_despawns_are_immediate_and_historical() -> None:
    world = World()
    entity_id = world.spawn(Position(x=1))
    world.step()
    world.update(entity_id, Position(x=4))
    world.despawn(entity_id)

    assert world.query(Position).to_pylist() == []
    assert world.history(Position).to_pylist()[-1] == {
        "entity_id": entity_id,
        "tick": 1,
        "is_active": False,
        "position__x": 4.0,
    }


def test_failed_processor_output_does_not_publish_a_partial_step() -> None:
    class DropsRows(Processor):
        components = (Position,)

        def process(self, df, *, tick):
            del tick
            return df.where(col("entity_id") < 0)

    world = World(processors=[DropsRows()])
    world.spawn(Position(x=3))
    before = world.history(Position).to_pylist()

    with pytest.raises(ValueError, match="preserve each input entity_id"):
        world.step()

    assert world.tick == 0
    assert world.query(Position).to_pylist()[0]["position__x"] == 3.0
    assert world.history(Position).to_pylist() == before


def test_failed_table_keeps_every_table_and_the_tick_unpublished() -> None:
    class IncrementPosition(Processor):
        components = (Position,)

        def process(self, df, *, tick):
            del tick
            return df.with_column("position__x", col("position__x") + 10)

    class FailsVelocity(Processor):
        components = (Velocity,)

        def process(self, df, *, tick):
            del df, tick
            raise RuntimeError("second table failed")

    world = World(processors=[IncrementPosition(), FailsVelocity()])
    position_only = world.spawn(Position(x=8))
    moving = world.spawn(Position(x=2), Velocity(dx=1))
    before = world.history(Position).to_pylist()

    with pytest.raises(RuntimeError, match="second table failed"):
        world.step()

    assert world.tick == 0
    assert world.query(Position).to_pylist() == [
        {"entity_id": position_only, "tick": 0, "is_active": True, "position__x": 8.0},
        {"entity_id": moving, "tick": 0, "is_active": True, "position__x": 2.0},
    ]
    assert world.history(Position).to_pylist() == before


def test_processors_cannot_reenter_world_mutation_or_nested_steps() -> None:
    attempted: list[str] = []

    class ReentersWorld(Processor):
        components = (Position,)

        def process(self, df, *, tick):
            del tick
            for name, operation in (
                ("spawn", lambda: world.spawn(Position(x=99))),
                ("update", lambda: world.update(entity_id, Position(x=99))),
                ("despawn", lambda: world.despawn(entity_id)),
                ("step", world.step),
                ("run", world.run),
                ("add", lambda: world.add_processor(Move())),
                ("remove", lambda: world.remove_processor(Move)),
            ):
                with pytest.raises(RuntimeError, match="while a step is executing"):
                    operation()
                attempted.append(name)
            return df

    world = World(processors=[ReentersWorld()])
    entity_id = world.spawn(Position(x=3))

    world.step()

    assert attempted == ["spawn", "update", "despawn", "step", "run", "add", "remove"]
    assert world.query(Position).to_pylist() == [
        {"entity_id": entity_id, "tick": 1, "is_active": True, "position__x": 3.0}
    ]
    assert len(world.history(Position).to_pylist()) == 2


def test_failed_reentrant_mutation_leaves_state_and_history_unchanged() -> None:
    class ReentrantSpawn(Processor):
        components = (Position,)

        def process(self, df, *, tick):
            del df, tick
            world.spawn(Position(x=99))
            raise AssertionError("unreachable")

    world = World(processors=[ReentrantSpawn()])
    entity_id = world.spawn(Position(x=3))
    before = world.history(Position).to_pylist()

    with pytest.raises(RuntimeError, match="spawn an entity"):
        world.step()

    assert world.tick == 0
    assert world.query(Position).to_pylist() == [
        {"entity_id": entity_id, "tick": 0, "is_active": True, "position__x": 3.0}
    ]
    assert world.history(Position).to_pylist() == before


def test_retained_component_values_cannot_rewrite_snapshots() -> None:
    class Inventory(Component):
        items: list[str]

    inventory = Inventory(items=["seed"])
    world = World()
    world.spawn(inventory)

    inventory.items.append("caller mutation")
    queried = world.query(Inventory).to_pylist()[0]
    queried["inventory__items"].append("query mutation")
    world.step()

    assert [row["inventory__items"] for row in world.history(Inventory).to_pylist()] == [
        ["seed"],
        ["seed"],
    ]


def test_aliased_fields_survive_noop_steps_and_validate_changed_rows_by_name() -> None:
    class Aliased(Component):
        value: int = Field(alias="wire_value")

    class Preserve(Processor):
        components = (Aliased,)

        def process(self, df, *, tick):
            del tick
            return df

    class Increment(Processor):
        components = (Aliased,)

        def process(self, df, *, tick):
            del tick
            return df.with_column("aliased__value", col("aliased__value") + 1)

    world = World(processors=[Preserve()])
    world.spawn(Aliased(wire_value=7))

    world.step()
    world.add_processor(Increment())
    world.step()

    assert [row["aliased__value"] for row in world.history(Aliased).to_pylist()] == [7, 7, 8]


def test_noop_steps_do_not_replay_component_validators() -> None:
    validated: list[int] = []

    class IncrementOnValidation(Component):
        value: int

        @field_validator("value")
        @classmethod
        def increment(cls, value: int) -> int:
            validated.append(value)
            return value + 1

    world = World()
    world.spawn(IncrementOnValidation(value=0))

    world.step()

    assert validated == [0]
    assert [
        row["incrementonvalidation__value"]
        for row in world.history(IncrementOnValidation).to_pylist()
    ] == [1, 1]


def test_partial_updates_do_not_replay_untouched_field_validators() -> None:
    validated: list[tuple[str, int]] = []

    class StatefulPosition(Component):
        position: int
        generation: int

        @field_validator("position")
        @classmethod
        def validate_position(cls, value: int) -> int:
            validated.append(("position", value))
            return value

        @field_validator("generation")
        @classmethod
        def increment_generation(cls, value: int) -> int:
            validated.append(("generation", value))
            return value + 1

    class MovePosition(Processor):
        components = (StatefulPosition,)

        def process(self, df, *, tick):
            del tick
            return df.with_column(
                "statefulposition__position",
                col("statefulposition__position") + 1,
            )

    world = World(processors=[MovePosition()])
    world.spawn(StatefulPosition(position=0, generation=0))

    world.step()

    assert validated == [("position", 0), ("generation", 0), ("position", 1)]
    assert world.history(StatefulPosition).to_pylist() == [
        {
            "entity_id": 1,
            "tick": 0,
            "is_active": True,
            "statefulposition__position": 0,
            "statefulposition__generation": 1,
        },
        {
            "entity_id": 1,
            "tick": 1,
            "is_active": True,
            "statefulposition__position": 1,
            "statefulposition__generation": 1,
        },
    ]


def test_selective_validation_survives_a_plain_core_class_validator() -> None:
    """Pin the standalone install shape, where no Pydantic plugin is present.

    A dev environment that installs a Pydantic plugin gets a
    `PluggableSchemaValidator` on the component class, which pydantic-core
    cannot reuse, so selective validation works by accident. A published Smol
    install has a plain `SchemaValidator` instead; pydantic-core reuses it when
    compiling the `model` node and discards the untouched-field wrappers,
    replaying every validator. Force the published shape so this suite fails
    here rather than only for installed users.
    """

    validated: list[tuple[str, int]] = []

    class DetachedPosition(Component):
        position: int
        generation: int

        @field_validator("position")
        @classmethod
        def validate_position(cls, value: int) -> int:
            validated.append(("position", value))
            return value

        @field_validator("generation")
        @classmethod
        def increment_generation(cls, value: int) -> int:
            validated.append(("generation", value))
            return value + 1

    class MoveDetached(Processor):
        components = (DetachedPosition,)

        def process(self, df, *, tick):
            del tick
            return df.with_column(
                "detachedposition__position",
                col("detachedposition__position") + 1,
            )

    DetachedPosition.__pydantic_validator__ = SchemaValidator(
        DetachedPosition.__pydantic_core_schema__
    )
    assert type(DetachedPosition.__pydantic_validator__) is SchemaValidator

    world = World(processors=[MoveDetached()])
    world.spawn(DetachedPosition(position=0, generation=0))

    world.step()

    assert validated == [("position", 0), ("generation", 0), ("position", 1)]
    assert world.query(DetachedPosition).to_pylist() == [
        {
            "entity_id": 1,
            "tick": 1,
            "is_active": True,
            "detachedposition__position": 1,
            "detachedposition__generation": 1,
        }
    ]


def test_multi_field_updates_validate_one_atomic_candidate() -> None:
    validated: list[tuple[str, int, int | None]] = []

    class Interval(Component):
        low: int = Field(strict=True)
        high: int = Field(strict=True)
        generation: int

        @field_validator("low", "high")
        @classmethod
        def record_bound(cls, value: int, info: ValidationInfo) -> int:
            validated.append((info.field_name, value, None))
            return value

        @field_validator("generation")
        @classmethod
        def increment_generation(cls, value: int) -> int:
            validated.append(("generation", value, None))
            return value + 1

        @model_validator(mode="after")
        def require_ordered_bounds(self) -> Self:
            validated.append(("model", self.low, self.high))
            if self.low > self.high:
                raise ValueError("low must not exceed high")
            return self

    class ShiftInterval(Processor):
        components = (Interval,)

        def process(self, df, *, tick):
            del tick
            return df.with_column("interval__low", col("interval__low") + 2).with_column(
                "interval__high", col("interval__high") + 2
            )

    world = World(processors=[ShiftInterval()])
    world.spawn(Interval(low=0, high=1, generation=0))

    world.step()

    assert validated == [
        ("low", 0, None),
        ("high", 1, None),
        ("generation", 0, None),
        ("model", 0, 1),
        ("low", 2, None),
        ("high", 3, None),
        ("model", 2, 3),
    ]
    assert world.history(Interval).to_pylist() == [
        {
            "entity_id": 1,
            "tick": 0,
            "is_active": True,
            "interval__low": 0,
            "interval__high": 1,
            "interval__generation": 1,
        },
        {
            "entity_id": 1,
            "tick": 1,
            "is_active": True,
            "interval__low": 2,
            "interval__high": 3,
            "interval__generation": 1,
        },
    ]


def test_ambiguous_structured_fields_fail_before_entity_admission() -> None:
    class Nested(BaseModel):
        value: int

    class Color(StrEnum):
        RED = "red"

    class Flexible(Component):
        value: Any

    world = World()
    cyclic: list[Any] = []
    cyclic.append(cyclic)
    unsupported = (
        (1, 2),
        {"left": 1},
        Nested(value=1),
        Color.RED,
        [[{"nested": 1}]],
        cyclic,
    )
    for value in unsupported:
        with pytest.raises(TypeError, match="Smol fields must be scalars"):
            world.spawn(Flexible(value=value))

    entity_id = world.spawn(Flexible(value=[1, [2, None], "three"]))
    before = world.history(Flexible).to_pylist()

    with pytest.raises(TypeError, match="Smol fields must be scalars"):
        world.update(entity_id, Flexible(value={"invalid": 1}))

    assert entity_id == 1
    assert world.history(Flexible).to_pylist() == before


def test_processor_cannot_publish_an_unsupported_field_shape() -> None:
    class Flexible(Component):
        value: Any

    class EmitMapping(Processor):
        components = (Flexible,)

        def process(self, df, *, tick):
            del tick
            return df.with_column("flexible__value", lit({"invalid": 1}))

    world = World(processors=[EmitMapping()])
    world.spawn(Flexible(value=1))
    before = world.history(Flexible).to_pylist()

    with pytest.raises(TypeError, match="Smol fields must be scalars"):
        world.step()

    assert world.tick == 0
    assert world.history(Flexible).to_pylist() == before


def test_before_model_validator_observes_changed_candidate_once() -> None:
    validated: list[tuple[int, int]] = []
    validated_y: list[int] = []

    class Pair(Component):
        x: int
        y: int

        @field_validator("y")
        @classmethod
        def record_y(cls, value: int) -> int:
            validated_y.append(value)
            return value

        @model_validator(mode="before")
        @classmethod
        def record_pair(cls, value: Any) -> Any:
            assert isinstance(value, dict)
            validated.append((value["x"], value["y"]))
            return value

    class IncrementX(Processor):
        components = (Pair,)

        def process(self, df, *, tick):
            del tick
            return df.with_column("pair__x", col("pair__x") + 1)

    world = World(processors=[IncrementX()])
    world.spawn(Pair(x=1, y=2))

    world.step()

    assert validated == [(1, 2), (2, 2)]
    assert validated_y == [2]
    assert world.tick == 1


def test_before_model_validator_cannot_bypass_untouched_field_validation() -> None:
    validated_y: list[int] = []

    class Pair(Component):
        x: int
        y: int = Field(strict=True)

        @field_validator("y")
        @classmethod
        def record_y(cls, value: int) -> int:
            validated_y.append(value)
            return value

        @model_validator(mode="before")
        @classmethod
        def corrupt_y_after_x_changes(cls, value: Any) -> Any:
            assert isinstance(value, dict)
            if value["x"] == 2:
                return {**value, "y": "invalid"}
            return value

    class IncrementX(Processor):
        components = (Pair,)

        def process(self, df, *, tick):
            del tick
            return df.with_column("pair__x", col("pair__x") + 1)

    world = World(processors=[IncrementX()])
    world.spawn(Pair(x=1, y=3))
    before = world.history(Pair).to_pylist()

    with pytest.raises(ValidationError):
        world.step()

    assert validated_y == [3]
    assert world.tick == 0
    assert world.history(Pair).to_pylist() == before


def test_wrap_model_validator_cannot_bypass_untouched_field_validation() -> None:
    class Pair(Component):
        x: int
        y: int = Field(strict=True)

        @model_validator(mode="wrap")
        @classmethod
        def corrupt_y_after_x_changes(cls, value: Any, handler: Any) -> Any:
            assert isinstance(value, dict)
            if value["x"] == 2:
                value = {**value, "y": "invalid"}
            return handler(value)

    class IncrementX(Processor):
        components = (Pair,)

        def process(self, df, *, tick):
            del tick
            return df.with_column("pair__x", col("pair__x") + 1)

    world = World(processors=[IncrementX()])
    world.spawn(Pair(x=1, y=3))
    before = world.history(Pair).to_pylist()

    with pytest.raises(ValidationError):
        world.step()

    assert world.tick == 0
    assert world.history(Pair).to_pylist() == before


def test_model_validator_cannot_change_the_component_type() -> None:
    class Other(Component):
        z: int

    class Original(Component):
        x: int

        @model_validator(mode="wrap")
        @classmethod
        def replace_type(cls, value: Any, handler: Any) -> Any:
            candidate = handler(value)
            if candidate.x == 2:
                return Other(z=9)
            return candidate

    class IncrementX(Processor):
        components = (Original,)

        def process(self, df, *, tick):
            del tick
            return df.with_column("original__x", col("original__x") + 1)

    world = World(processors=[IncrementX()])
    world.spawn(Original(x=1))
    before = world.history(Original).to_pylist()

    with pytest.raises(TypeError, match="validators must preserve the Component type"):
        world.step()

    assert world.tick == 0
    assert world.history(Original).to_pylist() == before
    assert world.query(Other).to_pylist() == []


def test_equal_but_differently_typed_processor_values_are_revalidated() -> None:
    class StrictCount(Component):
        value: int = Field(strict=True)

    class CastToFloat(Processor):
        components = (StrictCount,)

        def process(self, df, *, tick):
            del tick
            return df.with_column("strictcount__value", col("strictcount__value").cast("float64"))

    world = World(processors=[CastToFloat()])
    world.spawn(StrictCount(value=1))

    with pytest.raises(ValidationError):
        world.step()

    assert world.tick == 0


def test_computed_fields_do_not_enter_the_dataframe_schema() -> None:
    class ComputedPosition(Component):
        x: float = 2.0

        @computed_field
        @property
        def doubled(self) -> float:
            return self.x * 2

    world = World()
    world.spawn(ComputedPosition())

    world.step()

    assert world.query(ComputedPosition).to_pylist() == [
        {
            "entity_id": 1,
            "tick": 1,
            "is_active": True,
            "computedposition__x": 2.0,
        }
    ]


def test_custom_pydantic_serializers_do_not_change_dataframe_columns_or_values() -> None:
    class ModelSerialized(Component):
        x: int = 2

        @model_serializer
        def serialize_model(self) -> dict[str, int]:
            return {"renamed": self.x}

    class FieldSerialized(Component):
        value: int = 3

        @field_serializer("value")
        def serialize_value(self, value: int) -> str:
            return f"value={value}"

    world = World()
    world.spawn(ModelSerialized(), FieldSerialized())

    world.step()

    assert world.query(ModelSerialized, FieldSerialized).to_pylist() == [
        {
            "entity_id": 1,
            "tick": 1,
            "is_active": True,
            "modelserialized__x": 2,
            "fieldserialized__value": 3,
        }
    ]


def test_tick_is_read_only() -> None:
    world = World()

    with pytest.raises(AttributeError):
        world.tick = 10  # type: ignore[misc]


@pytest.mark.parametrize("entity_id", [True, 1.0, "1"])
def test_entity_mutations_require_exact_integer_identity(entity_id) -> None:
    world = World()
    world.spawn(Position())

    with pytest.raises(TypeError, match="entity_id must be an integer"):
        world.update(entity_id, Position(x=2))
    with pytest.raises(TypeError, match="entity_id must be an integer"):
        world.despawn(entity_id)


def test_spawn_rejects_non_component_values_before_mutating_state() -> None:
    world = World()

    with pytest.raises(TypeError, match="Component instances"):
        world.spawn(object())  # type: ignore[arg-type]

    assert world.query().to_pylist() == []


def test_equal_priority_processors_retain_registration_order() -> None:
    class Double(Processor):
        components = (Position,)

        def process(self, df, *, tick):
            del tick
            return df.with_column("position__x", col("position__x") * 2)

    class AddOne(Processor):
        components = (Position,)

        def process(self, df, *, tick):
            del tick
            return df.with_column("position__x", col("position__x") + 1)

    world = World(processors=[Double(), AddOne()])
    world.spawn(Position(x=3))

    world.step()

    assert world.query(Position).to_pylist()[0]["position__x"] == 7.0


def test_processors_cannot_change_metadata_types() -> None:
    class CastsEntityIdentity(Processor):
        components = (Position,)

        def process(self, df, *, tick):
            del tick
            return df.with_column("entity_id", col("entity_id").cast("float64"))

    world = World(processors=[CastsEntityIdentity()])
    world.spawn(Position())

    with pytest.raises(ValueError, match="entity_id metadata"):
        world.step()


def test_remove_processor_requires_a_processor_type() -> None:
    world = World(processors=[Move()])

    with pytest.raises(TypeError, match="Processor type"):
        world.remove_processor(Position)  # type: ignore[arg-type]


def test_run_rejects_negative_or_boolean_step_counts() -> None:
    world = World()

    with pytest.raises(ValueError, match="non-negative integer"):
        world.run(-1)
    with pytest.raises(ValueError, match="non-negative integer"):
        world.run(True)
    assert world.run(0).ticks_completed == 0


def test_distribution_is_not_a_framework_compatibility_layer() -> None:
    package_root = Path(__file__).resolve().parents[1]
    project = (package_root / "pyproject.toml").read_text(encoding="utf-8")
    sources = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (package_root / "src" / "archetype" / "smol").glob("*.py")
    )

    assert '"archetype-ecs' not in project
    assert "archetype.core" not in sources
    assert "archetype.runtime" not in sources


def test_distribution_requires_pydantic_assignment_validation_api() -> None:
    package_root = Path(__file__).resolve().parents[1]
    project = tomllib.loads((package_root / "pyproject.toml").read_text(encoding="utf-8"))

    assert "pydantic>=2.11" in project["project"]["dependencies"]
