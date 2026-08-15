# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""In-memory world implementation for the Smol teaching engine."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import daft
from daft import DataFrame

from .component import Component
from .processor import Processor

_METADATA_COLUMNS = ("entity_id", "tick", "is_active")
_Signature = tuple[type[Component], ...]


@dataclass(frozen=True, slots=True)
class RunResult:
    """Summary of one completed `World.run()` call."""

    ticks_completed: int
    tick: int


@dataclass(frozen=True, slots=True)
class _HistoryEntry:
    components: tuple[Component, ...]
    is_active: bool


def _component_key(component_type: type[Component]) -> tuple[str, str]:
    return (component_type.__module__, component_type.__qualname__)


def _canonical_components(components: Iterable[Component]) -> tuple[Component, ...]:
    values = tuple(components)
    if not all(isinstance(component, Component) for component in values):
        raise TypeError("component values must be Component instances")
    types = [type(component) for component in values]
    if len(types) != len(set(types)):
        raise ValueError("an entity may contain only one value of each Component type")

    prefixes = [component_type.prefix() for component_type in types]
    if len(prefixes) != len(set(prefixes)):
        raise ValueError("component class names must be unique within one entity")
    return tuple(sorted(values, key=lambda component: _component_key(type(component))))


def _signature(components: tuple[Component, ...]) -> _Signature:
    return tuple(type(component) for component in components)


def _snapshot(components: tuple[Component, ...]) -> tuple[Component, ...]:
    """Detach one retained state snapshot from every caller and later tick."""

    return tuple(component.model_copy(deep=True) for component in components)


class World:
    """A small synchronous ECS with atomic in-memory steps.

    Smol materializes each transformed Daft DataFrame at the in-memory world
    boundary. If any processor or validation fails, no entity state, history,
    or tick is published. Processors should still be pure: arbitrary Python
    side effects performed inside `process()` cannot be rolled back.
    """

    def __init__(self, *, processors: Iterable[Processor] = ()) -> None:
        self._tick = 0
        self._stepping = False
        self._next_entity_id = 1
        self._entities: dict[int, tuple[Component, ...]] = {}
        self._history: dict[tuple[int, int], _HistoryEntry] = {}
        self._processors: list[Processor] = []
        for processor in processors:
            self.add_processor(processor)

    @property
    def tick(self) -> int:
        """The latest successfully published tick."""

        return self._tick

    def _require_idle(self, operation: str) -> None:
        if self._stepping:
            raise RuntimeError(f"cannot {operation} while a step is executing")

    def add_processor(self, processor: Processor) -> None:
        """Register a processor; equal priorities retain registration order."""

        self._require_idle("add a processor")
        if not isinstance(processor, Processor):
            raise TypeError("add_processor requires a Processor instance")
        if isinstance(processor.priority, bool) or not isinstance(processor.priority, int):
            raise TypeError("Processor.priority must be an integer")
        components = processor.components
        if not isinstance(components, tuple) or not all(
            isinstance(value, type) and issubclass(value, Component) for value in components
        ):
            raise TypeError("Processor.components must be a tuple of Component types")
        if len(components) != len(set(components)):
            raise ValueError("Processor.components must not contain duplicates")
        self._processors.append(processor)

    def remove_processor(self, processor_type: type[Processor]) -> None:
        """Remove every registered processor of the given type."""

        self._require_idle("remove a processor")
        if not isinstance(processor_type, type) or not issubclass(processor_type, Processor):
            raise TypeError("remove_processor requires a Processor type")
        self._processors = [
            processor for processor in self._processors if not isinstance(processor, processor_type)
        ]

    def spawn(self, *components: Component) -> int:
        """Create an entity that is immediately visible at the current tick."""

        self._require_idle("spawn an entity")
        canonical = _snapshot(_canonical_components(components))
        if not canonical:
            raise ValueError("spawn requires at least one Component")
        history = _HistoryEntry(_snapshot(canonical), True)
        entity_id = self._next_entity_id
        self._next_entity_id += 1
        self._entities[entity_id] = canonical
        self._history[(self.tick, entity_id)] = history
        return entity_id

    def update(self, entity_id: int, *components: Component) -> None:
        """Replace existing component values on an active entity."""

        self._require_idle("update an entity")
        current = self._require_entity(entity_id)
        replacements = _snapshot(_canonical_components(components))
        if not replacements:
            raise ValueError("update requires at least one Component")
        current_by_type = {type(component): component for component in current}
        unknown = [
            type(component).__name__
            for component in replacements
            if type(component) not in current_by_type
        ]
        if unknown:
            raise ValueError(
                "update cannot change an entity signature; unknown Components: "
                + ", ".join(sorted(unknown))
            )
        current_by_type.update({type(component): component for component in replacements})
        updated = tuple(current_by_type[component_type] for component_type in _signature(current))
        history = _HistoryEntry(_snapshot(updated), True)
        self._entities[entity_id] = updated
        self._history[(self.tick, entity_id)] = history

    def despawn(self, entity_id: int) -> None:
        """Remove an entity and retain a tombstone in the current snapshot."""

        self._require_idle("despawn an entity")
        components = self._require_entity(entity_id)
        history = _HistoryEntry(_snapshot(components), False)
        del self._entities[entity_id]
        self._history[(self.tick, entity_id)] = history

    def step(self) -> None:
        """Transform all current archetype tables and publish one atomic tick."""

        self._require_idle("start a nested step")
        self._stepping = True
        try:
            grouped: dict[_Signature, list[int]] = {}
            for entity_id, components in self._entities.items():
                grouped.setdefault(_signature(components), []).append(entity_id)

            staged: dict[int, tuple[Component, ...]] = {}
            processors = sorted(
                enumerate(self._processors),
                key=lambda pair: (pair[1].priority, pair[0]),
            )
            for signature in sorted(
                grouped, key=lambda value: tuple(_component_key(t) for t in value)
            ):
                entity_ids = sorted(grouped[signature])
                rows = [
                    self._entity_row(entity_id, self.tick, self._entities[entity_id], signature)
                    for entity_id in entity_ids
                ]
                frame = daft.from_pylist(rows)
                for _index, processor in processors:
                    if set(processor.components).issubset(signature):
                        frame = processor.process(frame, tick=self.tick)
                        if not isinstance(frame, DataFrame):
                            raise TypeError(
                                f"{type(processor).__name__}.process() must return a Daft DataFrame"
                            )
                staged.update(
                    self._decode_frame(
                        frame,
                        signature,
                        entity_ids,
                        expected_tick=self.tick,
                    )
                )

            next_tick = self.tick + 1
            history = {
                (next_tick, entity_id): _HistoryEntry(_snapshot(components), True)
                for entity_id, components in staged.items()
            }
            self._entities = staged
            self._history.update(history)
            self._tick = next_tick
        finally:
            self._stepping = False

    def run(self, steps: int = 1) -> RunResult:
        """Run a non-negative number of steps and return a compact summary."""

        self._require_idle("run nested steps")
        if isinstance(steps, bool) or not isinstance(steps, int) or steps < 0:
            raise ValueError(f"steps must be a non-negative integer, got {steps!r}")
        for _ in range(steps):
            self.step()
        return RunResult(ticks_completed=steps, tick=self.tick)

    def query(self, *component_types: type[Component]) -> DataFrame:
        """Return the current active rows containing every requested Component."""

        selected = self._validate_component_types(component_types)
        rows = [
            self._entity_row(entity_id, self.tick, components, selected)
            for entity_id, components in sorted(self._entities.items())
            if set(selected).issubset(_signature(components))
        ]
        return self._frame(rows, selected)

    def history(self, *component_types: type[Component]) -> DataFrame:
        """Return every retained snapshot containing the requested Components."""

        selected = self._validate_component_types(component_types)
        rows: list[dict[str, Any]] = []
        for (tick, entity_id), entry in sorted(self._history.items()):
            if set(selected).issubset(_signature(entry.components)):
                row = self._entity_row(entity_id, tick, entry.components, selected)
                row["is_active"] = entry.is_active
                rows.append(row)
        return self._frame(rows, selected)

    def _require_entity(self, entity_id: int) -> tuple[Component, ...]:
        if type(entity_id) is not int:
            raise TypeError("entity_id must be an integer")
        try:
            return self._entities[entity_id]
        except KeyError as exc:
            raise KeyError(f"unknown active entity_id {entity_id}") from exc

    @staticmethod
    def _validate_component_types(
        component_types: tuple[type[Component], ...],
    ) -> tuple[type[Component], ...]:
        if len(component_types) != len(set(component_types)):
            raise ValueError("query component types must be unique")
        if not all(
            isinstance(value, type) and issubclass(value, Component) for value in component_types
        ):
            raise TypeError("query values must be Component types")
        return component_types

    @staticmethod
    def _entity_row(
        entity_id: int,
        tick: int,
        components: tuple[Component, ...],
        selected: _Signature,
    ) -> dict[str, Any]:
        by_type = {type(component): component for component in components}
        row: dict[str, Any] = {"entity_id": entity_id, "tick": tick, "is_active": True}
        for component_type in selected:
            row.update(by_type[component_type].to_row())
        return row

    @staticmethod
    def _columns(selected: _Signature) -> tuple[str, ...]:
        columns = list(_METADATA_COLUMNS)
        for component_type in selected:
            columns.extend(
                f"{component_type.prefix()}{name}" for name in component_type.model_fields
            )
        return tuple(columns)

    @classmethod
    def _frame(cls, rows: list[dict[str, Any]], selected: _Signature) -> DataFrame:
        if rows:
            return daft.from_pylist(rows)
        return daft.from_pydict({column: [] for column in cls._columns(selected)})

    @classmethod
    def _decode_frame(
        cls,
        frame: DataFrame,
        signature: _Signature,
        expected_entity_ids: list[int],
        *,
        expected_tick: int,
    ) -> dict[int, tuple[Component, ...]]:
        expected_columns = set(cls._columns(signature))
        actual_columns = set(frame.column_names)
        if actual_columns != expected_columns:
            missing = sorted(expected_columns - actual_columns)
            extra = sorted(actual_columns - expected_columns)
            raise ValueError(
                f"processor output columns changed; missing={missing!r}, extra={extra!r}"
            )

        rows = frame.to_pylist()
        actual_entity_ids = [row["entity_id"] for row in rows]
        if any(type(entity_id) is not int for entity_id in actual_entity_ids):
            raise ValueError("processors may not change entity_id metadata")
        if sorted(actual_entity_ids) != expected_entity_ids or len(set(actual_entity_ids)) != len(
            actual_entity_ids
        ):
            raise ValueError("processors must preserve each input entity_id exactly once")

        decoded: dict[int, tuple[Component, ...]] = {}
        for row in rows:
            if row["is_active"] is not True:
                raise ValueError("processors may not change is_active")
            if type(row["tick"]) is not int or row["tick"] != expected_tick:
                raise ValueError("processors may not change tick metadata")
            components: list[Component] = []
            for component_type in signature:
                prefix = component_type.prefix()
                values = {name: row[f"{prefix}{name}"] for name in component_type.model_fields}
                components.append(component_type.model_validate(values))
            decoded[row["entity_id"]] = tuple(components)
        return decoded
