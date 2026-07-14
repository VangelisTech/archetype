# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Trusted component identity and signature resolution contracts."""

import importlib

import pytest
from pydantic import create_model

from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.ledger.canonical import composite_schema_digest
from archetype.ledger.errors import ComponentResolutionError, ComponentSchemaConflictError
from archetype.ledger.models import ComponentRef
from archetype.ledger.registry import ComponentRegistry


class Position(Component):
    x: float
    y: float = 0.0


class RegistryLabel(Component):
    value: str


class Alpha(Component):
    value: int


class Zulu(Component):
    value: str


def test_register_and_resolve_top_level_component():
    registry = ComponentRegistry()

    ref = registry.register(Position)

    assert ref.component_id == f"python:{__name__}:Position"
    assert registry.resolve(ref) is Position
    assert registry.component_ref(Position) == ref
    assert registry.register(Position) == ref


def test_component_and_signature_golden_vectors():
    registry = ComponentRegistry()

    component = registry.register(Position, component_id="golden:position:v1")
    signature = registry.signature_ref([Position])

    assert component.schema_digest == (
        "sha256:4ab8efd06f3983a832dc5149da1b5ec7c1df997db7caf9e5749fb574dfb8188f"
    )
    assert signature.table_id == "a_1c_sa85ab0c4bb96f38f"
    assert signature.schema_digest == (
        "sha256:367240f4f9aa0e3140e757073da0be48c385521bc3581c0a38788e9a27fc5e0f"
    )
    assert signature.signature_digest == (
        "sha256:255d41417081cbae16cc9cd9740c3b00c4b1f5ff5e7269483c8d3489ce3540e6"
    )


def test_local_component_requires_explicit_stable_id():
    class Local(Component):
        value: int

    registry = ComponentRegistry()
    with pytest.raises(ComponentResolutionError, match="explicit stable"):
        registry.register(Local)

    ref = registry.register(Local, component_id="tests:local:v1")
    assert registry.resolve(ref) is Local

    with pytest.raises(ValueError, match="identifier length"):
        ComponentRegistry().register(Local, component_id="")


def test_schema_change_under_same_component_id_is_a_typed_conflict():
    first = create_model("FirstShape", value=(int, ...), __base__=Component)
    second = create_model("SecondShape", value=(str, ...), __base__=Component)
    registry = ComponentRegistry()
    original = registry.register(first, component_id="acme:metric:v1")

    with pytest.raises(ComponentSchemaConflictError) as caught:
        registry.register(second, component_id="acme:metric:v1")

    assert caught.value.component_id == "acme:metric:v1"
    assert caught.value.expected_digest == original.schema_digest
    assert caught.value.actual_digest != original.schema_digest


def test_resolve_unknown_persisted_id_never_imports_a_path(monkeypatch):
    registry = ComponentRegistry()
    calls: list[str] = []

    def forbidden_import(name, *args, **kwargs):
        calls.append(name)
        raise AssertionError("persisted paths must never be imported")

    monkeypatch.setattr(importlib, "import_module", forbidden_import)
    persisted = ComponentRef(
        component_id="python:malicious.package:Payload",
        schema_digest="sha256:" + "1" * 64,
    )

    with pytest.raises(ComponentResolutionError, match="trusted registry"):
        registry.resolve(persisted)
    assert calls == []


def test_signature_identity_is_component_id_ordered_but_table_id_is_legacy_compatible():
    registry = ComponentRegistry()
    registry.register(Position, component_id="z:position")
    registry.register(RegistryLabel, component_id="a:label")

    forward = registry.signature_ref([Position, RegistryLabel])
    reverse = registry.signature_ref([RegistryLabel, Position])

    assert forward == reverse
    assert [component.component_id for component in forward.components] == ["a:label", "z:position"]
    legacy_sig = tuple(sorted((Position, RegistryLabel), key=lambda value: value.__name__))
    assert forward.table_id == Archetype.get_name(legacy_sig)
    assert registry.resolve_signature(forward) == (RegistryLabel, Position)


def test_signature_resolution_recomputes_table_and_schema_identity():
    registry = ComponentRegistry()
    registry.register(Position, component_id="test:position")
    signature = registry.signature_ref([Position])
    tampered = signature.model_copy(update={"table_id": "a_1c_s0000000000000000"})

    with pytest.raises(ComponentResolutionError, match="table mismatch"):
        registry.resolve_signature(tampered)


def test_same_class_name_in_different_modules_gets_distinct_default_ids():
    first = create_model("SharedName", value=(int, ...), __base__=Component)
    second = create_model("SharedName", value=(int, ...), __base__=Component)
    first.__module__ = "trusted.one"
    second.__module__ = "trusted.two"
    registry = ComponentRegistry()

    first_ref = registry.register(first)
    second_ref = registry.register(second)

    assert first_ref.component_id == "python:trusted.one:SharedName"
    assert second_ref.component_id == "python:trusted.two:SharedName"
    assert first_ref.schema_digest != second_ref.schema_digest


def test_unregistered_component_cannot_be_smuggled_into_signature():
    registry = ComponentRegistry()
    registry.register(Position)

    with pytest.raises(ComponentResolutionError, match="not registered"):
        registry.signature_ref([Position, RegistryLabel])


def test_signature_schema_digest_matches_legacy_physical_field_order():
    registry = ComponentRegistry()
    registry.register(Alpha, component_id="z:alpha")
    registry.register(Zulu, component_id="a:zulu")

    signature = registry.signature_ref([Zulu, Alpha])
    physical_types = tuple(sorted((Zulu, Alpha), key=lambda value: value.__name__))
    physical_schema = Archetype.get_archetype_schema(physical_types)

    assert [component.component_id for component in signature.components] == [
        "a:zulu",
        "z:alpha",
    ]
    assert physical_types == (Alpha, Zulu)
    assert signature.schema_digest == composite_schema_digest(physical_schema)
    assert signature.table_id == Archetype.get_name(physical_types)
    assert registry.resolve_signature(signature) == (Zulu, Alpha)
