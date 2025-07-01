
# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest
import pyarrow as pa
from archetype.core.interfaces import Component, Archetype


class Position(Component):
    x: float
    y: float


class Velocity(Component):
    dx: float
    dy: float


def test_component_get_prefix():
    """Tests that the component prefix is generated correctly."""
    assert Position.get_prefix() == "position__"
    assert Velocity.get_prefix() == "velocity__"


def test_component_to_pyarrow_schema():
    """Tests that the component can be converted to a PyArrow schema."""
    schema = Position.to_pyarrow_schema()
    assert isinstance(schema, pa.Schema)
    assert schema.names == ["x", "y"]
    assert schema.field("x").type == pa.float64()
    assert schema.field("y").type == pa.float64()


def test_component_get_prefixed_schema():
    """Tests that the component can be converted to a prefixed PyArrow schema."""
    schema = Position.get_prefixed_schema()
    assert isinstance(schema, pa.Schema)
    assert schema.names == ["position__x", "position__y"]
    assert schema.field("position__x").type == pa.float64()
    assert schema.field("position__y").type == pa.float64()


def test_archetype_sig_from_components():
    """Tests that the archetype signature is generated correctly."""
    components = [Position(x=0, y=0), Velocity(dx=1, dy=1)]
    sig = Archetype.sig_from_components(components)
    assert sig == (Position, Velocity)

    # Test with different order
    components = [Velocity(dx=1, dy=1), Position(x=0, y=0)]
    sig = Archetype.sig_from_components(components)
    assert sig == (Position, Velocity)


def test_archetype_get_name():
    """Tests that the archetype name is generated correctly."""
    components = [Position(x=0, y=0), Velocity(dx=1, dy=1)]
    sig = Archetype.sig_from_components(components)
    name = Archetype.get_name(sig)
    assert name == "arch_PosVel"


def test_archetype_get_schema():
    """Tests that the archetype schema is generated correctly."""
    components = [Position(x=0, y=0), Velocity(dx=1, dy=1)]
    sig = Archetype.sig_from_components(components)
    schema = Archetype.get_archetype_schema(sig)
    assert isinstance(schema, pa.Schema)
    expected_names = [
        "world_id",
        "run_id",
        "entity_id",
        "step",
        "is_active",
        "position__x",
        "position__y",
        "velocity__dx",
        "velocity__dy",
    ]
    assert schema.names == expected_names
