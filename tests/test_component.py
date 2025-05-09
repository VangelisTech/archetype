import pytest
import daft
import pyarrow as pa
from pydantic import Field

from archetype.core.base import Component

# --- Test Component Definitions ---

class Position(Component):
    x: float = Field(default=0.0)
    y: float = Field(default=0.0)
    z: float = Field(default=0.0)

class Velocity(Component):
    dx: float = Field(default=0.0)
    dy: float = Field(default=0.0)
    dz: float = Field(default=0.0)

class Metadata(Component):
    name: str = Field(default="test_object")
    tag: int = Field(default=123)


# --- Pytest Tests ---

def test_component_instantiation():
    """Test basic component instantiation and field access."""
    pos = Position(x=1.0, y=2.0, z=3.0)
    assert pos.x == 1.0
    assert pos.y == 2.0
    assert pos.z == 3.0

    vel = Velocity(dx=0.1, dy=-0.2, dz=0.0)
    assert vel.dx == 0.1
    assert vel.dy == -0.2
    assert vel.dz == 0.0

    meta = Metadata(name="obj_A", tag=42)
    assert meta.name == "obj_A"
    assert meta.tag == 42

def test_component_to_arrow_schema():
    """Test the to_arrow_schema method (inherited from LanceModel via Component)."""
    # Position Component
    pos_schema = Position.to_arrow_schema()
    assert isinstance(pos_schema, pa.Schema)
    expected_pos_fields = {"x": pa.float64(), "y": pa.float64(), "z": pa.float64()}
    for name, pa_type in expected_pos_fields.items():
        assert pos_schema.field_by_name(name).type == pa_type
    assert len(pos_schema.names) == len(expected_pos_fields) # Ensure no extra fields

    # Velocity Component
    vel_schema = Velocity.to_arrow_schema()
    assert isinstance(vel_schema, pa.Schema)
    expected_vel_fields = {"dx": pa.float64(), "dy": pa.float64(), "dz": pa.float64()}
    for name, pa_type in expected_vel_fields.items():
        assert vel_schema.field_by_name(name).type == pa_type
    assert len(vel_schema.names) == len(expected_vel_fields)

    # Metadata Component
    meta_schema = Metadata.to_arrow_schema()
    assert isinstance(meta_schema, pa.Schema)
    expected_meta_fields = {"name": pa.string(), "tag": pa.int64()} # Pydantic int often maps to int64
    for name, pa_type in expected_meta_fields.items():
        assert meta_schema.field_by_name(name).type == pa_type
    assert len(meta_schema.names) == len(expected_meta_fields)


def test_component_to_daft_row():
    """Test the to_daft_row method for creating a Daft DataFrame row with prefixed columns."""
    # Position Component
    pos_instance = Position(x=10.0, y=-5.5, z=0.1)
    pos_df = pos_instance.to_daft_row()

    assert isinstance(pos_df, daft.DataFrame)
    assert len(pos_df) == 1 # Should contain one row

    expected_pos_cols = {
        "Position__x": [10.0],
        "Position__y": [-5.5],
        "Position__z": [0.1]
    }
    assert sorted(pos_df.column_names()) == sorted(expected_pos_cols.keys())
    pos_data = pos_df.to_pydict()
    for col_name, expected_val_list in expected_pos_cols.items():
        assert pos_data[col_name] == expected_val_list

    # Velocity Component
    vel_instance = Velocity(dx=1.0, dy=2.0, dz=3.0)
    vel_df = vel_instance.to_daft_row()

    assert isinstance(vel_df, daft.DataFrame)
    assert len(vel_df) == 1

    expected_vel_cols = {
        "Velocity__dx": [1.0],
        "Velocity__dy": [2.0],
        "Velocity__dz": [3.0]
    }
    assert sorted(vel_df.column_names()) == sorted(expected_vel_cols.keys())
    vel_data = vel_df.to_pydict()
    for col_name, expected_val_list in expected_vel_cols.items():
        assert vel_data[col_name] == expected_val_list

    # Metadata Component
    meta_instance = Metadata(name="example", tag=777)
    meta_df = meta_instance.to_daft_row()

    assert isinstance(meta_df, daft.DataFrame)
    assert len(meta_df) == 1

    expected_meta_cols = {
        "Metadata__name": ["example"],
        "Metadata__tag": [777]
    }
    assert sorted(meta_df.column_names()) == sorted(expected_meta_cols.keys())
    meta_data = meta_df.to_pydict()
    for col_name, expected_val_list in expected_meta_cols.items():
        assert meta_data[col_name] == expected_val_list
