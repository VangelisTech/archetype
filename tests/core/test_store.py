import pytest
import asyncio
import shutil
import tempfile
from pathlib import Path
from typing import List

from archetype.core.store import ArchetypeStore, Component
from lancedb.pydantic import LanceModel, Vector
from pydantic import BaseModel


# Define some mock components for testing
class Position(LanceModel):
    x: float
    y: float
    vec: Vector(2)


class Velocity(LanceModel):
    dx: float
    dy: float
    vec: Vector(2)


class Health(LanceModel):
    value: int
    vec: Vector(1)


@pytest.fixture
def temp_store_uri():
    """Create a temporary directory for the store URI."""
    temp_dir = tempfile.mkdtemp()
    yield f"sqlite:///{temp_dir}/catalog.db" # Using a file-based URI for the catalog part
    shutil.rmtree(temp_dir)

@pytest.fixture
def store(temp_store_uri: str) -> ArchetypeStore:
    """Fixture to create an ArchetypeStore instance with a temporary URI."""
    # The URI for SqlCatalog is just the path to the sqlite db.
    # The warehouse path needs to be separate.
    uri_path = Path(temp_store_uri.replace("sqlite:///", ""))
    warehouse_path = uri_path.parent / "warehouse"
    warehouse_path.mkdir(parents=True, exist_ok=True)
    
    # The actual URI passed to ArchetypeStore init should be the root path for catalog.db and warehouse
    return ArchetypeStore(uri=str(uri_path.parent))

# Basic test to ensure the store can be initialized
def test_store_initialization(store: ArchetypeStore):
    assert store is not None
    assert store.simulation.startswith("sim_")
    assert store.run.startswith("run_")
    assert store.namespace == "archetype"
    assert store.sess is not None
    assert store.catalog is not None

def test_store_initialization_custom_names(temp_store_uri: str):
    uri_path = Path(temp_store_uri.replace("sqlite:///", ""))
    warehouse_path = uri_path.parent / "warehouse"
    warehouse_path.mkdir(parents=True, exist_ok=True)
    
    store = ArchetypeStore(
        uri=str(uri_path.parent),
        simulation="test_sim",
        run="test_run",
        namespace="custom_namespace"
    )
    assert store.simulation == "test_sim"
    assert store.run == "test_run"
    assert store.namespace == "custom_namespace"

@pytest.mark.asyncio
async def test_add_entity_single_component(store: ArchetypeStore):
    pos_component = Position(x=1.0, y=2.0, vec=[1.0, 2.0])
    entity_id = await store.add_entity(components=[pos_component], step=0)
    assert entity_id == 1
    assert entity_id in store._entity2sig
    sig = store._sig_from_components([pos_component])
    assert store._entity2sig[entity_id] == sig
    
    # Check if table exists
    table_name = store._sig2hash(sig)
    tables = store.sess.list_tables()
    assert any(table.name == table_name for table in tables)

@pytest.mark.asyncio
async def test_add_entity_multiple_components(store: ArchetypeStore):
    pos_component = Position(x=1.0, y=2.0, vec=[1.0, 2.0])
    vel_component = Velocity(dx=0.1, dy=0.2, vec=[0.1, 0.2])
    
    entity_id = await store.add_entity(components=[pos_component, vel_component], step=0)
    assert entity_id == 1 # First entity in this store instance
    assert entity_id in store._entity2sig
    sig = store._sig_from_components([pos_component, vel_component])
    assert store._entity2sig[entity_id] == sig

    table_name = store._sig2hash(sig)
    tables = store.sess.list_tables()
    assert any(table.name == table_name for table in tables)

@pytest.mark.asyncio
async def test_add_entity_increments_id(store: ArchetypeStore):
    pos_component = Position(x=1.0, y=2.0, vec=[1.0, 2.0])
    vel_component = Velocity(dx=0.1, dy=0.2, vec=[0.1, 0.2])

    entity_id1 = await store.add_entity(components=[pos_component], step=0)
    entity_id2 = await store.add_entity(components=[vel_component], step=0)
    entity_id3 = await store.add_entity(components=[pos_component, vel_component], step=0)

    assert entity_id1 == 1
    assert entity_id2 == 2
    assert entity_id3 == 3

@pytest.mark.asyncio
async def test_add_entity_no_components_raises_error(store: ArchetypeStore):
    with pytest.raises(ValueError, match="Cannot create an entity with no components"):
        await store.add_entity(components=[], step=0)

@pytest.mark.asyncio
async def test_remove_entity_marks_inactive(store: ArchetypeStore):
    pos_component = Position(x=1.0, y=2.0, vec=[1.0, 2.0])
    entity_id = await store.add_entity(components=[pos_component], step=0)

    await store.remove_entity(entity_id=entity_id, step=0)

    sig = store._sig_from_components([pos_component])
    table_name = store._sig2hash(sig)
    df = store.sess.get_table(table_name).to_dataframe()
    
    # Filter for the specific entity, sim, run, and step
    entity_data = df.where(
        (df["entity_id"] == entity_id) & 
        (df["step"] == 0) &
        (df["simulation"] == store.simulation) &
        (df["run"] == store.run)
    ).to_pydict()

    assert len(entity_data["is_active"]) == 1
    assert entity_data["is_active"][0] is False

@pytest.mark.asyncio
async def test_remove_entity_non_existent(store: ArchetypeStore, caplog):
    # Try to remove an entity that wasn't added
    await store.remove_entity(entity_id=999, step=0)
    assert "Entity 999 not found in _entity2sig. Cannot remove." in caplog.text

@pytest.mark.asyncio
async def test_remove_entity_specific_step_sim_run(store: ArchetypeStore):
    pos_component = Position(x=1.0, y=2.0, vec=[1.0, 2.0])
    entity_id = await store.add_entity(components=[pos_component], step=0) # Active at step 0
    
    # Add same entity logical representation but at a different step, should be a new row
    # To do this correctly, we need to ensure the Daft table is updated for the new step.
    # We can treat this as adding a new state for the entity at step 1
    # The store's add_entity would create a new row with step=1 and is_active=True
    # For the sake of testing remove_entity, let's assume this is handled correctly by add_entity
    # or we directly manipulate for testing remove_entity's specificity.

    # Simulate adding the entity's data again but for step 1, and it's active
    # This would typically involve another call to add_entity or a similar mechanism
    # For simplicity, let's imagine there are two entries for entity_id: one for step 0, one for step 1.
    # We'll achieve this by adding the entity, removing it at step 0, then re-adding at step 1.
    # This isn't perfect but tests the point for remove_entity.

    await store.remove_entity(entity_id=entity_id, step=0) # Mark inactive at step 0

    # Now, let's add the "same" entity but at step 1
    # Since add_entity creates a *new* row, this is effectively a new state record
    # To properly test remove_entity's specificity, we need to ensure the data exists as expected.
    # A more direct way: add entity at step 0, add entity at step 1 (will be same ID if we manage counter or new ID)
    # Let's use a new store instance for a cleaner state for this specific test logic.
    
    uri_path = Path(store.catalog.metadata["uri"].replace("sqlite:///", "").replace("/catalog.db",""))
    store_2 = ArchetypeStore(uri=str(uri_path), simulation="test_sim_2", run="test_run_2")
    entity_id_s2 = await store_2.add_entity(components=[pos_component], step=0) # Active at step 0 in store_2
    entity_id_s2_step1 = await store_2.add_entity(components=[pos_component], step=1) # Active at step 1 in store_2

    # Remove entity from step 0 in store_2
    await store_2.remove_entity(entity_id=entity_id_s2, step=0)

    sig = store_2._sig_from_components([pos_component])
    table_name = store_2._sig2hash(sig)
    df = store_2.sess.get_table(table_name).to_dataframe()

    # Check entity at step 0 is inactive in store_2
    entity_data_step0_s2 = df.where(
        (df["entity_id"] == entity_id_s2) & 
        (df["step"] == 0) &
        (df["simulation"] == store_2.simulation) &
        (df["run"] == store_2.run)
    ).to_pydict()
    assert len(entity_data_step0_s2["is_active"]) == 1
    assert entity_data_step0_s2["is_active"][0] is False

    # Check entity at step 1 is still active in store_2
    # This actually refers to entity_id_s2_step1 if we assume _entity_counter increments
    # The test should be: add entity E1 at step 0. Add entity E1 at step 1. Remove E1 at step 0.
    # The current store.add_entity uses a global counter for entity_id.
    # So, let's test removing only one version of an entity when it exists across different sims/runs

    # Reset: Store 1 has entity_id at step 0
    # Store 3 (new one) will have same logical entity but different sim/run
    store_3_uri_path = uri_path.parent / "store3_db"
    store_3_uri_path.mkdir(parents=True, exist_ok=True)
    store_3 = ArchetypeStore(uri=str(store_3_uri_path), simulation="diff_sim", run="diff_run")
    entity_id_s3 = await store_3.add_entity(components=[pos_component], step=0)

    # Original store still has its entity (entity_id)
    await store.remove_entity(entity_id=entity_id, step=0) # remove from original store

    # Check original store's entity is inactive
    sig_s1 = store._sig_from_components([pos_component])
    table_name_s1 = store._sig2hash(sig_s1)
    df_s1 = store.sess.get_table(table_name_s1).to_dataframe()
    entity_data_s1 = df_s1.where(
        (df_s1["entity_id"] == entity_id) & 
        (df_s1["step"] == 0) &
        (df_s1["simulation"] == store.simulation) &
        (df_s1["run"] == store.run)
    ).to_pydict()
    assert entity_data_s1["is_active"][0] is False

    # Check store_3's entity is still active
    sig_s3 = store_3._sig_from_components([pos_component])
    table_name_s3 = store_3._sig2hash(sig_s3)
    df_s3 = store_3.sess.get_table(table_name_s3).to_dataframe()
    entity_data_s3 = df_s3.where(
        (df_s3["entity_id"] == entity_id_s3) & # Note: entity_id_s3 is 1 for store_3
        (df_s3["step"] == 0) &
        (df_s3["simulation"] == store_3.simulation) &
        (df_s3["run"] == store_3.run)
    ).to_pydict()
    assert len(entity_data_s3["is_active"]) == 1 # ensure it exists
    assert entity_data_s3["is_active"][0] is True


@pytest.mark.asyncio
async def test_get_archetypes_exact_match(store: ArchetypeStore):
    pos_component = Position(x=1.0, y=2.0, vec=[1.0, 2.0])
    await store.add_entity(components=[pos_component], step=0)

    archetypes = await store.get_archetypes(Position)
    assert len(archetypes) == 1
    sig = store._sig_from_components([pos_component])
    table_name = store._sig2hash(sig)
    assert table_name in archetypes
    df = archetypes[table_name]
    assert "position__x" in df.schema()
    data = df.to_pydict()
    assert data["position__x"][0] == 1.0
    assert data["is_active"][0] is True # Ensure is_active is present

@pytest.mark.asyncio
async def test_get_archetypes_superset_match(store: ArchetypeStore):
    """Requesting a subset of components of an existing archetype."""
    pos_component = Position(x=1.0, y=2.0, vec=[1.0, 2.0])
    vel_component = Velocity(dx=0.1, dy=0.2, vec=[0.1, 0.2])
    await store.add_entity(components=[pos_component, vel_component], step=0)

    archetypes = await store.get_archetypes(Position) # Request only Position
    assert len(archetypes) == 1
    sig = store._sig_from_components([pos_component, vel_component])
    table_name = store._sig2hash(sig)
    assert table_name in archetypes
    df = archetypes[table_name]
    assert "position__x" in df.schema()
    assert "velocity__dx" in df.schema() # The full archetype is returned

@pytest.mark.asyncio
async def test_get_archetypes_no_match_subset_requested(store: ArchetypeStore):
    """Requesting a superset of components of an existing archetype."""
    pos_component = Position(x=1.0, y=2.0, vec=[1.0, 2.0])
    await store.add_entity(components=[pos_component], step=0)

    archetypes = await store.get_archetypes(Position, Velocity) # Request Position and Velocity
    assert len(archetypes) == 0

@pytest.mark.asyncio
async def test_get_archetypes_no_match_disjoint(store: ArchetypeStore):
    pos_component = Position(x=1.0, y=2.0, vec=[1.0, 2.0])
    await store.add_entity(components=[pos_component], step=0)

    archetypes = await store.get_archetypes(Velocity) # Request only Velocity
    assert len(archetypes) == 0

@pytest.mark.asyncio
async def test_get_archetypes_multiple_matches(store: ArchetypeStore):
    pos_component = Position(x=1.0, y=2.0, vec=[1.0, 2.0])
    vel_component = Velocity(dx=0.1, dy=0.2, vec=[0.1, 0.2])
    health_component = Health(value=100, vec=[100.0])

    # Archetype 1: Position
    await store.add_entity(components=[pos_component], step=0)
    # Archetype 2: Position, Velocity
    await store.add_entity(components=[pos_component, vel_component], step=1) # step 1 to differentiate data if needed
    # Archetype 3: Health (won't match if we query for Position)
    await store.add_entity(components=[health_component], step=0)

    archetypes = await store.get_archetypes(Position)
    assert len(archetypes) == 2 # Both Archetype 1 and Archetype 2 contain Position

    sig1 = store._sig_from_components([pos_component])
    table_name1 = store._sig2hash(sig1)
    sig2 = store._sig_from_components([pos_component, vel_component])
    table_name2 = store._sig2hash(sig2)

    assert table_name1 in archetypes
    assert table_name2 in archetypes
    assert "position__x" in archetypes[table_name1].schema()
    assert "position__x" in archetypes[table_name2].schema()
    assert "velocity__dx" in archetypes[table_name2].schema()

@pytest.mark.asyncio
async def test_get_archetypes_empty_store(store: ArchetypeStore):
    archetypes = await store.get_archetypes(Position)
    assert len(archetypes) == 0

@pytest.mark.asyncio
async def test_get_archetypes_no_components_requested_raises_error(store: ArchetypeStore):
    with pytest.raises(ValueError, match="Must request at least one component type"):
        await store.get_archetypes()

@pytest.mark.asyncio
async def test_get_archetypes_includes_inactive_entities(store: ArchetypeStore):
    pos_component = Position(x=1.0, y=2.0, vec=[1.0, 2.0])
    entity_id = await store.add_entity(components=[pos_component], step=0)
    await store.remove_entity(entity_id, step=0) # Mark as inactive

    archetypes = await store.get_archetypes(Position)
    assert len(archetypes) == 1
    sig = store._sig_from_components([pos_component])
    table_name = store._sig2hash(sig)
    assert table_name in archetypes
    df = archetypes[table_name]
    data = df.to_pydict()
    assert len(data["is_active"]) == 1
    assert data["is_active"][0] is False # get_archetypes returns it, consumer should filter if needed

@pytest.mark.asyncio
async def test_get_archetypes_data_integrity(store: ArchetypeStore):
    pos_component1 = Position(x=1.0, y=2.0, vec=[1.0, 2.0])
    vel_component1 = Velocity(dx=0.1, dy=0.2, vec=[0.1, 0.2])
    entity_id1 = await store.add_entity(components=[pos_component1, vel_component1], step=0)

    pos_component2 = Position(x=3.0, y=4.0, vec=[3.0, 4.0])
    entity_id2 = await store.add_entity(components=[pos_component2], step=1)
    
    # Store with different sim/run to ensure separation
    uri_path = Path(store.catalog.metadata["uri"].replace("sqlite:///", "").replace("/catalog.db",""))
    other_store_path = uri_path.parent / "other_store_for_integrity_check"
    other_store_path.mkdir(parents=True, exist_ok=True)
    other_store = ArchetypeStore(uri=str(other_store_path), simulation="sim_B", run="run_B")
    pos_component3 = Position(x=5.0, y=6.0, vec=[5.0, 6.0])
    await other_store.add_entity(components=[pos_component3], step=0)

    # Query the original store
    archetypes = await store.get_archetypes(Position)
    assert len(archetypes) == 2 # Two archetypes in the original store match Position

    sig1 = store._sig_from_components([pos_component1, vel_component1])
    table_name1 = store._sig2hash(sig1)
    sig2 = store._sig_from_components([pos_component2])
    table_name2 = store._sig2hash(sig2)

    assert table_name1 in archetypes
    assert table_name2 in archetypes

    df1_data = archetypes[table_name1].where(archetypes[table_name1]["entity_id"] == entity_id1).to_pydict()
    df2_data = archetypes[table_name2].where(archetypes[table_name2]["entity_id"] == entity_id2).to_pydict()

    assert df1_data["position__x"][0] == 1.0
    assert df1_data["velocity__dx"][0] == 0.1
    assert df1_data["simulation"][0] == store.simulation
    assert df1_data["run"][0] == store.run
    assert df1_data["step"][0] == 0

    assert df2_data["position__x"][0] == 3.0
    assert df2_data["simulation"][0] == store.simulation
    assert df2_data["run"][0] == store.run
    assert df2_data["step"][0] == 1

    # Ensure data from other_store is not present
    for table_name, df_content in archetypes.items():
        sim_values = df_content.to_pydict()["simulation"]
        assert all(s == store.simulation for s in sim_values)
        run_values = df_content.to_pydict()["run"]
        assert all(r == store.run for r in run_values)


# --- Test Helper Methods ---

def test_sig_from_components(store: ArchetypeStore):
    sig1 = store._sig_from_components([Position(x=1,y=1,vec=[1,1]), Velocity(dx=1,dy=1,vec=[1,1])])
    sig2 = store._sig_from_components([Velocity(dx=1,dy=1,vec=[1,1]), Position(x=1,y=1,vec=[1,1])]) # Different order
    assert sig1 == sig2
    assert sig1 == (Position, Velocity) # Alphabetical by class name

def test_get_component_prefix(store: ArchetypeStore):
    prefix = store._get_component_prefix(Position)
    assert prefix == "position__"
    prefix_health = store._get_component_prefix(Health)
    assert prefix_health == "health__"

def test_sig2hash_consistency_and_uniqueness(store: ArchetypeStore):
    sig_pos = (Position,)
    sig_vel = (Velocity,)
    sig_pos_vel = (Position, Velocity)
    sig_vel_pos = (Velocity, Position) # Should be same as sig_pos_vel due to internal sorting

    hash_pos1 = store._sig2hash(sig_pos)
    hash_pos2 = store._sig2hash(sig_pos)
    assert hash_pos1 == hash_pos2

    hash_pos_vel1 = store._sig2hash(sig_pos_vel)
    hash_vel_pos1 = store._sig2hash(store._sig_from_components([Velocity(dx=1,dy=1,vec=[1,1]), Position(x=1,y=1,vec=[1,1])]))
    assert hash_pos_vel1 == hash_vel_pos1
    
    hash_vel = store._sig2hash(sig_vel)
    assert hash_pos1 != hash_vel
    assert hash_pos1 != hash_pos_vel1
    assert hash_vel != hash_pos_vel1
    assert len(hash_pos1) == 20 # blake2b digest_size=10 -> 20 hex characters


def test_convert_component_to_pyarrow_schema(store: ArchetypeStore):
    schema_pos = store._convert_component_to_pyarrow_schema(Position)
    assert "x" in schema_pos.names
    assert "y" in schema_pos.names
    assert "vec" in schema_pos.names
    assert schema_pos.field("x").type == "float64"
    assert schema_pos.field("vec").type == "list<item: double>"
    # For LanceModel, types are inferred. Pydantic float -> float64, Vector(N) -> list<item: double>[N elements]

def test_get_component_schema(store: ArchetypeStore):
    comp_schema = store._get_component_schema(Position)
    assert "position__x" in comp_schema.names
    assert "position__y" in comp_schema.names
    assert "position__vec" in comp_schema.names
    assert comp_schema.field("position__x").type == "float64"

    comp_schema_health = store._get_component_schema(Health)
    assert "health__value" in comp_schema_health.names
    assert comp_schema_health.field("health__value").type == "int64"

def test_get_archetype_schema(store: ArchetypeStore):
    sig = (Position, Velocity)
    arch_schema = store._get_archetype_schema(sig)

    # Base fields
    assert "simulation" in arch_schema.names
    assert "run" in arch_schema.names
    assert "entity_id" in arch_schema.names
    assert "step" in arch_schema.names
    assert "is_active" in arch_schema.names

    # Position fields
    assert "position__x" in arch_schema.names
    assert "position__y" in arch_schema.names
    assert "position__vec" in arch_schema.names

    # Velocity fields
    assert "velocity__dx" in arch_schema.names
    assert "velocity__dy" in arch_schema.names
    assert "velocity__vec" in arch_schema.names

    # Check some types
    assert arch_schema.field("entity_id").type == "uint64"
    assert arch_schema.field("is_active").type == "bool"
    assert arch_schema.field("position__x").type == "float64"
    assert arch_schema.field("velocity__dx").type == "float64"

@pytest.mark.asyncio # Made async due to potential underlying async operations in Daft if schema was complex
async def test_new_archetype_row(store: ArchetypeStore):
    entity_id = 99
    step = 5
    pos_component = Position(x=10.0, y=20.0, vec=[10.0, 20.0])
    vel_component = Velocity(dx=1.0, dy=2.0, vec=[1.0, 2.0])
    components = [pos_component, vel_component]
    sig = store._sig_from_components(components)
    pyarrow_schema = store._get_archetype_schema(sig)

    df_row = store._new_archetype_row(entity_id, step, components, pyarrow_schema)
    await df_row.collect() # Collect to materialize the DataFrame
    data = df_row.to_pydict()

    assert data["simulation"][0] == store.simulation
    assert data["run"][0] == store.run
    assert data["entity_id"][0] == entity_id
    assert data["step"][0] == step
    assert data["is_active"][0] is True

    assert data["position__x"][0] == 10.0
    assert data["position__y"][0] == 20.0
    assert data["position__vec"][0] == [10.0, 20.0] # Note: Daft might return list for vector here

    assert data["velocity__dx"][0] == 1.0
    assert data["velocity__dy"][0] == 2.0
    assert data["velocity__vec"][0] == [1.0, 2.0]

    assert store._entity2sig[entity_id] == sig
    # Clean up the entry from _entity2sig if it's undesirable for other tests
    # or ensure tests that rely on _entity_counter re-initialize store or account for this.
    # For this specific test, it's fine as is.
    del store._entity2sig[entity_id] # cleanup for isolation


# Mocking for _does_table_contain_components
# We need a mock Daft Table and Schema
import pyarrow as pa
from daft import DataFrame, Schema as daft_schema_builder 
from daft.catalog import Table as daft_table_builder
from unittest.mock import MagicMock


def test_does_table_contain_components_true(store: ArchetypeStore):
    # Table has Position and Velocity fields
    mock_table_schema_pa = pa.schema([
        pa.field("simulation", pa.string()),
        pa.field("position__x", pa.float64()),
        pa.field("position__y", pa.float64()),
        pa.field("position__vec", pa.list_(pa.float64())),
        pa.field("velocity__dx", pa.float64()),
        pa.field("velocity__dy", pa.float64()),
        pa.field("velocity__vec", pa.list_(pa.float64())),
    ])
    mock_df = DataFrame.from_pydict({
        "simulation": ["sim1"],
        "position__x": [1.0],
        "position__y": [1.0],
        "position__vec": [[1.0,1.0]],
        "velocity__dx": [0.1],
        "velocity__dy": [0.1],
        "velocity__vec": [[0.1,0.1]],
    })
    # In Daft, schema is attached to DataFrame, Table.to_dataframe().schema()
    mock_table = MagicMock(spec=daft_table_builder.Table)
    mock_table.to_dataframe.return_value = mock_df._df # Access internal Daft Rust DataFrame and wrap
    # Re-wrap to get a Python Daft DataFrame
    mock_table.to_dataframe.return_value = DataFrame(mock_df._df)


    assert store._does_table_contain_components(Position, table=mock_table) is True
    assert store._does_table_contain_components(Velocity, table=mock_table) is True
    assert store._does_table_contain_components(Position, Velocity, table=mock_table) is True

def test_does_table_contain_components_false_missing_field(store: ArchetypeStore):
    # Table only has Position fields
    mock_table_schema_pa = pa.schema([
        pa.field("position__x", pa.float64()),
        pa.field("position__y", pa.float64()),
        # position__vec is missing
    ])
    mock_df = DataFrame.from_pydict({
        "position__x": [1.0],
        "position__y": [1.0],
    })
    mock_table = MagicMock(spec=daft_table_builder.Table)
    mock_table.to_dataframe.return_value = DataFrame(mock_df._df)
    
    # Position requires x, y, vec. Table only has x, y for position prefix
    assert store._does_table_contain_components(Position, table=mock_table) is False
    # Requesting Velocity which is not in the table at all
    assert store._does_table_contain_components(Velocity, table=mock_table) is False

def test_does_table_contain_components_false_wrong_type(store: ArchetypeStore):
    # Table has Position fields but x is int instead of float
    mock_table_schema_pa = pa.schema([
        pa.field("position__x", pa.int64()), # Wrong type
        pa.field("position__y", pa.float64()),
        pa.field("position__vec", pa.list_(pa.float64())),
    ])
    mock_df = DataFrame.from_pydict({
        "position__x": [1],
        "position__y": [1.0],
        "position__vec": [[1.0,1.0]],
    })
    mock_table = MagicMock(spec=daft_table_builder.Table)
    mock_table.to_dataframe.return_value = DataFrame(mock_df._df)

    assert store._does_table_contain_components(Position, table=mock_table) is False

def test_does_table_contain_components_table_is_superset(store: ArchetypeStore):
    # Table has Position, Velocity, and Health fields
    mock_table_schema_pa = pa.schema([
        pa.field("position__x", pa.float64()),
        pa.field("position__y", pa.float64()),
        pa.field("position__vec", pa.list_(pa.float64())),
        pa.field("velocity__dx", pa.float64()),
        pa.field("velocity__dy", pa.float64()),
        pa.field("velocity__vec", pa.list_(pa.float64())),
        pa.field("health__value", pa.int64()),
        pa.field("health__vec", pa.list_(pa.float64())), # LanceModel requires a vec field
    ])
    mock_df = DataFrame.from_pydict({
        "position__x": [1.0], "position__y": [1.0], "position__vec": [[1.0,1.0]],
        "velocity__dx": [0.1], "velocity__dy": [0.1], "velocity__vec": [[0.1,0.1]],
        "health__value": [100], "health__vec": [[100.0]],
    })
    mock_table = MagicMock(spec=daft_table_builder.Table)
    mock_table.to_dataframe.return_value = DataFrame(mock_df._df)

    assert store._does_table_contain_components(Position, table=mock_table) is True
    assert store._does_table_contain_components(Velocity, table=mock_table) is True
    # Requesting only Position and Velocity, table also has Health
    assert store._does_table_contain_components(Position, Velocity, table=mock_table) is True

# Final check of the file, maybe some helper function tests if needed 