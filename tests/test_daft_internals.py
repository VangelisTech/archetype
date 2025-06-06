import daft
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from daft.catalog import Catalog
from daft.session import Session
from daft import Schema
import pytest
import tempfile
@pytest.fixture(scope="function")
def daft_session():
    # Mimic ArchetypeStore's Daft session setup
    tmpdir = tempfile.mkdtemp()
    iceberg_catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{tmpdir}/catalog.db",
            "warehouse": f"file://{tmpdir}",
        },
    )
    catalog = Catalog.from_iceberg(iceberg_catalog)
    sess = Session()
    sess.attach(object=catalog)
    sess.create_namespace_if_not_exists("test_namespace")
    sess.set_namespace("test_namespace")
    return sess

def test_daft_native_table_appends(daft_session: Session):
    sess = daft_session
    table_name = "my_archetype_table"

    # Define a PyArrow schema (similar to ArchetypeStore's base + components)
    archetype_pyarrow_schema = pa.schema([
        pa.field("simulation", pa.string(), nullable=False),
        pa.field("run", pa.string(), nullable=False),
        pa.field("entity_id", pa.uint64(), nullable=False),
        pa.field("step", pa.uint64(), nullable=False),
        pa.field("is_active", pa.bool_(), nullable=False),
        pa.field("position__x", pa.float64()),
        pa.field("position__y", pa.float64()),
    ])

    # 1. Create an empty Daft table from the PyArrow schema
    print(f"\nAttempting to create Daft table '{table_name}' from PyArrow schema.")
    try:
        # Pass the schema directly as the source for an empty table
        daft_table_ref = sess.create_table_if_not_exists(table_name, source=Schema.from_pyarrow_schema(archetype_pyarrow_schema))
        print(f"Successfully created or retrieved Daft table '{table_name}'.")
        # Let's see what type daft_table_ref is
        print(f"Type of table reference from create_table_if_not_exists: {type(daft_table_ref)}")
        # This reference should be a daft.Table object
        assert isinstance(daft_table_ref, daft.Table), "Expected a daft.Table object"

    except Exception as e:
        pytest.fail(f"Failed to create Daft table from schema: {e}")

    # 2. Simulate appending rows (as PyArrow tables)
    num_appends = 3
    for i in range(num_appends):
        entity_id = i + 1
        step_val = i
        # Create a single-row PyArrow table for the new entity
        entity_data_pydict = {
            "simulation": ["sim_1"],
            "run": ["run_A"],
            "entity_id": [entity_id],
            "step": [step_val],
            "is_active": [True],
            "position__x": [float(i * 10)],
            "position__y": [float(i * 5)],
        }
        # Ensure all columns from schema are present, even if some are null for this append
        # For this test, we assume all columns are always provided.
        new_row_pa_table = pa.Table.from_pydict(entity_data_pydict, schema=archetype_pyarrow_schema)
        
        print(f"\nAppending row {i+1} to Daft table '{table_name}'...")
        try:
            # Get the table reference again (or use the one from creation if it's stateful for appends)
            # The daft.Table object returned by create_table_if_not_exists or get_table is the one to use.
            table_to_append_to = sess.get_table(table_name)
            table_to_append_to.append(daft.from_arrow(new_row_pa_table))
            print(f"Successfully appended row {i+1}.")
        except Exception as e:
            pytest.fail(f"Failed to append row {i+1} to Daft table '{table_name}': {e}")

    # 3. Read the Daft table back and verify contents
    print(f"\nReading full Daft table '{table_name}' after appends...")
    try:
        # Persist the changes if `add` is lazy (it might be, need to confirm Daft's behavior)
        # For in-memory tables, `add` should be eager. If it were disk-based, a `commit` or similar might be needed.
        # sess.commit_table(table_name) # Example if such an API existed and was needed.
        
        # Retrieve the table and collect its data
        final_daft_table_ref = sess.get_table(table_name)
        # final_daft_table_ref.collect() # This materializes the results if Daft is lazy
        # For an in-memory workflow, .read() or .to_pandas() or .to_arrow() will also materialize.
        final_df = final_daft_table_ref.to_dataframe()
        final_pa_table = final_df.to_arrow()
        print("Successfully read Daft table into PyArrow table.")
    except Exception as e:
        pytest.fail(f"Failed to read Daft table '{table_name}' after appends: {e}")

    print("Schema of final PyArrow table:")
    print(final_pa_table.schema)
    print("Contents of final PyArrow table:")
    print(final_pa_table)

    assert len(final_pa_table) == num_appends, f"Expected {num_appends} rows, got {len(final_pa_table)}"
    # Check some data
    assert final_pa_table.column("entity_id").to_pylist() == [3, 2, 1]
    assert final_pa_table.column("position__x").to_pylist() == [20.0, 10.0, 0.0]

    print(f"\nTest Conclusion: Successfully created, appended to, and read Daft-native table '{table_name}'.")
    print("This suggests Daft can natively manage tables from schema and handle PyArrow appends.")

if __name__ == "__main__":
    pytest.main([__file__]) 