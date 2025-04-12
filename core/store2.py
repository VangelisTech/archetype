\
# -*- coding: utf-8 -*-
"""
Manages the storage and state of components using versioned LanceDB tables,
coordinated by a central catalog.

Key aspects of this implementation:
- Component data is stored historically in LanceDB tables.
- Each unique component structure (schema) gets a version hash.
- A catalog tracks schema versions, their evolution, and maps them to LanceDB tables.
- Updates append new rows (as Arrow data) to the appropriate versioned table.
- Component removal might involve appending a specific marker or managing metadata.
- Queries need to consult the catalog to find relevant tables for specific steps/versions.
"""

from typing import Dict, List, Optional, Type, Union, Any
from itertools import count as _count
from logging import getLogger
import hashlib
import json
import daft
from daft import col, DataFrame
import pyarrow as pa
import lancedb # Assuming lancedb library is used

# Import base types from our new structure
from .base import Component
# Import interface
from .interfaces import ComponentStoreInterface

logger = getLogger(__name__)

# --- Placeholder for Catalog Client ---
# This should be properly implemented and injected
class CatalogClientPlaceholder:
    def __init__(self):
        # Stores {component_type_name: {schema_hash: {"table_name": "...", "parent": "..."}}}
        self._registry = {}

    def get_schema_metadata(self, component_type_name: str, schema_hash: str) -> Optional[Dict[str, str]]:
        """Checks if a specific schema version exists and returns its metadata."""
        logger.debug(f"[Catalog] Checking for schema: {component_type_name} / {schema_hash}")
        return self._registry.get(component_type_name, {}).get(schema_hash)

    def register_schema_version(self, component_type_name: str, schema_hash: str, schema_definition: dict, parent_hash: Optional[str]) -> str:
        """Registers a new schema version and returns its assigned table name."""
        logger.info(f"[Catalog] Registering NEW schema: {component_type_name} / {schema_hash} (Parent: {parent_hash})")
        if component_type_name not in self._registry:
            self._registry[component_type_name] = {}

        # Simple table naming strategy (replace with robust one)
        table_name = f"{component_type_name.lower()}_{schema_hash[:8]}"
        self._registry[component_type_name][schema_hash] = {
            "table_name": table_name,
            "parent": parent_hash,
            # Store schema_definition if needed
        }
        # In a real scenario, this might also trigger table creation in LanceDB/storage if needed
        logger.debug(f"[Catalog] Assigned table name: {table_name}")
        return table_name

    def get_latest_schema_hash(self, component_type_name: str) -> Optional[str]:
        """Finds the latest schema hash (e.g., one with no children or newest timestamp). Placeholder logic."""
        # This is overly simplistic - needs proper DAG traversal or timestamp tracking
        schemas = self._registry.get(component_type_name, {})
        # Find a schema that isn't a parent to any other schema in the registry for this type (basic leaf node check)
        all_parents = {meta.get('parent') for sh, meta in schemas.items() if meta.get('parent')}
        latest_hashes = [sh for sh in schemas if sh not in all_parents]
        # If multiple leaves, return the first one found (needs better logic)
        return latest_hashes[0] if latest_hashes else None

    def get_table_name_for_latest_schema(self, component_type_name: str) -> Optional[str]:
        """Gets the table name associated with the latest schema version."""
        latest_hash = self.get_latest_schema_hash(component_type_name)
        if latest_hash:
            metadata = self.get_schema_metadata(component_type_name, latest_hash)
            return metadata.get("table_name") if metadata else None
        return None
# --- End Placeholder ---


class ComponentStore(ComponentStoreInterface): # Implement interface
    """
    Manages the historical state of components, stored in versioned LanceDB tables
    coordinated via a schema catalog.
    """

    def __init__(self, lancedb_uri: str, catalog_client: CatalogClientPlaceholder):
        """
        Initializes the ComponentStore.

        Args:
            lancedb_uri: The URI for the LanceDB database connection.
            catalog_client: An instance of the catalog client to manage schema versions.
        """
        logger.info(f"Initializing ComponentStore with LanceDB URI: {lancedb_uri}")
        self.lancedb_uri = lancedb_uri
        self.catalog = catalog_client
        self.db = lancedb.connect(lancedb_uri) # Connect to LanceDB

        # Stores {schema_hash: table_name} mapping for quick lookup after registration
        self._known_schema_tables: Dict[str, str] = {}

        # Stores {component_type_name: latest_schema_hash} cache (can be invalidated)
        self._latest_schema_cache: Dict[str, str] = {}

        # Entity Management
        self._entity_count = _count(start=1)
        self._dead_entities = set() # Keep this for now, might need revision

    def _calculate_schema_hash(self, component_type: Type[Component]) -> str:
        """Calculates a stable hash for the component's schema."""
        # Using Pydantic's schema() and hashing its JSON representation
        try:
            schema_dict = component_type.model_json_schema()
            # Sort keys for stability
            schema_str = json.dumps(schema_dict, sort_keys=True)
            return hashlib.sha256(schema_str.encode('utf-8')).hexdigest()
        except Exception as e:
            logger.error(f"Failed to generate schema or hash for {component_type.__name__}: {e}", exc_info=True)
            raise TypeError(f"Could not calculate schema hash for {component_type.__name__}") from e

    def _get_lance_table(self, table_name: str) -> Any:
        """Placeholder: Opens and returns a LanceDB table handle."""
        try:
            # In real usage, might need to handle table creation if not exists
            # based on catalog info / component schema
            logger.debug(f"Opening LanceDB table: {table_name}")
            return self.db.open_table(table_name)
        except Exception as e: # More specific exception handling needed
            logger.error(f"Failed to open LanceDB table '{table_name}' at {self.lancedb_uri}: {e}", exc_info=True)
            # Should we try to create it here based on the schema? Needs coordination.
            raise IOError(f"Could not open LanceDB table: {table_name}") from e

    def _append_to_lance_table(self, table_name: str, arrow_table: pa.Table) -> None:
        """Placeholder: Appends data (Arrow Table) to the specified LanceDB table."""
        if not arrow_table or arrow_table.num_rows == 0:
            logger.debug(f"Skipping append to LanceDB table '{table_name}': No data.")
            return
        try:
            logger.debug(f"Appending {arrow_table.num_rows} rows to LanceDB table: {table_name}. Columns: {arrow_table.schema.names}")
            # LanceDB automatically handles schema if table exists, might merge/evolve
            # Or potentially use db.create_table if needed based on catalog state
            table = self._get_lance_table(table_name)
            table.add(arrow_table) # Use add() for appending Arrow data
        except Exception as e:
            logger.error(f"Failed to append data to LanceDB table '{table_name}': {e}", exc_info=True)
            raise IOError(f"Could not append to LanceDB table: {table_name}") from e

    def register_component(self, component_type: Type[Component]) -> Tuple[str, str]:
        """
        Ensures a component type's current schema is known and registered in the catalog.
        Returns the schema hash and the associated LanceDB table name.
        """
        component_name = component_type.__name__
        schema_hash = self._calculate_schema_hash(component_type)

        # 1. Check local cache
        if schema_hash in self._known_schema_tables:
            return schema_hash, self._known_schema_tables[schema_hash]

        # 2. Check catalog
        catalog_meta = self.catalog.get_schema_metadata(component_name, schema_hash)
        if catalog_meta and "table_name" in catalog_meta:
            table_name = catalog_meta["table_name"]
            self._known_schema_tables[schema_hash] = table_name
            logger.debug(f"Found existing schema {schema_hash} for {component_name} in catalog. Table: {table_name}")
            # Update latest schema cache if needed
            self._update_latest_schema_cache(component_name, schema_hash)
            return schema_hash, table_name

        # 3. Register new schema version in catalog
        logger.info(f"Schema hash {schema_hash} for {component_name} not found. Registering new version.")
        # Find parent hash (naive: assume latest known is parent)
        parent_hash = self.catalog.get_latest_schema_hash(component_name)
        schema_def = component_type.model_json_schema() # Get schema definition for catalog
        try:
            table_name = self.catalog.register_schema_version(
                component_type_name=component_name,
                schema_hash=schema_hash,
                schema_definition=schema_def,
                parent_hash=parent_hash
            )
            # Try creating the table immediately upon registration? Needs schema.
            # self.db.create_table(table_name, schema=component_type.to_arrow_schema()) # Example

        except Exception as e:
            logger.error(f"Failed to register schema {schema_hash} for {component_name} in catalog: {e}", exc_info=True)
            raise RuntimeError(f"Catalog registration failed for {component_name}") from e

        self._known_schema_tables[schema_hash] = table_name
        self._update_latest_schema_cache(component_name, schema_hash) # This is now the latest
        logger.debug(f"Registered component type: {component_name} with schema {schema_hash}, table: {table_name}")
        return schema_hash, table_name

    def _update_latest_schema_cache(self, component_name: str, schema_hash: str):
        """Internal helper to update the cache of latest known schemas."""
        # This assumes the newly registered hash is the latest. More robust logic
        # might re-query the catalog after registration if parent/child links are complex.
        self._latest_schema_cache[component_name] = schema_hash

    # --- Methods related to getting data ---
    # These need significant changes as they no longer work with in-memory Daft DFs

    def get_component_table_name(self, component_type: Type[Component]) -> Optional[str]:
        """
        Gets the LanceDB table name for the *latest registered* schema version of the component type.
        Registers the component type if it's not known.
        Note: This provides the table for the LATEST version, querying specific versions
              or steps requires involving the QueryManager and catalog more directly.
        """
        component_name = component_type.__name__
        # Check cache first
        latest_hash = self._latest_schema_cache.get(component_name)
        if latest_hash and latest_hash in self._known_schema_tables:
            return self._known_schema_tables[latest_hash]

        # If not in cache or hash not known locally, consult catalog/register
        try:
            # Ensure the type is registered (gets the *current* schema hash and table)
            current_schema_hash, current_table_name = self.register_component(component_type)

            # Now, ask the catalog again for the absolute latest (might differ if registration happened elsewhere)
            table_name = self.catalog.get_table_name_for_latest_schema(component_name)
            if table_name:
                 # Update cache with potentially newer "latest" info from catalog
                 latest_hash_from_catalog = self.catalog.get_latest_schema_hash(component_name)
                 if latest_hash_from_catalog:
                    self._known_schema_tables[latest_hash_from_catalog] = table_name
                    self._latest_schema_cache[component_name] = latest_hash_from_catalog
                 return table_name
            else:
                 # Fallback to the table of the schema we just registered if catalog fails
                 logger.warning(f"Catalog couldn't find latest table for {component_name}, using table for current schema: {current_table_name}")
                 return current_table_name

        except Exception as e:
            logger.error(f"Failed to get/register component {component_name} to find table name: {e}", exc_info=True)
            raise RuntimeError(f"Failed to determine LanceDB table for {component_name}") from e

    # --- Update Methods ---

    def update_component_data(self, schema_hash: str, update_arrow_table: pa.Table) -> None:
        """
        Appends data (Arrow Table) to the LanceDB table corresponding to the given schema hash.
        Ensures the schema hash is known (implicitly via prior registration).
        """
        if schema_hash not in self._known_schema_tables:
             # This shouldn't happen if register_component was called beforehand
             # maybe via add_component or implicitly by Query/Update managers.
             # For robustness, we could try to look it up in the catalog again.
             logger.error(f"Attempted to update data for unknown schema hash: {schema_hash}. Registration might be missing.")
             # Option: try catalog lookup here?
             # catalog_meta = self.catalog.get_schema_metadata(component_name, schema_hash) # Need component_name too
             # if catalog_meta: self._known_schema_tables[schema_hash] = catalog_meta["table_name"]
             # else: raise ValueError(...)
             raise ValueError(f"Cannot update data for unknown schema hash: {schema_hash}")

        table_name = self._known_schema_tables[schema_hash]
        self._append_to_lance_table(table_name, update_arrow_table)

    def add_component(self, entity_id: int, component_instance: Component, step: int) -> None:
        """
        Adds/updates a single component instance for an entity at a specific step.
        Determines the schema, registers it if needed, and appends to the correct LanceDB table.
        """
        component_type = type(component_instance)
        try:
            # Ensure schema is registered and get hash/table
            schema_hash, _ = self.register_component(component_type) # We need the hash

            # Prepare data row as Arrow Table
            component_dict = component_instance.model_dump()
            component_dict["entity_id"] = entity_id
            component_dict["step"] = step
            component_dict["is_active"] = True # Assuming active on add

            # Create single-row Arrow Table - Use PyArrow directly
            # Need the full Arrow schema for this component version
            arrow_schema = component_type.to_arrow_schema(
                include_keys={"entity_id": pa.int64(), "step": pa.int64(), "is_active": pa.bool_()}
            )
            # Ensure dict keys match schema field names *exactly*
            data_list = [[val] for val in component_dict.values()] # Data in column format
            names_list = list(component_dict.keys())

            # Reorder data_list based on arrow_schema.names
            ordered_data = []
            schema_name_to_index = {name: i for i, name in enumerate(names_list)}
            for field_name in arrow_schema.names:
                if field_name in schema_name_to_index:
                    ordered_data.append(data_list[schema_name_to_index[field_name]])
                else:
                     # Handle potential schema mismatch or missing keys (shouldn't happen if model_dump is complete)
                     logger.error(f"Key '{field_name}' from Arrow schema not found in component dict for {component_type.__name__}. Data: {component_dict.keys()}")
                     # Add placeholder like None or raise error? Adding None for now.
                     # This assumes the Arrow type for field_name is nullable.
                     ordered_data.append([None])


            arrow_table = pa.Table.from_arrays(ordered_data, schema=arrow_schema)

            # Append using the specific update method
            self.update_component_data(schema_hash, arrow_table)

        except Exception as e:
            logger.error(f"Failed to add component {component_type.__name__} for entity {entity_id}: {e}", exc_info=True)
            # Decide on error handling: raise, log and skip? Raising for now.
            raise

    # --- Entity Lifecycle ---

    def add_entity(self, components: Optional[List[Component]] = None, step: Optional[int] = -1) -> int:
        """
        Adds a new entity and its initial components.
        """
        entity_id = next(self._entity_count)
        logger.debug(f"Adding new entity: {entity_id} at step {step}")

        if components:
            for component in components:
                # Add each component using the revised method
                self.add_component(entity_id, component, step)

        # self.entities dictionary is removed as state is now in LanceDB
        # We might need a way to query active entities if needed.

        return entity_id

    def remove_entity(self, entity_id: int, step: int) -> None:
        """
        Marks an entity as inactive across all its components at a given step.
        This likely requires appending an 'is_active=False' record to *all* relevant
        component tables for that entity at the specified step.
        (Simplistic approach: just add to internal set for now, QueryManager needs to handle)
        """
        logger.debug(f"Marking entity {entity_id} as inactive from step {step}.")
        # TODO: Implement strategy for marking inactive in LanceDB tables.
        # Option 1: Append row with is_active=False to relevant tables.
        # Option 2: Store dead entities separately and filter during queries.
        # Sticking with Option 2 for now via the internal set.
        self._dead_entities.add(entity_id) # Add to the set

    # --- Component Removal ---
    # Removing components also needs careful design with versioned tables.
    # Option: Append a record with is_active=False, similar to entity removal.

    def remove_component_from_entity(self,
            entity_id: int,
            component_type: Type[Component],
            step: int
        ) -> None:
        """
        Marks a specific component as inactive for an entity at a given step.
        Requires finding the correct table for the component type and appending
        an inactivation record.
        """
        logger.debug(f"Marking component {component_type.__name__} inactive for entity {entity_id} at step {step}.")
        try:
            # Find the table for the latest schema version (assuming removal applies to current state)
            # More complex logic might be needed if removing historically.
            table_name = self.get_component_table_name(component_type)
            if not table_name:
                logger.warning(f"Cannot remove component {component_type.__name__}: Table not found.")
                return

            schema_hash = self._latest_schema_cache.get(component_type.__name__) # Get corresponding hash
            if not schema_hash:
                 # This indicates an inconsistency; log and potentially skip
                 logger.error(f"Cannot remove component {component_type.__name__}: Latest schema hash not cached.")
                 return


            # Create an Arrow record indicating inactive state
            # We need the schema to create the record correctly. Fetch from type.
            arrow_schema = component_type.to_arrow_schema(
                 include_keys={"entity_id": pa.int64(), "step": pa.int64(), "is_active": pa.bool_()}
            )
            inactive_data = {}
            for field in arrow_schema:
                if field.name == "entity_id":
                    inactive_data[field.name] = entity_id
                elif field.name == "step":
                    inactive_data[field.name] = step
                elif field.name == "is_active":
                    inactive_data[field.name] = False
                else:
                    # Fill other fields with null/default if needed by LanceDB/Arrow
                    # Using None assumes fields are nullable in the Arrow schema
                    inactive_data[field.name] = None

            # Convert dict to Arrow Table row (similar to add_component)
            data_list = [[val] for val in inactive_data.values()]
            names_list = list(inactive_data.keys())
            ordered_data = []
            schema_name_to_index = {name: i for i, name in enumerate(names_list)}
            for field_name in arrow_schema.names:
                 if field_name in schema_name_to_index:
                     ordered_data.append(data_list[schema_name_to_index[field_name]])
                 else:
                     ordered_data.append([None]) # Should ideally not happen

            arrow_table = pa.Table.from_arrays(ordered_data, schema=arrow_schema)

            # Append the inactivation record
            self.update_component_data(schema_hash, arrow_table) # Use the specific hash

        except Exception as e:
            logger.error(f"Failed to remove component {component_type.__name__} for entity {entity_id}: {e}", exc_info=True)
            # Decide on error handling

    def remove_entity_from_component(self, entity_id: int, component_type: Type[Component], step: int) -> None:
        """Alias for remove_component_from_entity."""
        self.remove_component_from_entity(entity_id, component_type, step)


    # get_column_names is no longer relevant as data isn't in Daft dict
    # def get_column_names(self, component_type: Type[Component]) -> List[str]:
    #     # ... implementation removed ...

# --- Helper function for Pydantic models to include keys in Arrow schema ---
# Monkey-patch or add a base class method if preferred
def pydantic_to_arrow_schema_with_keys(model_cls: Type[Component], include_keys: Dict[str, pa.DataType]) -> pa.Schema:
    """Converts Pydantic model to Arrow schema, adding extra key fields."""
    # This requires a library or custom implementation to convert Pydantic types to Arrow types
    # Example using a hypothetical `pydantic_to_arrow` library:
    # base_schema = pydantic_to_arrow.schema(model_cls)

    # Manual basic conversion (highly simplified)
    type_mapping = {
        "string": pa.string(),
        "integer": pa.int64(),
        "number": pa.float64(),
        "boolean": pa.bool_(),
        # Add more complex types: list, nested dicts etc.
    }
    fields = []
    model_schema = model_cls.model_json_schema()
    required_fields = set(model_schema.get("required", []))

    for name, prop in model_schema.get("properties", {}).items():
        arrow_type = type_mapping.get(prop.get("type"), pa.string()) # Default to string
        is_nullable = name not in required_fields
        fields.append(pa.field(name, arrow_type, nullable=is_nullable))

    # Add keys
    key_fields = [pa.field(name, dtype, nullable=False) for name, dtype in include_keys.items()]

    # Combine and ensure keys come first (or desired order)
    final_fields = key_fields + fields
    return pa.schema(final_fields)

# Add the helper to the base Component class or use directly
Component.to_arrow_schema = classmethod(pydantic_to_arrow_schema_with_keys)
