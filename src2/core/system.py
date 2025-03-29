# -*- coding: utf-8 -*-
"""
Defines System implementations for orchestrating processor execution,
including the RayDagSystem for distributed, compiled execution.

Explanation and Key Points:
_SnapshotQueryInterface & _CapturingUpdateManager: These proxies are essential for isolating the remote processors. The querier reads from a static snapshot, and the updater captures outputs locally within the actor.
_RemoteProcessorWrapper: This Ray actor holds an instance of a user-defined Processor. Its process_wrapper method sets up the proxies and calls the real processor.process. It handles both sync and async processor methods. Crucially, it returns the captured updates.
RayDagSystem.__init__: Initializes Ray if needed and prepares internal processor storage.
add/remove_processor: Manages the list of processors and invalidates the compiled DAG whenever the processor list changes.
_build_and_compile_dag:
Creates the _RemoteProcessorWrapper actor handles for each processor. (TODO: Need to handle resource requests like GPUs here).
Uses ray.dag.InputNode and binds the process_wrapper method of the remote actors. Assumes a relatively flat DAG for now.
Creates a MultiOutputNode to gather results.
Calls dag.experimental_compile with recommended flags.
execute:
Snapshotting: Iterates through known component types, gets the committed DataFrame from the querier, and crucially .collect()s it to create a serializable snapshot. This is a key step but potentially expensive. Schemas are also collected.
DAG Execution: Prepares input for the DAG (snapshot, schemas, dt, args) and calls compiled_dag.execute_async. (TODO: Refine arg/kwarg passing).
Result Gathering: Uses ray.get() to wait for the DAG future to complete. ray.get can resolve awaitables from execute_async.
Update Queuing: Iterates through the results (list of dictionaries) from the DAG and uses the real updater (passed into execute) to queue the captured updates for the main commit phase.
Serialization: We are sending collected Daft DataFrames in the snapshot. This relies on Daft/Ray's ability to serialize Arrow data efficiently. Large states could still be a bottleneck.
Async: Uses async def for the wrapper and execute_async for the DAG, aligning with modern Ray practices.
"""

import ray
import time
import asyncio
from typing import Any, Dict, List, Set, Type, Tuple, TypeVar, Iterable, Optional
from collections import defaultdict
from dataclasses import is_dataclass, fields

import daft
from daft import DataFrame # Use specific import for clarity

# Import from our new structure
from .base import Component, EntityType, Processor, System, _C
from .store import EcsComponentStore
from .managers import EcsQueryInterface, EcsUpdateManager


# --- Helper Proxy/Wrapper Classes for Remote Execution ---

class _SnapshotQueryInterface:
    """
    A proxy QueryInterface that reads component data from a provided snapshot dictionary.
    Used by remote processors to access the initial state for the step.
    """
    def __init__(self, snapshot: Dict[Type[Component], Optional[DataFrame]], component_schemas: Dict[Type[Component], daft.Schema]):
        self._snapshot = snapshot
        self._schemas = component_schemas # Store schemas separately for empty DF creation

    def get_component(self, component_type: Type[_C]) -> DataFrame:
        """Gets the DataFrame from the snapshot or returns an empty one with schema."""
        df = self._snapshot.get(component_type, None)
        if df is not None:
            return df
        else:
            # Return empty DataFrame with the correct schema if not in snapshot
            schema = self._schemas.get(component_type)
            if schema:
                return daft.DataFrame.empty(schema=schema)
            else:
                # This indicates an issue - the processor is querying a type
                # that wasn't even known/snapshotted at the start of the step.
                raise ValueError(f"SnapshotQueryInterface: Schema not found for component type {component_type.__name__}. Was it registered?")

    def get_components(self, *component_types: Type[Component]) -> DataFrame:
        """Builds a join plan based on the snapshot DataFrames."""
        # This mirrors the logic in EcsQueryInterface but uses self.get_component
        if not component_types:
            return daft.DataFrame.from_pydict({"entity_id": []}).cast_to_schema(
                daft.Schema.from_py_dict({"entity_id": daft.DataType.int64()})
            )

        base_df = self.get_component(component_types[0])
        joined_df = base_df

        for i in range(1, len(component_types)):
            next_df = self.get_component(component_types[i])
            joined_df = joined_df.join(next_df, on="entity_id", how="inner")

        # Select only the entity_id and the requested component struct columns
        select_cols = ["entity_id"]
        # Need a way to get struct names without access to the store instance... Pass schemas?
        # We passed schemas, let's use them.
        select_cols.extend([self._schemas[ct].column_names()[1] for ct in component_types if ct in self._schemas]) # Assumes struct is 2nd col
        # Robustness: Check if struct name retrieval works as expected.

        # Filter select_cols to only those present in the final joined_df
        final_cols = [c for c in select_cols if c in joined_df.column_names()]

        return joined_df.select(*(daft.col(c) for c in final_cols))

    def component_for_entity(self, entity_id: int, component_type: Type[_C]) -> Optional[_C]:
        """
        Retrieves a Python component instance from the snapshot DataFrame.
        Note: This requires the snapshot DF to be collected, which might impact
        performance if large snapshots are sent.
        """
        df = self.get_component(component_type)
        if df is None:
            return None

        # Filter and collect (potential performance cost)
        result_df = df.where(daft.col("entity_id") == entity_id).limit(1)
        collected = result_df.collect()

        if len(collected) == 0:
            return None

        if not is_dataclass(component_type):
             print(f"Warning: Cannot reconstruct non-dataclass component {component_type.__name__} (Snapshot)")
             return None
        try:
            row_dict = collected.to_pydict()
            struct_name = self._schemas[component_type].column_names()[1] # Get struct name from schema
            struct_contents_list = row_dict.get(struct_name)
            if not struct_contents_list: return None
            struct_data = struct_contents_list[0]
            if struct_data is None: return None

            valid_field_names = {f.name for f in fields(component_type) if f.init}
            kwargs = {k: v for k, v in struct_data.items() if k in valid_field_names}
            return component_type(**kwargs)
        except Exception as e:
            print(f"ERROR: Failed reconstructing {component_type.__name__} from snapshot for entity {entity_id}: {e}")
            return None

    # Add other necessary methods like get_entity_type_for_entity if processors need them,
    # potentially requiring entity_type_map also be part of the snapshot.


class _CapturingUpdateManager:
    """
    A proxy UpdateManager that captures `add_update` calls into a dictionary
    instead of queueing them in the main UpdateManager. Used by remote processors.
    """
    def __init__(self):
        self.captured_updates: Dict[Type[Component], List[DataFrame]] = defaultdict(list)

    def add_update(self, component_type: Type[Component], update_df: DataFrame):
        """Captures the update DataFrame plan."""
        # We assume schema validation happened before the DAG execution
        # or that the processor correctly produces data matching the schema.
        # For robustness, could try a lightweight schema check here if needed.
        self.captured_updates[component_type].append(update_df)

    def get_captured_updates(self) -> Dict[Type[Component], List[DataFrame]]:
        """Returns the dictionary of captured update plans."""
        return dict(self.captured_updates) # Return a copy


@ray.remote
class _RemoteProcessorWrapper:
    """
    A Ray Actor that wraps a standard Processor instance for remote execution within a DAG.
    """
    def __init__(self, processor_cls: Type[Processor], processor_init_args: tuple, processor_init_kwargs: dict):
        # Instantiate the actual processor within the actor
        self._processor: Processor = processor_cls(*processor_init_args, **processor_init_kwargs)
        print(f"RemoteProcessorWrapper: Initialized processor {self._processor.__class__.__name__} in actor {ray.get_runtime_context().get_actor_id()}")

    async def process_wrapper(self,
                              snapshot: Dict[Type[Component], Optional[DataFrame]],
                              component_schemas: Dict[Type[Component], daft.Schema],
                              dt: float,
                              *args: Any,
                              **kwargs: Any) -> Dict[Type[Component], List[DataFrame]]:
        """
        Executes the wrapped processor's process method using proxy interfaces.

        Args:
            snapshot: A dictionary mapping component types to their collected DataFrame state at the start of the step.
            component_schemas: Schemas for all relevant component types.
            dt: Time delta.
            *args, **kwargs: Passthrough arguments for the processor's process method.

        Returns:
            A dictionary mapping component types to a list of update DataFrame plans generated by the processor.
        """
        # Create proxy interfaces for this execution
        proxy_querier = _SnapshotQueryInterface(snapshot, component_schemas)
        proxy_updater = _CapturingUpdateManager()

        # Execute the actual processor logic
        print(f"RemoteProcessorWrapper actor {ray.get_runtime_context().get_actor_id()}: Running {self._processor.__class__.__name__}.process...")
        try:
            # Check if the user-defined process method is async
            if asyncio.iscoroutinefunction(self._processor.process):
                 await self._processor.process(proxy_querier, proxy_updater, dt, *args, **kwargs)
            else:
                 # Run synchronous processor method (Ray actor handles threading if needed)
                 self._processor.process(proxy_querier, proxy_updater, dt, *args, **kwargs)
            print(f"RemoteProcessorWrapper actor {ray.get_runtime_context().get_actor_id()}: Finished {self._processor.__class__.__name__}.process.")
        except Exception as e:
            print(f"!!! ERROR in remote processor {self._processor.__class__.__name__} actor {ray.get_runtime_context().get_actor_id()}: {e}")
            import traceback
            traceback.print_exc()
            # Re-raise the exception so the DAG execution fails
            raise e

        # Return the updates captured by the proxy updater
        return proxy_updater.get_captured_updates()


# --- Ray DAG System Implementation ---

class RayDagSystem(System):
    """
    Orchestrates processor execution using a compiled Ray DAG for high performance.
    """
    def __init__(self, ray_init_args: Optional[dict] = None):
        """
        Initializes the Ray DAG System.

        Args:
            ray_init_args: Optional arguments to pass to `ray.init()`.
                           If None, assumes Ray is already initialized.
        """
        self._processors: Dict[Type[Processor], Processor] = {} # Store instances by type
        self._processor_priorities: Dict[Type[Processor], int] = {} # Store priorities
        self._sorted_processors: List[Processor] = [] # Keep a sorted list

        # DAG related state
        self._remote_actors: Dict[Type[Processor], Any] = {} # Type -> Ray ActorHandle
        self._compiled_dag: Optional[Any] = None # Stores the compiled DAG
        self._dag_built = False

        # Initialize Ray if necessary
        if ray_init_args is not None:
            ray.init(**ray_init_args, ignore_reinit_error=True)
        elif not ray.is_initialized():
            print("RayDagSystem Warning: Ray not initialized. Initializing Ray automatically.")
            ray.init(ignore_reinit_error=True)

        print("RayDagSystem Initialized.")
        print(f"  Ray version: {ray.__version__}")
        print(f"  Ray cluster resources: {ray.available_resources()}")


    def add_processor(self, processor: Processor, priority: Optional[int] = None) -> None:
        """Adds a processor instance and invalidates the current compiled DAG."""
        if not isinstance(processor, Processor):
            raise TypeError("Can only add Processor instances.")

        ptype = type(processor)
        if ptype in self._processors:
            print(f"RayDagSystem Warning: Replacing existing processor of type {ptype.__name__}")

        self._processors[ptype] = processor
        self._processor_priorities[ptype] = priority if priority is not None else processor.priority
        self._sort_processors()

        # Invalidate compiled DAG - it needs to be rebuilt
        self._compiled_dag = None
        self._dag_built = False
        self._remote_actors = {} # Also clear remote actors
        print(f"RayDagSystem: Added processor {ptype.__name__}. DAG invalidated.")

    def remove_processor(self, processor_type: Type[Processor]) -> None:
        """Removes a processor and invalidates the current compiled DAG."""
        if processor_type in self._processors:
            del self._processors[processor_type]
            del self._processor_priorities[processor_type]
            self._sort_processors()

            # Invalidate compiled DAG
            self._compiled_dag = None
            self._dag_built = False
            self._remote_actors = {} # Also clear remote actors
            print(f"RayDagSystem: Removed processor {processor_type.__name__}. DAG invalidated.")
        else:
            print(f"RayDagSystem Warning: Processor type {processor_type.__name__} not found for removal.")


    def get_processor(self, processor_type: Type[Processor]) -> Optional[Processor]:
         """Gets the managed instance of a specific processor type."""
         return self._processors.get(processor_type)

    def _sort_processors(self):
        """Sorts processors based on priority (descending)."""
        self._sorted_processors = sorted(
            self._processors.values(),
            key=lambda p: self._processor_priorities[type(p)],
            reverse=True
        )

    def _build_and_compile_dag(self):
        """Builds the Ray DAG from the current processors and compiles it."""
        print("RayDagSystem: Building and compiling execution DAG...")
        if not self._sorted_processors:
            print("RayDagSystem Warning: No processors added. DAG will be empty.")
            self._dag_built = True
            self._compiled_dag = None # No DAG to execute
            return

        # Create remote actor wrappers if they don't exist
        for processor in self._sorted_processors:
            ptype = type(processor)
            if ptype not in self._remote_actors:
                # TODO: Extract init args/kwargs if processor wasn't default constructible?
                # For now, assume default constructor or pre-initialized instance.
                # Need a way to pass resource requests (gpus etc) from Processor definition?
                # Example: processor_cls = type(processor)
                # resources = getattr(processor_cls, '_ray_resources', {}) # Define convention
                # self._remote_actors[ptype] = _RemoteProcessorWrapper.options(**resources).remote(ptype, (), {})
                self._remote_actors[ptype] = _RemoteProcessorWrapper.remote(ptype, (), {})


        # Build the DAG
        from ray.dag import InputNode, MultiOutputNode

        with InputNode() as input_data:
            # Input data expected to be: (snapshot_dict, schemas_dict, dt, *args, **kwargs) tuple?
            # Or just pass snapshot and dt? Let's refine this.
            # InputNode provides a tuple: (snapshot, schemas, dt_args_kwargs_tuple)
            # Need to unpack dt, *args, **kwargs inside bind? Ray DAGs might pass them directly.
            # Let's assume InputNode provides (snapshot, schemas, dt, *args, **kwargs)

            # Processors run mostly in parallel, reading the same initial snapshot.
            # Dependencies would arise if one processor needed the *output* of another
            # *before* the commit phase, which isn't the standard ECS pattern here.
            output_nodes = []
            for processor in self._sorted_processors:
                ptype = type(processor)
                actor_handle = self._remote_actors[ptype]
                # Bind the process_wrapper method to the input node
                # The arguments provided to dag.execute() will be passed here.
                bound_node = actor_handle.process_wrapper.bind(
                    input_data[0], # snapshot
                    input_data[1], # schemas
                    input_data[2], # dt
                    *input_data[3:] # args, kwargs (check if Ray DAGs handle * expansion)
                    # If * doesn't expand, we might need to pass args/kwargs as a tuple/dict
                )
                output_nodes.append(bound_node)

            # Create a single DAG output node that gathers results from all processors
            dag = MultiOutputNode(output_nodes)

        # Compile the DAG
        try:
            # Enable optimizations based on documentation
            self._compiled_dag = dag.experimental_compile(
                _overlap_gpu_communication=True, # From docs
                enable_asyncio=True # Seems beneficial based on async actor wrapper
            )
            self._dag_built = True
            print("RayDagSystem: DAG built and compiled successfully.")
        except Exception as e:
            print(f"!!! ERROR: RayDagSystem failed to compile DAG: {e}")
            import traceback
            traceback.print_exc()
            self._compiled_dag = None # Ensure it's None if compilation fails
            self._dag_built = False


    def execute(self, querier: EcsQueryInterface, updater: EcsUpdateManager, dt: float, *args: Any, **kwargs: Any) -> None:
        """
        Executes the compiled Ray DAG for one simulation step.

        Args:
            querier: Interface to read the current committed state for snapshotting.
            updater: Interface to queue the final updates collected from the DAG.
            dt: Time delta.
            *args, **kwargs: Additional arguments for processors.
        """
        if not self._dag_built:
            self._build_and_compile_dag()

        if self._compiled_dag is None:
            if not self._processors:
                 print("RayDagSystem Execute: No processors to run.")
                 return # Nothing to do
            else:
                 # This indicates a compilation failure previously
                 raise RuntimeError("RayDagSystem cannot execute because DAG compilation failed.")

        # 1. Create Snapshot of initial state
        print("RayDagSystem Execute: Creating state snapshot...")
        snapshot_start = time.time()
        snapshot: Dict[Type[Component], Optional[DataFrame]] = {}
        schemas: Dict[Type[Component], daft.Schema] = {}
        # Iterate through all *known* component types in the store
        known_types = querier._store._component_data.keys()
        for comp_type in known_types:
            df = querier.get_component(comp_type) # Get committed DF (might be plan)
            schema = querier._store.get_component_schema(comp_type)
            if schema:
                 schemas[comp_type] = schema
            # Collect the DataFrame for the snapshot. This is crucial but potentially costly.
            # Alternatives like passing plans or references are complex with Ray serialization.
            if df is not None:
                 # TODO: Optimization - only collect if the DataFrame reference has changed?
                 # Requires tracking previous references. For now, collect always for simplicity.
                 collected_df = df.collect()
                 if len(collected_df) > 0:
                      snapshot[comp_type] = collected_df
                 else:
                      snapshot[comp_type] = None # Explicitly store None if empty
            else:
                 snapshot[comp_type] = None
        print(f"RayDagSystem Execute: Snapshot created ({(time.time() - snapshot_start):.4f}s).")

        # 2. Execute the Compiled DAG asynchronously
        print(f"RayDagSystem Execute: Submitting data to compiled DAG...")
        dag_execute_start = time.time()
        # Prepare arguments for the DAG's InputNode
        # TODO: Verify how *args, **kwargs are best passed through DAG input
        dag_input = (snapshot, schemas, dt) + args # Combine dt and args
        # Need to handle kwargs separately? Maybe pass as a dict?
        # Let's assume for now that args contains everything needed or handle kwargs later.
        if kwargs:
             print("RayDagSystem Warning: kwargs are not currently passed through the DAG execution. Ignoring.")

        # Use execute_async for compatibility with async actors/methods
        dag_future = self._compiled_dag.execute_async(dag_input)
        print(f"RayDagSystem Execute: DAG submitted ({(time.time() - dag_execute_start):.4f}s). Waiting for results...")

        # 3. Wait for DAG completion and gather results
        dag_gather_start = time.time()
        try:
            # Use asyncio.run to wait for the async execution to complete
            # This might block if called from a non-async context, which is expected here.
            # Consider using ray.get if execute_async isn't strictly needed?
            # Let's stick with execute_async and await properly.
            # We need an event loop to await the future.
            async def wait_for_dag():
                return await dag_future

            # Run the async wait function in the current event loop or a new one
            # results = asyncio.run(wait_for_dag()) # Simplest for now
            # Alternative if already in an event loop:
            results = ray.get(dag_future) # ray.get can handle awaitables

            print(f"RayDagSystem Execute: DAG results received ({(time.time() - dag_gather_start):.4f}s).")

        except Exception as e:
            print(f"!!! ERROR during Ray DAG execution or result gathering: {e}")
            # Error might be ActorDiedError, RayTaskError, etc.
            import traceback
            traceback.print_exc()
            # Don't proceed to queue updates if DAG failed
            raise RuntimeError("Ray DAG execution failed.") from e


        # 4. Queue gathered updates via the main UpdateManager
        print("RayDagSystem Execute: Queueing updates from DAG results...")
        queue_start = time.time()
        total_updates_queued = 0
        # Results should be a list corresponding to the MultiOutputNode outputs
        if isinstance(results, list):
            for processor_result in results:
                # Each result is a Dict[Type[Component], List[DataFrame]]
                if isinstance(processor_result, dict):
                    for comp_type, update_df_list in processor_result.items():
                        for update_df in update_df_list:
                            if update_df is not None: # Ensure DF is not None
                                 updater.add_update(comp_type, update_df)
                                 total_updates_queued += 1 # Count individual DFs queued
                else:
                    print(f"RayDagSystem Warning: Unexpected result type from processor node: {type(processor_result)}. Expected dict.")
        else:
             print(f"RayDagSystem Warning: Unexpected result type from MultiOutputNode: {type(results)}. Expected list.")

        print(f"RayDagSystem Execute: Queued {total_updates_queued} update DataFrames from {len(results or [])} processors ({(time.time() - queue_start):.4f}s).")
