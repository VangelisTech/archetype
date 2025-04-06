from dependency_injector import containers, providers




class WorldContainer(containers.DeclarativeContainer):
    config = providers.Configuration()

    store = providers.Singleton(ComponentStore)

    query_interface = providers.Singleton(EcsQueryInterface, store=store)

    update_manager = providers.Singleton(EcsUpdateManager, store=store)


 
class SystemContainer(containers.DeclarativeContainer):
    config = providers.Configuration()

    world = providers.Dependency(WorldContainer)

    

class ProcessorContainer(containers.DeclarativeContainer):
    config = providers.Configuration()

    world = providers.Dependency(World)



    
class AppContainer(containers.DeclarativeContainer):
    config = providers.Configuration()

    world = providers.Singleton(World, config=config)

    world_container = providers.Container(WorldContainer, config=config)

    system_container = providers.Container(SystemContainer, config=config)

    processor_container = providers.Container(ProcessorContainer, config=config)

def set_daft_execution_config(
    config: Optional[PyDaftExecutionConfig] = None,
    # Performance tuning parameters
    scan_tasks_min_size_bytes: Optional[int] = None,
    scan_tasks_max_size_bytes: Optional[int] = None,
    max_sources_per_scan_task: Optional[int] = None,
    
    # Join optimization parameters
    broadcast_join_size_bytes_threshold: Optional[int] = None,
    sort_merge_join_sort_with_aligned_boundaries: Optional[bool] = None,
    hash_join_partition_size_leniency: Optional[float] = None,
    
    # Sorting and sampling parameters
    sample_size_for_sort: Optional[int] = None,
    num_preview_rows: Optional[int] = None,
    
    # File format parameters
    parquet_split_row_groups_max_files: Optional[int] = None,
    parquet_target_filesize: Optional[int] = None,
    parquet_target_row_group_size: Optional[int] = None,
    parquet_inflation_factor: Optional[float] = None,
    csv_target_filesize: Optional[int] = None,
    csv_inflation_factor: Optional[float] = None,
    
    # Aggregation parameters
    shuffle_aggregation_default_partitions: Optional[int] = None,
    partial_aggregation_threshold: Optional[int] = None,
    high_cardinality_aggregation_threshold: Optional[float] = None,
    
    # SQL and execution parameters
    read_sql_partition_size_bytes: Optional[int] = None,
    enable_aqe: Optional[bool] = None,
    enable_native_executor: Optional[bool] = None,
    default_morsel_size: Optional[int] = None,
    
    # Shuffle parameters
    shuffle_algorithm: Optional[str] = None,
    pre_shuffle_merge_threshold: Optional[int] = None,
    flight_shuffle_dirs: Optional[list[str]] = None,
    
    # Advanced features
    enable_ray_tracing: Optional[bool] = None,
    scantask_splitting_level: Optional[int] = None
) -> DaftContext:
    """
    Configure Daft execution parameters for optimized performance.
    
    Returns:
        DaftContext: The configured Daft execution context
    """
    # Implementation to be added
    pass


