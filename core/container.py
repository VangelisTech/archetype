# Import concrete implementations
from .store import ComponentStore
from .managers import QueryManager, UpdateManager
from .systems import SequentialSystem
from .world import World

# Import the interface being implemented/overridden
from .container_interface import CoreContainerInterface

from dependency_injector import containers, providers

import daft
import ray

# Override the interface container
@containers.override(CoreContainerInterface)
class CoreContainer(containers.DeclarativeContainer):
    config = providers.Configuration()

    store = providers.Singleton(ComponentStore) # Stateful

    query_interface = providers.Factory(QueryManager) # Stateless

    update_manager = providers.Factory(UpdateManager) # Stateless

    system = providers.Singleton(SequentialSystem) # Stateful
    
    world = providers.Singleton(World)
    


class DaftContainer(containers.DeclarativeContainer):
    config = providers.Configuration()

    # Daft
    s3_credentials = providers.Factory(
        daft.io.S3Credentials,
        key_id=config.aws.s3.key_id,
        access_key=config.aws.s3.access_key,
        session_token=config.aws.s3.session_token,
        expiry=config.aws.s3.expiry,
    )
    s3_config = providers.Factory(
        daft.io.S3Config,
        region_name=config.aws.s3.region_name,
        endpoint_url=config.aws.s3.endpoint_url,
        key_id=config.aws.s3.key_id,
        session_token=config.aws.s3.session_token,
        access_key=config.aws.s3.access_key,
        credentials_provider=s3_credentials,
        buffer_time=config.aws.s3.buffer_time,
        max_connections=config.aws.s3.max_connections,
        retry_initial_backoff_ms=config.aws.s3.retry_initial_backoff_ms,
        connect_timeout_ms=config.aws.s3.connect_timeout_ms,
        read_timeout_ms=config.aws.s3.read_timeout_ms,
        num_tries=config.aws.s3.num_tries,
        retry_mode=config.aws.s3.retry_mode,
        anonymous=config.aws.s3.anonymous,
        use_ssl=config.aws.s3.use_ssl,
        verify_ssl=config.aws.s3.verify_ssl,
        check_hostname_ssl=config.aws.s3.check_hostname_ssl,
        requester_pays=config.aws.s3.requester_pays,
        force_virtual_addressing=config.aws.s3.force_virtual_addressing,
        profile_name=config.aws.s3.profile_name,
    )
    gcs_config = providers.Factory(
        daft.io.GCSConfig,
        project_id=config.gcp.gcs.project_id,
        credentials=config.gcp.gcs.credentials,
        token=config.gcp.gcs.token,
        anonymous=config.gcp.gcs.anonymous,
        max_connections=config.gcp.gcs.max_connections,
        retry_initial_backoff_ms=config.gcp.gcs.retry_initial_backoff_ms,
        connect_timeout_ms=config.gcp.gcs.connect_timeout_ms,
        read_timeout_ms=config.gcp.gcs.read_timeout_ms,
        num_tries=config.gcp.gcs.num_tries,
    )
    azure_blob_config = providers.Factory(
        daft.io.AzureConfig,
        storage_account=config.azure.storage_account,
        access_key=config.azure.access_key,
        sas_token=config.azure.sas_token,
        bearer_token=config.azure.bearer_token,
        tenant_id=config.azure.tenant_id,
        client_id=config.azure.client_id,
        client_secret=config.azure.client_secret,
        use_fabric_endpoint=config.azure.use_fabric_endpoint,
        anonymous=config.azure.anonymous,
        endpoint_url=config.azure.endpoint_url,
        use_ssl=config.azure.use_ssl,
    )
    http_config = providers.Factory(
        daft.io.HTTPConfig,
        bearer_token=config.daft.io.http.bearer_token
    )

    io_config = providers.Factory(
        daft.io.IOConfig,
        s3=s3_config,
        gcs=gcs_config,
        azure=azure_blob_config,
        http=http_config,
    )

    # Set Planning Config
    daft = providers.Factory(
        daft.set_planning_config,
        config=None,
        default_io_config=io_config,
    )

    # Attach to Ray or Native
    if ray.is_initialized():
        daft = providers.Singleton(
            daft.context.set_runner_ray,
            address=ray.get_runtime_context().get_address(),
            noop_if_initialized=True, # If True, skip initialization if Ray is already running.
            max_task_backlog=config.daft.context.max_task_backlog,  # Maximum number of tasks that can be queued. None means Daft will automatically determine a good default.
            force_client_mode=config.daft.context.force_client_mode #  If True, forces Ray to run in client mode.
        )
    else:
        daft = providers.Factory(
            daft.context.set_runner_native,
        )

    
    # Set Execution Config
    daft = providers.Factory(daft.set_execution_config, 
        # Performance tuning parameters
        scan_tasks_min_size_bytes=config.daft.execution.scan_tasks_min_size_bytes,
        scan_tasks_max_size_bytes=config.daft.execution.scan_tasks_max_size_bytes,
        max_sources_per_scan_task=config.daft.execution.max_sources_per_scan_task,
        
        # Join optimization parameters
        broadcast_join_size_bytes_threshold=config.daft.execution.broadcast_join_size_bytes_threshold,
        sort_merge_join_sort_with_aligned_boundaries=config.daft.execution.sort_merge_join_sort_with_aligned_boundaries,
        hash_join_partition_size_leniency=config.daft.execution.hash_join_partition_size_leniency,
        
        # Sorting and sampling parameters
        sample_size_for_sort=config.daft.execution.sample_size_for_sort,
        num_preview_rows=config.daft.execution.num_preview_rows,
        
        # File format parameters    
        parquet_split_row_groups_max_files=config.daft.execution.parquet_split_row_groups_max_files,
        parquet_target_filesize=config.daft.execution.parquet_target_filesize,
        parquet_target_row_group_size=config.daft.execution.parquet_target_row_group_size,
        parquet_inflation_factor=config.daft.execution.parquet_inflation_factor,
        csv_target_filesize=config.daft.execution.csv_target_filesize,
        csv_inflation_factor=config.daft.execution.csv_inflation_factor,
        
        # Aggregation parameters
        shuffle_aggregation_default_partitions=config.daft.execution.shuffle_aggregation_default_partitions,
        partial_aggregation_threshold=config.daft.execution.partial_aggregation_threshold,
        high_cardinality_aggregation_threshold=config.daft.execution.high_cardinality_aggregation_threshold,
        
        # SQL and execution parameters
        read_sql_partition_size_bytes=config.daft.execution.read_sql_partition_size_bytes,
        enable_aqe=config.daft.execution.enable_aqe,
        enable_native_executor=config.daft.execution.enable_native_executor,
        default_morsel_size=config.daft.execution.default_morsel_size,
        
        # Shuffle parameters
        shuffle_algorithm=config.daft.execution.shuffle_algorithm,
        pre_shuffle_merge_threshold=config.daft.execution.pre_shuffle_merge_threshold,
        flight_shuffle_dirs=config.daft.execution.flight_shuffle_dirs,
        
        # Advanced features
        enable_ray_tracing=config.daft.execution.enable_ray_tracing,
        scantask_splitting_level=config.daft.execution.scantask_splitting_level
    )




