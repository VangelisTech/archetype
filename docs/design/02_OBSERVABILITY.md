# 02: Observability

This document details Archetype's strategy for observability, covering durable logging, metrics, and state management. The goal is to ensure that every action is auditable, every state is reproducible, and the system's health is transparent.

## 1. Core Principles

- **Log First, Act Second**: Every accepted command is written to a durable log *before* it is placed in the in-memory queue for processing.
- **Immutable History**: The command log is an append-only, immutable record of all actions taken in the simulation.
- **Reproducibility**: Given the same initial state and the same command log, the simulation must produce the exact same final state.
- **Actionable Metrics**: The system should emit metrics that provide clear insights into performance, throughput, and potential bottlenecks.

## 2. Durable Logging with a Parquet Buffer

The broker is responsible for durable logging. To balance performance and durability, we use an in-memory buffer that is periodically flushed to a Parquet file.

### 2.1. The Parquet Buffer

-   **In-Memory Buffer**: The `AsyncCommandQueue` will hold a list of Arrow `RecordBatch` objects in memory (`self._parquet_buffer`). Each `RecordBatch` represents a single `Command`.
-   **Arrow Serialization**: The `Command` model includes a `to_arrow()` method that serializes the command into a `RecordBatch` with a schema optimized for storage and querying.

### 2.2. Flush Logic

The buffer is flushed to disk under two conditions, whichever comes first:

1.  **Buffer Size Threshold**: When the number of commands in the buffer reaches a certain size (e.g., 10,000 rows).
2.  **Time Threshold**: If a certain amount of time has passed since the last flush (e.g., 1 second).

This batching strategy amortizes the cost of disk I/O and creates well-sized Parquet row groups, which is optimal for query performance.

**File Size Optimization**: While a specific file size like 512MB is a good target for data warehousing, for our command log, optimizing for latency and frequent, small writes is more important. The flush logic should be tuned to balance this trade-off. We can start with a smaller threshold (e.g., 1-5MB per flush) and adjust as needed. The use of `lancedb`'s optimization features can be explored for managing the resulting Parquet files.

### 2.3. Log Storage

-   **File Naming**: Log files will be named by date (e.g., `command_log/YYYY-MM-DD.parquet`).
-   **Partitioning**: For larger-scale deployments, we can partition the Parquet dataset by `world_id` and `run_id` to improve query performance.

## 3. State Snapshotting and Replays

Durable logs enable powerful state management capabilities.

### 3.1. State Snapshots

-   **Mechanism**: At regular intervals (e.g., every 1,000 ticks), the `World` will serialize its complete state to a durable format (e.g., a LanceDB table).
-   **Trigger**: This can be triggered by a special command or a configuration setting in the `World`.

### 3.2. Hydration and Replays

-   **Hydration**: To restore a world to a specific state, we load the most recent snapshot before the target tick.
-   **Replay**: After loading the snapshot, we read the command log from that point forward and apply the commands sequentially to reach the desired state. This guarantees state consistency.

## 4. Metrics and Tracing

We will use a standard metrics library (e.g., Prometheus) to expose key performance indicators.

### 4.1. Key Metrics

-   `commands_enqueued_total`: Counter for commands entering the broker.
-   `commands_rejected_total`: Counter for commands rejected by the guardrails.
-   `command_queue_depth`: Gauge for the current size of the in-memory heap.
-   `tick_duration_seconds`: Histogram of the time it takes to process a single tick.
-   `parquet_flush_duration_seconds`: Histogram of the time it takes to flush the Parquet buffer.

### 4.2. Tracing (Future Vision)

For more detailed debugging, we can integrate a distributed tracing system (e.g., OpenTelemetry). This would allow us to trace a command's lifecycle from the API request, through the broker, to its execution in the `World`.

## 5. Near-Term Implementation Plan

1.  **Refine `AsyncCommandQueue`**:
    -   Implement the flush logic based on both size and time thresholds.
    -   Add metrics for buffer size, flush duration, and commands processed.
2.  **Implement State Snapshotting**:
    -   Add a `snapshot()` method to the `AsyncWorld` that saves the current state of all archetypes.
    -   Add a corresponding `load_snapshot()` method.
3.  **Develop a Replay Utility**:
    -   Create a script or function that can take a world, a snapshot, and a command log, and replay the simulation to a specific tick.
