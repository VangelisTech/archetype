# 03: Broker Architecture

This document defines the architecture of the Command Broker, the central component responsible for receiving, ordering, and persisting all state-mutating commands in Archetype.

## 1. Core Responsibilities

The Broker has four primary responsibilities:

1.  **Authentication & Authorization**: It is the gatekeeper. It uses the `guardrail_allow` function to ensure that every command is authorized before being processed.
2.  **Durable Logging**: It logs every accepted command to an immutable, append-only log (e.g., a Parquet file) for auditability and replay.
3.  **Ordering**: It uses an in-memory priority queue (a min-heap) to order commands by tick, priority, and sequence number, ensuring deterministic execution.
4.  **Decoupling**: It decouples the command producers (agents, APIs) from the command consumer (the `World`), allowing them to operate at different rates.

## 2. The `iBroker` Interface

The `iBroker` protocol defines the contract that all broker implementations must adhere to.

```python
from typing import Protocol, List
from uuid import UUID
from .command import Command
from .auth import ActorCtx

class iBroker(Protocol):
    async def enqueue(self, cmd: Command, ctx: ActorCtx) -> None:
        """Enqueues a single command after passing it through the guardrails."""
        ...

    async def enqueue_bulk(self, cmds: List[Command], ctx: ActorCtx) -> None:
        """Enqueues a batch of commands."""
        ...

    async def dequeue_due(self, *, tick: int, limit: int = 1_000) -> List[Command]:
        """Dequeues all commands that are due to be processed for the given tick."""
        ...

    async def ack(self, cmd_ids: List[UUID]) -> None:
        """Acknowledges that a batch of commands has been successfully processed."""
        ...
```

## 3. Broker Implementations

### 3.1. `AsyncCommandQueue` (Current Implementation)

-   **Use Case**: Local development, unit tests, and single-process simulations.
-   **Backend**: A Python `asyncio.Lock` protecting a `heapq` for ordering and a `list` for the Parquet buffer.
-   **Strengths**: No external dependencies, fast, easy to debug.
-   **Limitations**: State is lost on process restart (unless durable logging is fully implemented and used for hydration). Not suitable for multi-process or distributed deployments.

### 3.2. `RayBroker` (Future Vision)

-   **Use Case**: Single-cluster, multi-process deployments on Ray.
-   **Backend**: A Ray actor (`@ray.remote`) that encapsulates the same logic as the `AsyncCommandQueue`.
-   **Architecture**:
    -   A detached Ray actor is created with a well-known name (e.g., "arche-broker").
    -   A local proxy object (`RayBroker`) implements the `iBroker` interface and forwards calls to the remote actor.
    -   This makes the broker a fault-tolerant, cluster-wide singleton.
-   **Strengths**: Enables multi-process communication, survives driver restarts, scales with the Ray cluster.

### 3.3. `KafkaBroker` / `PulsarBroker` (Future Vision)

-   **Use Case**: Multi-cluster, geo-distributed, or enterprise-level deployments requiring the highest level of durability and decoupling.
-   **Backend**: An Apache Kafka or Pulsar topic as the primary command log.
-   **Architecture**:
    -   `enqueue` publishes the command to a Kafka/Pulsar topic.
    -   The `World` runs a consumer that reads from the topic, effectively replacing the `dequeue_due` method.
-   **Strengths**: Extreme durability, native partitioning and scaling, ecosystem of connectors.
-   **Limitations**: Higher operational complexity, requires a separate messaging cluster.

## 4. Real-time Extensions: SSE and WebSockets

To provide real-time feedback to clients (e.g., UIs, agent terminals), the broker can be extended to support event streaming.

-   **Implementation**: This would likely be implemented as a separate service that subscribes to the broker's event stream (or reads the command log) and pushes updates to clients. It would not be part of the core broker logic itself, but rather a consumer of the broker's output.
-   **Server-Sent Events (SSE)**: A simple, efficient protocol for one-way communication (server to client). Ideal for broadcasting world state updates or command acknowledgements.
-   **WebSockets**: A bidirectional protocol, suitable for interactive applications that need to both send commands and receive real-time updates.

## 5. Near-Term Implementation Plan

1.  **Solidify `AsyncCommandQueue`**: Fully implement the durable logging and metrics as outlined in `02_OBSERVABILITY.md`.
2.  **Implement `RayBroker`**: Create the `RayBroker` implementation in `src/archetype/infra/ray/`. This will be the primary mechanism for scaling up simulations.
3.  **Defer Kafka/Pulsar**: The `KafkaBroker` is a long-term vision and will be deferred until there is a clear need for multi-cluster deployments.
4.  **Defer Real-time Extensions**: SSE/WebSocket integration will be deferred until the core API and agent models are more mature.
