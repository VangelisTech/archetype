# 05: LLM Integration

This document outlines the strategy for integrating Large Language Models (LLMs) into Archetype, focusing on scalability, performance, and cost management.

## 1. Core Principles

-   **LLMs as Processors**: LLMs can be integrated as a special type of `AsyncProcessor`, allowing them to operate on archetypes like any other processor.
-   **Inference as a Bottleneck**: We acknowledge that LLM inference is the most significant performance and cost bottleneck in the system.
-   **Decoupled Inference**: The core simulation should not be directly coupled to a specific LLM provider or inference engine. We will use a dedicated service to handle LLM inference.

## 2. The `LLMRouter`

The `LLMRouter` is a dedicated Ray Serve application responsible for handling all LLM inference requests from the simulation.

### 2.1. Architecture

-   **Ray Serve**: We will use Ray Serve to deploy and manage the `LLMRouter`. This provides scalability, fault tolerance, and the ability to manage multiple LLM models and replicas.
-   **vLLM**: We will use vLLM as the core inference engine. vLLM provides high-throughput, low-latency inference with features like paged attention and continuous batching.
-   **Directory Structure**: The `LLMRouter` code will live in `src/archetype/infra/ray/llm_router.py`.

### 2.2. Request Flow

1.  An `LLMProcessor` within the simulation needs to perform inference.
2.  It sends a request to the `LLMRouter` via a Ray Serve handle.
3.  The `LLMRouter` receives the request and forwards it to the appropriate vLLM replica.
4.  vLLM processes the request (potentially batching it with others) and returns the result.
5.  The `LLMRouter` returns the result to the `LLMProcessor`.

## 3. The `LLMProcessor`

The `LLMProcessor` is a specialized `AsyncProcessor` that knows how to interact with the `LLMRouter`.

```python
from .async_processor import AsyncProcessor
from .async_interfaces import iBroker
from daft import DataFrame
import asyncio

class LLMProcessor(AsyncProcessor):
    def __init__(self, llm_router_handle, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.llm_router = llm_router_handle

    async def process(self, df: DataFrame, broker: "iBroker", semaphore: asyncio.Semaphore, *args, **kwargs) -> DataFrame:
        # 1. Construct prompts from the dataframe
        prompts = self._construct_prompts(df)

        # 2. Send prompts to the LLMRouter for inference
        results = await self.llm_router.batch_generate.remote(prompts)

        # 3. Process the results and potentially enqueue new commands
        await self._process_results(results, broker)

        return df
```

## 4. Cost and Performance Optimization

### 4.1. Batching

The `LLMRouter` will use vLLM's continuous batching to maximize GPU utilization. The `LLMProcessor` should be designed to send batches of prompts whenever possible.

### 4.2. Caching

We can implement a caching layer (e.g., using Redis) to store the results of common prompts, reducing redundant inference requests.

### 4.3. Model Tiers

The `LLMRouter` can be configured to use different models for different tasks. For example, a smaller, faster model could be used for simple tasks, while a larger, more powerful model could be reserved for more complex reasoning.

### 4.4. Token Budgets

The budget guardrails in the broker (as defined in `01_SECURITY_AND_AUTH.md`) will be crucial for controlling LLM costs. The `LLMProcessor` will need to estimate the token cost of its requests and ensure they are within the actor's budget.

## 5. Near-Term Implementation Plan

1.  **Create `src/archetype/infra/ray/llm_router.py`**: Implement a basic `LLMRouter` using Ray Serve and vLLM.
2.  **Create a sample `LLMProcessor`**: This processor will demonstrate how to interact with the `LLMRouter`.
3.  **Develop a benchmark** to measure the performance and cost of the LLM integration.
4.  **Integrate token budget checks** into the `LLMProcessor` and the broker's guardrails.
