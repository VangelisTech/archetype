# 10: LLM UDF Integration

This document details the integration of LLM utilities and UDF helpers in
Archetype, focusing on runtime abstraction.

## 1. Core Principles

• Runtime Separation: LLM features in runtime layer, not core simulation.
• Concurrency Guards: Per-worker semaphores for safe scaling.
• Ray Native: Optional deployment actors for production.
• Processor Focus: UDF helpers exposed directly to processors.

## 2. Folder Structure

archetype/runtime/udf/
├── client.py         # Thin async LLM wrappers
└── llm.py           # Ray deployment actor (optional)

## 3. Key Components

### 3.1. UDF Helpers (client.py)

import asyncio
import os

_CONCURRENCY = int(os.getenv("ARCHETYPE_LLM_CONCURRENCY", "20"))

_sem = asyncio.Semaphore(_CONCURRENCY) if asyncio.current_task() else None

async def call_llm(messages, model: str):
    async with _sem:
        return await _real_llm_call(messages, model)

• parallel_llm: Batch version for lists.

### 3.2. Ray Deployment (llm.py)

@ray.remote(max_concurrency=20)
class LangChainActor:
    async def generate(self, msgs):
        ...

## 4. Integration

• Processors: Import and use via @processor.udf("llm").
• Runtime: Handles Ray/Thread modes transparently.
• Env Control: Tune concurrency via environment variables.

## 5. Relation to Other Designs

• Complements 05_LLM_INTEGRATION.md with runtime specifics.
• Uses 09-execution-runtime-strategy.md for Executor abstraction.

## 6. Near-Term Implementation Plan

1. Create udf/client.py with guarded wrappers.
2. Implement optional Ray actor in udf/llm.py.
3. Expose to processors and test concurrency.
4. Add env-based configuration.
