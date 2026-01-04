# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Vectorized Services Layer
==========================

Provides batched, vectorized services that agents can request asynchronously.
Services leverage Archetype's DataFrame engine for efficient batch processing.

Services include:
- LLM inference (batched prompt processing)
- Embedding generation
- Custom vectorized operations
"""

from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Any

import daft


class VectorizedService(ABC):
    """
    Base class for vectorized services.
    
    Services process batches of requests efficiently using DataFrames.
    """
    
    @abstractmethod
    async def process_batch(
        self,
        requests: list[dict[str, Any]],
    ) -> list[Any]:
        """
        Process a batch of requests.
        
        Args:
            requests: List of request dicts, each with request data
            
        Returns:
            List of responses, same length and order as requests
        """
        pass
    
    def get_name(self) -> str:
        """Get service name."""
        return self.__class__.__name__


class InferenceService(VectorizedService):
    """
    Batched LLM inference service.
    
    Leverages Daft's prompt() function for efficient batching.
    """
    
    def __init__(self, default_model: str = "gpt-4o-mini"):
        self.default_model = default_model
    
    async def process_batch(
        self,
        requests: list[dict[str, Any]],
    ) -> list[Any]:
        """
        Process batch of inference requests.
        
        Each request should have:
        - prompt: str
        - model: str (optional)
        - max_tokens: int (optional)
        - temperature: float (optional)
        """
        if not requests:
            return []
        
        # Build DataFrame from requests
        data = {
            "request_id": [i for i in range(len(requests))],
            "prompt": [r.get("prompt", "") for r in requests],
            "model": [r.get("model", self.default_model) for r in requests],
            "max_tokens": [r.get("max_tokens", 300) for r in requests],
            "temperature": [r.get("temperature", 0.7) for r in requests],
        }
        
        df = daft.from_pydict(data)
        
        # Use Daft's prompt function for batched inference
        try:
            from daft.functions import prompt
            
            df = df.with_column(
                "response",
                prompt(
                    daft.col("prompt"),
                    model=daft.col("model"),
                    max_tokens=daft.col("max_tokens"),
                    temperature=daft.col("temperature"),
                ),
            )
            
            # Collect results
            results = df.select("request_id", "response").collect()
            responses = results.to_pydict()
            
            # Return in original order
            response_map = dict(zip(responses["request_id"], responses["response"]))
            return [response_map.get(i, "") for i in range(len(requests))]
            
        except Exception as e:
            # Fallback to sequential if batching fails
            print(f"Batch inference failed, falling back to sequential: {e}")
            return await self._process_sequential(requests)
    
    async def _process_sequential(self, requests: list[dict[str, Any]]) -> list[Any]:
        """Fallback sequential processing."""
        import openai
        
        client = openai.AsyncOpenAI()
        results = []
        
        for req in requests:
            try:
                response = await client.chat.completions.create(
                    model=req.get("model", self.default_model),
                    messages=[{"role": "user", "content": req.get("prompt", "")}],
                    max_tokens=req.get("max_tokens", 300),
                    temperature=req.get("temperature", 0.7),
                )
                results.append(response.choices[0].message.content)
            except Exception as e:
                print(f"Inference request failed: {e}")
                results.append("")
        
        return results


class EmbeddingService(VectorizedService):
    """
    Batched embedding generation service.
    """
    
    def __init__(self, model: str = "text-embedding-3-small"):
        self.model = model
    
    async def process_batch(
        self,
        requests: list[dict[str, Any]],
    ) -> list[Any]:
        """
        Process batch of embedding requests.
        
        Each request should have:
        - text: str
        """
        if not requests:
            return []
        
        import openai
        
        client = openai.AsyncOpenAI()
        
        # Extract texts
        texts = [r.get("text", "") for r in requests]
        
        try:
            # Batch embedding request
            response = await client.embeddings.create(
                model=self.model,
                input=texts,
            )
            
            # Extract embeddings
            embeddings = [item.embedding for item in response.data]
            return embeddings
            
        except Exception as e:
            print(f"Embedding request failed: {e}")
            return [[]] * len(requests)


class ServiceBatcher:
    """
    Manages request batching for a vectorized service.
    
    Collects requests within a time window and processes them as a batch.
    """
    
    def __init__(
        self,
        service: VectorizedService,
        batch_window_ms: int = 50,
        max_batch_size: int = 100,
    ):
        """
        Initialize service batcher.
        
        Args:
            service: The vectorized service to batch requests for
            batch_window_ms: Time window to collect requests (milliseconds)
            max_batch_size: Maximum batch size (triggers early processing)
        """
        self.service = service
        self.batch_window_ms = batch_window_ms
        self.max_batch_size = max_batch_size
        
        # Request queue: (agent_id, request_data, future)
        self._pending: list[tuple[str, dict[str, Any], asyncio.Future]] = []
        self._lock = asyncio.Lock()
        self._last_batch_time = time.time()
        
        # Metrics
        self._total_requests = 0
        self._total_batches = 0
        self._batch_sizes: list[int] = []
    
    async def request(self, agent_id: str, request_data: dict[str, Any]) -> Any:
        """
        Submit a request from an agent.
        
        Args:
            agent_id: Requesting agent ID
            request_data: Request payload
            
        Returns:
            Service response (awaits batch processing)
        """
        future = asyncio.Future()
        
        async with self._lock:
            self._pending.append((agent_id, request_data, future))
            self._total_requests += 1
            
            # Trigger early batch if max size reached
            if len(self._pending) >= self.max_batch_size:
                asyncio.create_task(self._process_now())
        
        return await future
    
    async def process_batch(self) -> None:
        """
        Process pending requests if batch window has elapsed.
        """
        async with self._lock:
            if not self._pending:
                return
            
            # Check if window has elapsed
            elapsed_ms = (time.time() - self._last_batch_time) * 1000
            if elapsed_ms < self.batch_window_ms:
                return
            
            await self._process_now()
    
    async def _process_now(self) -> None:
        """Process pending requests immediately (must hold lock)."""
        if not self._pending:
            return
        
        # Take all pending requests
        batch = self._pending[:]
        self._pending.clear()
        self._last_batch_time = time.time()
        
        # Update metrics
        self._total_batches += 1
        self._batch_sizes.append(len(batch))
        
        # Extract request data
        requests = [req_data for _, req_data, _ in batch]
        
        try:
            # Process batch via service
            responses = await self.service.process_batch(requests)
            
            # Resolve futures
            for i, (_, _, future) in enumerate(batch):
                if not future.done():
                    future.set_result(responses[i] if i < len(responses) else None)
                    
        except Exception as e:
            # Fail all futures
            for _, _, future in batch:
                if not future.done():
                    future.set_exception(e)
    
    def get_metrics(self) -> dict[str, Any]:
        """Get batching metrics."""
        avg_batch_size = (
            sum(self._batch_sizes) / len(self._batch_sizes)
            if self._batch_sizes
            else 0
        )
        
        return {
            "total_requests": self._total_requests,
            "total_batches": self._total_batches,
            "avg_batch_size": avg_batch_size,
            "pending": len(self._pending),
        }


__all__ = [
    "VectorizedService",
    "InferenceService",
    "EmbeddingService",
    "ServiceBatcher",
]
