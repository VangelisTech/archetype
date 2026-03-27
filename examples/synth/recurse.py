#!/usr/bin/env python3
# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Recursive synthesis engine with parallel world exploration."""

import asyncio
import json
import time
from typing import Dict, Any, List
import typer
from uuid_utils import uuid7

from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from .processors import ClusterProcessor, SpectralClusterProcessor, Segment


app = typer.Typer(help="Recursive synthesis engine")


async def create_world(name: str):
    """Create a new archetype world for synthesis."""
    container = ServiceContainer()
    ctx = ActorCtx(id=uuid7(), roles={"admin"})
    
    world = await container.world_service.create_world(
        WorldConfig(name=name),
        StorageConfig(),
    )
    return world, container, ctx


async def spawn_segments(world, ctx, container, segments: List[str]) -> None:
    """Spawn segment entities in the world."""
    entities = []
    for i, text in enumerate(segments):
        # Generate mock embeddings for demo
        import random
        random.seed(hash(text) % 1000)
        embedding = [random.random() for _ in range(128)]
        
        # Spawn entity via command
        cmd = Command(
            type=CommandType.SPAWN_ENTITY,
            world_id=world.world_id,
            payload={
                "components": {
                    "segment": {
                        "text": text,
                        "embedding": embedding,
                        "cluster_id": None
                    }
                }
            }
        )
        result = await container.command_service.execute(cmd, ctx)
        entities.append(result.payload.get("entity_id"))
    
    return entities


async def run_world_methodology(world, processor, ctx, container, num_steps: int = 5) -> Dict[str, Any]:
    """Run a world with a specific clustering methodology."""
    # Add processor to the system
    await world.system.add_processor(processor)
    
    # Configure run
    config = RunConfig(num_steps=num_steps)
    
    # Run the simulation
    start_time = time.time()
    
    # Run simulation command
    cmd = Command(
        type=CommandType.RUN_SIMULATION,
        world_id=world.world_id,
        payload={"config": config.model_dump()}
    )
    await container.command_service.execute(cmd, ctx)
    
    end_time = time.time()
    
    # Query results - for demo, return mock data
    # In production this would query the world state
    clusters = {
        0: ["Sample segment 1", "Sample segment 2"],
        1: ["Sample segment 3", "Sample segment 4"],
    }
    
    return {
        "processor_type": type(processor).__name__,
        "num_clusters": getattr(processor, "n_clusters", "unknown"),
        "runtime_seconds": end_time - start_time,
        "clusters": clusters,
        "total_segments": 4,  # Mock data
        "clustered_segments": 4
    }


@app.command()
def run_parallel_worlds(
    cycles: int = typer.Option(5, "--cycles", help="Number of recursion cycles"),
    epochs: int = typer.Option(10, "--epochs", help="Training epochs per cycle"),
    output: str = typer.Option("synth_results.json", "--output", help="Output file"),
    segments_file: str = typer.Option(None, "--segments", help="Input segments file")
):
    """Run parallel worlds with different clustering methodologies."""
    asyncio.run(_run_parallel_worlds_async(cycles, epochs, output, segments_file))


async def _run_parallel_worlds_async(
    cycles: int,
    epochs: int,
    output: str,
    segments_file: str
):
    """Run parallel worlds with different clustering methodologies."""
    
    # Sample segments for demo
    if segments_file:
        with open(segments_file) as f:
            segments = [line.strip() for line in f if line.strip()]
    else:
        segments = [
            "This is a sample text segment about machine learning.",
            "Natural language processing enables text understanding.",
            "Deep learning models can process sequential data effectively.",
            "Clustering algorithms group similar data points together.",
            "Embeddings represent text in high-dimensional space.",
            "Similarity metrics help compare document representations.",
            "Feature extraction is crucial for text analysis.",
            "Dimensionality reduction techniques preserve important information.",
            "Vector databases enable efficient similarity search.",
            "Semantic search improves information retrieval accuracy.",
            "Transformer models revolutionized natural language understanding.",
            "Attention mechanisms focus on relevant input parts.",
            "Self-supervised learning reduces annotation requirements.",
            "Large language models demonstrate emergent capabilities.",
            "Fine-tuning adapts models to specific domains.",
            "Knowledge distillation transfers model capabilities efficiently.",
            "Multi-modal learning combines text and visual information.",
            "Reinforcement learning optimizes sequential decision making.",
            "Graph neural networks process structured relationships.",
            "Federated learning enables privacy-preserving training.",
        ]
    
    # Define parallel worlds with different clustering strategies
    worlds_config = {
        "kmeans_k4": ClusterProcessor(n_clusters=4, method="kmeans"),
        "kmeans_k8": ClusterProcessor(n_clusters=8, method="kmeans"),
        "spectral_k4": SpectralClusterProcessor(n_clusters=4, power_iterations=10),
        "spectral_k8": SpectralClusterProcessor(n_clusters=8, power_iterations=10),
    }
    
    all_results = {}
    
    print(f"Running {cycles} cycles with {epochs} epochs each...")
    print(f"Testing {len(worlds_config)} different methodologies on {len(segments)} segments")
    
    for world_name, processor in worlds_config.items():
        print(f"\\nRunning world: {world_name}")
        
        # Create fresh world for this methodology
        world, container, ctx = await create_world(f"synth_{world_name}")
        
        # Spawn entities
        await spawn_segments(world, ctx, container, segments)
        
        # Run with specified cycles (using cycles as num_steps for demo)
        results = await run_world_methodology(world, processor, ctx, container, num_steps=cycles)
        
        print(f"  - Processor: {results['processor_type']}")
        print(f"  - Clusters: {results['num_clusters']}")
        print(f"  - Runtime: {results['runtime_seconds']:.3f}s")
        print(f"  - Segments clustered: {results['clustered_segments']}/{results['total_segments']}")
        
        all_results[world_name] = results
    
    # Save results
    with open(output, 'w') as f:
        json.dump(all_results, f, indent=2)
    
    print(f"\\nResults saved to {output}")
    
    # Summary comparison
    print("\\n=== Performance Summary ===")
    for name, result in all_results.items():
        print(f"{name:12} | {result['runtime_seconds']:.3f}s | "
              f"{result['num_clusters']} clusters | "
              f"{result['clustered_segments']} segments")


@app.command()
def benchmark():
    """Run a quick benchmark of clustering methodologies."""
    print("Running clustering benchmark...")
    asyncio.run(_run_parallel_worlds_async(cycles=3, epochs=5, output="benchmark_results.json", segments_file=None))


if __name__ == "__main__":
    app()