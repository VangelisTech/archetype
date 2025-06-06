#!/usr/bin/env python3

import sys
import os
import asyncio
import time

# Ensure the parent directory is in sys.path so 'archetype' can be imported
# notebook_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
# project_root = os.path.abspath(os.path.join(notebook_dir, "..", "src"))
# if project_root not in sys.path:
#     sys.path.insert(0, project_root)
import daft
from daft import DataFrame, col, udf
from daft.functions import llm_generate
from openai import AsyncOpenAI

from archetype.core import processor, Component
from archetype.core.aio import make_async_world, AsyncProcessor
from archetype.utils.llm import OpenAIClientFactory, LLMProvider

# Define Components
class CompletionConfig(Component):
    provider: str 
    model: str 

    extra_body: dict
    response: str
    temperature: float
    max_tokens: int = 1024
    stop: list[str]
    stream: bool  = True
    response_format: dict
    seed: int


class Content(Component):
    messages: list[dict]

@daft.udf(return_dtype=daft.DataType.string())
async def oai_client_udf(self, provider, model, messages, *args, **kwargs):
    client_factory = OpenAIClientFactory()
    client = await client_factory(provider, *args, **kwargs)

    if provider == "OpenAI":
        response =  await client.chat.completions.create(
            model=model,
            messages=messages,
        )
    return response.choices[0].message.content

def oai_client_udf_wrapper(model, messages, extra_body):
    cls = oai_client_udf
    return udf(model, messages, extra_body)(cls).with_init_args(
        provider="OpenAI"
    ) 

@processor(CompletionConfig, Content, priority=1)
class OpenAIClientChatProcessor(AsyncProcessor):
    async def process(self, df: DataFrame, semaphore: asyncio.Semaphore, **kwargs) -> DataFrame:
        
        async with semaphore:
            return df.with_column("response", oai_client_udf(model="gpt-4.1-nano-2025-04-14"
                

    



async def main():
    """
    Async ECS simulation demo.
    
    Demonstrates real-time physics simulation with:
    - Entity Component System architecture
    - Daft DataFrames for processing 
    - LanceDB for persistent storage
    - Async processing with temporal coordination
    """
    
    print("🚀 Async Archetype ECS Engine Demo")
    print("=" * 45)
    print("📊 Daft DataFrames + LanceDB + AsyncIO")
    print()
    
    uri = "/Users/everett-founder/git/vangelis/internal/work/libs/archetype/data"

    # Create async world
    async_world = make_async_world(uri, debug=True, max_concurrent_archetypes=10)
    async_world.add_processor(AsyncMovementProcessor())
    
    # Spawn entities for physics simulation
    print("🎯 Spawning 5 entities with Position + Velocity components...")
    async_world.spawn()
    async_world.spawn(Position(x=2, y=2), Velocity(vx=2, vy=2))
    async_world.spawn(Position(x=3, y=3), Velocity(vx=3, vy=3))
    async_world.spawn(Position(x=4, y=4), Velocity(vx=4, vy=4))
    async_world.spawn(Position(x=5, y=5), Velocity(vx=5, vy=5))
    
    print("⚡ Running 10-step physics simulation (dt=0.1)...")
    print("   Each step: Query → Process → Update → Persist")
    print()
        
    start = time.time()
    for i in range(10):
        await async_world.step(dt=0.1)
    elapsed = time.time() - start
    
    print(f"Simulation completed in {elapsed:.3f}s")



if __name__ == "__main__":
    asyncio.run(main())