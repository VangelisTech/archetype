# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for Ray actor agent abstraction."""

import asyncio
import json

import pytest

from archetype.core.component import Component


class TestComponent(Component):
    """Test component."""
    name: str = ""
    value: int = 0


@pytest.mark.asyncio
async def test_ray_agent_initialization():
    """Test RayAgent initialization without Ray."""
    from archetype.ray import RayAgent
    
    # Create agent without Ray
    agent = RayAgent(
        agent_id="test_agent",
        world_ref=None,
        components={"test": TestComponent(name="test", value=42)},
    )
    
    assert agent.agent_id == "test_agent"
    assert len(agent.components) == 1
    assert agent.state == {}
    
    await agent.initialize()


@pytest.mark.asyncio
async def test_ray_agent_component_access():
    """Test component access methods."""
    from archetype.ray import RayAgent
    
    agent = RayAgent("test_agent")
    
    # Set component
    comp = TestComponent(name="test", value=42)
    agent.set_component("test", comp)
    
    # Get component
    retrieved = agent.get_component("test")
    assert retrieved is not None
    assert retrieved.name == "test"
    assert retrieved.value == 42
    
    # Get non-existent
    assert agent.get_component("nonexistent") is None


@pytest.mark.asyncio
async def test_ray_agent_state_serialization():
    """Test state conversion to dict."""
    from archetype.ray import RayAgent
    
    agent = RayAgent("test_agent")
    agent.state = {"key": "value", "count": 42}
    agent.set_component("test", TestComponent(name="test", value=100))
    
    # Component to dict conversion
    comp_dict = agent._component_to_dict(agent.get_component("test"))
    assert "name" in comp_dict
    assert "value" in comp_dict
    assert comp_dict["name"] == "test"
    assert comp_dict["value"] == 100


@pytest.mark.asyncio
async def test_ray_agent_message_receive():
    """Test message receiving."""
    from archetype.ray import RayAgent
    
    class TestAgent(RayAgent):
        async def receive_message(self, sender_id, content, metadata=None):
            self.state["last_message"] = {
                "sender": sender_id,
                "content": content,
                "metadata": metadata,
            }
    
    agent = TestAgent("test_agent")
    await agent.receive_message("sender_1", "Hello", {"type": "greeting"})
    
    assert "last_message" in agent.state
    assert agent.state["last_message"]["sender"] == "sender_1"
    assert agent.state["last_message"]["content"] == "Hello"


@pytest.mark.asyncio
async def test_service_batcher_basic():
    """Test service batcher without Ray."""
    from archetype.ray.services import ServiceBatcher, VectorizedService
    
    class TestService(VectorizedService):
        async def process_batch(self, requests):
            # Echo service - just return request data
            return [req.get("data", "") for req in requests]
    
    service = TestService()
    batcher = ServiceBatcher(service, batch_window_ms=10, max_batch_size=5)
    
    # Submit requests
    tasks = [
        batcher.request("agent_1", {"data": "request_1"}),
        batcher.request("agent_2", {"data": "request_2"}),
        batcher.request("agent_3", {"data": "request_3"}),
    ]
    
    # Process batch
    await batcher.process_batch()
    
    # Wait for results
    results = await asyncio.gather(*tasks)
    
    assert len(results) == 3
    assert "request_1" in results
    assert "request_2" in results
    assert "request_3" in results
    
    # Check metrics
    metrics = batcher.get_metrics()
    assert metrics["total_requests"] == 3
    assert metrics["total_batches"] >= 1


@pytest.mark.asyncio
async def test_service_batcher_max_batch_size():
    """Test that max batch size triggers early processing."""
    from archetype.ray.services import ServiceBatcher, VectorizedService
    
    class CountService(VectorizedService):
        def __init__(self):
            self.call_count = 0
        
        async def process_batch(self, requests):
            self.call_count += 1
            return list(range(len(requests)))
    
    service = CountService()
    batcher = ServiceBatcher(service, batch_window_ms=1000, max_batch_size=3)
    
    # Submit 6 requests (should trigger 2 batches)
    tasks = [
        batcher.request(f"agent_{i}", {"data": i})
        for i in range(6)
    ]
    
    # Small wait to allow batches to process
    await asyncio.sleep(0.1)
    
    results = await asyncio.gather(*tasks)
    assert len(results) == 6
    
    # Should have triggered at least 2 batches due to max size
    metrics = batcher.get_metrics()
    assert metrics["total_batches"] >= 2


@pytest.mark.asyncio
async def test_ray_world_initialization(tmp_path):
    """Test RayAgentWorld initialization."""
    from archetype.ray import RayAgentWorld
    
    storage_path = tmp_path / "test_world"
    
    async with RayAgentWorld("test_world", storage=storage_path) as world:
        assert world.name == "test_world"
        assert world._orchestrator is not None
        assert world._ecs_world is not None
        
        # Test metrics
        metrics = world.get_metrics()
        assert metrics["agent_count"] == 0
        assert "services" in metrics


@pytest.mark.asyncio
async def test_ray_world_service_registration(tmp_path):
    """Test service registration."""
    from archetype.ray import RayAgentWorld
    from archetype.ray.services import VectorizedService
    
    class DummyService(VectorizedService):
        async def process_batch(self, requests):
            return ["response"] * len(requests)
    
    storage_path = tmp_path / "test_world"
    
    async with RayAgentWorld("test_world", storage=storage_path) as world:
        # Register service
        world.register_service("dummy", DummyService())
        
        assert "dummy" in world._services
        assert "dummy" in world._batchers
        
        # Request service
        result = await world.request_service("dummy", "agent_1", {"test": "data"})
        assert result == "response"


@pytest.mark.asyncio
async def test_ray_world_agent_state_update(tmp_path):
    """Test agent state updates."""
    from archetype.ray import RayAgentWorld
    
    storage_path = tmp_path / "test_world"
    
    async with RayAgentWorld("test_world", storage=storage_path) as world:
        # Update agent state
        state_data = {
            "agent_id": "agent_1",
            "state_json": json.dumps({"key": "value"}),
            "components": {},
        }
        
        await world.update_agent_state("agent_1", state_data)
        
        # Query states
        states = world.query_agent_states()
        assert len(states) == 1
        assert states[0]["agent_id"] == "agent_1"


@pytest.mark.asyncio
async def test_vectorized_service_interface():
    """Test VectorizedService interface."""
    from archetype.ray.services import VectorizedService
    
    class TestService(VectorizedService):
        async def process_batch(self, requests):
            return [f"processed_{i}" for i in range(len(requests))]
    
    service = TestService()
    
    requests = [{"data": i} for i in range(5)]
    results = await service.process_batch(requests)
    
    assert len(results) == 5
    assert results[0] == "processed_0"
    assert results[4] == "processed_4"
