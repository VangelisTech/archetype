#!/usr/bin/env python3
# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Ray Actor Agent Example
========================

Demonstrates the Ray actor-based agent abstraction that leverages
Archetype's core engine for vectorized services and state tracking.

This shows how agents run as independent Ray actors with async execution,
while inference and state tracking are vectorized via Archetype.

Run: python examples/ray_agent_example.py
"""

import asyncio
import json
import os

try:
    import ray
    RAY_AVAILABLE = True
except ImportError:
    RAY_AVAILABLE = False
    print("Ray not available. Install with: pip install 'archetype[ray]'")

from archetype.core.component import Component


# =============================================================================
# Components for State Tracking
# =============================================================================


class AgentProfile(Component):
    """Agent profile component."""
    name: str = ""
    role: str = ""
    perspective: str = ""


class ConversationState(Component):
    """Conversation state component."""
    history_json: str = "[]"
    message_count: int = 0
    latest_response: str = ""


# =============================================================================
# Ray Agent Implementation
# =============================================================================


if RAY_AVAILABLE:
    from archetype.ray import RayAgent, RayAgentWorld
    
    @ray.remote
    class ConversationAgent(RayAgent):
        """
        A conversational agent that runs as a Ray actor.
        
        Demonstrates:
        - Async execution as Ray actor
        - Vectorized inference via Archetype
        - State tracking via ECS components
        """
        
        async def initialize(self) -> None:
            """Initialize agent with profile."""
            # Set up components for ECS state tracking
            profile = AgentProfile(
                name=self.agent_id,
                role=self.state.get("role", "participant"),
                perspective=self.state.get("perspective", "neutral"),
            )
            
            conv_state = ConversationState(
                history_json="[]",
                message_count=0,
                latest_response="",
            )
            
            self.set_component("profile", profile)
            self.set_component("conversation", conv_state)
            
            # Sync initial state to ECS
            await self.sync_state()
        
        async def act(self, tick: int, context: dict) -> str:
            """
            Main action: Generate a response in the conversation.
            
            Uses vectorized inference service for LLM calls.
            """
            # Get conversation topic from context
            topic = context.get("topic", "general discussion")
            
            # Build prompt based on role and history
            prompt = self._build_prompt(topic)
            
            # Request inference via vectorized service
            # This is automatically batched with other agents' requests
            response = await self.request_service(
                "inference",
                {
                    "prompt": prompt,
                    "model": "gpt-4o-mini",
                    "max_tokens": 200,
                    "temperature": 0.7,
                },
            )
            
            # Update local state
            self._update_history(response)
            
            # Sync state back to ECS for tracking
            await self.sync_state()
            
            return response
        
        def _build_prompt(self, topic: str) -> str:
            """Build conversation prompt."""
            role = self.state.get("role", "participant")
            perspective = self.state.get("perspective", "neutral")
            history = json.loads(self.state.get("history", "[]"))
            
            prompt = f"""You are a {role} with a {perspective} perspective.
Topic: {topic}

Previous exchanges:
"""
            for entry in history[-3:]:  # Last 3 exchanges
                prompt += f"- {entry}\n"
            
            prompt += f"\nProvide your {perspective} perspective on this topic in 2-3 sentences."
            
            return prompt
        
        def _update_history(self, response: str) -> None:
            """Update conversation history."""
            history = json.loads(self.state.get("history", "[]"))
            history.append(f"{self.agent_id}: {response}")
            
            self.state["history"] = json.dumps(history)
            self.state["message_count"] = len(history)
            self.state["latest_response"] = response
            
            # Update components
            conv = self.get_component("conversation")
            if conv:
                conv.history_json = self.state["history"]
                conv.message_count = self.state["message_count"]
                conv.latest_response = response
        
        async def receive_message(
            self,
            sender_id: str,
            content: str,
            metadata: dict = None,
        ) -> None:
            """Handle incoming message from another agent."""
            history = json.loads(self.state.get("history", "[]"))
            history.append(f"[Message from {sender_id}]: {content}")
            self.state["history"] = json.dumps(history)


# =============================================================================
# Main Simulation
# =============================================================================


async def run_ray_agent_simulation():
    """
    Run a multi-agent conversation using Ray actors.
    
    Demonstrates:
    - Agents as Ray actors
    - Vectorized inference batching
    - State tracking via Archetype ECS
    """
    if not RAY_AVAILABLE:
        print("Ray not available. Exiting.")
        return
    
    from archetype.ray.services import InferenceService
    
    # Initialize Ray
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)
    
    print("=" * 70)
    print("Ray Actor Agent Simulation")
    print("=" * 70)
    
    # Create world for managing Ray agents
    async with RayAgentWorld("conversation_sim") as world:
        # Register vectorized inference service
        world.register_service("inference", InferenceService())
        
        # Define agent roles and perspectives
        agent_configs = [
            {"id": "optimist", "role": "analyst", "perspective": "optimistic"},
            {"id": "realist", "role": "analyst", "perspective": "realistic"},
            {"id": "critic", "role": "analyst", "perspective": "critical"},
            {"id": "synthesizer", "role": "facilitator", "perspective": "integrative"},
        ]
        
        # Create agents as Ray actors
        print("\nCreating Ray actor agents...")
        agents = []
        for config in agent_configs:
            agent = ConversationAgent.remote(
                agent_id=config["id"],
                world_ref=world.get_ref(),
                components=None,
            )
            
            # Initialize with config
            agent.state = {
                "role": config["role"],
                "perspective": config["perspective"],
                "history": "[]",
            }
            
            await agent.initialize.remote()
            agents.append(agent)
            world._agent_refs[config["id"]] = agent
            
            print(f"  ✓ Created {config['id']} ({config['perspective']})")
        
        # Run conversation for multiple rounds
        topic = "The future of AI-native software development"
        print(f"\nTopic: {topic}")
        print("-" * 70)
        
        for round_num in range(3):
            print(f"\n--- Round {round_num + 1} ---\n")
            
            # All agents act in parallel
            # Requests are automatically batched by the service layer
            context = {"topic": topic, "round": round_num}
            results = await world.step(round_num, agents, context)
            
            # Display responses
            for i, (config, result) in enumerate(zip(agent_configs, results)):
                if isinstance(result, Exception):
                    print(f"{config['id']}: [Error: {result}]")
                else:
                    print(f"{config['id']} ({config['perspective']}):")
                    print(f"  {result}\n")
        
        # Get metrics
        print("-" * 70)
        print("\nSimulation Metrics:")
        metrics = world.get_metrics()
        print(f"  Agents: {metrics['agent_count']}")
        print(f"  Services: {', '.join(metrics['services'])}")
        
        for service_name, service_metrics in metrics["batch_metrics"].items():
            print(f"\n  {service_name}:")
            print(f"    Total requests: {service_metrics['total_requests']}")
            print(f"    Total batches: {service_metrics['total_batches']}")
            print(f"    Avg batch size: {service_metrics['avg_batch_size']:.2f}")
        
        # Query final agent states from ECS
        print("\nFinal Agent States (from ECS):")
        states = world.query_agent_states()
        for state in states:
            agent_id = state.get("agent_id", "unknown")
            msg_count = json.loads(state.get("state_json", "{}")).get("message_count", 0)
            print(f"  {agent_id}: {msg_count} messages")
    
    # Cleanup Ray
    ray.shutdown()
    print("\n" + "=" * 70)
    print("Simulation complete!")


# =============================================================================
# Fallback without Ray
# =============================================================================


async def run_without_ray():
    """Fallback when Ray is not available."""
    print("\nRay Actor Example")
    print("=" * 70)
    print("\nThis example requires Ray to be installed.")
    print("\nInstall with:")
    print("  pip install 'archetype[ray]'")
    print("\nOr:")
    print("  pip install ray>=2.47")
    print("\n" + "=" * 70)


# =============================================================================
# Entry Point
# =============================================================================


def main():
    """Main entry point."""
    # Check for OpenAI API key
    if not os.getenv("OPENAI_API_KEY"):
        print("Warning: OPENAI_API_KEY not set. LLM calls will fail.")
        print("Set it with: export OPENAI_API_KEY='your-key'")
    
    if RAY_AVAILABLE:
        asyncio.run(run_ray_agent_simulation())
    else:
        asyncio.run(run_without_ray())


if __name__ == "__main__":
    main()
