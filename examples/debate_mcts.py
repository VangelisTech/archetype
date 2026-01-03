#!/usr/bin/env python3
"""
Multi-Agent Debate with MCTS / Counterfactual Reasoning
========================================================

This demo shows the full vision:
1. Agent-centric DSL with @behavior
2. Auto message realization (Inbox auto-detected)
3. spawn_world() for the superjective's inner simulation
4. parallel_rollouts() for MCTS-style exploration

The superjective agent doesn't just ask an LLM for scenarios—it actually
SIMULATES different reorganization approaches by spawning inner worlds
and running mini-debates to see which approach generates the most consensus.

Run: uv run python examples/debate_mcts.py
"""

import asyncio
import json
import os

from dotenv import load_dotenv

from archetype.core.component import Component
from archetype.dsl import World, behavior, spawn_world
from archetype.dsl.core import parallel_rollouts
from archetype.dsl.primitives import Inbox

load_dotenv()


# =============================================================================
# Configuration
# =============================================================================

LLM_MODEL = "gpt-4o-mini"
DEBATE_ROUNDS = 2  # Outer debate rounds
INNER_ROUNDS = 2   # Inner simulation rounds


def ensure_list(data) -> list:
    """Handle auto-deserialized JSON or string."""
    if isinstance(data, list):
        return data
    if isinstance(data, str) and data:
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            return []
    return []


# =============================================================================
# Components
# =============================================================================


class Perspective(Component):
    name: str = ""
    type: str = ""
    system_prompt: str = ""


class DebateState(Component):
    round: int = 0
    history_json: str = "[]"
    latest: str = ""
    score: float = 0.0  # Agreement/quality score


class InnerWorldResult(Component):
    scenarios_json: str = "[]"
    best_scenario: str = ""
    synthesis: str = ""


# =============================================================================
# Perspectives
# =============================================================================

PERSPECTIVES = {
    "objective": "Analyze with metrics: dependencies, coupling, cohesion.",
    "subjective": "Analyze through developer experience and intuition.",
    "abjective": "Analyze through absence: what's missing or rejected.",
    "superjective": "Synthesize all perspectives into higher-order patterns.",
}

RL_MODULE = """
archetype/rl has 4 submodules:
- agents/ (bandits, control, OODA, trajectories)
- emergent/ (LLM simulation, rollouts, planning)
- grpo/ (policy optimization, training)
- nlp/ (document processing)

How should it be reorganized?
"""


# =============================================================================
# Behaviors
# =============================================================================


@behavior
class Debater:
    """Standard debater behavior—generate response and broadcast."""
    
    requires = [Perspective, DebateState, Inbox]
    priority = 10
    
    async def act(self, agent, world, tick):
        # Get context from inbox
        messages = ensure_list(agent.inbox.messages_json)
        
        if messages:
            context = "\n".join([f"- {m['sender']}: {m['content'][:100]}..." for m in messages[-3:]])
        else:
            context = "(You speak first)"
        
        # Generate response
        prompt = f"""{agent.perspective.system_prompt}

Topic: {RL_MODULE}

Context:
{context}

Round {tick + 1}. Give your perspective in under 150 words:"""
        
        response = await world.prompt(prompt, max_tokens=200)
        
        # Update state
        history = ensure_list(agent.debate_state.history_json)
        history.append({"agent": agent.perspective.name, "statement": response})
        agent.debate_state.history_json = json.dumps(history)
        agent.debate_state.latest = response
        agent.debate_state.round = tick + 1
        
        # Broadcast to others
        await world.broadcast(response, sender=agent, exclude=[agent])


@behavior
class SuperjectiveInnerWorld:
    """
    Superjective runs INNER SIMULATIONS to explore reorganization scenarios.
    
    Instead of just asking an LLM "what are good scenarios?", we actually
    simulate mini-debates for each scenario to see which generates consensus.
    """
    
    requires = [Perspective, DebateState, InnerWorldResult]
    priority = 20
    filter = lambda agent: agent.perspective.type == "superjective"
    runs_on = "final_tick"
    
    async def act(self, agent, world, tick):
        print("\n   [Superjective] Spawning inner worlds to explore scenarios...")
        
        # Define scenarios to explore
        scenarios = [
            {
                "name": "Merge agents+grpo",
                "prompt_modifier": "Assume agents/ and grpo/ are merged into 'learning/'.",
            },
            {
                "name": "Split emergent",  
                "prompt_modifier": "Assume emergent/ is split into 'simulation/' and 'llm_planning/'.",
            },
            {
                "name": "Flat structure",
                "prompt_modifier": "Assume all files are in a single flat directory.",
            },
        ]
        
        # Run parallel inner simulations
        async def setup_scenario(inner_world, scenario):
            """Configure inner world with scenario-specific agents."""
            # Create mini-debate agents with scenario context
            modified_context = f"{RL_MODULE}\n\nSCENARIO: {scenario['prompt_modifier']}"
            
            for name, ptype in [("A", "objective"), ("B", "subjective"), ("C", "abjective")]:
                await inner_world.spawn(
                    Perspective(
                        name=f"{name}_{scenario['name'][:10]}",
                        type=ptype,
                        system_prompt=f"{PERSPECTIVES[ptype]}\n\nContext: {modified_context}",
                    ),
                    DebateState(round=0, history_json="[]", latest="", score=0.0),
                    Inbox(messages_json="[]"),
                )
        
        def extract_metrics(inner_world):
            """Extract consensus score from inner simulation."""
            positive_words = ["agree", "good", "clear", "cohesive", "better", "improve"]
            negative_words = ["disagree", "confus", "unclear", "problem", "worse"]
            
            total_score = 0
            for inner_agent in inner_world.agents:
                history = ensure_list(inner_agent.debate_state.history_json)
                for entry in history:
                    text = entry.get("statement", "").lower()
                    total_score += sum(1 for w in positive_words if w in text)
                    total_score -= sum(1 for w in negative_words if w in text)
            
            return {"consensus_score": total_score}
        
        # Run the inner simulations
        results = []
        for i, scenario in enumerate(scenarios):
            print(f"      Scenario {i+1}: {scenario['name']}")
            
            async with spawn_world(f"scenario_{i}", parent=world) as inner:
                inner.add_behavior(Debater)
                await setup_scenario(inner, scenario)
                await inner.run(ticks=INNER_ROUNDS)
                
                metrics = extract_metrics(inner)
                results.append({
                    "scenario": scenario,
                    "metrics": metrics,
                    "final_statements": [
                        a.debate_state.latest for a in inner.agents
                    ],
                })
        
        # Find best scenario
        best = max(results, key=lambda r: r["metrics"]["consensus_score"])
        
        print(f"      Best scenario: {best['scenario']['name']} (score: {best['metrics']['consensus_score']})")
        
        # Store results
        agent.inner_world_result.scenarios_json = json.dumps([
            {"name": r["scenario"]["name"], "score": r["metrics"]["consensus_score"]}
            for r in results
        ])
        agent.inner_world_result.best_scenario = best["scenario"]["name"]
        
        # Generate synthesis based on inner simulation results
        synthesis_prompt = f"""You explored 3 reorganization scenarios through inner simulations:

{json.dumps(results, indent=2)[:1500]}

The scenario with highest consensus was: {best['scenario']['name']}

Synthesize your recommendation in 100 words:"""
        
        synthesis = await world.prompt(synthesis_prompt, max_tokens=150)
        agent.inner_world_result.synthesis = synthesis


# =============================================================================
# Main
# =============================================================================


async def main():
    print("=" * 70)
    print("DEBATE with MCTS / INNER WORLD SIMULATION")
    print("=" * 70)
    print()
    print("The superjective agent will spawn INNER WORLDS to explore scenarios,")
    print("running mini-debates to find which reorganization achieves consensus.")
    print()
    
    async with World("debate_mcts", storage="./debate_mcts_data") as world:
        
        # Register behaviors
        world.add_behavior(Debater)
        world.add_behavior(SuperjectiveInnerWorld)
        
        # Spawn agents
        print("[1/3] Creating agents...")
        for name, ptype in [
            ("Oracle", "objective"),
            ("Poet", "subjective"),
            ("Critic", "abjective"),
            ("Sage", "superjective"),
        ]:
            await world.spawn(
                Perspective(name=name, type=ptype, system_prompt=PERSPECTIVES[ptype]),
                DebateState(round=0, history_json="[]", latest="", score=0.0),
                Inbox(messages_json="[]"),
                InnerWorldResult(scenarios_json="[]", best_scenario="", synthesis=""),
            )
            print(f"   {name} ({ptype})")
        
        # Run outer debate
        print(f"\n[2/3] Running outer debate ({DEBATE_ROUNDS} rounds)...")
        print("   (Superjective spawns inner worlds on final tick)")
        await world.run(ticks=DEBATE_ROUNDS)
        
        # Display results
        print("\n[3/3] Results")
        print("\n" + "=" * 70)
        print("OUTER DEBATE TRANSCRIPT")
        print("=" * 70)
        
        for agent in world.agents:
            print(f"\n## {agent.perspective.name} ({agent.perspective.type.upper()})")
            print("-" * 40)
            
            history = ensure_list(agent.debate_state.history_json)
            for i, entry in enumerate(history, 1):
                if entry.get("agent") == agent.perspective.name:
                    print(f"\n**Round {i}:**")
                    print(entry.get("statement", "")[:400])
        
        # Show superjective's MCTS results
        print("\n" + "=" * 70)
        print("SUPERJECTIVE'S INNER WORLD EXPLORATION")
        print("=" * 70)
        
        sage = await world.find_one(lambda a: a.perspective.type == "superjective")
        if sage:
            scenarios = ensure_list(sage.inner_world_result.scenarios_json)
            
            print("\n### Scenarios Explored (via inner simulation):")
            for s in scenarios:
                score = s.get("score", 0)
                bar = "█" * max(0, score + 5)  # Offset for visibility
                print(f"   {s['name']}: {bar} ({score})")
            
            print(f"\n### Best Scenario: {sage.inner_world_result.best_scenario}")
            
            print("\n### Synthesis (informed by inner simulations):")
            print(sage.inner_world_result.synthesis)
    
    print("\n" + "=" * 70)
    print("COMPLETE")
    print("=" * 70)
    print("\nThe superjective didn't just ASK about scenarios—it SIMULATED them.")


if __name__ == "__main__":
    asyncio.run(main())
