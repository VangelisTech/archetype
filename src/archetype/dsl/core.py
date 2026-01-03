# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Archetype Agent DSL — Core
==========================

An ergonomic, agent-centric DSL that compiles to the DataFrame/Processor engine.

Features:
- Agent-centric access: agent.perspective.name
- Declarative behaviors: @behavior with runs_on, filter
- Auto message realization when Inbox present
- spawn_world() for inner simulations / MCTS / counterfactual reasoning
- Batched LLM via world.prompt()
"""

from __future__ import annotations

import asyncio
import json
import re
import shutil
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Type, TYPE_CHECKING

import daft
from daft import col
from daft.functions import prompt as daft_prompt

from archetype.app.broker import CommandBroker
from archetype.app.models import Command, CommandType
from archetype.app.orchestrator import WorldOrchestrator
from archetype.core.aio import AsyncProcessor, AsyncSystem, AsyncWorld
from archetype.core.component import Component
from archetype.core.config import (
    CacheConfig,
    RunConfig,
    StorageBackend,
    StorageConfig,
    WorldConfig,
)
from archetype.core.resources import Resources


# =============================================================================
# Component Name Utilities
# =============================================================================


def component_prefix(component_cls: Type[Component]) -> str:
    """Get the column prefix for a component class."""
    return component_cls.__name__.lower() + "__"


def component_column(component_cls: Type[Component], field_name: str) -> str:
    """Get the full column name for a component field."""
    return component_prefix(component_cls) + field_name


# =============================================================================
# AgentProxy: Agent-centric access to row data
# =============================================================================


class ComponentProxy:
    """
    Proxy for accessing a single component's fields on an agent.
    Provides natural attribute access: agent.perspective.name
    """
    
    def __init__(
        self,
        component_cls: Type[Component],
        row_data: dict[str, Any],
        mutations: dict[str, Any],
    ):
        object.__setattr__(self, "_component_cls", component_cls)
        object.__setattr__(self, "_row_data", row_data)
        object.__setattr__(self, "_mutations", mutations)
        object.__setattr__(self, "_prefix", component_prefix(component_cls))
    
    def __getattr__(self, name: str) -> Any:
        col_name = self._prefix + name
        if col_name in self._mutations:
            return self._mutations[col_name]
        if col_name in self._row_data:
            value = self._row_data[col_name]
            # Auto-deserialize JSON lists/dicts
            if isinstance(value, str) and value.startswith(("[", "{")):
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    pass
            return value
        raise AttributeError(f"{self._component_cls.__name__} has no field '{name}'")
    
    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith("_"):
            object.__setattr__(self, name, value)
            return
        col_name = self._prefix + name
        # Auto-serialize lists/dicts to JSON
        if isinstance(value, (list, dict)):
            value = json.dumps(value)
        self._mutations[col_name] = value


class AgentProxy:
    """
    Proxy object representing a single agent.
    Provides natural access to components and tracks mutations.
    """
    
    def __init__(
        self,
        entity_id: int,
        row_data: dict[str, Any],
        component_classes: list[Type[Component]],
        world: "World",
    ):
        object.__setattr__(self, "entity_id", entity_id)
        object.__setattr__(self, "_row_data", row_data)
        object.__setattr__(self, "_component_classes", component_classes)
        object.__setattr__(self, "_world", world)
        object.__setattr__(self, "_mutations", {})
        object.__setattr__(self, "_component_proxies", {})
        
        for cls in component_classes:
            attr_name = self._to_snake_case(cls.__name__)
            proxy = ComponentProxy(cls, row_data, self._mutations)
            self._component_proxies[attr_name] = proxy
    
    @staticmethod
    def _to_snake_case(name: str) -> str:
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
    
    def __getattr__(self, name: str) -> Any:
        proxies = object.__getattribute__(self, "_component_proxies")
        if name in proxies:
            return proxies[name]
        raise AttributeError(f"Agent has no component '{name}'")
    
    def get_mutations(self) -> dict[str, Any]:
        return dict(self._mutations)
    
    def clear_mutations(self) -> None:
        self._mutations.clear()


# =============================================================================
# BehaviorSpec: Declarative behavior definition
# =============================================================================


@dataclass
class BehaviorSpec:
    """Specification for an agent behavior that compiles to an AsyncProcessor."""
    name: str
    behavior_class: type
    requires: list[Type[Component]] = field(default_factory=list)
    priority: int = 10
    filter_fn: Callable[[AgentProxy], bool] | None = None
    runs_on: str | int | None = None  # "every_tick", "final_tick", "first_tick", or tick number


def behavior(cls: type) -> BehaviorSpec:
    """
    Decorator to define an agent behavior.
    
    Usage:
        @behavior
        class Debater:
            requires = [Perspective, DebateState]
            priority = 10
            runs_on = "final_tick"
            filter = lambda agent: agent.perspective.type == "superjective"
            
            async def act(self, agent, world, tick):
                ...
    """
    return BehaviorSpec(
        name=cls.__name__,
        behavior_class=cls,
        requires=getattr(cls, "requires", []),
        priority=getattr(cls, "priority", 10),
        filter_fn=getattr(cls, "filter", None),
        runs_on=getattr(cls, "runs_on", None),
    )


# =============================================================================
# BehaviorProcessor: Compiles BehaviorSpec to AsyncProcessor
# =============================================================================


class BehaviorProcessor(AsyncProcessor):
    """AsyncProcessor that executes a BehaviorSpec."""
    
    def __init__(self, spec: BehaviorSpec, world: "World"):
        self._spec = spec
        self._world = world
        self.components = tuple(spec.requires)
        self.priority = spec.priority
    
    async def process(
        self,
        df: daft.DataFrame,
        resources: Resources = None,
        tick: int = 0,
        **kwargs,
    ) -> daft.DataFrame:
        if not self._should_run(tick):
            return df
        
        rows = df.collect().to_pylist()
        all_mutations: list[tuple[int, dict[str, Any]]] = []
        
        for row in rows:
            entity_id = row.get("entity_id")
            agent = AgentProxy(
                entity_id=entity_id,
                row_data=row,
                component_classes=self._spec.requires,
                world=self._world,
            )
            
            if self._spec.filter_fn and not self._spec.filter_fn(agent):
                continue
            
            instance = self._spec.behavior_class()
            await instance.act(agent, self._world, tick)
            
            mutations = agent.get_mutations()
            if mutations:
                all_mutations.append((entity_id, mutations))
        
        if all_mutations:
            df = self._apply_mutations(df, all_mutations)
        
        return df
    
    def _should_run(self, tick: int) -> bool:
        runs_on = self._spec.runs_on
        if runs_on is None or runs_on == "every_tick":
            return True
        if runs_on == "first_tick":
            return tick == 0
        if runs_on == "final_tick":
            return tick == getattr(self._world, "_total_ticks", 0) - 1
        if isinstance(runs_on, int):
            return tick == runs_on
        return True
    
    def _apply_mutations(
        self,
        df: daft.DataFrame,
        mutations: list[tuple[int, dict[str, Any]]],
    ) -> daft.DataFrame:
        all_columns = set()
        for _, muts in mutations:
            all_columns.update(muts.keys())
        
        for col_name in all_columns:
            col_values = {eid: muts[col_name] for eid, muts in mutations if col_name in muts}
            if not col_values:
                continue
            
            sample_value = next(iter(col_values.values()))
            if isinstance(sample_value, str):
                return_dtype = daft.DataType.string()
            elif isinstance(sample_value, int):
                return_dtype = daft.DataType.int64()
            elif isinstance(sample_value, float):
                return_dtype = daft.DataType.float64()
            else:
                return_dtype = daft.DataType.string()
            
            def make_mutator(values_map, dtype):
                @daft.func(return_dtype=dtype)
                def apply_mutation(entity_id: int, current_value):
                    return values_map.get(entity_id, current_value)
                return apply_mutation
            
            mutator = make_mutator(col_values, return_dtype)
            df = df.with_column(col_name, mutator(col("entity_id"), col(col_name)))
        
        return df


# =============================================================================
# Auto Message Realization Processor
# =============================================================================


class AutoMessageRealizationProcessor(AsyncProcessor):
    """
    Auto-injected processor that realizes MESSAGE commands into Inboxes.
    """
    components = ()  # Will be set dynamically
    priority = -100  # Run first
    
    def __init__(self, world: "World", inbox_component: Type[Component]):
        self._world = world
        self._inbox_component = inbox_component
        # Set components to match archetypes with inbox
        self.components = (inbox_component,)
    
    async def process(
        self,
        df: daft.DataFrame,
        resources: Resources = None,
        tick: int = 0,
        **kwargs,
    ) -> daft.DataFrame:
        broker = self._world.broker
        if broker is None:
            return df
        
        cmds = await broker.dequeue(self._world.name, max_items=10000)
        message_cmds = [c for c in cmds if c.type == CommandType.MESSAGE]
        
        if not message_cmds:
            return df
        
        # Group by receiver
        messages_by_receiver: dict[int, list[dict]] = {}
        for cmd in message_cmds:
            receiver_id = cmd.payload.get("receiver_id")
            if receiver_id is not None:
                msg = {
                    "sender": cmd.payload.get("sender_name"),
                    "content": cmd.payload.get("content"),
                    "tick": tick,
                }
                messages_by_receiver.setdefault(receiver_id, []).append(msg)
        
        # Update inboxes
        inbox_col = component_prefix(self._inbox_component) + "messages_json"
        
        @daft.func
        def update_inbox(entity_id: int, current_json: str) -> str:
            current = json.loads(current_json) if current_json else []
            new_msgs = messages_by_receiver.get(entity_id, [])
            return json.dumps(current + new_msgs)
        
        if inbox_col in df.column_names:
            df = df.with_column(inbox_col, update_inbox(col("entity_id"), col(inbox_col)))
        
        return df


# =============================================================================
# World: The ergonomic simulation context
# =============================================================================


class World:
    """
    Ergonomic world context with auto message realization and spawn_world support.
    """
    
    def __init__(
        self,
        name: str,
        storage: str | Path = "./archetype_data",
        cache: bool = True,
        parent: "World" = None,
    ):
        self.name = name
        self._storage_path = Path(storage)
        self._use_cache = cache
        self._parent = parent
        
        self._orchestrator: WorldOrchestrator | None = None
        self._inner_world: AsyncWorld | None = None
        self._system: AsyncSystem | None = None
        self._broker: CommandBroker | None = None
        self._behaviors: list[BehaviorSpec] = []
        self._component_classes: set[Type[Component]] = set()
        self._inbox_component: Type[Component] | None = None
        self._total_ticks: int = 1
        self._agents_cache: list[AgentProxy] | None = None
        self._llm_model: str = "gpt-4o-mini"
    
    async def __aenter__(self) -> "World":
        if self._parent:
            # Child world inherits from parent
            self._orchestrator = self._parent._orchestrator
            self._broker = self._parent._broker
        else:
            # Clean previous run
            if self._storage_path.exists():
                shutil.rmtree(self._storage_path)
            self._orchestrator = WorldOrchestrator()
            self._broker = CommandBroker()
        
        self._system = AsyncSystem()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if not self._parent and self._orchestrator:
            await self._orchestrator.shutdown()
    
    def add_behavior(self, spec: BehaviorSpec) -> None:
        self._behaviors.append(spec)
        self._component_classes.update(spec.requires)
    
    async def spawn(self, *components: Component) -> AgentProxy:
        if self._inner_world is None:
            await self._ensure_world_created()
        
        for comp in components:
            cls = type(comp)
            self._component_classes.add(cls)
            # Track if any component is Inbox-like
            if "messages_json" in [f.name for f in cls.__dataclass_fields__.values()] if hasattr(cls, '__dataclass_fields__') else []:
                self._inbox_component = cls
            # Check by attribute
            if hasattr(comp, 'messages_json'):
                self._inbox_component = cls
        
        await self._inner_world.create_entity(list(components))
        self._agents_cache = None
        return None
    
    async def run(self, ticks: int = 1) -> None:
        self._total_ticks = ticks
        
        if self._inner_world is None:
            await self._ensure_world_created()
        
        # Auto-inject message realization if inbox present
        if self._inbox_component:
            auto_realizer = AutoMessageRealizationProcessor(self, self._inbox_component)
            await self._system.add_processor(auto_realizer)
        
        # Add behavior processors
        for spec in self._behaviors:
            processor = BehaviorProcessor(spec, self)
            await self._system.add_processor(processor)
        
        run_config = RunConfig.dev(steps=ticks, prefer_live_reads=True)
        await self._inner_world.run(run_config)
        self._agents_cache = None
    
    @property
    def agents(self) -> list[AgentProxy]:
        if self._agents_cache is not None:
            return self._agents_cache
        
        if self._inner_world is None:
            return []
        
        agents = []
        for sig, df in self._inner_world._live.items():
            rows = df.collect().to_pylist()
            component_classes = list(sig)
            
            for row in rows:
                agent = AgentProxy(
                    entity_id=row.get("entity_id"),
                    row_data=row,
                    component_classes=component_classes,
                    world=self,
                )
                agents.append(agent)
        
        self._agents_cache = agents
        return agents
    
    async def find(self, predicate: Callable[[AgentProxy], bool]) -> list[AgentProxy]:
        return [a for a in self.agents if predicate(a)]
    
    async def find_one(self, predicate: Callable[[AgentProxy], bool]) -> AgentProxy | None:
        for agent in self.agents:
            if predicate(agent):
                return agent
        return None
    
    async def prompt(
        self,
        text: str,
        model: str = None,
        max_tokens: int = 300,
        system_message: str = None,
    ) -> str:
        """
        Call LLM. For single calls outside DataFrame context.
        For batched calls, use daft.functions.prompt in processors.
        """
        import openai
        client = openai.AsyncOpenAI()
        
        messages = []
        if system_message:
            messages.append({"role": "system", "content": system_message})
        messages.append({"role": "user", "content": text})
        
        response = await client.chat.completions.create(
            model=model or self._llm_model,
            messages=messages,
            max_tokens=max_tokens,
        )
        return response.choices[0].message.content
    
    async def broadcast(
        self,
        content: str,
        sender: AgentProxy = None,
        metadata: dict = None,
        exclude: list[AgentProxy] = None,
    ) -> None:
        """Broadcast message to all agents except those in exclude list."""
        all_agents = self.agents
        exclude_ids = {a.entity_id for a in (exclude or [])}
        
        sender_id = sender.entity_id if sender else None
        sender_name = self._get_agent_name(sender) if sender else "unknown"
        
        for recipient in all_agents:
            if recipient.entity_id in exclude_ids:
                continue
            
            cmd = Command(
                type=CommandType.MESSAGE,
                payload={
                    "sender_id": sender_id,
                    "sender_name": sender_name,
                    "receiver_id": recipient.entity_id,
                    "receiver_name": self._get_agent_name(recipient),
                    "content": content,
                    **(metadata or {}),
                },
            )
            await self._broker.enqueue(self.name, cmd)
    
    def _get_agent_name(self, agent: AgentProxy) -> str:
        for attr in ["perspective", "agent_state", "identity", "profile", "state"]:
            try:
                comp = getattr(agent, attr)
                if hasattr(comp, "name"):
                    return comp.name
            except AttributeError:
                continue
        return f"agent_{agent.entity_id}"
    
    async def _ensure_world_created(self) -> None:
        if self._inner_world is not None:
            return
        
        storage_config = StorageConfig(
            uri=self._storage_path,
            namespace=f"{self.name}_archetypes",
            backend=StorageBackend.ICEBERG,
        )
        
        cache_config = CacheConfig() if self._use_cache else None
        
        self._inner_world = await self._orchestrator.create_world(
            config=WorldConfig(name=self.name),
            system=self._system,
            storage_config=storage_config,
            cache_config=cache_config,
        )
        
        self._inner_world.resources.insert(self._broker)
    
    @property
    def broker(self) -> CommandBroker:
        return self._broker
    
    @property
    def inner(self) -> AsyncWorld:
        return self._inner_world


# =============================================================================
# spawn_world: Inner Simulation / MCTS / Counterfactual Reasoning
# =============================================================================


@asynccontextmanager
async def spawn_world(
    name: str,
    parent: World,
    fork_state: bool = False,
) -> World:
    """
    Spawn a child world for inner simulation.
    
    Use cases:
    - MCTS: Explore multiple action sequences
    - Counterfactual reasoning: "What if agent X had said Y?"
    - Mental simulation: Agent imagines consequences
    
    Usage:
        async with spawn_world("scenario_1", parent=world) as inner:
            await inner.spawn(ScenarioAgent(...))
            await inner.run(ticks=5)
            result = analyze(inner.agents)
    
    Args:
        name: Name for the child world
        parent: Parent world (inherits broker, orchestrator)
        fork_state: If True, copy parent's current agent state to child
    """
    # Create child storage path
    child_storage = parent._storage_path / f"inner_{name}"
    
    child = World(
        name=f"{parent.name}_{name}",
        storage=child_storage,
        cache=parent._use_cache,
        parent=parent,
    )
    
    async with child:
        # If fork_state, copy parent agents to child
        if fork_state and parent.agents:
            for agent in parent.agents:
                # Reconstruct components from row data
                components = []
                for cls in agent._component_classes:
                    prefix = component_prefix(cls)
                    # Extract fields for this component
                    comp_data = {}
                    for key, value in agent._row_data.items():
                        if key.startswith(prefix):
                            field_name = key[len(prefix):]
                            comp_data[field_name] = value
                    # Create component instance
                    try:
                        comp = cls(**comp_data)
                        components.append(comp)
                    except Exception:
                        pass
                
                if components:
                    await child.spawn(*components)
        
        yield child


# =============================================================================
# MCTS Utilities
# =============================================================================


@dataclass
class RolloutResult:
    """Result from a simulation rollout."""
    world_name: str
    final_agents: list[AgentProxy]
    metrics: dict[str, Any] = field(default_factory=dict)
    
    def get_metric(self, key: str, default: Any = None) -> Any:
        return self.metrics.get(key, default)


async def parallel_rollouts(
    parent: World,
    scenarios: list[dict[str, Any]],
    ticks: int = 3,
    setup_fn: Callable[[World, dict], Any] = None,
    metric_fn: Callable[[World], dict] = None,
) -> list[RolloutResult]:
    """
    Run multiple simulation rollouts in parallel.
    
    This is the core primitive for MCTS-style exploration.
    
    Args:
        parent: Parent world providing context
        scenarios: List of scenario configs, each passed to setup_fn
        ticks: Number of ticks to run each rollout
        setup_fn: async fn(world, scenario) -> None, sets up the scenario
        metric_fn: fn(world) -> dict, extracts metrics after rollout
    
    Returns:
        List of RolloutResult, one per scenario
    
    Usage:
        scenarios = [
            {"action": "merge_modules", "target": "agents+grpo"},
            {"action": "split_modules", "target": "emergent"},
            {"action": "keep_current"},
        ]
        
        results = await parallel_rollouts(
            parent=world,
            scenarios=scenarios,
            ticks=3,
            setup_fn=configure_scenario,
            metric_fn=extract_metrics,
        )
        
        best = max(results, key=lambda r: r.get_metric("score"))
    """
    async def run_one(scenario: dict, index: int) -> RolloutResult:
        async with spawn_world(f"rollout_{index}", parent=parent) as inner:
            if setup_fn:
                await setup_fn(inner, scenario)
            
            await inner.run(ticks=ticks)
            
            metrics = metric_fn(inner) if metric_fn else {}
            
            return RolloutResult(
                world_name=inner.name,
                final_agents=inner.agents,
                metrics=metrics,
            )
    
    # Run all scenarios (could parallelize with asyncio.gather)
    results = []
    for i, scenario in enumerate(scenarios):
        result = await run_one(scenario, i)
        results.append(result)
    
    return results


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "World",
    "AgentProxy",
    "ComponentProxy",
    "behavior",
    "BehaviorSpec",
    "spawn_world",
    "parallel_rollouts",
    "RolloutResult",
    "component_prefix",
    "component_column",
]
