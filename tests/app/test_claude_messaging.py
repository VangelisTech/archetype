# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Tests: Claude SDK Processor → Outbox → MessageDeliveryProcessor → Inbox → ChatGraph

Proves end-to-end correctness of an Anthropic SDK-powered processor
sending messages through the ECS messaging pipeline.

All Claude API calls are mocked for deterministic, offline testing.

Architecture note: @daft.func closures must be serializable (picklable).
API clients (anthropic.AsyncAnthropic, mocks) are NOT serializable.
For production code, use @daft.cls() — the client lives in __init__,
which runs once per worker and doesn't need serialization.
For tests with mock injection, we use a targeted .collect() for the
API call step only, keeping prompt building and outbox writing row-wise.
"""

import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import daft
import pytest
from daft import DataFrame, col

from archetype.app.chat_graph import ChatGraphRegistry
from archetype.app.messaging import Inbox, MessageDeliveryProcessor, Outbox
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.resources import Resources

# ── Test Components ──


class Agent(Component):
    name: str = ""
    role: str = ""


# ── The processor under test ──


class ClaudeThinkProcessor(AsyncProcessor):
    """
    Calls Claude for each agent, writes response to Outbox.
    Reads ChatGraph context via Resources for conversation history.

    Uses @daft.func for prompt assembly (row-wise, serializable), then
    a targeted .collect() for the API call step (client is non-serializable),
    then @daft.func for outbox writing (row-wise, serializable).

    In production, use @daft.cls() instead (see examples/claude_messaging_agents.py).
    The test uses .collect() because mock clients aren't picklable.
    """

    components = (Agent, Outbox, Inbox)
    priority = 10

    def __init__(self, client=None, model: str = "claude-sonnet-4-6", channel: str = "general"):
        super().__init__()
        self.client = client
        self.model = model
        self.channel = channel

    async def process(
        self, df: DataFrame, resources: Resources, tick: int = 0, **kwargs
    ) -> DataFrame:
        import asyncio

        world_id = str(kwargs.get("world_id", "test"))
        registry: ChatGraphRegistry | None = resources.get(ChatGraphRegistry)

        # Cross-row context: name lookups for conversation history + targeting.
        # This is justified — we need global entity knowledge for name resolution
        # and round-robin targeting. Only entity_id + name, not the full DataFrame.
        id_name_rows = df.select("entity_id", "agent__name").collect().to_pylist()
        name_by_id = {r["entity_id"]: r["agent__name"] for r in id_name_rows}
        all_ids = sorted(name_by_id.keys())

        # Build shared conversation history from ChatGraph (Resource, not DataFrame)
        context_json = "[]"
        if registry is not None:
            graph = registry.channel(world_id, self.channel)
            context_msgs = []
            for cmd in graph.active_path():
                sender = cmd.payload.get("sender_id", "system")
                sender_name = name_by_id.get(sender, f"agent-{sender}")
                context_msgs.append(
                    {
                        "role": "user",
                        "content": f"[{sender_name}]: {cmd.payload.get('content', '')}",
                    }
                )
            context_json = json.dumps(context_msgs)

        # Step 1: Build prompts row-wise (all captured state is serializable)
        @daft.func
        def build_prompt(
            name: str,
            role: str,
            inbox_messages: list[str],
        ) -> str:
            """Row-wise: assemble Claude prompt from context + inbox."""
            history = json.loads(context_json)

            for raw in inbox_messages or []:
                parsed = json.loads(raw) if isinstance(raw, str) else raw
                sid = parsed.get("sender_id", "?")
                sname = name_by_id.get(sid, f"agent-{sid}")
                history.append(
                    {
                        "role": "user",
                        "content": f"[{sname}]: {parsed.get('content', '')}",
                    }
                )

            history.append(
                {
                    "role": "user",
                    "content": f"You are {name}. Tick {tick}. Respond in character.",
                }
            )

            return json.dumps({"system": role, "messages": history})

        df = df.with_column(
            "_prompt",
            build_prompt(col("agent__name"), col("agent__role"), col("inbox__messages")),
        )

        # Step 2: Call Claude API — targeted .collect() on prompts only.
        # The client (AsyncMock / AsyncAnthropic) is non-serializable,
        # so we can't capture it in a @daft.func closure. In production
        # code, use @daft.cls() (see examples/claude_messaging_agents.py).
        prompt_rows = df.select("entity_id", "agent__name", "_prompt").collect().to_pylist()

        async def call_claude(row: dict) -> tuple[int, str, str]:
            prompt_data = json.loads(row["_prompt"])
            response = await self.client.messages.create(
                model=self.model,
                max_tokens=150,
                system=prompt_data["system"],
                messages=prompt_data["messages"],
            )
            return row["entity_id"], row["agent__name"], response.content[0].text

        results = await asyncio.gather(*[call_claude(r) for r in prompt_rows])

        # Step 3: Write to outbox row-wise (all captured state is serializable)
        response_by_id = {eid: (name, text) for eid, name, text in results}
        channel = self.channel

        @daft.func
        def write_outbox(entity_id: int) -> list[str]:
            """Row-wise: build outbox message from Claude response."""
            if entity_id not in response_by_id:
                return []
            name, response_text = response_by_id[entity_id]
            other_ids = [aid for aid in all_ids if aid != entity_id]
            if not other_ids:
                return []
            target_id = other_ids[tick % len(other_ids)]
            return [
                json.dumps(
                    {
                        "receiver_id": target_id,
                        "channel": channel,
                        "content": f"{name}: {response_text}",
                    }
                )
            ]

        return df.with_column("outbox__messages", write_outbox(col("entity_id")))


# ── Helpers ──


def make_mock_client(responses: dict[str, str]):
    """
    Create a mock async Anthropic client that returns deterministic responses.

    responses: mapping of agent name → response text.
    The mock inspects the last message to find which agent is speaking.
    """
    client = AsyncMock()

    async def mock_create(**kwargs):
        messages = kwargs.get("messages", [])
        last_content = messages[-1]["content"] if messages else ""
        for agent_name, response_text in responses.items():
            if f"You are {agent_name}" in last_content:
                return SimpleNamespace(content=[SimpleNamespace(text=response_text)])
        return SimpleNamespace(content=[SimpleNamespace(text="(no response)")])

    client.messages.create = AsyncMock(side_effect=mock_create)
    return client


def build_df(entities: list[dict[str, Any]]) -> DataFrame:
    """Build a test DataFrame with Agent + Outbox + Inbox columns."""
    data: dict[str, list] = {
        "entity_id": [],
        "is_active": [],
        "agent__name": [],
        "agent__role": [],
        "outbox__messages": [],
        "inbox__messages": [],
    }
    for e in entities:
        data["entity_id"].append(e["id"])
        data["is_active"].append(True)
        data["agent__name"].append(e["name"])
        data["agent__role"].append(e.get("role", ""))
        data["outbox__messages"].append(e.get("outbox", []))
        data["inbox__messages"].append(e.get("inbox", []))
    return daft.from_pydict(data)


# ── Tests ──


class TestClaudeProcessorUnit:
    """Unit tests: ClaudeThinkProcessor in isolation."""

    @pytest.mark.asyncio
    async def test_claude_writes_to_outbox(self):
        """Claude's response should appear in the sender's Outbox."""
        mock_client = make_mock_client(
            {
                "Alice": "I think we should cooperate.",
                "Bob": "I disagree, competition drives innovation.",
            }
        )

        proc = ClaudeThinkProcessor(client=mock_client, channel="debate")
        resources = Resources()
        resources.insert(ChatGraphRegistry())

        df = build_df(
            [
                {"id": 1, "name": "Alice", "role": "optimist"},
                {"id": 2, "name": "Bob", "role": "skeptic"},
            ]
        )

        result = await proc.process(df, resources, tick=0, world_id="test")
        rows = result.collect().to_pylist()

        alice = [r for r in rows if r["entity_id"] == 1][0]
        bob = [r for r in rows if r["entity_id"] == 2][0]

        # Alice's outbox should have a message for Bob
        assert len(alice["outbox__messages"]) == 1
        alice_msg = json.loads(alice["outbox__messages"][0])
        assert alice_msg["receiver_id"] == 2
        assert alice_msg["channel"] == "debate"
        assert "I think we should cooperate" in alice_msg["content"]

        # Bob's outbox should have a message for Alice
        assert len(bob["outbox__messages"]) == 1
        bob_msg = json.loads(bob["outbox__messages"][0])
        assert bob_msg["receiver_id"] == 1
        assert "competition drives innovation" in bob_msg["content"]

    @pytest.mark.asyncio
    async def test_claude_receives_system_prompt(self):
        """The agent's role should be passed as Claude's system prompt."""
        mock_client = make_mock_client({"Alice": "hello"})

        proc = ClaudeThinkProcessor(client=mock_client, channel="general")
        resources = Resources()
        resources.insert(ChatGraphRegistry())

        df = build_df(
            [
                {"id": 1, "name": "Alice", "role": "You are a helpful scientist."},
                {"id": 2, "name": "Bob", "role": "You are a bold explorer."},
            ]
        )

        await proc.process(df, resources, tick=0, world_id="test")

        # Inspect what was passed to Claude
        calls = mock_client.messages.create.call_args_list
        systems = [call.kwargs["system"] for call in calls]
        assert "You are a helpful scientist." in systems
        assert "You are a bold explorer." in systems

    @pytest.mark.asyncio
    async def test_claude_sees_graph_context(self):
        """Claude should receive conversation history from ChatGraph."""
        from archetype.app.models import Command, CommandType

        mock_client = make_mock_client(
            {
                "Alice": "building on that...",
                "Bob": "interesting point",
            }
        )

        resources = Resources()
        registry = ChatGraphRegistry()
        resources.insert(registry)

        # Pre-populate the graph with history
        graph = registry.channel("test", "debate")
        graph.append(
            Command(
                type=CommandType.MESSAGE,
                tick=0,
                channel="debate",
                payload={"sender_id": 1, "content": "Let's discuss AI safety."},
            )
        )
        graph.append(
            Command(
                type=CommandType.MESSAGE,
                tick=0,
                channel="debate",
                payload={"sender_id": 2, "content": "Good topic. What concerns you?"},
            )
        )

        proc = ClaudeThinkProcessor(client=mock_client, channel="debate")

        df = build_df(
            [
                {"id": 1, "name": "Alice", "role": "optimist"},
                {"id": 2, "name": "Bob", "role": "skeptic"},
            ]
        )

        await proc.process(df, resources, tick=1, world_id="test")

        # Both agents should have received the 2-message history
        for call in mock_client.messages.create.call_args_list:
            messages = call.kwargs["messages"]
            # First two messages are history, last is the prompt
            assert len(messages) >= 3
            assert "Let's discuss AI safety" in messages[0]["content"]
            assert "What concerns you" in messages[1]["content"]

    @pytest.mark.asyncio
    async def test_claude_called_count(self):
        """Claude should be called exactly once per agent."""
        mock_client = make_mock_client(
            {
                "A": "hi",
                "B": "hi",
                "C": "hi",
            }
        )

        proc = ClaudeThinkProcessor(client=mock_client, channel="general")
        resources = Resources()
        resources.insert(ChatGraphRegistry())

        df = build_df(
            [
                {"id": 1, "name": "A", "role": ""},
                {"id": 2, "name": "B", "role": ""},
                {"id": 3, "name": "C", "role": ""},
            ]
        )

        await proc.process(df, resources, tick=0, world_id="test")

        assert mock_client.messages.create.call_count == 3


class TestEndToEndPipeline:
    """
    Integration tests: ClaudeThinkProcessor + MessageDeliveryProcessor
    running together prove the full pipeline.
    """

    @pytest.mark.asyncio
    async def test_claude_response_delivered_to_recipient(self):
        """
        Full pipeline:
            Tick 0: Claude writes to Outbox
            Delivery: MessageDeliveryProcessor routes Outbox → Inbox
            Verify: recipient has the message in Inbox + ChatGraph updated
        """
        mock_client = make_mock_client(
            {
                "Alice": "I propose we collaborate on safety research.",
                "Bob": "Let me think about the risks first.",
            }
        )

        resources = Resources()
        registry = ChatGraphRegistry()
        resources.insert(registry)

        # Step 1: Claude generates messages → Outbox
        claude_proc = ClaudeThinkProcessor(
            client=mock_client,
            channel="collab",
        )
        df = build_df(
            [
                {"id": 1, "name": "Alice", "role": "cooperative researcher"},
                {"id": 2, "name": "Bob", "role": "risk analyst"},
            ]
        )

        df = await claude_proc.process(df, resources, tick=0, world_id="test")

        # Verify outboxes are populated
        rows = df.collect().to_pylist()
        alice_out = [r for r in rows if r["entity_id"] == 1][0]["outbox__messages"]
        bob_out = [r for r in rows if r["entity_id"] == 2][0]["outbox__messages"]
        assert len(alice_out) == 1, "Alice should have 1 outgoing message"
        assert len(bob_out) == 1, "Bob should have 1 outgoing message"

        # Step 2: MessageDeliveryProcessor routes messages
        delivery_proc = MessageDeliveryProcessor()

        # Need to re-create df from the collected rows since Daft DFs are lazy
        df = daft.from_pydict(
            {
                "entity_id": [r["entity_id"] for r in rows],
                "is_active": [r["is_active"] for r in rows],
                "outbox__messages": [r["outbox__messages"] for r in rows],
                "inbox__messages": [r["inbox__messages"] for r in rows],
            }
        )

        df = await delivery_proc.process(df, resources, tick=0, world_id="test")
        rows = df.collect().to_pylist()

        # Verify: Bob received Alice's message in his Inbox
        bob = [r for r in rows if r["entity_id"] == 2][0]
        assert len(bob["inbox__messages"]) == 1
        bob_inbox = json.loads(bob["inbox__messages"][0])
        assert bob_inbox["sender_id"] == 1
        assert "collaborate on safety research" in bob_inbox["content"]
        assert bob_inbox["channel"] == "collab"

        # Verify: Alice received Bob's message in her Inbox
        alice = [r for r in rows if r["entity_id"] == 1][0]
        assert len(alice["inbox__messages"]) == 1
        alice_inbox = json.loads(alice["inbox__messages"][0])
        assert alice_inbox["sender_id"] == 2
        assert "risks first" in alice_inbox["content"]

        # Verify: Outboxes are cleared
        for row in rows:
            assert row["outbox__messages"] == []

        # Verify: ChatGraph has the conversation
        graph = registry.channel("test", "collab")
        assert graph.size == 2
        path = graph.active_path()
        contents = [cmd.payload["content"] for cmd in path]
        assert any("collaborate" in c for c in contents)
        assert any("risks" in c for c in contents)

    @pytest.mark.asyncio
    async def test_multi_tick_conversation_builds_context(self):
        """
        Over multiple ticks, the ChatGraph accumulates messages.
        Claude sees growing context each tick.
        """
        tick_responses = {
            0: {"Alice": "Let's start with introductions.", "Bob": "Sure, I'm Bob."},
            1: {"Alice": "Nice to meet you Bob.", "Bob": "Likewise Alice."},
        }

        resources = Resources()
        registry = ChatGraphRegistry()
        resources.insert(registry)

        delivery_proc = MessageDeliveryProcessor()

        entities = [
            {"id": 1, "name": "Alice", "role": "friendly"},
            {"id": 2, "name": "Bob", "role": "polite"},
        ]

        # Track context depth seen by Claude at each tick
        context_depths: dict[int, list[int]] = {}

        prev_rows: list[dict] = []
        for tick in range(2):
            mock_client = make_mock_client(tick_responses[tick])

            # Wrap the mock to capture message count
            original_create = mock_client.messages.create.side_effect

            async def capturing_create(_tick=tick, _orig=original_create, **kwargs):
                msgs = kwargs.get("messages", [])
                context_depths.setdefault(_tick, []).append(len(msgs))
                return await _orig(**kwargs)

            mock_client.messages.create = AsyncMock(side_effect=capturing_create)

            claude_proc = ClaudeThinkProcessor(
                client=mock_client,
                channel="chat",
            )

            if tick == 0:
                df = build_df(entities)
            else:
                # Carry forward: entities keep their inbox from prior delivery
                df = daft.from_pydict(
                    {
                        "entity_id": [r["entity_id"] for r in prev_rows],
                        "is_active": [True, True],
                        "agent__name": [r["agent__name"] for r in prev_rows],
                        "agent__role": [r["agent__role"] for r in prev_rows],
                        "outbox__messages": [[], []],
                        "inbox__messages": [r["inbox__messages"] for r in prev_rows],
                    }
                )

            # Claude thinks → Outbox
            df = await claude_proc.process(df, resources, tick=tick, world_id="test")
            rows = df.collect().to_pylist()

            # Delivery → Inbox + ChatGraph
            df = daft.from_pydict(
                {
                    "entity_id": [r["entity_id"] for r in rows],
                    "is_active": [True, True],
                    "outbox__messages": [r["outbox__messages"] for r in rows],
                    "inbox__messages": [r["inbox__messages"] for r in rows],
                }
            )
            df = await delivery_proc.process(df, resources, tick=tick, world_id="test")
            prev_rows = df.collect().to_pylist()
            # Preserve agent columns for next tick
            for i, r in enumerate(prev_rows):
                r["agent__name"] = entities[i]["name"]
                r["agent__role"] = entities[i]["role"]

        # Verify: ChatGraph grew over time
        graph = registry.channel("test", "chat")
        assert graph.size == 4  # 2 messages per tick * 2 ticks

        # Verify: context depth increased from tick 0 to tick 1
        # Tick 0: just the prompt (1 message per agent)
        # Tick 1: 2 graph messages + inbox + prompt (more messages)
        assert all(d == 1 for d in context_depths[0]), (
            f"Tick 0 should have minimal context, got {context_depths[0]}"
        )
        assert all(d > 1 for d in context_depths[1]), (
            f"Tick 1 should have growing context, got {context_depths[1]}"
        )

    @pytest.mark.asyncio
    async def test_ephemeral_claude_message_skips_graph(self):
        """
        If _append_history=False, the message is delivered but not in the graph.
        """
        resources = Resources()
        registry = ChatGraphRegistry()
        resources.insert(registry)

        # Manually place an ephemeral message in the outbox
        ephemeral_msg = json.dumps(
            {
                "receiver_id": 2,
                "channel": "system",
                "content": "heartbeat",
                "_append_history": False,
            }
        )

        df = daft.from_pydict(
            {
                "entity_id": [1, 2],
                "is_active": [True, True],
                "outbox__messages": [[ephemeral_msg], []],
                "inbox__messages": [[], []],
            }
        )

        delivery_proc = MessageDeliveryProcessor()
        df = await delivery_proc.process(df, resources, tick=0, world_id="test")
        rows = df.collect().to_pylist()

        # Message delivered to inbox
        entity2 = [r for r in rows if r["entity_id"] == 2][0]
        assert len(entity2["inbox__messages"]) == 1

        # But NOT in the graph
        assert "system" not in registry.channels("test")

    @pytest.mark.asyncio
    async def test_three_agent_round_robin(self):
        """
        Three agents: Claude response routing is round-robin.
        Each agent messages one other agent per tick.
        """
        mock_client = make_mock_client(
            {
                "Alice": "hello from alice",
                "Bob": "hello from bob",
                "Charlie": "hello from charlie",
            }
        )

        resources = Resources()
        registry = ChatGraphRegistry()
        resources.insert(registry)

        claude_proc = ClaudeThinkProcessor(client=mock_client, channel="general")
        delivery_proc = MessageDeliveryProcessor()

        df = build_df(
            [
                {"id": 1, "name": "Alice", "role": ""},
                {"id": 2, "name": "Bob", "role": ""},
                {"id": 3, "name": "Charlie", "role": ""},
            ]
        )

        # Claude thinks
        df = await claude_proc.process(df, resources, tick=0, world_id="test")
        rows = df.collect().to_pylist()

        # Each agent should have exactly 1 outgoing message
        for row in rows:
            assert len(row["outbox__messages"]) == 1, (
                f"Agent {row['entity_id']} should have 1 outgoing message"
            )

        # Deliver
        df = daft.from_pydict(
            {
                "entity_id": [r["entity_id"] for r in rows],
                "is_active": [True, True, True],
                "outbox__messages": [r["outbox__messages"] for r in rows],
                "inbox__messages": [r["inbox__messages"] for r in rows],
            }
        )
        df = await delivery_proc.process(df, resources, tick=0, world_id="test")
        rows = df.collect().to_pylist()

        # At least some agents should have received messages
        total_inbox = sum(len(r["inbox__messages"]) for r in rows)
        assert total_inbox == 3, f"3 messages sent, 3 should be delivered, got {total_inbox}"

        # All outboxes cleared
        for row in rows:
            assert row["outbox__messages"] == []

        # Graph should have 3 nodes
        graph = registry.channel("test", "general")
        assert graph.size == 3
