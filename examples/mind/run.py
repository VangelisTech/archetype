#!/usr/bin/env python3
# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Mind Extraction — Theory of Mind from Conversation History
==========================================================

Uses Archetype ECS to extract, classify, and bucket memories from
.claude conversation transcripts. Each user message becomes an entity
with four components processed in parallel by LLM-powered processors:

    Segment → Extract → Voice → Perspective

Output: A structured theory-of-mind map organized by Ben Davies'
perspectival framework (objective/subjective/abjective/superjective).

Usage:
    cd /Users/everettkleven/git/other/archetype
    source .env  # needs OPENAI_API_KEY
    .venv/bin/python -m examples.mind.run [conversation_dir]
"""

import asyncio
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

from archetype.app.container import ServiceContainer
from archetype.core.config import RunConfig, StorageConfig, WorldConfig

from .loader import load_conversations
from .processors import ExtractProcessor, PerspectiveProcessor, VoiceProcessor

DEFAULT_DIR = os.environ.get(
    "MIND_CONVERSATION_DIR",
    str(Path.home() / ".claude/projects/-Users-everettkleven-git-other"),
)


async def main(conversation_dir: str = DEFAULT_DIR):
    # ── Load conversations ──
    print(f"Loading conversations from {conversation_dir}...")
    max_segments = int(os.environ.get("MIND_MAX_SEGMENTS", "50"))
    entities = load_conversations(conversation_dir, max_segments=max_segments)
    print(f"Loaded {len(entities)} user message segments\n")

    if not entities:
        print("No messages found.")
        return

    # ── Set up Archetype world ──
    storage_uri = str(Path.cwd() / "mind_data")
    print(f"Storage: {storage_uri} (LanceDB)\n")

    container = ServiceContainer()
    world = await container.world_service.create_world(
        WorldConfig(name="mind-extraction"),
        StorageConfig(uri=storage_uri),
    )

    # Register processors
    await world.system.add_processor(ExtractProcessor())
    await world.system.add_processor(VoiceProcessor())
    await world.system.add_processor(PerspectiveProcessor())

    # Spawn entities
    for components in entities:
        await world.create_entity(components)

    print(f"Spawned {len(entities)} entities into world")
    print("Running extraction pipeline (1 tick = Extract → Voice → Perspective)...\n")

    # ── Run the pipeline ──
    rc = RunConfig(num_steps=1, prefer_live_reads=True)
    await world.run(rc)

    # ── Collect results ──
    results = []
    for _sig, df in world._live.items():
        rows = df.collect().to_pylist()
        for row in rows:
            results.append(row)

    def parse_field(text, pattern, default=""):
        m = re.search(pattern, text or "", re.IGNORECASE)
        return m.group(1).strip() if m else default

    def first_word(text):
        return (text or "").strip().split()[0].lower() if text else "unknown"

    # ── Display results ──
    by_perspective = defaultdict(list)
    by_voice = defaultdict(list)
    memorable_count = 0

    for row in results:
        raw_extract = row.get("extraction__memory_text") or ""
        is_memorable = parse_field(raw_extract, r"MEMORABLE:\s*(\w+)", "no")
        if is_memorable.lower() != "yes":
            continue

        memorable_count += 1
        memory = parse_field(raw_extract, r"MEMORY:\s*(.+?)(?:\n|$)", "")
        mem_type = parse_field(raw_extract, r"TYPE:\s*(\w+)", "unknown")
        conf_str = parse_field(raw_extract, r"CONFIDENCE:\s*([\d.]+)", "0.0")
        try:
            confidence = float(conf_str)
        except ValueError:
            confidence = 0.0

        voice = first_word(row.get("voice__reasoning") or "")
        lens = first_word(row.get("perspective__reasoning") or "")
        content_preview = (row.get("segment__content") or "")[:80]

        entry = {
            "memory": memory,
            "type": mem_type,
            "confidence": confidence,
            "voice": voice,
            "lens": lens,
            "source": content_preview,
        }

        by_perspective[lens].append(entry)
        by_voice[voice].append(entry)

    print(f"{'='*70}")
    print(f"THEORY OF MIND — {memorable_count} memories extracted")
    print(f"{'='*70}\n")

    # ── Perspective view (Davies' framework) ──
    lens_descriptions = {
        "objective": "What IS — facts about the world",
        "subjective": "What they FEEL — inner experience",
        "abjective": "What they REJECT — cast off, avoided",
        "superjective": "What they AIM FOR — beyond self",
    }

    for lens in ["objective", "subjective", "abjective", "superjective"]:
        entries = by_perspective.get(lens, [])
        desc = lens_descriptions.get(lens, "")
        print(f"## {lens.upper()} — {desc}")
        if not entries:
            print("  (none)\n")
            continue
        for e in entries:
            conf = e["confidence"] if isinstance(e["confidence"], float) else 0.0
            print(f"  [{e['voice']:>8}] {e['type']:>10} ({conf:.1f}) {e['memory'][:100]}")
        print()

    # ── Voice distribution ──
    print(f"{'='*70}")
    print("VOICE DISTRIBUTION")
    print(f"{'='*70}")
    for voice in ["actor", "observer", "observed"]:
        count = len(by_voice.get(voice, []))
        bar = "█" * count
        print(f"  {voice:>10}: {count:3d} {bar}")
    print()

    # ── Export as JSON ──
    output = {
        "total_segments": len(entities),
        "memorable": memorable_count,
        "by_perspective": {k: v for k, v in by_perspective.items()},
        "by_voice": {k: len(v) for k, v in by_voice.items()},
    }
    output_path = Path("mind_extraction.json")
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"Full results written to {output_path}")

    await container.shutdown()


if __name__ == "__main__":
    conv_dir = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DIR
    asyncio.run(main(conv_dir))
