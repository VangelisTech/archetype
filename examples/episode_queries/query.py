"""Cross-episode GiT audit over Archetype 0.7's verified cold-read handles.

Run with ``archetype-ecs[analysis]`` and the native library/store configured.
This is an offline query: Daft never enters DDlog's live admission path.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from archetype import ArchetypeRuntime


@dataclass(frozen=True, slots=True)
class Episode:
    episode_id: str
    pair_id: str
    split: str
    task: str
    instruction: str
    required_target: str
    history_event: str
    world: str
    run: str
    context: str
    tick: int
    cut_id: str
    history_artifact_id: str
    current_artifact_id: str

    @classmethod
    def parse(cls, row: dict) -> Episode:
        expected = set(cls.__dataclass_fields__)
        if set(row) != expected:
            raise ValueError(f"Episode fields differ: {sorted(set(row) ^ expected)}")
        if any(type(row[key]) is not str or not row[key] for key in expected - {"tick"}):
            raise ValueError("Episode text fields must be nonempty strings")
        if type(row["tick"]) is not int or row["tick"] < 0:
            raise ValueError("Episode tick must be a nonnegative integer")
        return cls(**row)


def read_manifest(path: Path) -> tuple[Episode, ...]:
    episodes = tuple(
        Episode.parse(json.loads(line)) for line in path.read_text().splitlines() if line.strip()
    )
    if len({e.episode_id for e in episodes}) != len(episodes):
        raise ValueError("Duplicate episode ID")
    return episodes


async def _exact_cut(world, episode: Episode):
    offset = 0
    while True:
        page = await world.history(offset=offset, limit=32)
        for cut in page.cuts:
            if (cut.world, cut.run, cut.tick, cut.cut_id) == (
                episode.world,
                episode.run,
                episode.tick,
                episode.cut_id,
            ):
                return cut
        if page.next_offset is None:
            raise ValueError(f"No complete cut for {episode.episode_id}")
        offset = page.next_offset


async def _evidence(artifacts, episode: Episode):
    found = {}
    offset = 0
    wanted = {episode.history_artifact_id, episode.current_artifact_id}
    while True:
        page = await artifacts.occurrences(all=True, offset=offset, limit=32)
        for item in page.items:
            if item.artifact_id in wanted:
                found[item.artifact_id] = item
        if len(found) == len(wanted) or page.next_offset is None:
            break
        offset = page.next_offset
    if len(found) != 2:
        raise ValueError(f"Missing history/current occurrence for {episode.episode_id}")
    history = found[episode.history_artifact_id]
    current = found[episode.current_artifact_id]
    if current.exact_cut != (episode.tick, episode.cut_id):
        raise ValueError(f"Current observation is not bound to exact cut: {episode.episode_id}")
    if history.exact_cut is not None and history.exact_cut[0] >= episode.tick:
        raise ValueError(f"History is not prior to the decision cut: {episode.episode_id}")
    if history.media_type.split("/")[0] not in {"image", "video"}:
        raise ValueError(f"History is not visual: {episode.episode_id}")
    if current.media_type.split("/")[0] != "image":
        raise ValueError(f"Current observation is not an image: {episode.episode_id}")
    return history.sha256, current.sha256


async def verified_rows(runtime: ArchetypeRuntime, episodes: tuple[Episode, ...]) -> list[dict]:
    """Resolve every logical episode, complete cut and occurrence through native reads."""
    rows = []
    for episode in episodes:
        world = runtime.world(episode.world, run=episode.run)
        await _exact_cut(world, episode)
        artifacts = world.artifacts(episode.context)
        history_sha, current_sha = await _evidence(artifacts, episode)
        rows.append(
            {
                "episode_id": episode.episode_id,
                "pair_id": episode.pair_id,
                "split": episode.split,
                "task": episode.task,
                "instruction": episode.instruction,
                "required_target": episode.required_target,
                "history_event": episode.history_event,
                "world": episode.world,
                "run": episode.run,
                "context": episode.context,
                "tick": episode.tick,
                "cut_id": episode.cut_id,
                "history_artifact_id": episode.history_artifact_id,
                "current_artifact_id": episode.current_artifact_id,
                "history_sha": history_sha,
                "current_sha": current_sha,
            }
        )
    return rows


def git_pairs(rows: list[dict]):
    """Lazy Daft query for annotated A/B pairs with exact present-frame evidence.

    A matching hash establishes byte identity, not causal validity or annotation
    correctness. The pair and target labels are supplied by the manifest curator.
    """
    import daft
    from daft import col

    if not rows:
        raise ValueError("No verified episodes")
    frame = daft.from_pylist(rows)
    names = (
        "episode_id",
        "split",
        "task",
        "instruction",
        "required_target",
        "history_event",
        "world",
        "run",
        "context",
        "tick",
        "cut_id",
        "history_artifact_id",
        "current_artifact_id",
        "history_sha",
        "current_sha",
    )
    left = frame.select("pair_id", *(col(n).alias(f"a_{n}") for n in names))
    right = frame.select("pair_id", *(col(n).alias(f"b_{n}") for n in names))
    return (
        left.join(right, on="pair_id")
        .where(
            (col("a_episode_id") < col("b_episode_id"))
            & (col("a_split") == col("b_split"))
            & (col("a_task") == col("b_task"))
            & (col("a_instruction") == col("b_instruction"))
            & (col("a_current_sha") == col("b_current_sha"))
            & (col("a_history_sha") != col("b_history_sha"))
            & (col("a_history_event") != col("b_history_event"))
            & (col("a_required_target") != col("b_required_target"))
        )
        .select(
            "pair_id",
            "a_episode_id",
            "b_episode_id",
            "a_task",
            "a_instruction",
            "a_required_target",
            "b_required_target",
            "a_history_event",
            "b_history_event",
            "a_current_sha",
            "a_history_sha",
            "b_history_sha",
            "a_world",
            "a_run",
            "a_context",
            "a_cut_id",
            "a_history_artifact_id",
            "a_current_artifact_id",
            "b_world",
            "b_run",
            "b_context",
            "b_cut_id",
            "b_history_artifact_id",
            "b_current_artifact_id",
        )
    )


async def _run(args):
    from archetype import ArchetypeRuntime

    episodes = read_manifest(args.manifest)
    async with ArchetypeRuntime(storage_only=True) as runtime:
        rows = await verified_rows(runtime, episodes)
    # Explicit terminal bound: the source records and full pair query are lazy.
    result = git_pairs(rows).limit(args.limit).to_pydict()
    print(json.dumps([dict(zip(result, values)) for values in zip(*result.values())], indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path, help="Curated JSONL episode references")
    parser.add_argument("--limit", type=int, default=32, choices=range(1, 33))
    asyncio.run(_run(parser.parse_args()))


if __name__ == "__main__":
    main()
