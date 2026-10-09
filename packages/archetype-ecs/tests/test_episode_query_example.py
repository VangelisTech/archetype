"""The GiT example must distinguish a historical pair from a frame shortcut."""

import pytest

from examples.episode_queries.query import git_pairs

pytest.importorskip("daft")


def test_git_query_requires_same_present_and_different_history_and_target():
    base = {
        "pair_id": "pair-1",
        "split": "test",
        "task": "H3",
        "instruction": "Return the bowl used first",
        "world": "world-a",
        "run": "main",
        "context": "media-a",
        "tick": 2,
        "cut_id": "cut-2",
        "history_artifact_id": "history-a",
        "current_artifact_id": "current-a",
        "current_sha": "identical-current-image",
    }
    a = base | {
        "episode_id": "a",
        "history_event": "left-first",
        "history_sha": "past-a",
        "required_target": "left",
    }
    b = base | {
        "episode_id": "b",
        "history_event": "right-first",
        "history_sha": "past-b",
        "required_target": "right",
        "world": "world-b",
    }
    result = git_pairs([a, b]).to_pydict()
    assert result["a_episode_id"] == ["a"]
    assert result["b_episode_id"] == ["b"]

    # A similar but non-identical present cannot establish GiT's paired control.
    changed_present = b | {"current_sha": "different-image"}
    assert git_pairs([a, changed_present]).to_pydict()["pair_id"] == []

    # An unchanged target means the history did not change the labeled decision.
    same_target = b | {"required_target": "left"}
    assert git_pairs([a, same_target]).to_pydict()["pair_id"] == []
