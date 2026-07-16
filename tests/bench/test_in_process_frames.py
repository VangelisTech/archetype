# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Frame-reference provenance for the in-process LIBERO client.

Ledger rows persist volume-relative frame refs; the files behind them must
be append-only like the rows themselves. Sweep rounds restart env_key at 0
and step labels repeat after every reset, so uniqueness must come from the
client session (per construction) and the episode ordinal (per reset).
"""

from bench.libero.in_process import InProcessLiberoEnvClient, InProcessLiberoEnvSpec, _frame_rel_dir


def test_session_is_unique_per_client_construction():
    a = InProcessLiberoEnvClient(suite="libero_spatial", task_id=0)
    b = InProcessLiberoEnvClient(suite="libero_spatial", task_id=0)
    assert a._session != b._session, "same-config clients must not share frame dirs"
    assert a._session.startswith("libero_spatial-t0-")


def test_frame_dir_is_unique_per_episode_and_env():
    dirs = {
        _frame_rel_dir("s-t0-abcd1234", env_key, episode)
        for env_key in (0, 1)
        for episode in (1, 2, 3)
    }
    assert len(dirs) == 6, "every (env_key, reset ordinal) gets its own directory"


def test_repeated_step_labels_cannot_collide_across_episodes():
    ep1 = _frame_rel_dir("s-t0-abcd1234", 0, 1)
    ep2 = _frame_rel_dir("s-t0-abcd1234", 0, 2)
    assert ep1 != ep2
    # The overwrite scenario from review: same env_key, same step label,
    # different sweep rounds — now distinct paths.
    assert f"{ep1}/00001-agentview.png" != f"{ep2}/00001-agentview.png"


def test_env_spec_threads_env_seed_to_client():
    client = InProcessLiberoEnvSpec(env_seed=23).build()
    assert client._env_seed == 23
