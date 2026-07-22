# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Session supervisor contracts: supervised PTYs are alive, recorded, joinable."""

import shutil
import time
import urllib.request

import pytest

from archetype.missions.sandboxes.contracts import ProcessRequest, SandboxSpec
from archetype.missions.sandboxes.local import LocalTmuxBackend
from archetype.missions.sessions import TmuxSessionSupervisor

pytestmark = pytest.mark.skipif(shutil.which("tmux") is None, reason="tmux is not installed")

_HAS_TTYD = shutil.which("ttyd") is not None


@pytest.fixture
def supervisor(tmp_path):
    sup = TmuxSessionSupervisor(
        socket_name=f"archetype-test-{tmp_path.name[-8:]}", run_dir=tmp_path / "run"
    )
    yield sup
    sup.shutdown()


def _wait(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.1)
    return False


class TestSupervisor:
    def test_start_records_stream_and_stays_alive(self, supervisor, tmp_path):
        rec = supervisor.start(
            "worker",
            ("sh", "-c", "while true; do echo tick; sleep 0.1; done"),
            cwd=str(tmp_path),
        )
        assert supervisor.alive("worker")
        assert _wait(lambda: rec.stream_path.exists() and rec.stream_path.stat().st_size > 0)
        assert "tick" in supervisor.capture("worker")

    def test_session_survives_and_kill_ends_it(self, supervisor, tmp_path):
        supervisor.start("shortlived", ("sh", "-c", "sleep 30"), cwd=str(tmp_path))
        assert supervisor.alive("shortlived")
        supervisor.kill("shortlived")
        assert _wait(lambda: not supervisor.alive("shortlived"))

    def test_crash_scene_is_preserved(self, supervisor, tmp_path):
        # remain-on-exit keeps the dead pane (and the session) for forensics.
        supervisor.start("crasher", ("sh", "-c", "echo boom; exit 3"), cwd=str(tmp_path))
        assert _wait(lambda: "boom" in supervisor.capture("crasher"))
        assert supervisor.alive("crasher")
        assert "boom" in supervisor.capture("crasher")

    def test_writable_lane_requires_credential(self, supervisor, tmp_path):
        supervisor.start("gated", ("sh", "-c", "sleep 30"), cwd=str(tmp_path))
        with pytest.raises(ValueError, match="credential"):
            supervisor.serve("gated", writable=True)

    def test_path_escaping_session_names_are_rejected(self, supervisor, tmp_path):
        # Session names become recording filenames; separators would let a
        # caller write logs outside run_dir.
        with pytest.raises(ValueError, match="session name"):
            supervisor.start("../evil", ("sh", "-c", "sleep 1"), cwd=str(tmp_path))
        with pytest.raises(ValueError, match="session name"):
            supervisor.recording("a/b")

    @pytest.mark.skipif(not _HAS_TTYD, reason="ttyd is not installed")
    def test_spectate_lane_serves_http(self, supervisor, tmp_path):
        supervisor.start("watched", ("sh", "-c", "sleep 30"), cwd=str(tmp_path))
        url, port = supervisor.serve("watched")
        assert port > 0

        def probe():
            try:
                with urllib.request.urlopen(url, timeout=1) as resp:
                    return resp.status == 200
            except OSError:
                return False

        assert _wait(probe)


class TestLocalBackend:
    @pytest.fixture
    def backend(self, tmp_path):
        b = LocalTmuxBackend(
            run_dir=tmp_path / "run", socket_name=f"archetype-lb-{tmp_path.name[-8:]}"
        )
        yield b
        b.shutdown()

    @pytest.mark.asyncio
    async def test_exec_returns_factual_result(self, backend, tmp_path):
        spec = SandboxSpec(provider="local", environment="test", workdir=str(tmp_path / "wd"))
        session = await backend.create(spec)
        result = await session.exec(ProcessRequest(argv=("sh", "-c", "echo out; exit 0")))
        assert result.returncode == 0
        assert result.stdout.strip() == "out"
        await session.close()

    @pytest.mark.asyncio
    async def test_interactive_session_is_supervised_and_closed(self, backend, tmp_path):
        spec = SandboxSpec(provider="local", environment="test", workdir=str(tmp_path / "wd"))
        session = await backend.create(spec)
        handle = await session.start_interactive(
            ("sh", "-c", "while true; do echo live; sleep 0.1; done"),
            serve_lanes=False,
        )
        assert session.session_alive(handle.session_name)
        assert _wait(lambda: handle.recording.stream_path.exists())
        assert handle.takeover_credential.startswith("operator:")
        await session.close()
        assert _wait(lambda: not session.session_alive(handle.session_name))

    @pytest.mark.asyncio
    async def test_wrong_provider_is_rejected(self, backend, tmp_path):
        spec = SandboxSpec(provider="modal", environment="test", workdir=str(tmp_path / "wd"))
        with pytest.raises(ValueError, match="provider"):
            await backend.create(spec)

    @pytest.mark.asyncio
    async def test_secret_requests_are_rejected_not_ignored(self, backend, tmp_path):
        spec = SandboxSpec(provider="local", environment="test", workdir=str(tmp_path / "wd"))
        session = await backend.create(spec)
        with pytest.raises(ValueError, match="secret"):
            await session.exec(ProcessRequest(argv=("sh", "-c", "true"), secret_names=("github",)))
        await session.close()

    @pytest.mark.asyncio
    async def test_timeout_transitions_sandbox_to_errored(self, backend, tmp_path):
        from archetype.missions.sandboxes.contracts import SandboxStatus

        spec = SandboxSpec(provider="local", environment="test", workdir=str(tmp_path / "wd"))
        session = await backend.create(spec)
        with pytest.raises(TimeoutError):
            await session.exec(ProcessRequest(argv=("sleep", "5"), timeout_seconds=1))
        assert await session.status() is SandboxStatus.ERRORED
        await session.close()


class TestStartUnwind:
    def test_pipe_failure_kills_the_orphan_session(self, supervisor, tmp_path, monkeypatch):
        import subprocess as sp

        real_run = sp.run

        def failing_pipe(argv, **kwargs):
            if "pipe-pane" in argv:
                return sp.CompletedProcess(argv, 1, stdout="", stderr="boom")
            return real_run(argv, **kwargs)

        monkeypatch.setattr("archetype.missions.sessions.tmux.subprocess.run", failing_pipe)
        with pytest.raises(RuntimeError, match="pipe-pane failed"):
            supervisor.start("orphan", ("sh", "-c", "sleep 30"), cwd=str(tmp_path))
        assert not supervisor.alive("orphan")
