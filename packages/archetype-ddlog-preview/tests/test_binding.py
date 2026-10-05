"""SIMULATED native transport plus real local Iceberg. Native test is opt-in."""

from __future__ import annotations

import asyncio
import concurrent.futures
import ctypes
import json
import os
import sqlite3
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from archetype_ddlog_preview import ConstructionCleanupError, Host, NativeError, _Buffer

REPO = Path(__file__).resolve().parents[3]
LIBRARY = Path(os.environ["DDLOG_PYTHON_LIBRARY"]).resolve()
COMPONENTS = [
    {"name": "label", "output": "labels", "fields": ["entity_id", "name"], "entity_field": 0},
    {"name": "status", "output": "statuses", "fields": ["entity_id", "state"], "entity_field": 0},
]
ENTITY = 2**53 + 117


def wait(probe, done, timeout=180):
    deadline = time.monotonic() + timeout
    while True:
        value = probe()
        if done(value):
            return value
        if time.monotonic() >= deadline:
            raise AssertionError(f"Timed out: {value}")
        time.sleep(0.01)


def pin(record):
    return {k: record[k] for k in ("processor_id", "version")}


class Fixture:
    def __init__(self, native=False, root=None):
        self.root = root or Path(tempfile.mkdtemp(prefix="ddlog-python-"))
        if native:
            self.driver = Path(os.environ["ARCHETYPE_DDLOG_DRIVER"])
        else:
            source = (REPO / "crates/archetype-ddlog/tests/fixtures/hosted_native.py").read_text()
            source = source.replace("__CONTROL__", repr(str(self.root)))
            self.driver = self.root / "build.py"
            # Marker-controlled compiler fixture, no runtime/ABI test hooks.
            self.driver.write_text(
                f"#!{sys.executable}\nimport os,sys,time\nfrom pathlib import Path\n"
                f"root=Path({str(self.root)!r})\n(root/'compiler_entered').touch()\n"
                "while (root/'hold_compile').exists(): time.sleep(.01)\n"
                f"Path(sys.argv[2]).write_text({source!r})\nPath(sys.argv[2]).chmod(0o700)\n"
            )
            self.driver.chmod(0o700)
        self.host = self.open()
        self.native = native

    def open(self):
        return Host(
            library=LIBRARY,
            registry_root=self.root / "registry",
            build_root=self.root / "worlds",
            driver=self.driver,
            store_root=self.root / "storage",
        )

    def program(self):
        a = self.host.register(
            "Labels",
            {
                "rules": "label(E,N) :- seed(E,N).",
                "schemas": {
                    "seed": {"input": True, "fields": ["int", "string"]},
                    "label": {"input": False, "fields": ["int", "string"]},
                },
                "interface": {"inputs": ["seed"], "outputs": ["label"]},
            },
        )
        b = self.host.register(
            "Statuses",
            {
                "rules": 'status(E,"ready") :- label(E,N).',
                "schemas": {
                    "label": {"input": True, "fields": ["int", "string"]},
                    "status": {"input": False, "fields": ["int", "string"]},
                },
                "interface": {"inputs": ["label"], "outputs": ["status"]},
            },
        )
        return pin(
            self.host.register(
                "Two programs",
                {
                    "composition": {
                        "nodes": {"labels": pin(a), "statuses": pin(b)},
                        "inputs": {
                            "seed": {
                                "fields": ["int", "string"],
                                "targets": [{"node": "labels", "relation": "seed"}],
                            }
                        },
                        "bindings": [
                            {
                                "from": {"node": "labels", "relation": "label"},
                                "to": {"node": "statuses", "relation": "label"},
                            }
                        ],
                        "outputs": {
                            "labels": {"node": "labels", "relation": "label"},
                            "statuses": {"node": "statuses", "relation": "status"},
                        },
                    }
                },
            )
        )

    def world(self, name, program):
        id = self.host.create(name, program, ["labels", "statuses"])
        return self.host.bind({"native_world": id, "world": name, "run": "run_a"}, COMPONENTS)

    def running(self, binding):
        result = wait(
            lambda: self.host.status(binding["scope"]["native_world"]),
            lambda s: s["state"] != "starting",
        )
        assert result["state"] == "running", result
        return result

    def submit(self, binding, name="first", head=None, delete=False, value="héllo world"):
        id = binding["scope"]["native_world"]
        s = self.host.status(id)
        result = self.host.admit(
            binding,
            expected_head=head,
            generation=s["generation"],
            revision=s["revision"],
            key=name,
            changes=[
                {
                    "op": "delete" if delete else "insert",
                    "predicate": "seed",
                    "values": [ENTITY, value],
                }
            ],
        )
        return result["boundary"]["key"]

    def frozen(self, key):
        result = wait(
            lambda: self.host.admission_status(
                key["world_id"], key["generation"], key["admission_key"]
            ),
            lambda s: s["state"] != "pending",
        )
        assert result["state"] == "frozen", result
        return result

    def confirm(self, binding, key, receipt):
        return self.host.confirm(
            binding, key, tick=receipt["tick"], expected_parent=receipt["parent"]
        )

    def close(self):
        self.host.close()


class BindingTests(unittest.TestCase):
    def setUp(self):
        self.f = Fixture()
        self.addCleanup(self.f.close)

    def test_strict_raw_abi_and_fork(self):
        h = self.f.host
        for raw in [
            b'{"op":"inventory","op":"start"}',
            b'{"op":"inventory","extra":1}',
            b'{"op":"status","id":false}',
            b'{"op":"inventory","a":1.0}',
            b"{} {}",
            b"\xff",
            b'{"op":"register","request":{"name":"a","definition":{"x":1,"x":2}}}',
        ]:
            output = _Buffer()
            try:
                self.assertEqual(
                    h._lib.arct_ddlog_call(h._handle, raw, len(raw), ctypes.byref(output)), 1
                )
                result = json.loads(ctypes.string_at(output.data, output.length))
                self.assertEqual(result["error"]["kind"], "request")
            finally:
                h._lib.arct_ddlog_buffer_free(ctypes.byref(output))
                h._lib.arct_ddlog_buffer_free(ctypes.byref(output))
            self.assertIsNone(output.data)
        for request in [{"x": float("nan")}, {1: "x"}, {"x": object()}, {"x": 2**64}]:
            with self.assertRaises((TypeError, ValueError)):
                h.request("inventory", **{"bad": request})
        # Check child rejection before inherited native/Python locks; parent survives.
        pid = os.fork()
        if pid == 0:
            try:
                try:
                    h.close()
                except NativeError as e:
                    assert e.kind == "forked"
                else:
                    os._exit(2)
                output = _Buffer()
                status = h._lib.arct_ddlog_close(h._handle, ctypes.byref(output))
                result = json.loads(ctypes.string_at(output.data, output.length))
                os._exit(0 if status == 1 and result["error"]["kind"] == "forked" else 3)
            except BaseException:
                os._exit(4)
        self.assertEqual(os.waitpid(pid, 0)[1], 0)
        self.assertEqual(h.request("inventory")["worlds"], [])
        stale = h._handle
        h.close()
        with self.assertRaises(NativeError):
            h.status("anything")
        output = _Buffer()
        raw = b'{"op":"inventory"}'
        try:
            self.assertEqual(h._lib.arct_ddlog_call(stale, raw, len(raw), ctypes.byref(output)), 1)
        finally:
            h._lib.arct_ddlog_buffer_free(ctypes.byref(output))
        self.f.host = self.f.open()  # owner locks actually released

    def test_interrupted_open_releases_ownership(self):
        f = self.f
        f.host.close()
        real = ctypes.CDLL(str(LIBRARY))
        original = real.arct_ddlog_open

        class Interrupted:
            def __call__(self, *args):
                result = original(*args)
                assert result == 0
                raise KeyboardInterrupt("delivered at native return")

        real.arct_ddlog_open = Interrupted()
        with patch("archetype_ddlog_preview.ctypes.CDLL", return_value=real):
            with self.assertRaises(KeyboardInterrupt):
                f.open()
        f.host = f.open()
        self.assertEqual(f.host.request("inventory")["worlds"], [])

    def test_interrupted_open_failed_cleanup_retains_owner(self):
        f = self.f
        a = f.world("alpha", f.program())
        id = a["scope"]["native_world"]
        f.host.close()
        blocker = f.root / "worlds" / id / "world.json.tmp"
        real = ctypes.CDLL(str(LIBRARY))
        original = real.arct_ddlog_open

        class Interrupted:
            def __call__(self, *args):
                result = original(*args)
                assert result == 0
                blocker.mkdir()
                raise KeyboardInterrupt("after successful open")

        real.arct_ddlog_open = Interrupted()
        with patch("archetype_ddlog_preview.ctypes.CDLL", return_value=real):
            with self.assertRaises(ConstructionCleanupError) as raised:
                f.open()
        retained = raised.exception.host
        self.assertNotEqual(retained._handle, 0)
        self.assertIsInstance(raised.exception.original, KeyboardInterrupt)
        blocker.rmdir()
        retained.close()
        f.host = f.open()
        self.assertEqual(f.host.status(id)["state"], "stopped")

    def test_wire_operations_cannot_select_transport(self):
        h = self.f.host
        handle = h._handle
        for op in ("open", "close"):
            with self.assertRaises(NativeError) as raised:
                h.request(op, extra=1)
            self.assertEqual(raised.exception.kind, "request")
            self.assertEqual(h._handle, handle)
            self.assertEqual(h.request("inventory")["worlds"], [])

    def test_persistence_failure_repair_close(self):
        f = self.f
        a = f.world("alpha", f.program())
        id = a["scope"]["native_world"]
        f.host.start(id)
        f.running(a)
        pid = f.host.status(id)["resources"]["pid"]
        hold = f.root / f"hold_commit_{pid}"
        hold.touch()
        key = f.submit(a)
        wait(lambda: (f.root / f"commit_held_{pid}").exists(), bool)
        blocker = f.root / "worlds" / id / "world.json.tmp"
        blocker.mkdir()
        hold.unlink()
        f.frozen(key)
        self.assertIsNotNone(f.host.status(id)["persistence"]["error"])
        with self.assertRaises(NativeError) as raised:
            f.host.close()
        self.assertEqual(raised.exception.kind, "close")
        with self.assertRaises(NativeError):
            f.host.status(id)
        blocker.rmdir()
        f.host.close()
        f.host = f.open()
        self.assertEqual(f.host.status(id)["state"], "stopped")

    def test_close_during_restore(self):
        f = self.f
        a = f.world("alpha", f.program())
        id = a["scope"]["native_world"]
        f.host.start(id)
        f.running(a)
        key = f.submit(a)
        f.frozen(key)
        receipt = f.host.publish(a, key)
        f.confirm(a, key, receipt)
        f.host.stop(id)
        (f.root / "compiler_entered").unlink()
        (f.root / "hold_compile").touch()
        f.host.restore(a, receipt, expected_generation=1)
        wait(lambda: (f.root / "compiler_entered").exists(), bool)
        f.host.close()
        f.host = f.open()
        self.assertEqual(f.host.status(id)["state"], "stopped")

    def test_close_during_compile(self):
        f = self.f
        a = f.world("alpha", f.program())
        (f.root / "hold_compile").touch()
        f.host.start(a["scope"]["native_world"])
        wait(lambda: (f.root / "compiler_entered").exists(), bool)
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            list(pool.map(lambda _: f.host.close(), range(2)))
        f.host = f.open()
        self.assertEqual(f.host.status(a["scope"]["native_world"])["state"], "stopped")

    def test_held_native_does_not_block_sibling_or_stop(self):
        f = self.f
        program = f.program()
        a, b = f.world("alpha", program), f.world("beta", program)
        for binding in (a, b):
            f.host.start(binding["scope"]["native_world"])
            f.running(binding)
        pid = f.host.status(a["scope"]["native_world"])["resources"]["pid"]
        (f.root / f"hold_commit_{pid}").touch()
        ka = f.submit(a)
        wait(lambda: (f.root / f"commit_held_{pid}").exists(), bool)
        kb = f.submit(b)
        f.frozen(kb)
        rb = f.host.publish(b, kb)
        f.confirm(b, kb, rb)
        f.host.stop(a["scope"]["native_world"])
        result = wait(
            lambda: f.host.admission_status(ka["world_id"], ka["generation"], ka["admission_key"]),
            lambda s: s["state"] != "pending",
        )
        self.assertEqual(result["state"], "uncertain")
        self.assertEqual(f.host.read(rb, "label")["rows"], [[ENTITY, "héllo world"]])

    def test_cancelled_publication_waiter_and_two_closes(self):
        f = self.f
        a = f.world("alpha", f.program())
        f.host.start(a["scope"]["native_world"])
        f.running(a)
        key = f.submit(a)
        f.frozen(key)
        # Hold the real catalog write boundary, not a binding-specific hook.
        db = sqlite3.connect(f.root / "storage/catalog.sqlite")
        db.execute("BEGIN IMMEDIATE")

        async def scenario():
            task = asyncio.create_task(asyncio.to_thread(f.host.publish, a, key))
            journal = f.root / "storage/cuts/alpha.run_a.1.json"
            await asyncio.to_thread(wait, journal.exists, bool)
            self.assertFalse(task.done())
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            c1 = asyncio.create_task(asyncio.to_thread(f.host.close))
            c2 = asyncio.create_task(asyncio.to_thread(f.host.close))
            await asyncio.sleep(0.05)
            self.assertFalse(c1.done())
            self.assertFalse(c2.done())
            db.rollback()
            await asyncio.gather(c1, c2)

        try:
            asyncio.run(scenario())
        finally:
            db.rollback()
            db.close()
        f.host = f.open()
        receipt = f.host.reconcile(a, key, tick=1, expected_parent=None)
        f.confirm(a, key, receipt)
        self.assertEqual(f.host.read(receipt, "label")["rows"], [[ENTITY, "héllo world"]])

    def test_recovery_contract(self):
        recovery(self, self.f)


def recovery(test, f):
    program = f.program()
    a, b = f.world("alpha", program), f.world("beta", program)
    for binding in (a, b):
        f.host.start(binding["scope"]["native_world"])
        f.running(binding)
    for bad in (True, False, None, 1.0, 2**63, -(2**63) - 1):
        s = f.host.status(a["scope"]["native_world"])
        with test.assertRaises(ValueError):
            f.host.admit(
                a,
                expected_head=None,
                generation=s["generation"],
                revision=s["revision"],
                key="invalid",
                changes=[{"op": "insert", "predicate": "seed", "values": [bad, "x"]}],
            )
    with test.assertRaises(NativeError):
        f.submit(a, name="nul-rejected", value="unsupported\x00cell")
    ka, kb = f.submit(a), f.submit(b, value="beta")
    f.frozen(ka)
    f.frozen(kb)
    with test.assertRaises(NativeError):
        f.host.confirm(a, ka, tick=1, expected_parent=None)
    first = f.host.publish(a, ka)
    sibling = f.host.publish(b, kb)
    f.confirm(b, kb, sibling)
    test.assertEqual(f.host.read(first, "label")["rows"], [[ENTITY, "héllo world"]])
    test.assertEqual(
        f.host.read(first, "status")["rows"], [[ENTITY, "ready" if f.native else "héllo world"]]
    )
    # Native stop is local. Published-but-unconfirmed evidence survives owner exit.
    f.host.stop(a["scope"]["native_world"])
    test.assertEqual(f.host.status(b["scope"]["native_world"])["state"], "running")
    f.host.close()
    f.host = f.open()
    with test.assertRaises(NativeError):
        f.host.start(a["scope"]["native_world"])
    recovered = f.host.reconcile(a, ka, tick=1, expected_parent=None)
    test.assertEqual(recovered, first)
    f.confirm(a, ka, recovered)
    # Public receipt is consumed only as an exact compact catalog reference.
    wide = dict(recovered, advisory_metadata="x" * (1024 * 1024 + 1))
    test.assertEqual(f.host.read(wide, "label")["rows"], [[ENTITY, "héllo world"]])
    with test.assertRaises(NativeError):
        f.host.read(dict(first, cut_id="0" * 64), "label")
    f.host.restore(a, wide, expected_generation=1)
    test.assertEqual(f.running(a)["generation"], 2)
    key = f.submit(a, name="delete", head=first["cut_id"], delete=True)
    f.frozen(key)
    empty = f.host.publish(a, key)
    f.confirm(a, key, empty)
    for name in ("label", "status"):
        test.assertEqual(f.host.read(empty, name)["rows"], [])
        test.assertEqual(empty["components"][name]["rows"], 0)
    f.host.stop(a["scope"]["native_world"])
    with test.assertRaises(NativeError):
        f.host.restore(a, first, expected_generation=2)
    f.host.restore(a, empty, expected_generation=2)
    test.assertEqual(f.running(a)["generation"], 3)
    key = f.submit(a, name="again", head=empty["cut_id"], value="again")
    f.frozen(key)
    third = f.host.publish(a, key)
    f.confirm(a, key, third)
    test.assertEqual(f.host.read(third, "label")["rows"], [[ENTITY, "again"]])
    test.assertEqual(f.host.read(first, "label")["rows"], [[ENTITY, "héllo world"]])
    test.assertEqual(f.host.history("alpha", "run_a", limit=1)["next_offset"], 1)
    test.assertEqual(f.host.history("alpha", "run_a")["total"], 3)
    test.assertFalse(
        any(
            n == "daft" or n.startswith(("daft.", "archetype.runtime", "archetype.core"))
            for n in sys.modules
        )
    )
    f.close()
    if f.native:
        (f.root / "python-evidence.json").write_text(
            json.dumps(
                {
                    "first": first,
                    "empty": empty,
                    "third": third,
                    "sibling": sibling,
                    "ddlog_revision": f.host.ddlog_revision,
                },
                indent=2,
            )
        )
        print(f"ACTUAL DDLOG + ICEBERG Python evidence: {f.root}", flush=True)


@unittest.skipUnless(
    os.environ.get("ARCHETYPE_DDLOG_DRIVER"), "actual installed DDlog compiler opt-in"
)
class NativeBindingTests(unittest.TestCase):
    def test_actual_ddlog_iceberg_recovery(self):
        f = Fixture(native=True)
        self.addCleanup(f.close)
        recovery(self, f)


if __name__ == "__main__":
    unittest.main()
