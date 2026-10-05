"""Actual C ABI/Iceberg type round trips; compiler fixture is explicitly labeled."""

import asyncio
import json
import math
import os
import struct
import unittest
from unittest.mock import patch

from archetype_ddlog_preview import NativeError
from archetype_ddlog_preview.ingress import Grant, Ingress, Resource
from archetype_ddlog_preview.values import float_bits
from test_binding import ENTITY, Fixture, pin, wait
from test_ingress import CAPS, TOKEN, directory, request

COMPONENTS = [
    {
        "name": "live",
        "output": "out",
        "fields": ["entity_id", "enabled", "value"],
        "entity_field": 0,
    }
]
VALUES = [
    0.0,
    1.0,
    math.nextafter(1.0, 2.0),
    float.fromhex("0x1.fffffffffffffp1023"),
    -float.fromhex("0x1.fffffffffffffp1023"),
    float.fromhex("0x0.0000000000001p-1022"),
    -float.fromhex("0x0.0000000000001p-1022"),
]


def bits(value):
    return struct.pack(">d", value).hex()


def exercise(test, native):
    runner = asyncio.Runner()
    test.addCleanup(runner.close)
    f = Fixture(native=native)
    test.addCleanup(f.close)
    selected = pin(
        f.host.register(
            "Live types",
            {
                "rules": "out(E,B,D) :- seed(E,B,D).",
                "schemas": {
                    "seed": {"input": True, "fields": ["int", "bool", "double"]},
                    "out": {"input": False, "fields": ["int", "bool", "double"]},
                },
                "interface": {"inputs": ["seed"], "outputs": ["out"]},
            },
        )
    )
    selected = pin(
        f.host.register(
            "Composed live types",
            {
                "composition": {
                    "nodes": {"first": selected, "second": selected},
                    "inputs": {
                        "seed": {
                            "fields": ["int", "bool", "double"],
                            "targets": [{"node": "first", "relation": "seed"}],
                        }
                    },
                    "bindings": [
                        {
                            "from": {"node": "first", "relation": "out"},
                            "to": {"node": "second", "relation": "seed"},
                        }
                    ],
                    "outputs": {"out": {"node": "second", "relation": "out"}},
                }
            },
        )
    )
    id = f.host.create("typed", selected, ["out"])
    binding = f.host.bind({"native_world": id, "world": "typed", "run": "run_a"}, COMPONENTS)
    f.host.start(id)
    f.running(binding)
    ingress = Ingress(
        f.host,
        verifier=directory(),
        resources=(
            Resource.from_binding("typed", binding, inputs={"seed": ("int64", "bool", "float64")}),
        ),
        grants=(Grant("agent", "typed", CAPS),),
    )
    rows = [[ENTITY + i, i % 2 == 0, v] for i, v in enumerate(VALUES)]
    args = {
        "generation": "1",
        "revision": "1",
        "admission_key": "typed_first",
        "expected_head": None,
        "changes": [
            {
                "op": "insert",
                "predicate": "seed",
                "values": [{"int64": str(e)}, {"bool": b}, {"float64": float_bits(v)}],
            }
            for e, b, v in rows
        ],
    }

    async def send(arguments):
        return json.loads(await ingress.invoke(TOKEN, request("admit", arguments, "typed")))

    result = runner.run(send(args))
    test.assertTrue(result["ok"], result)
    key = {
        "world_id": id,
        "generation": 1,
        "admission_key": "typed_first",
        "request_sha256": result["value"]["boundary"]["request_sha256"],
    }
    f.frozen(key)
    # A local opposite-zero retry binds the exact same native admission digest.
    changes = [{"op": "insert", "predicate": "seed", "values": r.copy()} for r in rows]
    changes[0]["values"][2] = -0.0
    retry = f.host.admit(
        binding, expected_head=None, generation=1, revision=1, key="typed_first", changes=changes
    )
    test.assertEqual(retry["boundary"]["key"], key)
    cut = f.host.publish(binding, key)
    f.confirm(binding, key, cut)
    observed = f.host.read(cut, "live")["rows"]
    expected = {e: (b, bits(v)) for e, b, v in rows}
    test.assertEqual({e: (b, bits(v)) for e, b, v in observed}, expected)
    # Schema-invalid direct input cannot advance the native revision.
    before = f.host.status(id)["revision"]
    for invalid in [1, True, "1.0", None]:
        with test.assertRaises((ValueError, NativeError)):
            f.host.admit(
                binding,
                expected_head=cut["cut_id"],
                generation=1,
                revision=before,
                key="bad",
                changes=[{"op": "insert", "predicate": "seed", "values": [999, True, invalid]}],
            )
        test.assertEqual(f.host.status(id)["revision"], before)
    # Invalid tagged cells are rejected before invoking the configured Host.
    for invalid in [
        {"float64": "8000000000000000"},
        {"float64": "7ff0000000000000"},
        {"bool": True},
        {"bool": 1},
    ]:
        bad = json.loads(json.dumps(args))
        bad["changes"][0]["values"][2] = invalid
        with patch.object(f.host, "admit", side_effect=AssertionError("invalid wire dispatched")):
            test.assertFalse(runner.run(send(bad))["ok"])
    # Restore the nonempty historical cut into another world, retain inherited
    # typed fields, then publish both inherited and fresh values.
    fork_args = {
        "world": "typed_child",
        "run": "run_a",
        "label": "Child",
        "request_key": "typed_child",
    }
    wait(
        lambda: f.host.fork(binding, cut, **fork_args),
        lambda r: r["status"]["external_publication"]["fork"]["ready"],
    )
    child = f.host.fork_binding("typed_child", "run_a", COMPONENTS)
    status = f.host.status(child["scope"]["native_world"])
    admitted = f.host.admit(
        child,
        expected_head=cut["cut_id"],
        generation=status["generation"],
        revision=status["revision"],
        key="child_added",
        changes=[{"op": "insert", "predicate": "seed", "values": [999, False, 1.25]}],
    )
    child_key = admitted["boundary"]["key"]
    f.frozen(child_key)
    child_cut = f.host.publish(child, child_key)
    f.confirm(child, child_key, child_cut)
    test.assertEqual(
        {e: (b, bits(v)) for e, b, v in f.host.read(child_cut, "live")["rows"]},
        expected | {999: (False, bits(1.25))},
    )
    # Deleting the zero with its opposite sign retracts it with every other row.
    changes = [{"op": "delete", "predicate": "seed", "values": r.copy()} for r in rows]
    changes[0]["values"][2] = -0.0
    status = f.host.status(id)
    empty_key = f.host.admit(
        binding,
        expected_head=cut["cut_id"],
        generation=1,
        revision=status["revision"],
        key="typed_empty",
        changes=changes,
    )["boundary"]["key"]
    f.frozen(empty_key)
    empty = f.host.publish(binding, empty_key)
    f.confirm(binding, empty_key, empty)
    test.assertEqual(f.host.read(empty, "live")["rows"], [])
    runner.run(ingress.drain())
    f.host.close()
    # Preserve proof sources and binaries while removing their original paths
    # from the cold reader's environment.
    (f.root / "registry").rename(f.root / "retained-registry")
    (f.root / "worlds").rename(f.root / "retained-worlds")
    f.host = f.open()
    test.assertEqual({e: (b, bits(v)) for e, b, v in f.host.read(cut, "live")["rows"]}, expected)
    test.assertEqual(f.host.read(empty, "live")["rows"], [])
    test.assertEqual(
        {e: (b, bits(v)) for e, b, v in f.host.read(child_cut, "live")["rows"]},
        expected | {999: (False, bits(1.25))},
    )
    print(
        f"{'ACTUAL DDLOG' if native else 'SIMULATED COMPILER'} C ABI/Iceberg live-value proof: {f.root}",
        flush=True,
    )


class LiveBindingTests(unittest.TestCase):
    def test_simulated_compiler_actual_cabi_iceberg_live_cells(self):
        exercise(self, False)


@unittest.skipUnless(
    os.environ.get("ARCHETYPE_DDLOG_DRIVER"), "requires real operator-provided DDlog driver"
)
class ActualLiveBindingTests(unittest.TestCase):
    def test_actual_compiler_cabi_iceberg_wire_fork_and_cold_read(self):
        exercise(self, True)
