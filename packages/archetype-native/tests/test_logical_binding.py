"""Logical identities through the real C ABI/Iceberg, simulated compiler only."""

import concurrent.futures
import copy
import json
import os
import unittest

from archetype_native import NativeError
from archetype_native.ingress import Component, LogicalResource, _project
from archetype_native.wire import Fork, Receipt
from test_binding import COMPONENTS, Fixture, wait


def logical(host, op, **kwargs):
    return host.request("logical", request={"op": op, **kwargs})


def program(f):
    exact = f.program()
    # Fixture setup only: clone its authored composition into registry-owned creation.
    record = (
        f.root / "registry" / exact["processor_id"] / "versions" / f"{exact['version'][7:]}.json"
    )
    request = {
        "resource": "pipeline",
        "request_key": "publish_pipeline",
        "description": "Pipeline",
        "definition": json.loads(record.read_text())["definition"],
        "git_provenance": None,
        "lowering_version": 2,
    }
    result = logical(f.host, "program_publish", request=request)
    return {"resource": "pipeline", "processor": result["processor"]}, request


def destination(name):
    return {"resource": name, "world": name, "run": "run_a"}


class LogicalBindingTests(unittest.TestCase):
    def setUp(self):
        self.f = Fixture()
        self.addCleanup(self.f.close)

    def test_fresh_exact_retries_cold_identity_conflicts_and_no_compiler(self):
        f = self.f
        selected, publication = program(f)
        args = {
            "destination": destination("fresh"),
            "request_key": "create_fresh",
            "label": "Fresh",
            "program": selected,
            "declarations": {"components": COMPONENTS, "inputs": {"seed": ["int", "string"]}},
        }
        with concurrent.futures.ThreadPoolExecutor(3) as pool:
            results = list(pool.map(lambda _: logical(f.host, "world_create", **args), range(3)))
        first = results[0]
        self.assertTrue(all(r["creation"] == first["creation"] for r in results))
        self.assertTrue(first["creation"]["context_confirmed"])
        self.assertEqual(first["status"]["generation"], 0)
        self.assertFalse((f.root / "compiler_entered").exists())
        self.assertEqual(len(f.host.request("inventory")["worlds"]), 1)
        f.host.close()
        f.host = f.open()
        self.assertEqual(
            logical(f.host, "world_resolve", destination=args["destination"])["creation"],
            first["creation"],
        )
        self.assertEqual(logical(f.host, "world_create", **args)["creation"], first["creation"])
        self.assertEqual(
            logical(f.host, "program_publish", request=publication)["processor"],
            selected["processor"],
        )
        for changed in (
            {"label": "Changed"},
            {"request_key": "different"},
            {"destination": {**destination("fresh"), "world": "other"}},
        ):
            with self.assertRaises(NativeError):
                logical(f.host, "world_create", **(args | changed))
        invalid = copy.deepcopy(args)
        invalid["destination"] = destination("invalid")
        invalid["request_key"] = "invalid"
        invalid["declarations"]["inputs"]["seed"] = ["string", "string"]
        with self.assertRaises(NativeError):
            logical(f.host, "world_create", **invalid)
        f.host.publish_collection("collection", "run_a")
        with self.assertRaises(NativeError):
            logical(
                f.host,
                "world_create",
                **(args | {"destination": destination("collection"), "request_key": "collection"}),
            )
        self.assertEqual(len(f.host.request("inventory")["worlds"]), 1)

    def test_fork_context_before_origin_cold_retry_and_lost_ready_origin(self):
        f = self.f
        parent = f.world("parent", f.program())
        f.host.start(parent["scope"]["native_world"])
        f.running(parent)
        key = f.submit(parent)
        f.frozen(key)
        receipt = f.host.publish(parent, key)
        f.confirm(parent, key, receipt)
        args = {
            "source_binding": parent,
            "receipt": {k: receipt[k] for k in ("world", "run", "tick", "cut_id")},
            "destination": destination("child"),
            "request_key": "fork_child",
            "label": "Child",
            "inputs": {"seed": ["int", "string"]},
            "expected_generation": 0,
        }
        origin = f.root / "storage/origins/child.run_a.json"
        blocker = origin.with_suffix(f".{os.getpid()}.tmp")
        blocker.mkdir()
        with self.assertRaises(NativeError):
            logical(f.host, "world_fork", **args)
        pending = logical(f.host, "world_resolve", destination=args["destination"])
        self.assertTrue(pending["creation"]["context_confirmed"])
        self.assertFalse(pending["status"]["external_publication"]["fork"]["ready"])
        self.assertFalse(origin.exists())
        f.host.close()
        f.host = f.open()
        self.assertEqual(
            logical(f.host, "world_resolve", destination=args["destination"])["creation"],
            pending["creation"],
        )
        blocker.rmdir()
        ready = wait(
            lambda: logical(f.host, "world_fork", **args),
            lambda r: r["status"]["external_publication"]["fork"]["ready"],
        )
        self.assertEqual(ready["creation"]["reservation"], pending["creation"]["reservation"])
        configured = LogicalResource(
            "child",
            "child",
            "run_a",
            tuple(
                Component(c["name"], c["output"], tuple(c["fields"]), c["entity_field"])
                for c in COMPONENTS
            ),
            (("seed", ("int64", "string")),),
        )
        operation = Fork(
            "parent",
            Receipt(receipt["world"], receipt["run"], receipt["tick"], receipt["cut_id"]),
            "fork_child",
            0,
        )
        projected = _project(configured, operation, ready)
        self.assertTrue(projected["lineage_ready"])
        self.assertEqual(projected["source"]["cut_id"], receipt["cut_id"])
        bad_source = copy.deepcopy(ready)
        bad_source["origin"]["source"]["cut_id"] = "0" * 64
        bad_reservation = copy.deepcopy(ready)
        bad_reservation["status"]["external_publication"]["fork"]["reservation"]["request_key"] = (
            "unrelated"
        )
        for reply in (bad_source, bad_reservation):
            with self.assertRaises(ValueError):
                _project(configured, operation, reply)
        saved = origin.read_bytes()
        origin.unlink()
        with self.assertRaises(NativeError):
            logical(f.host, "world_resolve", destination=args["destination"])
        origin.write_bytes(b"{}")
        with self.assertRaises(NativeError):
            logical(f.host, "world_resolve", destination=args["destination"])
        origin.write_bytes(saved)
        f.host.close()
        f.host = f.open()
        restored = logical(f.host, "world_resolve", destination=args["destination"])
        self.assertEqual(restored["creation"], ready["creation"])
        self.assertEqual(f.host.history("child", "run_a")["total"], 1)
