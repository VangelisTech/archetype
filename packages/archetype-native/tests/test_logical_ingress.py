"""Logical program/world admission through the shared authorized boundary."""

import asyncio
import copy
import json
import threading
import unittest
from dataclasses import replace

from archetype_native.ingress import Grant, Ingress, LogicalResource, ProgramResource
from archetype_native.programs import Composition, LeafProgram, Relation
from archetype_native.wire import Request
from test_ingress import CAPS, TOKEN, directory, request, resource


def logical_resource(name="alpha"):
    old = resource()
    return LogicalResource(name, name, "run_a", old.components, old.inputs)


def program_ref(name, processor=None):
    return {"resource": name, "processor_id": processor or name, "version": "sha256:" + "a" * 64}


def leaf():
    return {
        "rules": "label(E,N) :- seed(E,N).\n",
        "schemas": [
            {"name": "seed", "input": True, "fields": ["int64", "string"]},
            {"name": "label", "input": False, "fields": ["int64", "string"]},
        ],
        "inputs": ["seed"],
        "outputs": ["label"],
    }


def composition(a=None, b=None):
    return {
        "nodes": [
            {"name": "first", "program": a or program_ref("first")},
            {"name": "second", "program": b or program_ref("second")},
        ],
        "inputs": [
            {
                "name": "seed",
                "fields": ["int64", "string"],
                "targets": [
                    {"node": "first", "relation": "seed"},
                    {"node": "second", "relation": "seed"},
                ],
            }
        ],
        "bindings": [],
        "outputs": [
            {"name": "labels", "source": {"node": "first", "relation": "label"}},
            {"name": "other", "source": {"node": "second", "relation": "label"}},
        ],
    }


class Backend:
    def __init__(self):
        self.calls = []
        self.entered, self.release = threading.Event(), threading.Event()
        self.release.set()

    def request(self, op, **args):
        self.calls.append((op, args))
        self.entered.set()
        if not self.release.wait(5):
            raise AssertionError("Test barrier timed out")
        req = args["request"]
        if req["op"] == "program_publish":
            req = req["request"]
        return {
            "resource": req["resource"],
            "request_key": req.get("request_key", "publish"),
            "request_sha256": "b" * 64,
            "processor": {"processor_id": req["resource"], "version": "sha256:" + "a" * 64},
            "phase": "published",
        }


class LogicalIngressTests(unittest.IsolatedAsyncioTestCase):
    async def test_omitted_declared_input_is_rejected_before_dispatch(self):
        definition = leaf()
        definition["inputs"] = []
        raw = request(
            "program_create",
            {"request_key": "publish", "description": "First", "definition": definition},
            "first",
        )
        constructors = (
            lambda: LeafProgram(
                definition["rules"],
                tuple(Relation.decode(r) for r in definition["schemas"]),
                (),
                ("label",),
            ),
            lambda: Request.decode(raw),
        )
        for construct in constructors:
            with self.subTest(construct=construct), self.assertRaises(ValueError):
                construct()
        backend = Backend()
        ingress = Ingress(
            backend,
            verifier=directory(),
            resources=(ProgramResource("first"),),
            grants=(Grant("agent", "first", CAPS),),
        )
        result = json.loads(await ingress.invoke(TOKEN, raw))
        self.assertEqual(
            result.get("error"), {"code": "invalid_request", "outcome": "not_dispatched"}
        )
        self.assertEqual(backend.calls, [])
        await ingress.drain()

    async def test_all_program_grants_precede_every_lookup_even_absent_names(self):
        class NoLookup:
            def __getitem__(self, key):
                raise AssertionError("Configuration consulted before every grant")

        configured = (
            logical_resource(),
            *(ProgramResource(n) for n in ("pipeline", "first", "second")),
        )
        cases = [
            (
                request(
                    "create",
                    {"request_key": "create", "label": "Alpha", "program": program_ref("first")},
                ),
                ("alpha", "first"),
            ),
            (
                request(
                    "program_compose",
                    {
                        "request_key": "compose",
                        "description": "Pipeline",
                        "composition": composition(),
                    },
                    "pipeline",
                ),
                ("pipeline", "first", "second"),
            ),
        ]
        for raw, names in cases:
            for missing in names:
                backend = Backend()
                ingress = Ingress(
                    backend,
                    verifier=directory(),
                    resources=configured,
                    grants=tuple(Grant("agent", n, CAPS) for n in names if n != missing),
                )
                ingress._resources = NoLookup()
                for payload in (raw, raw.replace(b'"second"', b'"absent"')):
                    result = json.loads(await ingress.invoke(TOKEN, payload))
                    self.assertEqual(
                        result["error"], {"code": "forbidden", "outcome": "not_dispatched"}
                    )
                self.assertEqual(backend.calls, [])
                await ingress.drain()
        # Exact grants alone cannot supplement missing principal capability.
        ingress = Ingress(
            Backend(),
            verifier=directory(frozenset({"simulation:create"})),
            resources=configured,
            grants=tuple(Grant("agent", r.name, CAPS) for r in configured),
        )
        ingress._resources = NoLookup()
        self.assertEqual(
            json.loads(await ingress.invoke(TOKEN, cases[0][0]))["error"]["code"], "forbidden"
        )
        await ingress.drain()

    async def test_protected_name_does_not_authorize_an_unrelated_exact_pin(self):
        backend = Backend()
        ingress = Ingress(
            backend,
            verifier=directory(),
            resources=tuple(ProgramResource(n) for n in ("pipeline", "first", "second")),
            grants=tuple(Grant("agent", n, CAPS) for n in ("pipeline", "first", "second")),
        )
        bad = composition(b=program_ref("second", "unrelated"))
        result = json.loads(
            await ingress.invoke(
                TOKEN,
                request(
                    "program_compose",
                    {"request_key": "compose", "description": "Pipeline", "composition": bad},
                    "pipeline",
                ),
            )
        )
        self.assertEqual(result["error"]["code"], "operation_failed")
        self.assertEqual(
            [v[1]["request"]["op"] for v in backend.calls], ["program_resolve", "program_resolve"]
        )
        await ingress.drain()

    async def test_cancelled_program_creation_keeps_capacity_and_drain(self):
        backend = Backend()
        backend.release.clear()
        ingress = Ingress(
            backend,
            verifier=directory(),
            resources=(ProgramResource("first"),),
            grants=(Grant("agent", "first", CAPS),),
            max_inflight=1,
        )
        raw = request(
            "program_create",
            {"request_key": "publish", "description": "First", "definition": leaf()},
            "first",
        )
        task = asyncio.create_task(ingress.invoke(TOKEN, raw))
        await asyncio.to_thread(backend.entered.wait, 2)
        self.assertTrue(backend.entered.is_set())
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(json.loads(await ingress.invoke(TOKEN, raw))["error"]["code"], "busy")
        draining = asyncio.create_task(ingress.drain())
        await asyncio.sleep(0)
        self.assertFalse(draining.done())
        backend.release.set()
        await draining
        self.assertEqual(len(backend.calls), 1)

    def test_closed_models_and_mixed_execution_aliases(self):
        definition = leaf()
        definition["composition"] = composition()
        with self.assertRaises(ValueError):
            Request.decode(
                request(
                    "program_create",
                    {"request_key": "publish", "description": "First", "definition": definition},
                    "first",
                )
            )
        with self.assertRaises(ValueError):
            LeafProgram(
                "label(E,N) :- seed(E,N).",
                [Relation("seed", True, ("int64", "string"))],
                (),
                ("label",),
            )
        with self.assertRaises(ValueError):
            Composition([], (), (), ())
        with self.assertRaises(ValueError):
            Ingress(
                Backend(),
                verifier=directory(),
                resources=(resource(), replace(logical_resource(), name="alias")),
                grants=(),
            )

    async def test_real_bridge_program_creation_composition_cold_world_identity(self):
        from test_binding import Fixture

        f = Fixture()
        self.addCleanup(f.close)
        configured = (
            logical_resource(),
            *(ProgramResource(n) for n in ("first", "second", "pipeline")),
        )
        grants = tuple(Grant("agent", r.name, CAPS) for r in configured)
        ingress = Ingress(f.host, verifier=directory(), resources=configured, grants=grants)

        async def invoke(op, args, name):
            result = json.loads(await ingress.invoke(TOKEN, request(op, args, name)))
            self.assertTrue(result["ok"], result)
            return result["value"]

        a = await invoke(
            "program_create",
            {"request_key": "first", "description": "First", "definition": leaf()},
            "first",
        )
        b = await invoke(
            "program_create",
            {"request_key": "second", "description": "Second", "definition": leaf()},
            "second",
        )
        pipeline = await invoke(
            "program_compose",
            {
                "request_key": "pipeline",
                "description": "Pipeline",
                "composition": composition(a["program"], b["program"]),
            },
            "pipeline",
        )
        description = await invoke("program_describe", {}, "pipeline")
        self.assertEqual(description["program"], pipeline["program"])
        self.assertEqual({r["name"] for r in description["relations"]}, {"seed", "labels", "other"})
        self.assertNotIn("physical", json.dumps(description))
        self.assertNotIn("rules", json.dumps(description))
        args = {"request_key": "alpha", "label": "Alpha", "program": pipeline["program"]}
        created = await invoke("create", args, "alpha")
        self.assertEqual(created["generation"], "0")
        self.assertTrue(created["context_ready"])
        self.assertFalse((f.root / "compiler_entered").exists())
        self.assertEqual(created, await invoke("create", args, "alpha"))
        self.assertNotIn("native", json.dumps(created))
        native_ids = {r["id"] for r in f.host.request("inventory")["worlds"]}
        self.assertFalse(any(n in json.dumps(created) for n in native_ids))
        self.assertEqual((await invoke("status", {}, "alpha"))["generation"], "0")
        await ingress.drain()
        f.host.close()
        f.host = f.open()
        ingress = Ingress(f.host, verifier=directory(), resources=configured, grants=grants)
        cold = await invoke("resolve", {}, "alpha")
        for key in ("destination", "request_sha256", "program", "context_id", "context_ready"):
            self.assertEqual(cold[key], created[key])
        self.assertEqual(await invoke("program_resolve", {}, "pipeline"), pipeline)
        conflicting = copy.deepcopy(args)
        conflicting["label"] = "Different"
        failed = json.loads(await ingress.invoke(TOKEN, request("create", conflicting)))
        self.assertEqual(failed["error"]["code"], "operation_failed")
        await ingress.drain()
