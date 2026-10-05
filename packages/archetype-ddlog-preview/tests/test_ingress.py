"""Offline ingress contracts: real principal verifier, explicitly simulated backend.

The final test uses the existing C ABI + real Iceberg + simulated native driver.
No network host, credential file or DDlog compiler is started.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import sys
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from archetype_ddlog_preview import NativeError
from archetype_ddlog_preview.ingress import ContextResource, Grant, Ingress, Resource
from archetype_ddlog_preview.wire import CAPABILITIES, MAX_REQUEST_BYTES, Request

# Reuse the actual stdlib verifier from source without importing app/wiring.
REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "packages/archetype-ecs/src"))
from archetype.api.principals import PrincipalDirectory  # noqa: E402

TOKEN = "service-credential-" + "A" * 32  # existing principal-test fixture
OTHER = "service-credential-" + "B" * 32
CAPS = frozenset(CAPABILITIES.values())
DIGEST = "a" * 64
BINDING = {
    "scope": {"native_world": "native-a", "world": "alpha", "run": "run_a"},
    "components": [
        {"name": "label", "output": "labels", "fields": ["entity_id", "name"], "entity_field": 0}
    ],
}


def directory(caps=CAPS, **overrides):
    # In-memory test verifiers only; no runtime configuration or access is created.
    return PrincipalDirectory.from_provisioning(
        (
            {
                "id": "agent",
                "credential_sha256": hashlib.sha256(TOKEN.encode()).hexdigest(),
                "capabilities": list(caps),
                **overrides,
            },
            {
                "id": "other",
                "credential_sha256": hashlib.sha256(OTHER.encode()).hexdigest(),
                "capabilities": list(CAPS),
            },
        ),
        {},
    )


def resource(name="alpha", binding=None):
    return Resource.from_binding(name, binding or BINDING, inputs={"seed": ("int64", "string")})


def request(op="status", args=None, name="alpha"):
    return json.dumps(
        {"version": 1, "operation": op, "resource": name, "arguments": args or {}}
    ).encode()


def admit_args(integer="9007199254741109"):
    return {
        "generation": "1",
        "revision": "0",
        "admission_key": "first",
        "expected_head": None,
        "changes": [
            {
                "op": "insert",
                "predicate": "seed",
                "values": [{"int64": integer}, {"string": "héllo world"}],
            }
        ],
    }


class Backend:
    """Recording test double, never a verifier or replacement native engine."""

    def __init__(self):
        self.calls = []
        self.entered, self.release = threading.Event(), threading.Event()
        self.release.set()
        self.result = {
            "id": "native-a",
            "state": "running",
            "generation": 2**53 + 1,
            "revision": 2**53 + 117,
            "error": "/private/operator/path",
            "definition": {"secret": "source"},
            "build": {"log_tail": "secret"},
        }

    def status(self, native_id):
        self.calls.append(("status", native_id))
        self.entered.set()
        if not self.release.wait(5):
            raise AssertionError("Test barrier timed out")
        return self.result


class IngressTests(unittest.IsolatedAsyncioTestCase):
    async def test_context_and_source_grants_precede_every_lookup(self):
        class NoLookup:
            def __getitem__(self, key):
                raise AssertionError("Unauthorized context lookup")

        context = ContextResource("context", "alpha", "run_a", source_resource="alpha")
        caps = frozenset({"artifacts:publish", "artifacts:read"})
        for grant_names in [(), ("context",), ("alpha",)]:
            ingress = Ingress(
                Backend(),
                verifier=directory(caps),
                resources=(context, resource()),
                grants=tuple(Grant("agent", name, caps) for name in grant_names),
            )
            ingress._resources = NoLookup()
            result = json.loads(
                await ingress.invoke(
                    TOKEN, request("publish_context", {"source_resource": "alpha"}, "context")
                )
            )
            self.assertEqual(result["error"], {"code": "forbidden", "outcome": "not_dispatched"})
            await ingress.drain()
        ingress = Ingress(
            Backend(), verifier=directory(caps), resources=(context, resource()), grants=()
        )
        ingress._resources = NoLookup()
        result = json.loads(await ingress.invoke(TOKEN, request("read_context", {}, "context")))
        self.assertEqual(result["error"]["code"], "forbidden")
        await ingress.drain()

    async def test_hosted_context_origin_is_pinned_before_backend_dispatch(self):
        backend = Backend()
        context = ContextResource("context", "alpha", "run_a", source_resource="alpha")
        other = replace(resource(), name="other", native_world="native-other", world="other")
        caps = frozenset({"artifacts:publish"})
        ingress = Ingress(
            backend,
            verifier=directory(caps),
            resources=(context, resource(), other),
            grants=tuple(Grant("agent", name, caps) for name in ("context", "alpha", "other")),
        )
        for source in (None, "other"):
            result = json.loads(
                await ingress.invoke(
                    TOKEN, request("publish_context", {"source_resource": source}, "context")
                )
            )
            self.assertEqual(
                result["error"], {"code": "invalid_request", "outcome": "not_dispatched"}
            )
        self.assertEqual(backend.calls, [])
        with self.assertRaises(ValueError):
            Ingress(
                backend,
                verifier=directory(caps),
                resources=(replace(context, source_resource=None), resource()),
                grants=(),
            )
        await ingress.drain()

    async def test_context_projection_strips_paths_and_preserves_exact_attribution(self):
        class ContextBackend:
            def context(self, world, run):
                return {
                    "version": 1,
                    "world": world,
                    "run": run,
                    "context_id": DIGEST,
                    "origin": {
                        "kind": "hosted",
                        "evidence": {"native_id": "private-native", "path": "/private/operator"},
                    },
                }

            def request(self, operation, **kwargs):
                return {
                    "items": [
                        {
                            "receipt": {
                                "version": 1,
                                "artifact_id": "occurrence-1",
                                "target": {"context": kwargs["context"], "exact_cut": None},
                                "common": {"object": "/private/object"},
                            },
                            "common": "private-parquet",
                            "typed": {"text": "private-metadata"},
                            "sha256": "b" * 64,
                            "media_type": "text/plain",
                            "size_bytes": 2**53 + 1,
                        }
                    ],
                    "total": 1,
                    "next_offset": None,
                }

        context = ContextResource("context", "alpha", "run_a")
        caps = frozenset({"artifacts:read"})
        ingress = Ingress(
            ContextBackend(),
            verifier=directory(caps),
            resources=(context,),
            grants=(Grant("agent", "context", caps),),
        )
        result = json.loads(await ingress.invoke(TOKEN, request("read_context", {}, "context")))
        self.assertEqual(
            result["value"],
            {"world": "alpha", "run": "run_a", "context_id": DIGEST, "origin": "hosted"},
        )
        args = {"context_id": DIGEST, "exact_cut": None, "all": True, "offset": "0", "limit": "32"}
        result = json.loads(
            await ingress.invoke(TOKEN, request("context_artifacts", args, "context"))
        )
        self.assertNotIn("private", json.dumps(result))
        self.assertEqual(result["value"]["items"][0]["size_bytes"], str(2**53 + 1))
        self.assertIsNone(result["value"]["items"][0]["exact_cut"])
        args["all"] = False
        args["exact_cut"] = {"tick": "1", "cut_id": "c" * 64}
        result = json.loads(
            await ingress.invoke(TOKEN, request("context_artifacts", args, "context"))
        )
        self.assertEqual(result["error"], {"code": "operation_failed", "outcome": "unknown"})
        args["exact_cut"] = {"tick": 1, "cut_id": "c" * 64}
        self.assertEqual(
            json.loads(await ingress.invoke(TOKEN, request("context_artifacts", args, "context")))[
                "error"
            ]["code"],
            "invalid_request",
        )
        await ingress.drain()

    async def test_fork_requires_both_exact_grants_before_either_lookup(self):
        class NoLookup:
            def __getitem__(self, key):
                raise AssertionError("Unauthorized fork lookup")

        dest = replace(resource(), name="child", native_world=None, world="child")
        args = {
            "source_resource": "alpha",
            "receipt": {"world": "alpha", "run": "run_a", "tick": "1", "cut_id": DIGEST},
            "request_key": "fork_one",
            "expected_generation": "0",
        }
        for caps, grants in (
            (CAPS, (Grant("agent", "child", CAPS),)),
            (CAPS, (Grant("agent", "alpha", CAPS),)),
            (
                CAPS - {"simulation:fork"},
                (Grant("agent", "alpha", CAPS), Grant("agent", "child", CAPS)),
            ),
        ):
            ingress = self.make(
                verifier=directory(caps), resources=(resource(), dest), grants=grants
            )
            ingress._resources = NoLookup()
            result = json.loads(await ingress.invoke(TOKEN, request("fork", args, "child")))
            self.assertEqual(result["error"], {"code": "forbidden", "outcome": "not_dispatched"})
        self.assertEqual(self.backend.calls, [])
        for changed in (
            {"expected_generation": 0},
            {"expected_generation": "18446744073709551616"},
            {"checkpoint": "private"},
        ):
            with self.assertRaises(ValueError):
                Request.decode(request("fork", {**args, **changed}, "child"))

    async def test_fork_projection_hides_native_identity_and_checks_exact_result(self):
        dest = replace(resource(), name="child", native_world=None, world="child")
        args = {
            "source_resource": "alpha",
            "receipt": {"world": "alpha", "run": "run_a", "tick": "1", "cut_id": DIGEST},
            "request_key": "fork_one",
            "expected_generation": "0",
        }
        reservation = {
            "request_key": "fork_one",
            "destination": {"world": "child", "run": "run_a"},
            "child_world_id": "private-child",
        }
        raw = {
            "request_key": "fork_one",
            "destination": reservation["destination"],
            "source": {**args["receipt"], "tick": 1},
            "origin": {
                "reservation": reservation,
                "lineage_sha256": DIGEST,
                "secret": "/private/checkpoint",
            },
            "status": {
                "id": "private-child",
                "state": "starting",
                "generation": 2**53 + 1,
                "external_publication": {"fork": {"ready": False, "reservation": reservation}},
            },
        }
        calls = []

        def fork(binding, receipt, **kw):
            calls.append((binding, receipt, kw))
            return raw

        self.backend.fork = fork
        ingress = self.make(
            resources=(resource(), dest),
            grants=(Grant("agent", "alpha", CAPS), Grant("agent", "child", CAPS)),
        )
        result = json.loads(await ingress.invoke(TOKEN, request("fork", args, "child")))
        self.assertTrue(result["ok"], result)
        self.assertFalse(result["value"]["lineage_ready"])
        self.assertEqual(result["value"]["generation"], str(2**53 + 1))
        self.assertNotIn("private", json.dumps(result))
        self.assertEqual(calls[0][0], BINDING)
        raw["source"] = {**raw["source"], "tick": 2}
        result = json.loads(await ingress.invoke(TOKEN, request("fork", args, "child")))
        self.assertEqual(result["error"]["outcome"], "unknown")

    def setUp(self):
        self.backend = Backend()
        self.ingress = self.make()

    def make(self, *, verifier=None, grants=None, resources=None, max_inflight=4):
        return Ingress(
            self.backend,
            verifier=verifier or directory(),
            resources=resources or (resource(),),
            grants=(Grant("agent", "alpha", CAPS),) if grants is None else grants,
            max_inflight=max_inflight,
        )

    async def invoke(self, op="status", args=None, name="alpha", token=TOKEN):
        return json.loads(await self.ingress.invoke(token, request(op, args, name)))

    async def test_real_verifier_missing_wrong_revoked_expired(self):
        for token in (None, "admin", "Bearer admin", "x" * 32, "x" * 4097):
            result = await self.ingress.invoke(token, request())
            self.assertEqual(json.loads(result)["error"]["code"], "unauthenticated")
        for options in ({"revoked": True}, {"expires_at": "2000-01-01T00:00:00Z"}):
            self.ingress = self.make(verifier=directory(**options))
            self.assertEqual((await self.invoke())["error"]["code"], "unauthenticated")
        self.assertEqual(self.backend.calls, [])
        with self.assertRaises(ValueError):
            self.make(verifier=PrincipalDirectory.empty())

    async def test_both_capability_and_resource_grants_before_lookup(self):
        class NoLookup:
            def __getitem__(self, key):
                raise AssertionError("Unauthorized resource lookup")

        for verifier, grants, token, name in (
            (directory(frozenset()), (Grant("agent", "alpha", CAPS),), TOKEN, "alpha"),
            (directory(), (), TOKEN, "alpha"),
            (directory(), (Grant("agent", "alpha", CAPS),), OTHER, "alpha"),
            (directory(), (Grant("agent", "alpha", CAPS),), TOKEN, "beta"),
        ):
            self.ingress = self.make(verifier=verifier, grants=grants)
            self.ingress._resources = NoLookup()
            result = await self.invoke(token=token, name=name)
            self.assertEqual(result["error"], {"code": "forbidden", "outcome": "not_dispatched"})
        self.assertEqual(self.backend.calls, [])

    async def test_control_and_confirmation_require_separate_capabilities(self):
        self.ingress = self.make(
            verifier=directory(frozenset({"simulation:read", "simulation:publish"}))
        )
        for op, args in (
            ("stop", {}),
            ("start", {}),
            (
                "confirm",
                {
                    "boundary": {
                        "generation": "1",
                        "admission_key": "first",
                        "request_sha256": DIGEST,
                    },
                    "tick": "1",
                    "expected_parent": None,
                },
            ),
        ):
            self.assertEqual((await self.invoke(op, args))["error"]["code"], "forbidden")
        self.assertEqual(self.backend.calls, [])

    async def test_paths_actor_raw_operations_and_full_scan_reads_rejected(self):
        for op in (
            "open",
            "close",
            "request",
            "register",
            "create",
            "bind",
            "inventory",
            "definitions",
            "history",
            "read",
            "step",
            "run",
            "fork",
        ):
            self.assertEqual((await self.invoke(op))["error"]["code"], "invalid_request")
        for key in ("actor", "roles", "scope", "components", "driver", "store_root", "id"):
            self.assertEqual(
                (await self.invoke(args={key: "admin"}))["error"]["code"], "invalid_request"
            )
        self.assertEqual(self.backend.calls, [])

    async def test_strict_json_and_request_size_before_dispatch(self):
        for raw in (
            b"{}",
            request() + b"{}",
            request().replace(b'"version": 1', b'"version": 1, "version": 1'),
            request().replace(b'"version": 1', b'"version": true'),
            b"[" * 1100,
            b"x" * (MAX_REQUEST_BYTES + 1),
            request().replace(b'"alpha"', b'"../alpha"'),
            request().replace(b'"version": 1', b'"version": NaN'),
        ):
            self.assertEqual(
                json.loads(await self.ingress.invoke(TOKEN, raw))["error"]["code"],
                "invalid_request",
            )
        self.assertEqual(self.backend.calls, [])

    async def test_exact_integer_codec_and_cell_schema(self):
        for integer in ("-9223372036854775808", "9223372036854775807", "9007199254741109"):
            decoded = Request.decode(request("admit", admit_args(integer)))
            self.assertEqual(decoded.operation.changes[0].cells[0].value, int(integer))
        for integer in (
            2**53 + 117,
            True,
            None,
            1.0,
            "01",
            "-0",
            "+1",
            "1e3",
            " 1",
            "9223372036854775808",
            "-9223372036854775809",
        ):
            self.assertEqual(
                (await self.invoke("admit", admit_args(integer)))["error"]["code"],
                "invalid_request",
            )
        for generation in (True, 1, "01", "18446744073709551616", "-1"):
            self.assertEqual(
                (
                    await self.invoke(
                        "admission_status", {"generation": generation, "admission_key": "first"}
                    )
                )["error"]["code"],
                "invalid_request",
            )
        args = admit_args()
        args["changes"][0]["predicate"] = "private"
        self.assertEqual((await self.invoke("admit", args))["error"]["code"], "invalid_request")
        args = admit_args()
        args["changes"][0]["values"][0] = {"string": "123"}
        self.assertEqual((await self.invoke("admit", args))["error"]["code"], "invalid_request")
        self.assertEqual(self.backend.calls, [])

    async def test_receipt_and_boundary_scope_before_native(self):
        args = {
            "receipt": {"world": "beta", "run": "run_a", "tick": "1", "cut_id": DIGEST},
            "expected_generation": "1",
        }
        self.assertEqual((await self.invoke("restore", args))["error"]["code"], "invalid_request")
        args = {
            "boundary": {
                "world_id": "native-b",
                "generation": "1",
                "admission_key": "first",
                "request_sha256": DIGEST,
            }
        }
        self.assertEqual((await self.invoke("publish", args))["error"]["code"], "invalid_request")
        self.assertEqual(self.backend.calls, [])

    async def test_safe_projection_and_exact_response_integers(self):
        result = await self.invoke()
        self.assertEqual(
            result["value"],
            {
                "state": "running",
                "generation": "9007199254740993",
                "revision": "9007199254741109",
                "has_error": True,
            },
        )
        self.assertEqual(self.backend.calls, [("status", "native-a")])
        self.assertNotIn("secret", json.dumps(result))
        self.assertNotIn("/private", json.dumps(result))
        self.backend.result["id"] = "native-b"
        self.assertEqual(
            (await self.invoke())["error"], {"code": "operation_failed", "outcome": "unknown"}
        )

    async def test_errors_never_infer_rollback_or_retry_from_text(self):
        with patch.object(
            self.backend,
            "status",
            side_effect=NativeError("operation", "Unknown world /private/key", "status"),
        ):
            result = await self.invoke()
        self.assertEqual(result["error"], {"code": "operation_failed", "outcome": "unknown"})
        self.assertNotIn("/private", json.dumps(result))

    async def test_factual_native_codes_preserve_unknown_mutation_outcome(self):
        for code in (
            "resource_limit",
            "corrupt_data",
            "invalid_request",
            "unsupported_format",
            "rollback",
            None,
        ):
            with (
                self.subTest(code=code),
                patch.object(
                    self.backend,
                    "admit",
                    create=True,
                    side_effect=NativeError(
                        "operation", "/private/key " + TOKEN, "admit", code=code
                    ),
                ),
            ):
                result = await self.invoke("admit", admit_args("9007199254740993"))
            expected = (
                code
                if code
                in {"resource_limit", "corrupt_data", "invalid_request", "unsupported_format"}
                else "operation_failed"
            )
            self.assertEqual(result["error"], {"code": expected, "outcome": "unknown"})
            self.assertNotIn("/private", json.dumps(result))
            self.assertNotIn(TOKEN, json.dumps(result))

    async def test_known_apply_failed_freeze_remains_inspectable(self):
        result = {
            "id": "native-a",
            "generation": 1,
            "admission_key": "first",
            "state": "applied_but_unpublished",
            "publication": "uncertain",
            "applied_revision": 2,
            "error": "/private/freeze/failure",
            "boundary": {
                "key": {
                    "world_id": "native-a",
                    "generation": 1,
                    "admission_key": "first",
                    "request_sha256": DIGEST,
                },
                "manifest": {"path": "/private"},
                "external_receipt": None,
            },
        }
        with patch.object(self.backend, "admission_status", return_value=result, create=True):
            observed = await self.invoke(
                "admission_status", {"generation": "1", "admission_key": "first"}
            )
        self.assertTrue(observed["ok"])
        self.assertEqual(observed["value"]["state"], "applied_but_unpublished")
        self.assertEqual(observed["value"]["applied_revision"], "2")
        self.assertTrue(observed["value"]["has_error"])
        self.assertEqual(observed["value"]["boundary"]["request_sha256"], DIGEST)
        self.assertNotIn("/private", json.dumps(observed))

    async def test_cancelled_waiter_retains_capacity_and_drain(self):
        self.backend.release.clear()
        self.ingress = self.make(max_inflight=1)
        task = asyncio.create_task(self.ingress.invoke(TOKEN, request()))
        try:
            self.assertTrue(await asyncio.to_thread(self.backend.entered.wait, 2))
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertEqual((await self.invoke())["error"]["code"], "busy")
            drain = asyncio.create_task(self.ingress.drain())
            await asyncio.sleep(0)
            self.assertFalse(drain.done())
            drain.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await drain
            self.assertEqual((await self.invoke())["error"]["code"], "unavailable")
        finally:
            self.backend.release.set()
            await self.ingress.drain()
        self.assertEqual(len(self.backend.calls), 1)
        self.assertFalse(self.ingress._pending)

    async def test_immutable_operator_binding_and_alias_rejection(self):
        binding = json.loads(json.dumps(BINDING))
        configured = resource(binding=binding)
        binding["scope"]["native_world"] = "native-b"
        binding["components"][0]["name"] = "changed"
        self.assertEqual(configured.binding(), BINDING)
        with self.assertRaises(ValueError):
            self.make(resources=(configured, resource("alias")))
        with self.assertRaises(ValueError):
            Grant("agent", "alpha", frozenset({"*"}))
        with self.assertRaises(ValueError):
            replace(configured, inputs=(["seed", ("int64", "string")],))

    async def test_no_old_world_dafts_or_auth_default_imports(self):
        self.assertFalse(
            any(
                name == "daft"
                or name.startswith(
                    (
                        "daft.",
                        "archetype.core",
                        "archetype.runtime",
                        "archetype.commands",
                        "archetype.api.deps",
                    )
                )
                for name in sys.modules
            )
        )


class NativeArtifactIngressTests(unittest.IsolatedAsyncioTestCase):
    async def test_existing_abi_real_iceberg_simulated_driver(self):
        from test_binding import Fixture

        fixture = await asyncio.to_thread(Fixture)
        print(f"Existing ABI / simulated driver evidence: {fixture.root}")
        ingress = None
        try:
            binding = await asyncio.to_thread(
                fixture.world, "alpha", await asyncio.to_thread(fixture.program)
            )
            ingress = Ingress(
                fixture.host,
                verifier=directory(),
                resources=(
                    resource(binding=binding),
                    replace(
                        resource(binding=binding), name="child", world="child", native_world=None
                    ),
                ),
                grants=(Grant("agent", "alpha", CAPS), Grant("agent", "child", CAPS)),
            )

            async def call(op, args=None, name="alpha"):
                raw = await ingress.invoke(TOKEN, request(op, args, name))
                result = json.loads(raw)
                self.assertTrue(result["ok"], (op, result))
                return result["value"]

            await call("start")
            await asyncio.to_thread(fixture.running, binding)
            status = await call("status")
            args = admit_args()
            args.update(generation=status["generation"], revision=status["revision"])
            admitted = await call("admit", args)
            boundary = admitted["boundary"]
            native_key = {
                **boundary,
                "world_id": binding["scope"]["native_world"],
                "generation": int(boundary["generation"]),
            }
            await asyncio.to_thread(fixture.frozen, native_key)
            observed = await call(
                "admission_status", {"generation": boundary["generation"], "admission_key": "first"}
            )
            self.assertEqual(observed["boundary"], boundary)
            published = await call("publish", {"boundary": boundary})
            self.assertEqual(
                (
                    await call(
                        "admission_status",
                        {"generation": boundary["generation"], "admission_key": "first"},
                    )
                )["state"],
                "frozen",
            )
            confirm = {
                "boundary": boundary,
                "tick": published["receipt"]["tick"],
                "expected_parent": published["parent"],
            }
            self.assertEqual(await call("reconcile", confirm), published)
            self.assertEqual((await call("confirm", confirm))["state"], "published")
            # Trusted verification only; full-scan read is absent from ingress.
            selected = dict(published["receipt"], tick=int(published["receipt"]["tick"]))
            rows = await asyncio.to_thread(fixture.host.read, selected, "label")
            self.assertEqual(rows["rows"], [[9007199254741109, "héllo world"]])
            stopped = await call("stop")
            await call(
                "restore",
                {"receipt": published["receipt"], "expected_generation": stopped["generation"]},
            )
            await asyncio.to_thread(fixture.running, binding)
            self.assertEqual(
                int((await call("status"))["generation"]), int(stopped["generation"]) + 1
            )
            fork_args = {
                "source_resource": "alpha",
                "receipt": published["receipt"],
                "request_key": "public_fork",
                "expected_generation": "0",
            }
            async with asyncio.timeout(20):
                while True:
                    forked = await call("fork", fork_args, "child")
                    if forked["lineage_ready"]:
                        break
                    await asyncio.sleep(0.01)
            self.assertEqual(forked["source"], published["receipt"])
            self.assertEqual((await call("status", name="child"))["lineage_ready"], True)
            self.assertEqual(forked["destination"], {"world": "child", "run": "run_a"})
            self.assertNotIn(binding["scope"]["native_world"], json.dumps(forked))
            self.assertEqual(
                (await asyncio.to_thread(fixture.host.history, "child", "run_a"))["receipts"][0][
                    "cut_id"
                ],
                published["receipt"]["cut_id"],
            )
        finally:
            if ingress is not None:
                ingress.stop_accepting()
            await asyncio.to_thread(fixture.close)
            if ingress is not None:
                await ingress.drain()


if __name__ == "__main__":
    unittest.main()
