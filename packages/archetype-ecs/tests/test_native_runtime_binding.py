# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Public facade through real C ABI/Iceberg; compiler fixture is simulated."""

import asyncio
import base64
import io
import json
import math
import os
import shutil
import time
import unittest
import wave
from contextlib import AsyncExitStack, asynccontextmanager

from test_binding import ENTITY, LIBRARY, Fixture
from uuid_utils import uuid7

from archetype import (
    ArchetypeRuntime,
    ArtifactSource,
    Change,
    ComponentProjection,
    Composition,
    Connection,
    Endpoint,
    InputPort,
    LeafProgram,
    OutputPort,
    ProgramNode,
    Relation,
    RuntimeOperationError,
)

PROJECTIONS = (ComponentProjection("live", "out", ("entity_id", "enabled", "value"), 0),)
INPUTS = (("seed", ("int64", "bool", "float64")),)
PROGRAM = LeafProgram(
    "out(E,B,D) :- seed(E,B,D).",
    (
        Relation("seed", True, ("int64", "bool", "float64")),
        Relation("out", False, ("int64", "bool", "float64")),
    ),
    ("seed",),
    ("out",),
)


def composition(first, second):
    return Composition(
        (ProgramNode("first", first), ProgramNode("second", second)),
        (InputPort("seed", ("int64", "bool", "float64"), (Endpoint("first", "seed"),)),),
        (Connection(Endpoint("first", "out"), Endpoint("second", "seed")),),
        (OutputPort("out", Endpoint("second", "out")),),
    )


async def wait(probe, ready):
    deadline = time.monotonic() + 180
    while True:
        value = await probe()
        if ready(value):
            return value
        if time.monotonic() >= deadline:
            raise AssertionError(f"Timed out: {value}")
        await asyncio.sleep(0.01)


@asynccontextmanager
async def live_interfaces(runtime):
    """Trusted test wiring borrows the sole Runtime owner; it creates no second Host."""
    import httpx2
    from archetype_native.ingress import ContextResource, Grant, Ingress, LogicalResource
    from archetype_transports import create_app
    from test_ingress import CAPS, directory, request
    from test_transports import headers, mcp, session

    from archetype.wiring import artifact_workflow

    host = await runtime._activate()
    resources = (
        LogicalResource("experiment", "experiment", "main", PROJECTIONS, INPUTS),
        LogicalResource("child", "child", "main", PROJECTIONS, INPUTS),
        ContextResource("experiment_files", "experiment", "main", "experiment"),
        ContextResource("collection", "collection", "main"),
    )
    ingress = Ingress(
        host,
        verifier=directory(),
        resources=resources,
        grants=tuple(Grant("agent", r.name, CAPS) for r in resources),
        artifact_workflow=artifact_workflow(host),
    )
    app = create_app(ingress)
    calls = []
    exchange = host._exchange

    def traced(*args, **kwargs):
        calls.append(args[0])
        return exchange(*args, **kwargs)

    host._exchange = traced
    async with app.router.lifespan_context(app):
        async with (
            httpx2.AsyncClient(
                transport=httpx2.ASGITransport(app), base_url="http://127.0.0.1", headers=headers()
            ) as http,
            session(http, read_timeout_seconds=180) as client,
        ):

            async def invoke(
                operation, arguments=None, name="experiment", via="http", require_ok=True
            ):
                raw = request(operation, arguments, name)
                if via == "mcp":
                    result = await mcp(client, raw)
                else:
                    response = await http.post("/invoke", content=raw)
                    result = response.json()
                if require_ok:
                    assert result["ok"], result
                assert str(runtime._config["store_root"]) not in json.dumps(result)
                return result["value"] if result["ok"] else result

            try:
                yield invoke, http, app, calls
            finally:
                await ingress.drain()
                host._exchange = exchange


def boundary(value):
    return {
        "generation": str(value.generation),
        "admission_key": value.admission_key,
        "request_sha256": value.request_sha256,
    }


class RuntimeBindingTests(unittest.TestCase):
    def test_two_program_full_empty_cut_historical_fork_and_storage_only_cold_reads(self):
        f = Fixture()
        f.host.close()
        native_driver = os.environ.get("ARCHETYPE_ACCEPTANCE_DRIVER")
        if native_driver:
            shutil.copyfile(native_driver, f.driver)
            f.driver.chmod(0o700)
        options = dict(
            library=LIBRARY,
            store=f.root / "storage",
            registry=f.root / "registry",
            builds=f.root / "worlds",
            driver=f.driver,
        )

        async def exercise():
            async with ArchetypeRuntime(**options) as runtime, AsyncExitStack() as stack:
                invoke, live_http, live_app, native_calls = await stack.enter_async_context(
                    live_interfaces(runtime)
                )
                one = await runtime.program("first_program").publish(PROGRAM, request_key="first")
                two = await runtime.program("second_program").publish(PROGRAM, request_key="second")
                selected = await runtime.program("pipeline").publish(
                    composition(one, two), request_key="pipeline"
                )
                world = runtime.world("experiment", components=PROJECTIONS, inputs=INPUTS)
                created = await world.create(selected, request_key="experiment")
                self.assertEqual(created.state, "created")
                self.assertFalse((f.root / "compiler_entered").exists())
                await invoke("start")
                running = await wait(world.status, lambda s: s.state == "running")
                rows = ((ENTITY, True, math.nextafter(1.0, 2.0)), (ENTITY + 1, False, -0.0))
                await world.admit(
                    tuple(Change("seed", row) for row in rows),
                    generation=running.generation,
                    revision=running.revision,
                    admission_key="first_cut",
                    expected_head=None,
                )
                frozen = await wait(
                    lambda: world.admission_status(running.generation, "first_cut"),
                    lambda a: a.state == "frozen",
                )
                public_a = await invoke("publish", {"boundary": boundary(frozen.boundary)})
                a = await world.publish(frozen.boundary)
                self.assertEqual(public_a["receipt"], a._receipt())
                self.assertEqual(
                    public_a,
                    await invoke("publish", {"boundary": boundary(frozen.boundary)}, via="mcp"),
                )
                await invoke(
                    "confirm",
                    {
                        "boundary": boundary(frozen.boundary),
                        "tick": str(a.tick),
                        "expected_parent": a.parent,
                    },
                    via="mcp",
                )
                self.assertEqual((await world.confirm(frozen.boundary, a)).publication, "published")
                page = await a.read("live")
                self.assertEqual(page.fields, ("entity_id", "enabled", "value"))
                self.assertEqual(
                    set(page.rows), {(ENTITY, True, rows[0][2]), (ENTITY + 1, False, 0.0)}
                )
                self.assertEqual(
                    math.copysign(1, next(r[2] for r in page.rows if r[0] == ENTITY + 1)), 1
                )
                state = await world.status()
                await invoke(
                    "admit",
                    {
                        "generation": str(state.generation),
                        "revision": str(state.revision),
                        "admission_key": "empty_cut",
                        "expected_head": a.cut_id,
                        "changes": [Change("seed", row, "delete")._wire() for row in rows],
                    },
                    via="mcp",
                )
                await invoke(
                    "admit",
                    {
                        "generation": str(state.generation),
                        "revision": str(state.revision),
                        "admission_key": "empty_cut",
                        "expected_head": a.cut_id,
                        "changes": [Change("seed", row, "delete")._wire() for row in rows],
                    },
                )
                await world.admit(
                    tuple(Change("seed", row, "delete") for row in rows),
                    generation=state.generation,
                    revision=state.revision,
                    admission_key="empty_cut",
                    expected_head=a.cut_id,
                )
                empty = await wait(
                    lambda: world.admission_status(state.generation, "empty_cut"),
                    lambda a: a.state == "frozen",
                )
                b = await world.publish(empty.boundary)
                self.assertEqual(
                    (await invoke("publish", {"boundary": boundary(empty.boundary)}, via="mcp"))[
                        "receipt"
                    ],
                    b._receipt(),
                )
                await invoke(
                    "confirm",
                    {
                        "boundary": boundary(empty.boundary),
                        "tick": str(b.tick),
                        "expected_parent": b.parent,
                    },
                )
                await world.confirm(empty.boundary, b)
                self.assertEqual((await b.read("live")).rows, ())
                self.assertEqual(
                    [c.cut_id for c in (await world.history()).cuts], [a.cut_id, b.cut_id]
                )
                child = runtime.world("child", components=PROJECTIONS, inputs=INPUTS)

                async def fork():
                    try:
                        return await child.fork(world, a, request_key="child")
                    except RuntimeOperationError:
                        return None

                fork_args = {
                    "source_resource": "experiment",
                    "receipt": a._receipt(),
                    "request_key": "child",
                    "expected_generation": "0",
                }

                async def public_fork(via):
                    result = await invoke("fork", fork_args, "child", via=via, require_ok=False)
                    return result if "error" not in result else None

                initial = await wait(lambda: public_fork("http"), lambda value: value is not None)
                retried = await wait(
                    lambda: public_fork("mcp"),
                    lambda value: value is not None and value["lineage_ready"],
                )
                self.assertEqual(initial["request_sha256"], retried["request_sha256"])
                self.assertEqual(initial["destination"], retried["destination"])
                self.assertEqual(initial["generation"], retried["generation"])
                self.assertEqual(retried["source"], a._receipt())
                forked = await wait(fork, lambda s: s is not None and s.lineage_ready)
                self.assertEqual(str(forked.generation), retried["generation"])

                # Private harness inventory of authoritative native records: a
                # conflicting retry must not allocate an orphan destination.
                def destination_inventory():
                    return tuple(
                        sorted(
                            path.parent.name for path in (f.root / "worlds").glob("*/world.json")
                        )
                    )

                inventory = destination_inventory()
                self.assertGreaterEqual(len(inventory), 2)
                conflict = await invoke(
                    "fork", {**fork_args, "receipt": b._receipt()}, "child", require_ok=False
                )
                self.assertFalse(conflict["ok"])
                self.assertEqual(conflict["error"]["code"], "conflict")
                with self.assertRaises(RuntimeOperationError) as changed:
                    await child.fork(world, b, request_key="child")
                self.assertEqual(changed.exception.code, "conflict")
                self.assertEqual(destination_inventory(), inventory)
                resolved = await invoke("resolve", name="child")
                self.assertEqual(resolved["request_sha256"], retried["request_sha256"])
                self.assertEqual(resolved["destination"], retried["destination"])
                self.assertEqual(resolved["generation"], retried["generation"])
                self.assertEqual(resolved["source"], a._receipt())
                self.assertEqual((await child.history()).cuts[0].cut_id, a.cut_id)
                await child.admit(
                    (Change("seed", (999, False, 1.25)),),
                    generation=forked.generation,
                    revision=forked.revision,
                    admission_key="child_cut",
                    expected_head=a.cut_id,
                )
                admitted = await wait(
                    lambda: child.admission_status(forked.generation, "child_cut"),
                    lambda a: a.state == "frozen",
                )
                public_c = await invoke(
                    "publish", {"boundary": boundary(admitted.boundary)}, "child", via="mcp"
                )
                c = await child.publish(admitted.boundary)
                self.assertEqual(public_c["receipt"], c._receipt())
                await invoke(
                    "confirm",
                    {
                        "boundary": boundary(admitted.boundary),
                        "tick": str(c.tick),
                        "expected_parent": c.parent,
                    },
                    "child",
                )
                await child.confirm(admitted.boundary, c)
                read_args = {
                    "receipt": c._receipt(),
                    "component": "live",
                    "offset": "0",
                    "limit": "32",
                }
                http_rows = await invoke("read", read_args, "child")
                self.assertEqual(http_rows, await invoke("read", read_args, "child", via="mcp"))
                expected_child = {
                    json.dumps(
                        [{"int64": str(ENTITY)}, {"bool": True}, {"float64": "3ff0000000000001"}],
                        sort_keys=True,
                    ),
                    json.dumps(
                        [
                            {"int64": str(ENTITY + 1)},
                            {"bool": False},
                            {"float64": "0000000000000000"},
                        ],
                        sort_keys=True,
                    ),
                    json.dumps(
                        [{"int64": "999"}, {"bool": False}, {"float64": "3ff4000000000000"}],
                        sort_keys=True,
                    ),
                }
                self.assertEqual(
                    {json.dumps(row, sort_keys=True) for row in http_rows["rows"]}, expected_child
                )
                self.assertEqual(
                    set((await c.read("live")).rows),
                    {(ENTITY, True, rows[0][2]), (ENTITY + 1, False, 0.0), (999, False, 1.25)},
                )
                if native_driver:
                    compiled = tuple((f.root / "worlds").glob("**/program_cli"))
                    self.assertGreaterEqual(len(compiled), 2)
                    for executable in compiled:
                        with executable.open("rb") as stream:
                            magic = stream.read(4)
                        self.assertIn(
                            magic,
                            (
                                b"\x7fELF",
                                b"\xcf\xfa\xed\xfe",
                                b"\xfe\xed\xfa\xcf",
                                b"\xca\xfe\xba\xbe",
                            ),
                        )
                await child.shutdown()
                self.assertEqual((await world.status()).state, "running")
                hosted = world.artifacts("experiment_files")
                self.assertEqual((await hosted.publish()).origin, "hosted")
                audio = io.BytesIO()
                with wave.open(audio, "wb") as recording:
                    recording.setnchannels(1)
                    recording.setsampwidth(2)
                    recording.setframerate(16000)
                    recording.writeframes(b"\0\0" * 8000)
                occurrence = str(uuid7())
                uploaded = await hosted.upload(
                    audio.getvalue(), logical_path="evidence.wav", artifact_id=occurrence, cut=a
                )
                self.assertEqual(
                    uploaded,
                    await hosted.upload(
                        audio.getvalue(),
                        logical_path="evidence.wav",
                        artifact_id=occurrence,
                        cut=a,
                    ),
                )
                audio_page = await hosted.occurrences(cut=a)
                self.assertEqual(audio_page.total, 1)
                facts = dict(audio_page.items[0].facts("audio"))
                self.assertEqual((facts["sample_rate"], facts["duration_seconds"]), (16000, 0.5))
                self.assertIs(type(facts["sample_rate"]), int)
                self.assertIs(type(facts["duration_seconds"]), float)
                self.assertNotIn("object_uri", dict(audio_page.items[0].common))
                self.assertEqual((await hosted.occurrences(cut=b)).total, 0)
                cutless = await hosted.upload(
                    b"unattributed\n", logical_path="note.txt", artifact_id=str(uuid7())
                )
                self.assertIsNone(cutless.exact_cut)
                self.assertEqual((await hosted.occurrences()).total, 1)
                self.assertEqual((await hosted.occurrences(all=True)).total, 2)
                self.assertEqual(len((await c.read("live")).rows), 3)
                collection = runtime.artifacts("collection")
                context = await collection.publish()
                self.assertEqual(context.origin, "artifact_collection")
                self.assertEqual(await collection.context(), context)
                standalone = await collection.upload(
                    b"standalone\n", logical_path="standalone.txt", artifact_id=str(uuid7())
                )
                batch_source = f.root / "batch_source"
                batch_source.mkdir()
                (batch_source / "large.txt").write_bytes(b"batch evidence\n" * 3000)
                (batch_source / "small.csv").write_text("name,value\nfirst,1\n")
                prepared = await collection.prepare_files(
                    (ArtifactSource(source_uri=str(batch_source / "*")),)
                )
                self.assertEqual(len(prepared.artifact_ids), 2)
                batch_receipts = await collection.publish_files(prepared)
                self.assertEqual(batch_receipts, await collection.publish_files(prepared))
                self.assertTrue(any(item.size_bytes > 32768 for item in batch_receipts))
                self.assertNotIn("object_uri", repr(prepared))
                self.assertEqual((await collection.occurrences()).total, 3)
                artifact_args = {
                    "context_id": context.context_id,
                    "exact_cut": None,
                    "all": False,
                    "offset": "0",
                    "limit": "32",
                }
                projected = await invoke("context_artifacts", artifact_args, "collection")
                self.assertEqual(
                    projected,
                    await invoke("context_artifacts", artifact_args, "collection", via="mcp"),
                )
                self.assertEqual(projected["total"], "3")
                import httpx2
                from test_ingress import OTHER, request
                from test_transports import headers, mcp, session

                before = len(native_calls)
                denied = await live_http.post(
                    "/invoke",
                    content=request("history", {"offset": "0", "limit": "32"}, "experiment"),
                    headers=headers(OTHER),
                )
                self.assertEqual(denied.status_code, 403)
                async with (
                    httpx2.AsyncClient(
                        transport=httpx2.ASGITransport(live_app),
                        base_url="http://127.0.0.1",
                        headers=headers(OTHER),
                    ) as other_http,
                    session(other_http) as other,
                ):
                    self.assertFalse(
                        (
                            await mcp(
                                other,
                                request("history", {"offset": "0", "limit": "32"}, "experiment"),
                            )
                        )["ok"]
                    )
                malformed = request(
                    "admit",
                    {
                        "generation": str(2**64),
                        "revision": "0",
                        "admission_key": "invalid_overflow",
                        "expected_head": None,
                        "changes": [],
                    },
                    "experiment",
                )
                self.assertEqual(
                    (await live_http.post("/invoke", content=malformed)).status_code, 400
                )
                self.assertFalse(
                    (
                        await invoke(
                            "admit",
                            {
                                "generation": str(2**64),
                                "revision": "0",
                                "admission_key": "invalid_overflow",
                                "expected_head": None,
                                "changes": [],
                            },
                            via="mcp",
                            require_ok=False,
                        )
                    )["ok"]
                )
                self.assertEqual(
                    len(native_calls), before, "denied/malformed requests reached native lookup"
                )
                from unittest.mock import patch

                with patch(
                    "archetype.artifacts.uploads.FileIngestionPipeline",
                    side_effect=AssertionError("invalid target reached file scan"),
                ) as scan:
                    invalid = await invoke(
                        "artifact_upload",
                        {
                            "context_id": "0" * 64,
                            "exact_cut": None,
                            "artifact_id": str(uuid7()),
                            "logical_path": "invalid.txt",
                            "content_base64": base64.b64encode(b"invalid target").decode(),
                        },
                        "collection",
                        require_ok=False,
                    )
                    self.assertFalse(invalid["ok"])
                    scan.assert_not_called()
                self.assertNotIn("native_world", repr((a, b, c, context)))
                self.assertFalse(hasattr(world, "spawn"))
                self.assertFalse(hasattr(world, "step"))
            # Remove original registry, native source/builds and compiler from
            # every path the cold reader receives; retain them for proof.
            for name in ("registry", "worlds", "batch_source"):
                (f.root / name).rename(f.root / ("retained_" + name))
            f.driver.rename(f.root / "retained_driver")
            async with ArchetypeRuntime(library=LIBRARY, store=f.root / "storage") as cold:
                history = await cold.world("experiment").history()
                self.assertEqual(len(history.cuts), 2)
                self.assertEqual(
                    set((await history.cuts[0].read("live")).rows),
                    {(ENTITY, True, rows[0][2]), (ENTITY + 1, False, 0.0)},
                )
                self.assertEqual((await history.cuts[1].read("live")).rows, ())
                child_history = await cold.world("child").history()
                self.assertEqual(
                    set((await child_history.cuts[-1].read("live")).rows),
                    {(ENTITY, True, rows[0][2]), (ENTITY + 1, False, 0.0), (999, False, 1.25)},
                )
                self.assertEqual(await cold.artifacts("collection").context(), context)
                self.assertEqual((await cold.artifacts("collection").occurrences()).total, 3)
                cold_files = cold.artifacts("experiment_files", world="experiment")
                self.assertEqual((await cold_files.occurrences(all=True)).total, 2)
                recovered_audio = await cold_files.occurrences(cut=history.cuts[0])
                self.assertEqual(recovered_audio.total, 1)
                facts = dict(recovered_audio.items[0].facts("audio"))
                self.assertEqual((facts["sample_rate"], facts["duration_seconds"]), (16000, 0.5))
                analyzed = await cold_files.analyze(index="audio", cut=history.cuts[0])
                self.assertEqual(analyzed.to_pydict()["sample_rate"], [16000])
                self.assertEqual((await cold_files.occurrences()).total, 1)
            # Default public server opens its sole storage-only owner after the
            # Python owner has closed. Official SDK/auth reads the same cold facts.
            from unittest.mock import patch

            import httpx2
            from archetype_native.ingress import ContextResource, Grant, LogicalResource
            from test_ingress import CAPS, directory, request
            from test_transports import headers, mcp, session

            from archetype.api.app import create_app
            from archetype.wiring import RuntimeBootstrapConfig

            resources = (
                LogicalResource("experiment", "experiment", "main", (), ()),
                ContextResource("experiment_files", "experiment", "main", "experiment"),
                ContextResource("collection", "collection", "main"),
                LogicalResource("child", "child", "main", (), ()),
            )
            config = RuntimeBootstrapConfig(
                tuple(
                    (key, value)
                    for key, value in {
                        "library": str(LIBRARY),
                        "store": str(f.root / "storage"),
                        "registry": None,
                        "builds": None,
                        "driver": None,
                    }.items()
                ),
                resources,
                tuple(Grant("agent", r.name, CAPS) for r in resources),
            )
            with patch("archetype.api.app.PrincipalDirectory.from_env", return_value=directory()):
                app = create_app(config=config)
                async with app.router.lifespan_context(app):
                    async with (
                        httpx2.AsyncClient(
                            transport=httpx2.ASGITransport(app),
                            base_url="http://127.0.0.1",
                            headers=headers(),
                        ) as http,
                        session(http) as client,
                    ):
                        calls = [
                            request(
                                "read",
                                {
                                    "receipt": a._receipt(),
                                    "component": "live",
                                    "offset": "0",
                                    "limit": "32",
                                },
                                "experiment",
                            ),
                            request(
                                "read",
                                {
                                    "receipt": b._receipt(),
                                    "component": "live",
                                    "offset": "0",
                                    "limit": "32",
                                },
                                "experiment",
                            ),
                            request("history", {"offset": "0", "limit": "32"}, "experiment"),
                            request(
                                "read",
                                {
                                    "receipt": c._receipt(),
                                    "component": "live",
                                    "offset": "0",
                                    "limit": "32",
                                },
                                "child",
                            ),
                            request("read_context", {}, "collection"),
                            request(
                                "context_artifacts",
                                {
                                    "context_id": context.context_id,
                                    "exact_cut": None,
                                    "all": False,
                                    "offset": "0",
                                    "limit": "32",
                                },
                                "collection",
                            ),
                            request(
                                "context_artifacts",
                                {
                                    "context_id": uploaded.context_id,
                                    "exact_cut": {"tick": str(a.tick), "cut_id": a.cut_id},
                                    "all": False,
                                    "offset": "0",
                                    "limit": "32",
                                },
                                "experiment_files",
                            ),
                        ]
                        values = []
                        for raw in calls:
                            response = await http.post("/invoke", content=raw)
                            self.assertEqual(response.status_code, 200, response.text)
                            self.assertEqual(await mcp(client, raw), response.json())
                            self.assertNotIn(str(f.root), response.text)
                            values.append(response.json()["value"])
                        expected_a = {
                            json.dumps(row, sort_keys=True)
                            for row in http_rows["rows"]
                            if row[0] != {"int64": "999"}
                        }
                        self.assertEqual(
                            {json.dumps(row, sort_keys=True) for row in values[0]["rows"]},
                            expected_a,
                        )
                        self.assertEqual(values[0]["receipt"], a._receipt())
                        self.assertEqual(values[1]["rows"], [])
                        self.assertEqual(values[1]["receipt"], b._receipt())
                        self.assertEqual(
                            [item["receipt"] for item in values[2]["receipts"]],
                            [a._receipt(), b._receipt()],
                        )
                        self.assertEqual(
                            {json.dumps(row, sort_keys=True) for row in values[3]["rows"]},
                            expected_child,
                        )
                        self.assertEqual(values[4]["context_id"], context.context_id)
                        self.assertEqual(values[4]["origin"], "artifact_collection")
                        self.assertEqual(values[5]["total"], "3")
                        self.assertEqual(
                            {item["artifact_id"] for item in values[5]["items"]},
                            {standalone.artifact_id, *prepared.artifact_ids},
                        )
                        for item in values[5]["items"]:
                            common = item["common"]
                            self.assertEqual(common["artifact_id"], {"string": item["artifact_id"]})
                            self.assertEqual(common["sha256"], {"string": item["sha256"]})
                            self.assertEqual(common["context_id"], {"string": context.context_id})
                            self.assertIsNone(item["exact_cut"])
                        text = next(
                            item
                            for item in values[5]["items"]
                            if item["common"]["logical_path"] == {"string": "large.txt"}
                        )
                        self.assertEqual(text["typed"]["text"]["line_count"], {"int64": "3000"})
                        self.assertEqual(text["common"]["size_bytes"], {"int64": "45000"})
                        facts = response.json()["value"]["items"][0]["typed"]["audio"]
                        self.assertEqual(facts["sample_rate"], {"int64": "16000"})
                        self.assertEqual(facts["duration_seconds"], {"float64": "3fe0000000000000"})
            print(
                "Public runtime actual-DDlog proof retained:"
                if native_driver
                else "Public runtime simulated-compiler proof retained:",
                f.root,
            )

        # One process/loop for async ownership; sync has its own Runner and must
        # run outside this loop, so separate the last cold sync check.
        # The cold sync block is extracted below to preserve that contract.
        asyncio.run(exercise())
        with ArchetypeRuntime.sync(library=LIBRARY, store=f.root / "storage") as cold:
            history = cold.world("experiment").history()
            self.assertEqual(history.cuts[1].read("live").rows, ())
            self.assertEqual(cold.artifacts("collection").context().origin, "artifact_collection")
