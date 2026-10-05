"""Published context/file pipeline through the installed native storage ABI."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

if "DDLOG_PYTHON_LIBRARY" not in os.environ:
    raise unittest.SkipTest("Set DDLOG_PYTHON_LIBRARY to the context-capable library")

# Fixtures only. Installed validation must not inject product source paths.
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "packages/archetype-native/tests"))
from archetype_native import NativeError, Store
from archetype_native.ingress import ContextResource, Grant, Ingress
from test_binding import COMPONENTS, LIBRARY, Fixture, wait

from archetype.artifacts.context_attachments import (
    prepare_context_attachments,
    publish_context_attachments,
)
from archetype.artifacts.cut_attachments import prepare_attachments, publish_attachments
from archetype.artifacts.models import ArtifactSource
from archetype.storage.context_artifacts import (
    ArtifactTarget,
    ContextArtifactStorage,
    ExactContextCut,
    PublishedContextRef,
)
from archetype.storage.cut_artifacts import CutArtifactStorage, CutCoordinates


class ContextArtifactTests(unittest.TestCase):
    def test_child_context_retains_inherited_cut_attribution_on_cold_read(self):
        f = Fixture()
        self.addCleanup(f.close)
        parent = f.world("parent", f.program())
        f.host.start(parent["scope"]["native_world"])
        f.running(parent)
        key = f.submit(parent)
        f.frozen(key)
        source_cut = f.host.publish(parent, key)
        f.confirm(parent, key, source_cut)
        args = dict(world="child", run="run_a", label="Child", request_key="context_fork")
        (f.root / "hold_compile").touch()
        initial = f.host.fork(parent, source_cut, **args)
        self.assertEqual(initial["status"]["state"], "starting")
        self.assertFalse(initial["status"]["external_publication"]["fork"]["ready"])
        child = f.host.fork_binding("child", "run_a", COMPONENTS)
        published = f.host.publish_hosted_context(child)
        context = PublishedContextRef.from_publication(published)
        storage = ContextArtifactStorage(f.host, context)
        exact = ArtifactTarget(context, ExactContextCut(source_cut["tick"], source_cut["cut_id"]))
        source = f.root / "inherited.txt"
        source.write_text("inherited evidence\n")
        sources = (ArtifactSource(source_uri=str(source)),)
        for target in (exact, ArtifactTarget(context)):
            publish_context_attachments(
                storage, prepare_context_attachments(storage, target, sources)
            )
        (f.root / "hold_compile").unlink()
        wait(
            lambda: f.host.fork(parent, source_cut, **args),
            lambda result: result["status"]["external_publication"]["fork"]["ready"],
        )
        later_key = f.submit(parent, "parent_delete", source_cut["cut_id"], delete=True)
        f.frozen(later_key)
        later = f.host.publish(parent, later_key)
        f.confirm(parent, later_key, later)
        with self.assertRaises(NativeError):
            storage.verify(ArtifactTarget(context, ExactContextCut(later["tick"], later["cut_id"])))
        self.assertEqual(f.host.history("child", "run_a")["receipts"], [source_cut])
        f.close()
        for name in ("registry", "worlds", "build.py"):
            (f.root / name).rename(f.root / ("retained-" + name))
        with Store(library=LIBRARY, store_root=f.root / "storage") as host:
            storage = ContextArtifactStorage(host, context)
            rows = storage.read_all()["items"]
            self.assertEqual(len(rows), 2)
            for row in rows:
                attribution = row["receipt"]["target"]["exact_cut"]
                if attribution is not None:
                    self.assertEqual(
                        attribution,
                        {"tick": source_cut["tick"], "cut_id": source_cut["cut_id"]},
                    )
                for payload in (row["common"], row["typed"]["text"]):
                    batch = storage.decode(payload)
                    self.assertEqual(batch["world"].to_pylist(), ["child"])
                    self.assertEqual(batch["run"].to_pylist(), ["run_a"])
                    self.assertEqual(batch["context_id"].to_pylist(), [context.context_id])
                    self.assertEqual(
                        batch["tick"].to_pylist(), [attribution["tick"] if attribution else None]
                    )
                    self.assertEqual(
                        batch["cut_id"].to_pylist(), [source_cut["cut_id"] if attribution else None]
                    )
            self.assertEqual(storage.read(exact)["total"], 1)
            self.assertEqual(storage.read(ArtifactTarget(context))["total"], 1)

    def test_collection_has_no_execution_side_effects_and_cold_reads(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.txt"
            source.write_text("artifact only\n")
            with Store(library=LIBRARY, store_root=root / "store") as host:
                published = host.publish_collection("files", "run_a")
                self.assertEqual(published, host.publish_collection("files", "run_a"))
                self.assertEqual(published["origin"], {"kind": "artifact_collection"})
                self.assertEqual(
                    set(published), {"version", "world", "run", "context_id", "origin"}
                )
                context = PublishedContextRef.from_publication(published)
                storage = ContextArtifactStorage(host, context)
                target = ArtifactTarget(context)
                sources = (ArtifactSource(source_uri=str(source)),)
                prepared = prepare_context_attachments(storage, target, sources)
                receipt = publish_context_attachments(storage, prepared)
                self.assertEqual(receipt, publish_context_attachments(storage, prepared))
                second = prepare_context_attachments(storage, target, sources)
                self.assertNotEqual(
                    prepared.artifacts[0].artifact_id, second.artifacts[0].artifact_id
                )
                self.assertEqual(prepared.artifacts[0].uri, second.artifacts[0].uri)
                publish_context_attachments(storage, second)
                from test_ingress import TOKEN, directory, request

                async def public_read():
                    caps = frozenset({"artifacts:read"})
                    ingress = Ingress(
                        host,
                        verifier=directory(caps),
                        resources=(ContextResource("files", "files", "run_a"),),
                        grants=(Grant("agent", "files", caps),),
                    )
                    response = await ingress.invoke(
                        TOKEN,
                        request(
                            "context_artifacts",
                            {
                                "context_id": context.context_id,
                                "exact_cut": None,
                                "all": True,
                                "offset": "0",
                                "limit": "32",
                            },
                            "files",
                        ),
                    )
                    await ingress.drain()
                    return json.loads(response)

                public = asyncio.run(public_read())
                self.assertTrue(public["ok"], public)
                self.assertEqual(public["value"]["total"], "2")
                self.assertEqual(
                    {r["artifact_id"] for r in public["value"]["items"]},
                    {prepared.artifacts[0].artifact_id, second.artifacts[0].artifact_id},
                )
                self.assertNotIn(str(root), json.dumps(public))
                self.assertTrue(all(r["exact_cut"] is None for r in public["value"]["items"]))
                with self.assertRaises(NativeError):
                    host.start("missing")
                self.assertEqual(list((root / "store/cuts").iterdir()), [])
            source.unlink()
            with Store(library=LIBRARY, store_root=root / "store") as host:
                self.assertEqual(host.context("files", "run_a"), published)
                storage = ContextArtifactStorage(host, context)
                rows = storage.read_all()["items"]
                self.assertEqual(len(rows), 2)
                for row in rows:
                    self.assertIsNone(row["receipt"]["target"]["exact_cut"])
                    common = storage.decode(row["common"])
                    self.assertEqual(common["tick"].to_pylist(), [None])
                    self.assertEqual(common["cut_id"].to_pylist(), [None])
                    self.assertTrue(common.schema.field("tick").nullable)
                    self.assertTrue(common.schema.field("cut_id").nullable)
            self.assertEqual({p.name for p in root.iterdir()}, {"store"})

    def test_hosted_cutless_exact_and_legacy_survive_without_registry_or_manager(self):
        f = Fixture()
        self.addCleanup(f.close)
        binding = f.world("files", f.program())
        native = binding["scope"]["native_world"]
        published = f.host.publish_hosted_context(binding)
        self.assertEqual(f.host.status(native)["state"], "created")
        self.assertEqual(f.host.history("files", "run_a")["receipts"], [])
        self.assertFalse((f.root / "compiler_entered").exists())
        context = PublishedContextRef.from_publication(published)
        storage = ContextArtifactStorage(f.host, context)
        source = f.root / "context.txt"
        source.write_text("same original\n")
        sources = (ArtifactSource(source_uri=str(source)),)
        cutless = prepare_context_attachments(storage, ArtifactTarget(context), sources)
        publish_context_attachments(storage, cutless)
        with self.assertRaises(NativeError):
            f.host.publish_collection("files", "run_a")
        f.host.start(native)
        f.running(binding)
        key = f.submit(binding)
        f.frozen(key)
        receipt = f.host.publish(binding, key)
        f.confirm(binding, key, receipt)
        cut = CutCoordinates(**{k: receipt[k] for k in ("world", "run", "tick", "cut_id")})
        target = ArtifactTarget(context, ExactContextCut.from_cut(cut))
        exact = prepare_context_attachments(storage, target, sources)
        publish_context_attachments(storage, exact)
        with self.assertRaises(NativeError):
            storage.publish(target, cutless.occurrences)
        old_storage = CutArtifactStorage(f.host, binding)
        legacy = prepare_attachments(old_storage, cut, sources)
        publish_attachments(old_storage, legacy)
        f.host.close()
        for name in ("registry", "worlds", "build.py"):
            path = f.root / name
            if path.exists():
                path.rename(f.root / ("retained-" + name))
        with Store(library=LIBRARY, store_root=f.root / "storage") as host:
            self.assertEqual(host.context("files", "run_a"), published)
            storage = ContextArtifactStorage(host, context)
            all_rows = storage.read_all()["items"]
            self.assertEqual(len(all_rows), 2)
            self.assertEqual(storage.read(cutless.target)["total"], 1)
            self.assertEqual(storage.read(target)["total"], 1)
            for row in all_rows:
                common = storage.decode(row["common"])
                self.assertTrue(common.schema.field("tick").nullable)
                self.assertTrue(common.schema.field("cut_id").nullable)
            old = host.request("read_cut_artifacts", receipt=cut.as_dict(), offset=0, limit=32)
            self.assertEqual(old["total"], 1)
            self.assertEqual(
                old["items"][0]["receipt"]["artifact_id"], legacy.artifacts[0].artifact_id
            )
        self.assertFalse((f.root / "registry").exists())
        self.assertFalse((f.root / "worlds").exists())

    def test_unpublished_or_wrong_target_rejects_before_source_discovery(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with Store(library=LIBRARY, store_root=root / "store") as host:
                context = PublishedContextRef("files", "run_a", "0" * 64)
                storage = ContextArtifactStorage(host, context)
                with patch("archetype.artifacts.context_attachments.scan_sources") as scan:
                    with self.assertRaises(NativeError):
                        prepare_context_attachments(
                            storage,
                            ArtifactTarget(context),
                            (ArtifactSource(source_uri=str(root / "missing")),),
                        )
                    scan.assert_not_called()


if __name__ == "__main__":
    unittest.main()
