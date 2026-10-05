"""Existing simulated DDlog driver, real native CutStore and file pipeline."""

from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
import unittest
import wave
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

if "DDLOG_PYTHON_LIBRARY" not in os.environ:
    raise unittest.SkipTest(
        "Set DDLOG_PYTHON_LIBRARY to the built artifact-capable preview library"
    )

# Test fixture only; do not add a production source tree to the installed run.
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "packages/archetype-native/tests"))

import av
import numpy as np
from archetype_native import NativeError
from pypdf import PdfWriter
from test_binding import LIBRARY, Fixture

from archetype.artifacts.cut_attachments import prepare_attachments, publish_attachments
from archetype.artifacts.models import ArtifactSource
from archetype.storage.cut_artifacts import CutArtifactStorage, CutCoordinates


class CutArtifactTests(unittest.TestCase):
    def setUp(self):
        self.f = Fixture()
        self.addCleanup(self.f.close)
        self.binding = self.f.world("files", self.f.program())
        self.id = self.binding["scope"]["native_world"]
        self.f.host.start(self.id)
        self.f.running(self.binding)
        key = self.f.submit(self.binding)
        self.f.frozen(key)
        self.receipt = self.f.host.publish(self.binding, key)
        self.f.confirm(self.binding, key, self.receipt)
        self.cut = CutCoordinates(
            **{k: self.receipt[k] for k in ("world", "run", "tick", "cut_id")}
        )
        self.storage = CutArtifactStorage(self.f.host, self.binding)
        self.source = self.f.root / "a file.txt"
        self.source.write_text("first line\nsecond line\n")
        self.sources = (ArtifactSource(source_uri=str(self.source)),)

    def test_historical_cut_two_occurrences_and_exact_retry(self):
        before = self.f.host.read(self.receipt, "label")
        prepared = prepare_attachments(self.storage, self.cut, self.sources)
        # Move the live head after preparation. Publication must retain cut 1.
        key = self.f.submit(self.binding, "second", self.cut.cut_id, delete=True)
        self.f.frozen(key)
        second = self.f.host.publish(self.binding, key)
        self.f.confirm(self.binding, key, second)
        status = self.f.host.status(self.id)
        first = publish_attachments(self.storage, prepared)
        self.assertEqual(first, publish_attachments(self.storage, prepared))
        fresh = prepare_attachments(self.storage, self.cut, self.sources)
        self.assertNotEqual(prepared.artifacts[0].artifact_id, fresh.artifacts[0].artifact_id)
        self.assertEqual(prepared.artifacts[0].sha256, fresh.artifacts[0].sha256)
        publish_attachments(self.storage, fresh)
        result = self.storage.read(self.cut, limit=1)
        self.assertEqual((result["total"], result["next_offset"]), (2, 1))
        row = self.storage.decode(result["items"][0]["common"]).to_pylist()[0]
        self.assertEqual(
            (row["world"], row["run"], row["tick"], row["cut_id"]),
            (self.cut.world, self.cut.run, 1, self.cut.cut_id),
        )
        typed = self.storage.decode(result["items"][0]["typed"]["text"]).to_pylist()[0]
        self.assertEqual(typed["line_count"], 2)
        cut2 = CutCoordinates(**{k: second[k] for k in ("world", "run", "tick", "cut_id")})
        self.assertEqual(self.storage.read(cut2)["total"], 0)
        self.assertEqual(self.f.host.read(self.receipt, "label"), before)
        self.assertEqual(self.f.host.status(self.id)["revision"], status["revision"])
        self.assertEqual(self.f.host.history("files", "run_a")["receipts"], [self.receipt, second])

    def test_invalid_scope_and_unpublished_cut_fail_before_discovery(self):
        with patch(
            "archetype.artifacts.cut_attachments.scan_sources",
            side_effect=AssertionError("source effects"),
        ):
            for invalid in (replace(self.cut, world="other"), replace(self.cut, run="other")):
                with self.assertRaises(ValueError):
                    prepare_attachments(self.storage, invalid, self.sources)
            for invalid in (replace(self.cut, tick=2), replace(self.cut, cut_id="0" * 64)):
                with self.assertRaises(NativeError):
                    prepare_attachments(self.storage, invalid, self.sources)
        self.assertFalse((self.f.root / "storage/artifact_objects").exists())
        self.binding["scope"]["run"] = "changed"
        prepared = prepare_attachments(self.storage, self.cut, self.sources)
        self.assertEqual(prepared.cut.run, "run_a")

    def test_immutable_metadata_retry_in_fresh_process_and_corruption(self):
        prepared = prepare_attachments(self.storage, self.cut, self.sources)
        first = publish_attachments(self.storage, prepared)
        self.source.unlink()  # Retry cannot depend on source or scanner execution.
        request = {
            "binding": self.binding,
            "receipt": self.cut.as_dict(),
            "attachments": [item.as_dict() for item in prepared.occurrences],
        }
        payload = self.f.root / "retained-metadata.json"
        payload.write_text(json.dumps(request))
        self.f.close()
        script = """
import json, sys
from pathlib import Path
from archetype_native import Host
root = Path(sys.argv[1])
with Host(library=sys.argv[2], registry_root=root/'registry', build_root=root/'worlds', driver=root/'build.py', store_root=root/'storage') as host:
    request = json.loads((root/'retained-metadata.json').read_text())
    print(json.dumps(host.request('attach_artifacts', **request)))
"""
        result = subprocess.run(
            [sys.executable, "-c", script, str(self.f.root), str(LIBRARY)],
            check=True,
            capture_output=True,
            text=True,
            env=os.environ.copy(),
        )
        self.assertEqual(json.loads(result.stdout), list(first))
        self.f.host = self.f.open()
        storage = CutArtifactStorage(self.f.host, self.binding)
        self.assertEqual(storage.read(self.cut)["total"], 1)
        from urllib.parse import unquote, urlsplit

        Path(unquote(urlsplit(prepared.artifacts[0].uri).path)).write_bytes(b"corrupt")
        with self.assertRaisesRegex(NativeError, "Content size/digest"):
            storage.read(self.cut)
        with self.assertRaisesRegex(NativeError, "Content size/digest"):
            publish_attachments(storage, prepared)

    def test_all_six_typed_indexes_and_optional_empty_submission(self):
        audio = self.f.root / "tone.wav"
        with wave.open(str(audio), "wb") as writer:
            writer.setnchannels(1)
            writer.setsampwidth(2)
            writer.setframerate(8000)
            writer.writeframes(b"\x00\x00" * 2000)
        image = self.f.root / "pixel.png"
        image.write_bytes(
            base64.b64decode(
                "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M/wHwAF/gL+XfBvAAAAAElFTkSuQmCC"
            )
        )
        pdf = self.f.root / "paper.pdf"
        writer = PdfWriter()
        writer.add_blank_page(width=72, height=72)
        writer.write(pdf)
        video = self.f.root / "clip.mp4"
        with av.open(str(video), mode="w") as container:
            stream = container.add_stream("mpeg4", rate=10)
            stream.width = stream.height = 16
            stream.pix_fmt = "yuv420p"
            for index in range(2):
                frame = av.VideoFrame.from_ndarray(
                    np.full((16, 16, 3), index, dtype=np.uint8), format="rgb24"
                )
                for packet in stream.encode(frame):
                    container.mux(packet)
            for packet in stream.encode():
                container.mux(packet)
        diff = self.f.root / "change.patch"
        diff.write_text(
            "diff --git a/a.txt b/a.txt\n--- a/a.txt\n+++ b/a.txt\n@@ -1 +1 @@\n-old\n+new\n"
        )
        prepared = prepare_attachments(
            self.storage,
            self.cut,
            tuple(
                ArtifactSource(source_uri=str(path)) for path in (audio, image, pdf, video, diff)
            ),
        )
        publish_attachments(self.storage, prepared)
        typed = {
            name: self.storage.decode(encoded).to_pylist()[0]
            for item in self.storage.read(self.cut)["items"]
            for name, encoded in item["typed"].items()
        }
        self.assertEqual(set(typed), {"audio", "images", "pdf", "video", "text", "diff"})
        self.assertEqual(typed["audio"]["duration_seconds"], 0.25)
        self.assertEqual(typed["images"]["width"], 1)
        self.assertIs(typed["pdf"]["encrypted"], False)
        self.assertEqual(typed["video"]["fps"], 10.0)
        self.assertEqual(typed["diff"]["additions"], 1)
        empty = prepare_attachments(
            self.storage,
            self.cut,
            (ArtifactSource(source_uri=str(self.f.root / "missing"), required=False),),
        )
        self.assertEqual(publish_attachments(self.storage, empty), ())


class CutArtifactImportTests(unittest.TestCase):
    def test_import_does_not_load_retained_engine(self):
        subprocess.run(
            [
                sys.executable,
                "-c",
                "import sys; import archetype.artifacts.cut_attachments; assert not any(n.startswith('archetype.core') or n.startswith('archetype.runtime') for n in sys.modules)",
            ],
            check=True,
            capture_output=True,
            text=True,
        )


if __name__ == "__main__":
    unittest.main()
