# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""LIBERO-Pro suite loader — issue #289 step 1.

Registers LIBERO-Pro's perturbed task variants (arXiv 2510.03827) as
first-class LIBERO benchmarks, from the HF-bucket copy at
``EverettKleven/models`` under ``all-libero/LIBERO-pro`` (private; needs
``HF_TOKEN``). Bucket layout, verified 2026-07-16 via Daft glob:

    bddl_files/<base>_<perturbation>/<TASK>.bddl      (16 variants x 10 tasks)
    init_files/<base>_<perturbation>/<TASK>.pruned_init   (paired 1:1)

with ``<base>`` in {libero_spatial, libero_object, libero_goal, libero_10}
and ``<perturbation>`` in {lan, object, swap, task}.

THE LANGUAGE TRAP (do not regress this): stock LIBERO derives a task's
instruction from its FILENAME, but LIBERO-Pro's perturbed BDDLs keep the
original filename and carry the perturbed instruction in the ``(:language
...)`` field — verified 2026-07-16: ``libero_spatial_lan`` says "lift the
black bowl ... and set it on the plate" where the filename says "pick up
...". A filename-derived loader would evaluate ORIGINAL instructions on
perturbed scenes and silently corrupt the experiment, so ``_discover_tasks``
parses the BDDL text and only falls back to the filename when no language
field exists.

RUN LEDGER (same rule as image.py — update only from watched runs):

    download_libero_pro   verified 2026-07-16 (local laptop): 322 files listed
                          from the bucket via Daft; full download NOT yet run.
    register_libero_pro   NEVER RUN against a real LIBERO install as of
                          2026-07-16 (pure parts covered by unit tests).
"""

from __future__ import annotations

import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_BUCKET_ROOT = "hf://buckets/EverettKleven/models/all-libero/LIBERO-pro"
_LANGUAGE_RE = re.compile(r"\(:language\s+(.+?)\s*\)", re.DOTALL)


@dataclass(frozen=True)
class ProTask:
    """One LIBERO-Pro task: files by stem, instruction from the BDDL text."""

    name: str
    language: str
    bddl_file: str
    init_states_file: str


def parse_language(bddl_text: str, filename: str) -> str:
    """Instruction for a BDDL: the ``(:language ...)`` field, else the stock
    filename derivation (LIBERO's ``grab_language_from_filename`` rules)."""
    m = _LANGUAGE_RE.search(bddl_text)
    if m:
        return " ".join(m.group(1).split())
    stem = filename[: filename.find(".bddl")] if ".bddl" in filename else filename
    if stem[:1].isupper():  # LIBERO-100-style SCENE names
        offset = 8 if "SCENE10" in stem else 7
        return " ".join(stem[stem.find("SCENE") + offset :].split("_"))
    return " ".join(stem.split("_"))


def _discover_tasks(root: str | Path) -> dict[str, list[ProTask]]:
    """Map variant -> tasks from a local ``bddl_files``/``init_files`` tree,
    validating the 1:1 bddl/init pairing (fail loudly on orphans)."""
    root = Path(root)
    variants: dict[str, list[ProTask]] = {}
    for variant_dir in sorted((root / "bddl_files").iterdir()):
        if not variant_dir.is_dir():
            continue
        tasks: list[ProTask] = []
        for bddl in sorted(variant_dir.glob("*.bddl")):
            stem = bddl.stem
            init = root / "init_files" / variant_dir.name / f"{stem}.pruned_init"
            if not init.is_file():
                raise FileNotFoundError(f"unpaired BDDL (no init state): {bddl} expects {init}")
            tasks.append(
                ProTask(
                    name=stem,
                    language=parse_language(bddl.read_text(), bddl.name),
                    bddl_file=f"{stem}.bddl",
                    init_states_file=f"{stem}.pruned_init",
                )
            )
        if tasks:
            variants[variant_dir.name] = tasks
    if not variants:
        raise FileNotFoundError(f"no LIBERO-Pro variants under {root}/bddl_files")
    return variants


def download_libero_pro(dest: str | Path, hf_token: str | None = None) -> Path:
    """Download the bucket's ``bddl_files`` + ``init_files`` trees to ``dest``
    via Daft (glob + download; the same reader the rest of the bench uses).
    Idempotent: files already present are not re-downloaded."""
    import daft  # noqa: PLC0415
    from daft import col  # noqa: PLC0415
    from daft.functions import download  # noqa: PLC0415
    from daft.io import HuggingFaceConfig, IOConfig  # noqa: PLC0415

    dest = Path(dest)
    io_config = IOConfig(hf=HuggingFaceConfig(token=hf_token or os.environ.get("HF_TOKEN")))
    listing = daft.from_glob_path(f"{_BUCKET_ROOT}/**", io_config=io_config)
    paths = [
        p for p in listing.collect().to_pydict()["path"] if p.endswith((".bddl", ".pruned_init"))
    ]
    missing = []
    for p in paths:
        rel = p.split("LIBERO-pro/")[-1]
        if not (dest / rel).is_file():
            missing.append(p)
    if missing:
        df = daft.from_pydict({"path": missing}).with_column(
            "data", download(col("path"), io_config=io_config)
        )
        rows = df.collect().to_pydict()
        for p, data in zip(rows["path"], rows["data"], strict=True):
            target = dest / p.split("LIBERO-pro/")[-1]
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
    return dest


def register_libero_pro(root: str | Path) -> list[str]:
    """Install a downloaded LIBERO-Pro tree into LIBERO's own bddl/init dirs
    and register one benchmark per variant in ``BENCHMARK_MAPPING``.
    Idempotent; returns the registered suite names."""
    from libero.libero import get_libero_path  # noqa: PLC0415
    from libero.libero.benchmark import (  # noqa: PLC0415
        Benchmark,
        Task,
        register_benchmark,
        task_maps,
    )

    variants = _discover_tasks(root)
    bddl_root = Path(get_libero_path("bddl_files"))
    init_root = Path(get_libero_path("init_states"))

    registered: list[str] = []
    for variant, tasks in variants.items():
        for kind, lib_root in (("bddl_files", bddl_root), ("init_files", init_root)):
            src_dir = Path(root) / kind / variant
            dst_dir = lib_root / variant
            dst_dir.mkdir(parents=True, exist_ok=True)
            for src in src_dir.iterdir():
                dst = dst_dir / src.name
                if not dst.is_file():
                    shutil.copy2(src, dst)

        task_maps[variant] = {
            t.name: Task(
                name=t.name,
                language=t.language,
                problem="Libero",
                problem_folder=variant,
                bddl_file=t.bddl_file,
                init_states_file=t.init_states_file,
            )
            for t in tasks
        }

        def _init(self: Any, task_order_index: int = 0, _name: str = variant) -> None:
            Benchmark.__init__(self, task_order_index=task_order_index)
            self.name = _name
            self._make_benchmark()

        suite_cls = type(variant.upper(), (Benchmark,), {"__init__": _init})
        register_benchmark(suite_cls)
        registered.append(variant)
    return registered
