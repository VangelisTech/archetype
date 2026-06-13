# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Prepare RoboSemanticBench payloads for local and Modal eval runs."""

from __future__ import annotations

import argparse
import json
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

DEFAULT_RSB_SOURCE = "/Users/darin/src/vendor/github.com/ZGC-EmbodyAI/RoboSemanticBench"
DATA_VOLUME = "robosemantic-rsb-data"

MATH_PAYLOADS = (
    (
        "VLyb/RSB-Math",
        "data/rsb_math/rsb_math_train_500",
    ),
    (
        "VLyb/RSB-Math-10blocks",
        "data/rsb_math_10blocks/rsb_math_10blocks_train_500",
    ),
)

GSM8K_FILES = (
    "main/train-00000-of-00001.parquet",
    "main/test-00000-of-00001.parquet",
)

MMLU_FILES = (
    "all/auxiliary_train-00000-of-00001.parquet",
    "all/test-00000-of-00001.parquet",
)


def run(cmd: list[str], *, cwd: Path | None = None) -> None:
    rendered = " ".join(shlex.quote(part) for part in cmd)
    if cwd is not None:
        print(f"+ cd {cwd} && {rendered}")
    else:
        print(f"+ {rendered}")
    subprocess.run(cmd, cwd=cwd, check=True)


def require_executable(name: str) -> str:
    executable = shutil.which(name)
    if executable is None:
        raise SystemExit(f"Missing required executable: {name}")
    return executable


def require_pyarrow() -> None:
    try:
        import pyarrow.parquet  # noqa: F401
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "Missing pyarrow. Run this script with: "
            "uv run --with pyarrow python bench/robosemantic/bootstrap_payloads.py"
        ) from exc


def hf_download(
    *,
    hf: str,
    repo: str,
    local_dir: Path,
    include: tuple[str, ...],
    exclude: tuple[str, ...] = (),
    workers: int = 16,
) -> None:
    cmd = [
        hf,
        "download",
        repo,
        "--repo-type",
        "dataset",
        "--local-dir",
        str(local_dir),
        "--max-workers",
        str(workers),
    ]
    for pattern in include:
        cmd.extend(["--include", pattern])
    for pattern in exclude:
        cmd.extend(["--exclude", pattern])
    run(cmd)


def validate_parquet(path: Path) -> None:
    import pyarrow.parquet as pq

    pq.read_metadata(path)


def curl_hf_file(*, curl: str, repo: str, remote_file: str, destination: Path) -> None:
    if destination.exists() and destination.stat().st_size > 0:
        print(f"Already present: {destination}")
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    url = f"https://huggingface.co/datasets/{repo}/resolve/main/{remote_file}"
    run([curl, "-L", "--fail", "--retry", "5", "--output", str(destination), url])


def ensure_hf_parquet(*, curl: str, repo: str, remote_file: str, destination: Path) -> None:
    if destination.exists() and destination.stat().st_size > 0:
        try:
            validate_parquet(destination)
            print(f"Verified existing parquet: {destination}")
            return
        except Exception:
            print(f"Removing invalid partial parquet: {destination}")
            destination.unlink()
    curl_hf_file(curl=curl, repo=repo, remote_file=remote_file, destination=destination)
    validate_parquet(destination)


def prepare_math_payloads(rsb_source: Path, *, hf: str, workers: int) -> None:
    for repo, relative_dir in MATH_PAYLOADS:
        hf_download(
            hf=hf,
            repo=repo,
            local_dir=rsb_source / relative_dir,
            include=("data/*", "instructions/*", "scene_info.json", "seed.txt"),
            exclude=("video/*", "_traj_data/*"),
            workers=workers,
        )


def extract_final_answer(answer: str) -> str:
    marker = "####"
    if marker in answer:
        return answer.split(marker, 1)[1].strip()
    return str(answer).strip().splitlines()[-1].strip()


def load_parquet_rows(path: Path) -> list[dict]:
    import pyarrow.parquet as pq

    return pq.read_table(path).to_pylist()


def write_json(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {len(records)} records to {path}")


def prepare_gsm8k(rsb_source: Path, *, curl: str) -> None:
    source_dir = rsb_source / "gsm8k" / "source"
    for remote_file in GSM8K_FILES:
        ensure_hf_parquet(
            curl=curl,
            repo="openai/gsm8k",
            remote_file=remote_file,
            destination=source_dir / remote_file,
        )

    output_dir = rsb_source / "gsm8k" / "data"
    split_files = {
        "train": source_dir / "main" / "train-00000-of-00001.parquet",
        "test": source_dir / "main" / "test-00000-of-00001.parquet",
    }
    for split, parquet_path in split_files.items():
        rows = load_parquet_rows(parquet_path)
        records = [
            {
                "question": str(row.get("question", "")),
                "answer": str(row.get("answer", "")),
                "final_answer": extract_final_answer(str(row.get("answer", ""))),
                # RSB can synthesize numeric distractors at eval time, but the
                # env expects the key to exist for four-choice HardMath.
                "extra_options": [],
            }
            for row in rows
        ]
        write_json(output_dir / f"{split}.json", records)


def prepare_mmluqa2(rsb_source: Path, *, curl: str, allow_test_train_fallback: bool) -> None:
    source_dir = rsb_source / "mmlu" / "mmlu"
    for remote_file in MMLU_FILES:
        try:
            ensure_hf_parquet(
                curl=curl,
                repo="cais/mmlu",
                remote_file=remote_file,
                destination=source_dir / remote_file,
            )
        except Exception:
            if remote_file != MMLU_FILES[0] or not allow_test_train_fallback:
                raise
            test_path = source_dir / MMLU_FILES[1]
            ensure_hf_parquet(
                curl=curl,
                repo="cais/mmlu",
                remote_file=MMLU_FILES[1],
                destination=test_path,
            )
            fallback_path = source_dir / remote_file
            fallback_path.parent.mkdir(parents=True, exist_ok=True)
            fallback_path.write_bytes(test_path.read_bytes())
            print(
                "WARNING: using MMLU test parquet as auxiliary_train fallback. "
                "Use this only for smoke tests, not paper-protocol eval."
            )
    run([sys.executable, str(rsb_source / "mmluqa2" / "generate_dataset.py")], cwd=rsb_source)


def verify_payloads(
    rsb_source: Path,
    *,
    verify_math_payloads: bool,
    verify_gsm8k: bool,
    verify_mmluqa2: bool,
) -> None:
    expected: list[str] = []
    if verify_math_payloads:
        expected.extend(
            [
                "data/rsb_math/rsb_math_train_500/scene_info.json",
                "data/rsb_math_10blocks/rsb_math_10blocks_train_500/scene_info.json",
            ]
        )
    if verify_gsm8k:
        expected.extend(["gsm8k/data/train.json", "gsm8k/data/test.json"])
    if verify_mmluqa2:
        expected.extend(["mmluqa2/data/train.json", "mmluqa2/data/test.json"])
    missing = [path for path in expected if not (rsb_source / path).exists()]
    if missing:
        rendered = "\n".join(f"  - {path}" for path in missing)
        raise SystemExit(f"RoboSemanticBench payload bootstrap is incomplete:\n{rendered}")
    print("RoboSemanticBench payload bootstrap verified.")


def upload_modal_data_volume(rsb_source: Path, *, modal: str, force: bool) -> None:
    create_cmd = [modal, "volume", "create", DATA_VOLUME]
    print("+ " + " ".join(shlex.quote(part) for part in create_cmd))
    created = subprocess.run(create_cmd, check=False, capture_output=True, text=True)
    if created.returncode != 0 and "exist" not in f"{created.stdout}\n{created.stderr}".lower():
        raise subprocess.CalledProcessError(
            created.returncode,
            create_cmd,
            output=created.stdout,
            stderr=created.stderr,
        )
    cmd = [modal, "volume", "put", DATA_VOLUME, str(rsb_source / "data"), "/"]
    if force:
        cmd.insert(3, "--force")
    run(cmd)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bootstrap RoboSemanticBench payloads")
    parser.add_argument("--rsb-source", default=DEFAULT_RSB_SOURCE)
    parser.add_argument("--hf-workers", type=int, default=16)
    parser.add_argument("--skip-math-payloads", action="store_true")
    parser.add_argument("--skip-gsm8k", action="store_true")
    parser.add_argument("--skip-mmluqa2", action="store_true")
    parser.add_argument("--allow-mmlu-test-train-fallback", action="store_true")
    parser.add_argument("--upload-modal-data-volume", action="store_true")
    parser.add_argument("--force-upload", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rsb_source = Path(args.rsb_source).expanduser().resolve()
    hf = require_executable("hf")
    curl = require_executable("curl")
    require_pyarrow()

    if not args.skip_math_payloads:
        prepare_math_payloads(rsb_source, hf=hf, workers=args.hf_workers)
    if not args.skip_gsm8k:
        prepare_gsm8k(rsb_source, curl=curl)
    if not args.skip_mmluqa2:
        prepare_mmluqa2(
            rsb_source,
            curl=curl,
            allow_test_train_fallback=args.allow_mmlu_test_train_fallback,
        )

    verify_payloads(
        rsb_source,
        verify_math_payloads=not args.skip_math_payloads,
        verify_gsm8k=not args.skip_gsm8k,
        verify_mmluqa2=not args.skip_mmluqa2,
    )

    if args.upload_modal_data_volume:
        modal = require_executable("modal")
        upload_modal_data_volume(rsb_source, modal=modal, force=args.force_upload)


if __name__ == "__main__":
    main()
