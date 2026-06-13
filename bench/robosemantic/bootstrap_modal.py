# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Populate RoboSemanticBench Modal volumes directly from Hugging Face."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import modal

DATA_DIR = "/rsb-data"
GSM8K_DATA_DIR = "/rsb-gsm8k-data"
MMLUQA2_DATA_DIR = "/rsb-mmluqa2-data"
MODEL_CACHE_DIR = "/models"
PI05_CHECKPOINT_DIR = "/rsb-pi05-checkpoints"

MATH_PAYLOADS: tuple[dict[str, str], ...] = (
    {
        "repo_id": "VLyb/RSB-Math",
        "target": "rsb_math/rsb_math_train_500",
    },
    {
        "repo_id": "VLyb/RSB-Math-10blocks",
        "target": "rsb_math_10blocks/rsb_math_10blocks_train_500",
    },
)

GSM8K_FILES = {
    "train": "main/train-00000-of-00001.parquet",
    "test": "main/test-00000-of-00001.parquet",
}

MMLU_FILES = {
    "train": "all/auxiliary_train-00000-of-00001.parquet",
    "test": "all/test-00000-of-00001.parquet",
}

ANSWER_LETTERS = ["A", "B", "C", "D"]

ROBOTWIN_PI05_CHECKPOINT = {
    "repo_id": "HITdongdong/robotwin_pi05_aloha_agilex_randomized_5tasks_step20000",
    "train_config_name": "pi05_base_aloha_lora",
    "model_name": "robotwin_pi05_aloha_agilex_randomized_5tasks_step20000",
    "checkpoint_id": 20000,
}


def hf_cache_env(model_cache_dir: str = MODEL_CACHE_DIR) -> dict[str, str]:
    return {
        "HF_HOME": f"{model_cache_dir}/huggingface",
        "HF_HUB_CACHE": f"{model_cache_dir}/huggingface/hub",
        "TRANSFORMERS_CACHE": f"{model_cache_dir}/huggingface/hub",
        "HF_XET_HIGH_PERFORMANCE": "1",
    }


image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install("huggingface_hub", "hf_xet", "pyarrow")
    .env(hf_cache_env())
)

app = modal.App("archetype-robosemantic-bootstrap", image=image)
hf_secret = modal.Secret.from_name("hf-token")
data_volume = modal.Volume.from_name("robosemantic-rsb-data", create_if_missing=True)
gsm8k_data_volume = modal.Volume.from_name("robosemantic-rsb-gsm8k-data", create_if_missing=True)
mmluqa2_data_volume = modal.Volume.from_name("robosemantic-rsb-mmluqa2-data", create_if_missing=True)
model_cache_volume = modal.Volume.from_name("robosemantic-model-cache", create_if_missing=True)
pi05_checkpoints_volume = modal.Volume.from_name(
    "robosemantic-rsb-pi05-checkpoints",
    create_if_missing=True,
)


def load_parquet_rows(path: str | Path) -> list[dict[str, Any]]:
    import pyarrow.parquet as pq

    return pq.read_table(path).to_pylist()


def write_json(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")


def pi05_checkpoint_target_path(checkpoint: dict[str, Any]) -> Path:
    return (
        Path(PI05_CHECKPOINT_DIR)
        / str(checkpoint["train_config_name"])
        / str(checkpoint["model_name"])
        / str(checkpoint["checkpoint_id"])
    )


def repair_pi05_asset_layout(target_dir: Path) -> list[str]:
    """Make HF Robotwin assets match RSB's simple first-directory lookup."""
    assets_dir = target_dir / "assets"
    repaired: list[str] = []
    for nested_stats in assets_dir.glob("*/*/norm_stats.json"):
        flat_stats = nested_stats.parent.parent / "norm_stats.json"
        if not flat_stats.exists():
            flat_stats.write_bytes(nested_stats.read_bytes())
            repaired.append(str(flat_stats.relative_to(assets_dir)))
    return repaired


def extract_final_answer(answer: str) -> str:
    marker = "####"
    if marker in answer:
        return answer.split(marker, 1)[1].strip()
    return str(answer).strip().splitlines()[-1].strip()


def normalize_text(text: Any) -> str:
    return " ".join(str(text).strip().replace("/", " or ").replace("\\", " ").split())


def is_meaningful_text(text: str) -> bool:
    return any(ch.isalnum() for ch in text)


def answer_to_index(answer_value: Any) -> int:
    if isinstance(answer_value, int):
        return answer_value
    answer_text = normalize_text(answer_value)
    if answer_text in ANSWER_LETTERS:
        return ANSWER_LETTERS.index(answer_text)
    if answer_text.isdigit():
        return int(answer_text)
    raise ValueError(f"Unsupported MMLU answer label: {answer_value}")


def build_choice_pool(rows: list[dict[str, Any]]) -> list[str]:
    pool: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for choice in row.get("choices", []):
            normalized = normalize_text(choice)
            if normalized and is_meaningful_text(normalized) and normalized not in seen:
                pool.append(normalized)
                seen.add(normalized)
    return pool


def build_extra_options(
    choices: list[str],
    answer_idx: int,
    choice_pool: list[str],
) -> list[str]:
    final_answer = choices[answer_idx]
    options: list[str] = []
    seen = {final_answer}

    for idx, choice in enumerate(choices):
        if idx == answer_idx or not is_meaningful_text(choice) or choice in seen:
            continue
        options.append(choice)
        seen.add(choice)

    for candidate in choice_pool:
        if len(options) == 3:
            break
        if candidate not in seen:
            options.append(candidate)
            seen.add(candidate)

    if len(options) != 3:
        raise ValueError(f"Cannot build three MMLU distractors for answer: {final_answer}")
    return options


def build_mmlu_record(row: dict[str, Any], choice_pool: list[str]) -> dict[str, Any] | None:
    question = normalize_text(row.get("question", ""))
    choices = [normalize_text(choice) for choice in row.get("choices", [])]
    if len(choices) != 4:
        raise ValueError(f"Expected four MMLU choices, got {len(choices)}")
    answer_idx = answer_to_index(row.get("answer"))
    final_answer = choices[answer_idx]
    if not is_meaningful_text(question) or not is_meaningful_text(final_answer):
        return None
    correct_letter = ANSWER_LETTERS[answer_idx]
    return {
        "question": question,
        "answer": (
            f"The correct option is {correct_letter}, which corresponds to "
            f"{final_answer}. #### {final_answer}"
        ),
        "final_answer": final_answer,
        "extra_options": build_extra_options(choices, answer_idx, choice_pool),
        "category": normalize_text(row.get("subject") or "mmlu"),
    }


@app.function(
    volumes={DATA_DIR: data_volume, MODEL_CACHE_DIR: model_cache_volume},
    cpu=8,
    memory=16384,
    timeout=6 * 3600,
    secrets=[hf_secret],
    enable_memory_snapshot=True,
)
def download_math_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Download one RSB-Math payload directly into the data volume."""
    from huggingface_hub import snapshot_download

    repo_id = str(payload["repo_id"])
    target = str(payload["target"])
    force = bool(payload.get("force", False))
    target_dir = Path(DATA_DIR) / target
    scene_info = target_dir / "scene_info.json"
    data_dir = target_dir / "data"
    instruction_dir = target_dir / "instructions"
    hdf5_files = list(data_dir.glob("*.hdf5")) if data_dir.exists() else []
    instruction_files = list(instruction_dir.glob("*.json")) if instruction_dir.exists() else []

    if (
        not force
        and scene_info.exists()
        and (target_dir / "seed.txt").exists()
        and len(hdf5_files) >= 500
        and len(instruction_files) >= 500
    ):
        status = "already_present"
    else:
        target_dir.mkdir(parents=True, exist_ok=True)
        snapshot_download(
            repo_id=repo_id,
            repo_type="dataset",
            allow_patterns=["data/*", "instructions/*", "scene_info.json", "seed.txt"],
            ignore_patterns=["video/*", "_traj_data/*"],
            local_dir=str(target_dir),
            max_workers=16,
        )
        status = "downloaded"
        hdf5_files = list(data_dir.glob("*.hdf5")) if data_dir.exists() else []
        instruction_files = list(instruction_dir.glob("*.json")) if instruction_dir.exists() else []

    data_volume.commit()
    model_cache_volume.commit()
    return {
        "repo_id": repo_id,
        "target": str(target_dir),
        "status": status,
        "hdf5_files": len(hdf5_files),
        "instruction_files": len(instruction_files),
        "has_scene_info": scene_info.exists(),
        "has_seed": (target_dir / "seed.txt").exists(),
    }


@app.function(
    volumes={
        PI05_CHECKPOINT_DIR: pi05_checkpoints_volume,
        MODEL_CACHE_DIR: model_cache_volume,
    },
    cpu=4,
    memory=8192,
    timeout=6 * 3600,
    secrets=[hf_secret],
    enable_memory_snapshot=True,
)
def download_robotwin_pi05_checkpoint(force: bool = False) -> dict[str, Any]:
    """Download the public Robotwin pi05 OpenPI checkpoint into a Modal volume."""
    from huggingface_hub import snapshot_download

    checkpoint = ROBOTWIN_PI05_CHECKPOINT
    target_dir = pi05_checkpoint_target_path(checkpoint)
    params_dir = target_dir / "params"
    assets_dir = target_dir / "assets"
    has_checkpoint = (
        params_dir.exists()
        and (params_dir / "manifest.ocdbt").exists()
        and any(assets_dir.glob("*/norm_stats.json"))
    )
    if force or not has_checkpoint:
        target_dir.mkdir(parents=True, exist_ok=True)
        snapshot_download(
            repo_id=str(checkpoint["repo_id"]),
            repo_type="model",
            allow_patterns=["params/**", "assets/**", "_CHECKPOINT_METADATA"],
            local_dir=str(target_dir),
            max_workers=16,
        )
        status = "downloaded"
    else:
        status = "already_present"
    repaired_assets = repair_pi05_asset_layout(target_dir)

    pi05_checkpoints_volume.commit()
    model_cache_volume.commit()
    return {
        **checkpoint,
        "target": str(target_dir),
        "status": status,
        "has_params": (params_dir / "manifest.ocdbt").exists(),
        "asset_norm_stats": sorted(str(path.relative_to(assets_dir)) for path in assets_dir.glob("**/norm_stats.json")),
        "repaired_assets": repaired_assets,
    }


@app.function(
    volumes={
        GSM8K_DATA_DIR: gsm8k_data_volume,
        MMLUQA2_DATA_DIR: mmluqa2_data_volume,
        MODEL_CACHE_DIR: model_cache_volume,
    },
    cpu=4,
    memory=8192,
    timeout=2 * 3600,
    secrets=[hf_secret],
    enable_memory_snapshot=True,
)
def build_semantic_payloads(force: bool = False) -> dict[str, Any]:
    """Build GSM8K and MMLUQA2 JSON payloads into Modal volumes."""
    from huggingface_hub import hf_hub_download

    gsm8k_counts: dict[str, int] = {}
    for split, filename in GSM8K_FILES.items():
        out_path = Path(GSM8K_DATA_DIR) / f"{split}.json"
        if force or not out_path.exists():
            parquet_path = hf_hub_download(
                repo_id="openai/gsm8k",
                repo_type="dataset",
                filename=filename,
            )
            rows = load_parquet_rows(parquet_path)
            records = [
                {
                    "question": str(row.get("question", "")),
                    "answer": str(row.get("answer", "")),
                    "final_answer": extract_final_answer(str(row.get("answer", ""))),
                    "extra_options": [],
                }
                for row in rows
            ]
            write_json(out_path, records)
        gsm8k_counts[split] = len(json.loads(out_path.read_text(encoding="utf-8")))

    mmlu_rows: dict[str, list[dict[str, Any]]] = {}
    for split, filename in MMLU_FILES.items():
        parquet_path = hf_hub_download(
            repo_id="cais/mmlu",
            repo_type="dataset",
            filename=filename,
        )
        mmlu_rows[split] = load_parquet_rows(parquet_path)

    choice_pool = build_choice_pool([*mmlu_rows["train"], *mmlu_rows["test"]])
    mmlu_counts: dict[str, int] = {}
    for split, rows in mmlu_rows.items():
        out_path = Path(MMLUQA2_DATA_DIR) / f"{split}.json"
        if force or not out_path.exists():
            records = [
                record
                for row in rows
                if (record := build_mmlu_record(row, choice_pool)) is not None
            ]
            write_json(out_path, records)
        mmlu_counts[split] = len(json.loads(out_path.read_text(encoding="utf-8")))

    gsm8k_data_volume.commit()
    mmluqa2_data_volume.commit()
    model_cache_volume.commit()
    return {"gsm8k": gsm8k_counts, "mmluqa2": mmlu_counts}


@app.local_entrypoint()
def main(
    skip_math_payloads: bool = False,
    skip_semantic_payloads: bool = False,
    skip_pi05_checkpoint: bool = False,
    force: bool = False,
):
    """Populate Modal volumes used by bench/robosemantic/runner.py."""
    summaries: list[dict[str, Any]] = []
    if not skip_math_payloads:
        payloads = [{**payload, "force": force} for payload in MATH_PAYLOADS]
        summaries.extend(download_math_payload.map(payloads, order_outputs=True))

    semantic_summary = None
    if not skip_semantic_payloads:
        semantic_summary = build_semantic_payloads.remote(force=force)

    pi05_summary = None
    if not skip_pi05_checkpoint:
        pi05_summary = download_robotwin_pi05_checkpoint.remote(force=force)

    print("=== RoboSemanticBench Modal volume bootstrap ===")
    for summary in summaries:
        print(
            f"{summary['repo_id']} -> {summary['target']} "
            f"status={summary['status']} hdf5={summary['hdf5_files']} "
            f"instructions={summary['instruction_files']} "
            f"scene_info={summary['has_scene_info']} seed={summary['has_seed']}"
        )
    if semantic_summary is not None:
        print(f"semantic: {semantic_summary}")
    if pi05_summary is not None:
        print(
            f"pi05 checkpoint: {pi05_summary['repo_id']} -> {pi05_summary['target']} "
            f"status={pi05_summary['status']} has_params={pi05_summary['has_params']} "
            f"assets={pi05_summary['asset_norm_stats']}"
        )
