# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Measure Daft coding frontiers against one instrumented Modal vLLM GPU."""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import statistics
import subprocess
import threading
import time
import urllib.request
from collections.abc import Iterable
from typing import Any

METRICS = {
    "generation_tokens": "vllm:generation_tokens_total",
    "kv_cache": "vllm:kv_cache_usage_perc",
    "preemptions": "vllm:num_preemptions_total",
    "prefix_hits": "vllm:prefix_cache_hits_total",
    "prefix_queries": "vllm:prefix_cache_queries_total",
    "prompt_tokens": "vllm:prompt_tokens_total",
    "running": "vllm:num_requests_running",
    "successes": "vllm:request_success_total",
    "waiting": "vllm:num_requests_waiting",
}
SAMPLE = re.compile(r"^([^\s{]+)(?:{[^}]*})?\s+([-+eE.0-9]+)$")


def endpoint_json(
    url: str,
    key: str,
    path: str,
) -> Any:
    request = urllib.request.Request(
        f"{url.rstrip('/')}{path}",
        headers={"Authorization": f"Bearer {key}"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[round((len(ordered) - 1) * quantile)]


def gpu_summary(samples: list[dict[str, float]]) -> dict[str, float | int | None]:
    active = [sample for sample in samples if sample["active_requests"] > 0]
    sm = [sample["sm"] for sample in active]
    power = [sample["power_w"] for sample in active]
    return {
        "gpu_active_samples": len(active),
        "gpu_sm_avg": round(statistics.fmean(sm), 1) if sm else None,
        "gpu_sm_p50": percentile(sm, 0.50),
        "gpu_sm_p95": percentile(sm, 0.95),
        "gpu_sm_peak": max(sm, default=None),
        "gpu_power_avg_w": round(statistics.fmean(power), 1) if power else None,
        "gpu_power_peak_w": max(power, default=None),
        "gpu_memory_peak_mib": max(
            (sample["memory_mib"] for sample in active),
            default=None,
        ),
    }


def metrics(url: str, key: str) -> dict[str, float]:
    request = urllib.request.Request(
        f"{url.rstrip('/')}/metrics",
        headers={"Authorization": f"Bearer {key}"},
    )
    result = {name: 0.0 for name in METRICS}
    found = set()
    with urllib.request.urlopen(request, timeout=10) as response:
        for raw in response:
            if match := SAMPLE.match(raw.decode().strip()):
                for name, metric in METRICS.items():
                    if match.group(1) == metric:
                        result[name] += float(match.group(2))
                        found.add(name)
    missing = set(METRICS) - found
    if missing:
        raise RuntimeError(f"missing vLLM metrics: {sorted(missing)}")
    return result


def monitor(
    url: str,
    key: str,
    stop: threading.Event,
    samples: list[dict[str, float]],
) -> None:
    while not stop.wait(0.2):
        try:
            samples.append(metrics(url, key))
        except OSError:
            pass


def last_json(output: str) -> dict[str, object]:
    for line in reversed(output.splitlines()):
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    return {}


def delta(after: dict[str, float], before: dict[str, float], name: str) -> float:
    value = after[name] - before[name]
    if value < 0:
        raise RuntimeError(f"vLLM counter reset during sample: {name}")
    return value


def run_frontier(
    *,
    agents: int,
    endpoint: str,
    key: str,
    model: str,
) -> dict[str, object]:
    before = metrics(endpoint, key)
    gpu_start = int(endpoint_json(endpoint, key, "/gpu/mark")["time_ns"])
    samples: list[dict[str, float]] = []
    stop = threading.Event()
    watcher = threading.Thread(
        target=monitor,
        args=(endpoint, key, stop, samples),
        daemon=True,
    )
    watcher.start()
    started = time.perf_counter()
    completed = subprocess.run(
        [
            "uv",
            "run",
            "--script",
            "experiments/vectorized_coding_agents.py",
            "--agents",
            str(agents),
            "--rounds",
            "1",
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=600,
        env={
            **os.environ,
            "LLM_API_KEY": key,
            "LLM_BASE_URL": f"{endpoint.rstrip('/')}/v1",
            "LLM_MODEL": model,
            "LLM_REASONING_EFFORT": "none",
        },
    )
    elapsed = time.perf_counter() - started
    gpu_end = int(endpoint_json(endpoint, key, "/gpu/mark")["time_ns"])
    gpu_samples = endpoint_json(
        endpoint,
        key,
        f"/gpu/samples?since_ns={gpu_start}&until_ns={gpu_end}",
    )
    stop.set()
    watcher.join()
    after = metrics(endpoint, key)
    summary = last_json(completed.stdout)
    frontier = next(
        (
            value
            for line in completed.stdout.splitlines()
            if isinstance((value := last_json(line)), dict) and "frontier" in value
        ),
        {},
    )
    inference_s = float(frontier.get("inference_s", elapsed))
    frontier_s = float(summary.get("frontier_compute_s", elapsed))
    generated = delta(after, before, "generation_tokens")
    prompted = delta(after, before, "prompt_tokens")
    valid = completed.returncode == 0
    return {
        "agents": agents,
        "exit": completed.returncode,
        "verified": summary.get("verified", 0),
        "inference_s": inference_s,
        "frontier_compute_s": frontier_s,
        "lifecycle_s": round(elapsed, 3),
        "prompt_tokens": int(prompted),
        "generation_tokens": int(generated),
        "total_tokens_s": round((prompted + generated) / inference_s, 1) if valid else None,
        "generation_tokens_s": round(generated / inference_s, 1) if valid else None,
        "accepted_per_frontier_s": (
            round(float(summary.get("verified", 0)) / frontier_s, 2) if valid else None
        ),
        "peak_running": max((sample["running"] for sample in samples), default=0),
        "peak_waiting": max((sample["waiting"] for sample in samples), default=0),
        "peak_kv_cache_fraction": round(
            max((sample["kv_cache"] for sample in samples), default=0),
            4,
        ),
        "preemptions": int(delta(after, before, "preemptions")),
        "prefix_cache_hits": int(delta(after, before, "prefix_hits")),
        "prefix_cache_queries": int(delta(after, before, "prefix_queries")),
        "server_successes": int(delta(after, before, "successes")),
        "stderr_tail": completed.stderr[-500:] if completed.returncode else "",
        **gpu_summary(gpu_samples),
    }


def levels(raw: str) -> Iterable[int]:
    return (int(value) for value in raw.split(","))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--model", default="gemma-4-e4b")
    parser.add_argument("--levels", default="1,4,8,16,32,64,96,128")
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--warmup-agents", type=int, default=3)
    args = parser.parse_args()
    key = os.environ["LLM_API_KEY"]
    results = []
    run_frontier(
        agents=args.warmup_agents,
        endpoint=args.endpoint,
        key=key,
        model=args.model,
    )
    generator = random.Random(0)
    for repeat in range(args.repeats):
        order = list(levels(args.levels))
        generator.shuffle(order)
        for agents in order:
            result = run_frontier(
                agents=agents,
                endpoint=args.endpoint,
                key=key,
                model=args.model,
            )
            result["repeat"] = repeat
            results.append(result)
            print(json.dumps(result), flush=True)
    failures = [result for result in results if result["exit"]]
    print(json.dumps({"runs": len(results), "failures": len(failures)}))
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
