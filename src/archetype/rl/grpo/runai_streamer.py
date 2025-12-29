"""
Run:ai Model Streamer integration (optional).

We use this to accelerate loading safetensors checkpoints (and/or adapters)
from local disk or object storage into CPU/GPU memory.

Docs:
- https://github.com/run-ai/runai-model-streamer/blob/master/docs/src/usage.md
- https://github.com/run-ai/runai-model-streamer/blob/master/docs/src/installation.md
- https://github.com/run-ai/runai-model-streamer/blob/master/docs/src/env-vars.md
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import torch


def is_available() -> bool:
    try:
        import runai_model_streamer  # noqa: F401
    except Exception:
        return False
    return True


def load_safetensors_with_streamer(
    *,
    file_path: str,
    device: str,
    is_distributed: bool = False,
    clone_tensors: bool = True,
) -> dict[str, torch.Tensor]:
    """
    Load tensors from a `.safetensors` file using Run:ai Model Streamer.

    Returns a state_dict (name -> tensor) on `device`.

    Notes from upstream docs:
    - yielded tensors may be overwritten; clone+detach to persist
    - distributed streaming can be enabled for multi-process workloads
      (https://github.com/run-ai/runai-model-streamer/blob/master/docs/src/usage.md)
    """
    import torch
    from runai_model_streamer import SafetensorsStreamer

    state: dict[str, torch.Tensor] = {}

    # Prefer `stream_files(..., device=..., is_distributed=...)` when possible.
    with SafetensorsStreamer() as streamer:
        try:
            streamer.stream_files([file_path], device=device, is_distributed=is_distributed)
        except TypeError:
            # Older versions may not accept device/is_distributed here; fallback to CPU streaming.
            streamer.stream_file(file_path)

        for name, tensor in streamer.get_tensors():
            t = tensor
            if t.device.type != torch.device(device).type or str(t.device) != device:
                t = t.to(device)
            if clone_tensors:
                t = t.clone().detach()
            state[name] = t

    return state
