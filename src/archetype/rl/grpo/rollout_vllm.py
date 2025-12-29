# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
vLLM Rollout Pattern (Inference-only)
====================================

This file shows the *minimal* vLLM pattern you need for GRPO:
- Generate completions
- Capture the exact generated token IDs (no retokenization later)
- Capture per-token logprobs (behavior policy logprobs)

Training still happens in PyTorch (see `pytorch_grpo.py`).

Note: This rollout UDF is **text-only**. For multimodal (image + question),
you can either:
- use your `daft.functions.prompt` vLLM provider (preferred in your stack), or
- extend this class to pass vLLM multimodal inputs (model-dependent).
"""

from __future__ import annotations

import math

import daft  # type: ignore[import-not-found]
from daft import DataType, Series  # type: ignore[import-not-found]

_ROLLOUT_DTYPE = DataType.struct(
    {
        "text": DataType.string(),
        "token_ids": DataType.list(DataType.int64()),
        "token_logprobs": DataType.list(DataType.float64()),
    }
)


@daft.cls()
class VllmTextRollout:
    """
    A stateful Daft class-UDF wrapper around vLLM's offline `LLM()`.

    Instantiated once per worker; batches run via `@daft.method.batch`.
    """

    def __init__(
        self,
        *,
        model: str,
        temperature: float = 0.0,
        max_tokens: int = 16,
        logprobs: int = 1,
    ):
        # Local import so environments without vllm can still import this module.
        from vllm import LLM, SamplingParams  # type: ignore[import-not-found]

        self._llm = LLM(model=model, trust_remote_code=True)
        self._params = SamplingParams(
            temperature=temperature,
            max_tokens=max_tokens,
            logprobs=logprobs,
        )

    @daft.method.batch(return_dtype=_ROLLOUT_DTYPE)
    def generate(self, prompts: Series) -> Series:
        outputs = self._llm.generate(prompts.to_pylist(), self._params)

        rows: list[dict] = []
        for out in outputs:
            choice = out.outputs[0]
            token_ids = list(choice.token_ids)

            # vLLM returns per-position top-k logprobs (dict[token_id -> Logprob]).
            token_logprobs: list[float] = []
            if choice.logprobs:
                for tid, lp_dict in zip(token_ids, choice.logprobs, strict=False):
                    if lp_dict and tid in lp_dict:
                        token_logprobs.append(float(lp_dict[tid].logprob))
                    else:
                        token_logprobs.append(math.nan)
            else:
                token_logprobs = [math.nan] * len(token_ids)

            rows.append(
                {
                    "text": choice.text,
                    "token_ids": token_ids,
                    "token_logprobs": token_logprobs,
                }
            )

        return Series.from_pylist(rows)
