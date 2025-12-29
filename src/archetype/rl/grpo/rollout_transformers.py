"""
Transformers Rollout Pattern (CPU-friendly)
===========================================

This is the fallback rollout engine when vLLM is unavailable (e.g. macOS dev).

It generates completions with `transformers` and returns the GRPO-critical artifacts:
- completion token IDs
- per-token logprobs for the sampled completion tokens

This keeps the same "artifact contract" as the vLLM rollout UDF so you can swap
engines later without changing downstream GRPO code.
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
class TransformersTextRollout:
    """
    Batch rollout using HuggingFace Transformers.

    Notes:
    - This is CPU-friendly and works on macOS.
    - It is slower than vLLM but perfect for correctness smoke tests.
    """

    def __init__(
        self,
        *,
        model: str,
        temperature: float = 0.7,
        max_tokens: int = 32,
        top_p: float = 1.0,
    ):
        import torch
        from transformers import AutoModelForCausalLM, AutoTokenizer

        self._torch = torch
        self._tokenizer = AutoTokenizer.from_pretrained(model, use_fast=True)
        if self._tokenizer.pad_token_id is None:
            self._tokenizer.pad_token = self._tokenizer.eos_token

        self._model = AutoModelForCausalLM.from_pretrained(
            model,
            torch_dtype=torch.float32,
        )
        self._device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self._model.to(self._device)
        self._model.eval()

        self._temperature = float(temperature)
        self._max_tokens = int(max_tokens)
        self._top_p = float(top_p)

    @daft.method.batch(return_dtype=_ROLLOUT_DTYPE)
    def generate(self, prompts: Series) -> Series:
        import torch

        prompt_texts = prompts.to_pylist()
        enc = self._tokenizer(
            prompt_texts,
            return_tensors="pt",
            padding=True,
            truncation=False,
            add_special_tokens=True,
        )
        input_ids = enc["input_ids"].to(self._device)
        attention_mask = enc["attention_mask"].to(self._device)

        do_sample = self._temperature > 0.0

        with torch.no_grad():
            out = self._model.generate(
                input_ids=input_ids,
                attention_mask=attention_mask,
                max_new_tokens=self._max_tokens,
                do_sample=do_sample,
                temperature=self._temperature if do_sample else None,
                top_p=self._top_p if do_sample else None,
                return_dict_in_generate=True,
                output_scores=True,
            )

        # out.sequences: [B, prompt_len + gen_len]
        # out.scores: list length gen_len, each [B, V] logits
        sequences = out.sequences
        scores = out.scores or []

        # Determine prompt lengths per row (number of non-pad tokens).
        prompt_lens = attention_mask.sum(dim=1).to(torch.int64).tolist()

        rows: list[dict] = []
        for i in range(sequences.shape[0]):
            pl = int(prompt_lens[i])
            gen_ids = sequences[i, pl:].tolist()

            token_logprobs: list[float] = []
            for t, tok_id in enumerate(gen_ids):
                if t >= len(scores):
                    token_logprobs.append(math.nan)
                    continue
                step_logits = scores[t][i]  # [V]
                lp = torch.log_softmax(step_logits, dim=-1)[tok_id].item()
                token_logprobs.append(float(lp))

            text = self._tokenizer.decode(gen_ids, skip_special_tokens=True)
            rows.append({"text": text, "token_ids": gen_ids, "token_logprobs": token_logprobs})

        return Series.from_pylist(rows)
