"""
Minimal vLLM rollout pattern (what you'll swap into Daft prompt provider).

This example demonstrates the *artifact contract* GRPO needs:
- token_ids
- token_logprobs (behavior)

It is text-only; your multimodal provider will extend the same contract to
image+question messages.
"""

from __future__ import annotations

import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

import daft  # type: ignore[import-not-found]
from daft import col  # type: ignore[import-not-found]

from archetype.rl.grpo.rollout_vllm import VllmTextRollout


def main() -> None:
    # Tiny prompts: constrained to short generations.
    df = daft.from_pydict(
        {
            "prompt": [
                "Respond with a single letter A, B, C, or D. Answer: A",
                "Respond with a single letter A, B, C, or D. Answer: B",
            ]
        }
    )

    rollout = VllmTextRollout(
        model="Qwen/Qwen2.5-0.5B-Instruct", max_tokens=2, temperature=0.0, logprobs=1
    )
    df = df.with_column("rollout", rollout.generate(col("prompt")))
    df = df.select(
        "prompt",
        col("rollout")["text"].alias("text"),
        col("rollout")["token_ids"].alias("token_ids"),
    )
    df.show()


if __name__ == "__main__":
    main()
