# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Archetype evaluation suite.

Structured after Anthropic's eval taxonomy (unit → integration → end-to-end):
https://www.anthropic.com/engineering/demystifying-evals-for-ai-agents

- **Unit evals**: Test a single operation in isolation (deterministic, fast).
- **Integration evals**: Test multi-step component interactions (storage, command pipeline).
- **End-to-end evals**: Test full simulation workflows against real environments.

Each eval is an EvalCase with inputs, expected outputs, and a Grader that
produces an EvalResult with a score.  The runner collects results by tier
and reports pass/fail with timing.
"""
