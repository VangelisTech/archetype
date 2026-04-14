# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Archetype evaluation suite.

Structured after Anthropic's guide to agent evals:
https://www.anthropic.com/engineering/demystifying-evals-for-ai-agents

Core concepts:
- **Task**: A single test with defined inputs, success criteria, and graders.
- **Trial**: One attempt at a task.  Multiple trials handle non-determinism.
- **Grader**: Logic that scores an aspect of agent performance (code-based,
  model-based, or human).  A task can have multiple graders.
- **Eval suite**: A collection of tasks measuring specific capabilities.
  - *Regression suites* should have ~100% pass rate (protect against backsliding).
  - *Capability suites* start at a low pass rate (give teams a hill to climb).
- **pass@k / pass^k**: Metrics for success across multiple trials.
"""
