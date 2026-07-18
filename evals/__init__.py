# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Repository-level verification scenarios for Archetype.

This package checks the framework itself. Its task, trial, and suite labels are
runner implementation terms, not the public dataset/evaluation domain model.

Runner concepts:
- **Task**: A single test with defined inputs, success criteria, and graders.
- **Trial**: One attempt at a task.  Multiple trials handle non-determinism.
- **Grader**: Logic that scores an aspect of agent performance (code-based,
  model-based, or human).  A task can have multiple graders.
- **Suite**: A collection of tasks with one repository-checking concern.
- **pass rate**: The observed correct fraction ``c / n`` for one task.
- **pass@k**: The unbiased expectation that any sample in a size-``k`` subset
  succeeds, averaged across tasks (Chen et al., arXiv:2107.03374).
- **pass^k**: Strict repeatability; every one of the ``k`` trials succeeds.
"""
