# 09: Execution Runtime Strategy

This document outlines the dual-runtime execution model for Archetype,
aligned with Daft's runner configuration.

## 1. Core Principles

• Dual Runtime: Support Ray (distributed, eager=True) and
ThreadPoolExecutor (local, eager=False).
• Abstraction: Single Executor interface hides backend details.
• Daft Alignment: Mirrors set_runner_ray for configuration and fallbacks.
• Flexibility: Seamless switching without core simulation changes.

## 2. Executor Abstraction

Location: archetype/runtime/ray_or_thread.py

class Executor:
    def __init__(self, eager: bool = False, core_count: int = None):
        if eager and ray.is_initialized():
            self.backend = RayBackend(core_count)
        else:
            self.backend = NativeBackend(core_count)

    def execute(self, task):
        return self.backend.run(task)

• Ray Mode: Uses Ray tasks/actors for distributed execution.
• Thread Mode: Falls back to concurrent.futures.ThreadPoolExecutor.

## 3. Configuration

• eager: bool - Enable Ray if available (default False).
• core_count: int - Optional limit for parallelism.
• Fallback: Log warning and downgrade to threads if Ray unavailable.

## 4. Integration Points

• AsyncSystem: Uses Executor in execute() for DAG building/resolution.
• WorldOrchestrator: Enqueues via Executor, awaits single ref for atomic
updates.
• Backwards Compatibility: eager=False preserves existing behavior.

## 5. Relation to Other Designs

• Builds on 03_BROKER_ARCHITECTURE.md for scalable command processing.
• Enables 05_LLM_INTEGRATION.md via Ray actors for LLM routing.

## 6. Near-Term Implementation Plan

1. Implement Executor class with Ray/Thread backends.
2. Update AsyncSystem to use Executor.
3. Add config flags and logging.
4. Test both modes with benchmarks.
