# 11: Folder Structure Specification

This document specifies the high-level folder structure for Archetype,
ensuring clean separation of concerns.

## 1. Core Principles

• Modularity: Each folder has a focused purpose.
• Scalability: Easy to extend without core changes.
• Boundaries: Clear separation between simulation, runtime, and infra.

## 2. Structure Overview

src/archetype/
├── core/           # Pure simulation engine (ECS, archetypes, systems)
│   ├── aio/        # Async implementations
│   ├── sync/       # Sync implementations
│   └── ...         # Base classes, interfaces
├── runtime/        # Execution environment (Ray/Thread backends)
│   ├── udf/        # UDF and LLM helpers
│   └── ray_or_thread.py  # Executor abstraction
├── infra/          # Infrastructure (Ray cluster, deployments)
│   └── ray/        # Ray-specific infra
├── app/            # Application layer (services, broker, auth)
│   └── auth/       # Authentication components
└── scripting/      # Examples and scripting utilities

## 3. Key Folders Explained

• core/: Simulation logic only - no runtime dependencies.
• runtime/: Execution abstraction - Ray/Thread, UDF clients.
• infra/: Deployment and scaling - Ray cluster management.
• app/: Business logic - command flow, services.

## 4. Guidelines

• Avoid cross-folder dependencies (e.g., core imports nothing from runtime).
• New features: Place in runtime if execution-related, infra if deployment.

## 5. Relation to Other Designs

• Supports 09-execution-runtime-strategy.md via runtime/.
• Aligns with 10-llm-udf-integration.md for udf/ placement.

## 6. Near-Term Implementation Plan

1. Create missing folders (runtime/udf/).
2. Move existing files if needed.
3. Update imports and test structure.