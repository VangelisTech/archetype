# 01: Security, Authentication, and Authorization

This document outlines the security model for Archetype, covering how identity is established, permissions are enforced, and actions are controlled. The primary goal is to create a secure, auditable, and scalable system that can support both trusted and untrusted agents.

## 1. Core Principles

- **Zero Trust by Default**: No action is permitted without explicit authorization.
- **Deterministic Enforcement**: Security rules are applied deterministically within the broker before any state is mutated.
- **Separation of Concerns**: Authentication (who you are) is handled at the edge, while Authorization (what you can do) is handled by the broker.
- **Scalable Policies**: The system should support a range of policies from simple role-based access control (RBAC) to fine-grained, attribute-based rules.

## 2. The `ActorCtx` (Actor Context)

The `ActorCtx` is the cornerstone of our security model. It's a Pydantic model that represents the identity and context of the actor initiating a command. It is created at the API edge (e.g., in a FastAPI dependency) and passed to the broker with every `enqueue` call.

### 2.1. `ActorCtx` Schema

```python
from uuid import UUID
from typing import Set
from pydantic import BaseModel, Field

class ActorCtx(BaseModel):
    """
    Represents the context of an actor for a single command or batch of commands.
    """
    id: UUID
    roles: Set[str] = Field(default_factory=set)
    org_id: UUID | None = None

    # --- Runtime budget counters ---
    # These are not part of the identity token but are attached by the broker
    # for the duration of a tick or session.
    tokens_used_today: int = 0
    commands_this_tick: int = 0

    model_config = dict(frozen=True)
```

### 2.2. Context Creation Flow

1.  An incoming request hits the API (e.g., FastAPI).
2.  An authentication middleware (e.g., a FastAPI dependency) validates the credential (API key, JWT, OAuth token).
3.  Upon successful validation, the middleware constructs the `ActorCtx` by looking up the actor's ID, roles, and organization from a trusted source (e.g., a database, a JWT payload).
4.  The `ActorCtx` is injected into the request and passed to the `broker.enqueue()` method.

## 3. Authorization: Guardrails and RBAC

Guardrails are checks performed by the broker's `guardrail_allow()` function *before* a command is added to the heap or the durable log. This prevents invalid commands from ever entering the system.

### 3.1. The `guardrail_allow()` Function

This function, located within the broker, will perform a series of checks. It returns `True` if the command is allowed and `False` otherwise.

```python
# Inside the broker implementation
def guardrail_allow(cmd: Command, ctx: ActorCtx) -> bool:
    # 1. RBAC Check
    if not _check_rbac(cmd, ctx):
        return False

    # 2. Budget Check
    if not _check_budget(cmd, ctx):
        return False

    # 3. Safety/Content Check (Future Extension)
    # e.g., check for malicious payloads, OWASP concerns
    # if not _check_safety(cmd, ctx):
    #     return False

    return True
```

### 3.2. Role-Based Access Control (RBAC)

For our initial implementation, we'll use a simple, dictionary-based RBAC model.

-   **`ROLE_PERMS`**: A dictionary mapping roles to a set of allowed `op` codes.

```python
# Example RBAC configuration
ROLE_PERMS = {
    "player": {"add_component", "remove_component"},
    "world_builder": {"spawn_entity", "delete_entity"},
    "admin": {"add_processor", "remove_processor"},
}

def _check_rbac(cmd: Command, ctx: ActorCtx) -> bool:
    """Checks if any of the actor's roles permit the command's operation."""
    for role in ctx.roles:
        if cmd.op in ROLE_PERMS.get(role, set()):
            return True
    return False
```

### 3.3. Budget Guardrails

Budgets prevent resource abuse. We'll start with two simple budgets:

-   **Commands per Tick**: Prevents a single actor from spamming the command queue in a single tick.
-   **Tokens per Day**: A simple proxy for LLM inference costs.

These will be tracked in simple in-memory dictionaries within the broker, reset as appropriate (per tick or daily).

## 4. Authentication Strategies (Future Vision)

While the initial implementation will rely on a simple API key or trusted header, the design must accommodate more robust authentication mechanisms.

-   **API Keys**: Simple to implement, good for service-to-service communication.
-   **JWTs (JSON Web Tokens)**: Standard for web and mobile clients. The JWT payload can directly contain the `actor_id` and `roles`.
-   **OAuth2**: For third-party integrations and user-delegated authority.
-   **IAM / Workload Identity**: For services running in a cloud environment (e.g., GCP, AWS), allowing cloud-managed service accounts to be mapped to an `ActorCtx`.
-   **Post-Quantum Cryptography**: For future-proofing token and signature verification. This will likely involve replacing standard cryptographic libraries with PQC-compliant ones (e.g., for JWT signing).

## 5. Near-Term Implementation Plan

1.  **Create `src/archetype/core/auth.py`**: This file will contain the `ActorCtx` Pydantic model.
2.  **Implement `guardrail_allow` in `AsyncCommandQueue`**: Add the `guardrail_allow` function and the simple, in-memory `ROLE_PERMS` dictionary.
3.  **Update `AsyncCommandQueue.enqueue`**: The `enqueue` method will now take `ctx: ActorCtx` as an argument and call `guardrail_allow` before processing the command.
4.  **Update `AsyncWorld` Facade**: The public methods on `AsyncWorld` (like `create_entity`) will need to be aware of the current `ActorCtx` to pass to the broker. For now, we can have the `AsyncWorld` hold a default "admin" context.
5.  **No API Changes Yet**: We will defer the FastAPI and authentication middleware changes until the core broker logic is in place.
