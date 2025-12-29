# 08: Command Flow Architecture

This document synthesizes our command flow discussions, defining the
layered architecture for command processing in Archetype.

## 1. Core Principles

• Separation of Concerns: Each layer has a single responsibility for
scalability and maintainability.
• Decoupling: Commands are queued independently of processing for
reliability.
• Auditability: Full command history via broker logging.

## 2. Flow Overview

broker → service → orchestrator

• CommandBroker: Receives, orders, persists commands with RBAC/quotas.
• Service Layer (CommandService/WorldService): Unpacks payloads, validates,
applies business logic.
• Orchestrator (WorldOrchestrator): Executes validated commands on
simulation state.

## 3. Responsibilities by Layer

### 3.1. Broker Layer

• Authentication & authorization via guardrail_allow.
• Durable logging to Parquet.
• Priority queuing (heap-based ordering by tick/priority/seq).

### 3.2. Service Layer

• Command unpacking: Convert payloads to business objects.
• Validation: Parameters, permissions, state checks.
• Business logic: e.g., entity limits, component validation.
• Preparation: Transform for orchestrator.

### 3.3. Orchestrator Layer

• Pure execution: Apply prepared commands.
• State management: Update entities/components/systems.
• No business logic or validation.

## 4. Example Implementation

# Service Layer (WorldService)
async def create_entity(self, world_id: str, components: List[Component]):
    validated = [self._validate(c) for c in components]
    if len(validated) > MAX_COMPONENTS:
        raise ValueError("Limit exceeded")
    cmd = Command(op="create_entity", payload={"components": [c.dict() for
c in validated]})
    await self.broker.enqueue(cmd, actor_ctx)

# Orchestrator Layer
def apply_command(self, world: World, cmd: Command):
    if cmd.op == "create_entity":
        entity_id = world.create_entity()
        for comp_data in cmd.payload["components"]:
            world.add_component(entity_id, Component.from_dict(comp_data))

## 5. Integration with Other Systems

• Aligns with 03_BROKER_ARCHITECTURE.md for broker details.
• Complements 05_LLM_INTEGRATION.md by allowing LLM processors to enqueue
via services.

## 6. Near-Term Implementation Plan

1. Refine CommandService to handle unpacking for all ops.
2. Add validation hooks in WorldService.
3. Update orchestrator to expect prepared commands only.
4. Test end-to-end flow with sample commands.
