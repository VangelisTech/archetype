# 04: API and Agents

This document defines the external-facing contract for Archetype, including the web API, the command-line interface (CLI), and the formal definition of an "agent."

## 1. Core Principles

-   **API-First**: The primary way to interact with a running Archetype simulation is through a well-defined, versioned API.
-   **Stateless API, Stateful World**: The API endpoints themselves are stateless. All state is managed by the `World` and the `Broker`.
-   **Agent as a Configuration**: An "agent" is not a class within the simulation but rather an external process or policy defined by a configuration that interacts with the `World` via the API.

## 2. Web API (FastAPI)

We will use FastAPI to create a modern, asynchronous web API.

### 2.1. Directory Structure

The API code will live in `src/archetype/api/`, separate from the core simulation logic.

```
src/archetype/api/
├── __init__.py
├── main.py          # FastAPI app factory
├── deps.py          # Dependencies (e.g., get_broker, get_actor_ctx)
└── routers/
    ├── command.py   # /commands endpoint
    └── world.py     # /worlds/{id} endpoints
```

### 2.2. Key Endpoints

-   **`POST /v1/commands`**: The primary endpoint for submitting commands to the simulation.
    -   **Request Body**: A JSON representation of a `Command` object.
    -   **Authentication**: Requires a valid authentication token (e.g., JWT) which is used to construct the `ActorCtx`.
    -   **Response**: `202 Accepted` with the command ID.

-   **`GET /v1/worlds/{world_id}/tick`**: Returns the current tick of a given world.

-   **`GET /v1/worlds/{world_id}/snapshot`**: Returns a snapshot of the world state at a given tick. (This is a long-term vision and requires the snapshotting mechanism to be implemented).

## 3. Command-Line Interface (CLI)

The CLI provides a convenient way for developers and operators to interact with the simulation. We will use a library like Typer or Click to build the CLI.

### 3.1. Key Commands

-   **`archetype api run`**: Starts the FastAPI server.
-   **`archetype sim run`**: Runs a simulation directly (for local development and testing).
-   **`archetype command send <op> [payload]`**: A convenience wrapper for sending a command to a running simulation via the API.

## 4. Agent Definition

An agent is an external process that observes the state of the `World` and submits `Commands` to influence it.

### 4.1. Agent Configuration (`agent.yaml`)

An agent's behavior is defined by a configuration file, not by a class that is loaded into the simulation.

```yaml
id: "agent-007"
name: "MarketTrader"
roles: ["player", "trader"]

# The policy defines how the agent makes decisions.
policy:
  type: "llm" # or "heuristic", "random", etc.
  model: "gpt-4o-mini"
  system_prompt: "You are a stock trader in a simulated market..."

# The observation defines what slice of the world state the agent can see.
observe:
  query: "SELECT * FROM market_data WHERE tick = :current_tick"

# The budget defines the agent's resource limits.
budget:
  commands_per_tick: 10
  tokens_per_day: 100000
```

### 4.2. Agent Runner

An "agent runner" is a separate process that:
1.  Loads the `agent.yaml` configuration.
2.  Connects to the Archetype API.
3.  In a loop:
    a.  Queries the `World` state based on the `observe` configuration.
    b.  Passes the observation to the `policy` to generate a `Command`.
    c.  Submits the `Command` to the `/v1/commands` endpoint.

This decoupled architecture allows for agents to be written in any language and to be run on separate infrastructure from the simulation itself.

## 5. Near-Term Implementation Plan

1.  **Create `src/archetype/api/` directory structure**.
2.  **Implement a basic FastAPI server** in `api/main.py` with a health check endpoint.
3.  **Implement the `/v1/commands` endpoint** in `api/routers/command.py`.
4.  **Create a simple CLI** with the `api run` command.
5.  **Develop a sample agent runner** that can load a YAML file and interact with the API.
