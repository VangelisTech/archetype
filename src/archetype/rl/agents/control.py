# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Control Surfaces and Tools
==========================

Define desired agent behavior through:
    - Tools: The action space
    - Sandboxes: Execution environments
    - Control Surfaces: Rubric weight configurations
    - Regimes: Behavior presets

The math decouples:
    - Schema defines dimensions (what can be evaluated)
    - Weights define emphasis (what matters more)
    - Tools define actions (what the agent can do)
    - Sandbox defines ground truth (did it actually work?)
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

# =============================================================================
# Tool Definitions
# =============================================================================


class ToolParameter(BaseModel):
    """A parameter for a tool."""

    name: str
    type: str = "string"
    description: str = ""
    required: bool = True
    enum: list[str] | None = None


class Tool(BaseModel):
    """
    A tool the agent can call.

    Tools are the action space. Each tool has:
    - Name and description (for LLM)
    - Parameters (structured input)
    - Executor (Python function that runs it)
    """

    name: str
    description: str
    parameters: list[ToolParameter] = Field(default_factory=list)

    _executor: Callable | None = None

    class Config:
        underscore_attrs_are_private = True

    def with_executor(self, executor: Callable) -> "Tool":
        """Attach an executor function to this tool."""
        self._executor = executor
        return self

    def execute(self, **kwargs) -> Any:
        """Execute the tool with given arguments."""
        if self._executor is None:
            raise RuntimeError(f"Tool {self.name} has no executor attached")
        return self._executor(**kwargs)

    def to_openai_schema(self) -> dict[str, Any]:
        """Convert to OpenAI function calling schema."""
        properties = {}
        required = []

        for param in self.parameters:
            properties[param.name] = {
                "type": param.type,
                "description": param.description,
            }
            if param.enum:
                properties[param.name]["enum"] = param.enum
            if param.required:
                required.append(param.name)

        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": {
                    "type": "object",
                    "properties": properties,
                    "required": required,
                },
            },
        }


class ToolCall(BaseModel):
    """A tool call made by the agent."""

    tool_name: str
    arguments: dict[str, Any]
    result: Any | None = None
    success: bool = False
    error: str | None = None
    execution_time_ms: float = 0.0


# =============================================================================
# Sandbox Execution
# =============================================================================


class SandboxResult(BaseModel):
    """Result from sandbox code execution."""

    stdout: str = ""
    stderr: str = ""
    return_value: Any | None = None
    exit_code: int = 0
    execution_time_ms: float = 0.0
    error: str | None = None

    @property
    def success(self) -> bool:
        return self.exit_code == 0 and self.error is None


class Sandbox(BaseModel):
    """
    Code execution sandbox.

    In production, connects to Docker, E2B, Modal, etc.
    """

    name: str = "default"
    language: str = "python"
    timeout_ms: int = 30000
    memory_limit_mb: int = 512

    _executor: Callable | None = None

    class Config:
        underscore_attrs_are_private = True

    def with_executor(self, executor: Callable) -> "Sandbox":
        """Attach a code executor."""
        self._executor = executor
        return self

    def execute(self, code: str) -> SandboxResult:
        """Execute code in the sandbox."""
        if self._executor is None:
            # Default: local exec (unsafe, for testing only)
            import time

            start = time.time()
            try:
                exec_globals = {}
                exec(code, exec_globals)
                return SandboxResult(
                    return_value=exec_globals.get("result"),
                    execution_time_ms=(time.time() - start) * 1000,
                )
            except Exception as e:
                return SandboxResult(
                    error=str(e),
                    exit_code=1,
                    execution_time_ms=(time.time() - start) * 1000,
                )
        return self._executor(code)


# =============================================================================
# Control Surface
# =============================================================================


class Regime(str, Enum):
    """Behavior regime presets."""

    DECISIVE = "decisive"  # Optimize for speed, accept more risk
    CAUTIOUS = "cautious"  # Optimize for safety, thorough analysis
    BALANCED = "balanced"  # Default balance
    EXPLORATORY = "exploratory"  # Prioritize learning, accept failures


@dataclass
class RubricBias:
    """Bias rubric weights toward specific behaviors."""

    # Phase emphasis
    observe_bias: float = 0.0
    orient_bias: float = 0.0
    decide_bias: float = 0.0
    act_bias: float = 0.0

    # Decision style
    confidence_bias: float = 0.0  # + = more confident actions
    risk_tolerance: float = 0.0  # + = accept more risk


@dataclass
class ControlSurface:
    """
    Define desired agent behavior.

    A control surface specifies:
    - Tools available to the agent
    - Sandbox for code execution
    - Rubric biases that shape behavior
    - Current operating regime
    """

    name: str = "default"
    description: str = ""

    tools: list[Tool] = field(default_factory=list)
    sandbox: Sandbox | None = None

    rubric_bias: RubricBias = field(default_factory=RubricBias)
    regime: Regime = Regime.BALANCED

    def add_tool(self, tool: Tool) -> "ControlSurface":
        """Add a tool to the surface."""
        self.tools.append(tool)
        return self

    def set_regime(self, regime: Regime) -> "ControlSurface":
        """Set the operating regime."""
        self.regime = regime
        # Apply regime-specific biases
        if regime == Regime.DECISIVE:
            self.rubric_bias.confidence_bias = 0.2
            self.rubric_bias.decide_bias = 0.1
        elif regime == Regime.CAUTIOUS:
            self.rubric_bias.risk_tolerance = -0.2
            self.rubric_bias.orient_bias = 0.1
        elif regime == Regime.EXPLORATORY:
            self.rubric_bias.risk_tolerance = 0.2
            self.rubric_bias.observe_bias = 0.1
        return self


# =============================================================================
# Pipeline Types (for multi-step agents)
# =============================================================================


class PipelineNodeType(str, Enum):
    """Types of pipeline nodes."""

    LLM_CALL = "llm_call"
    TOOL_CALL = "tool_call"
    SANDBOX = "sandbox"
    CONDITIONAL = "conditional"
    PARALLEL = "parallel"


@dataclass
class PipelineNode:
    """A node in an agent pipeline."""

    name: str
    node_type: PipelineNodeType
    config: dict[str, Any] = field(default_factory=dict)
    next_nodes: list[str] = field(default_factory=list)


@dataclass
class Pipeline:
    """
    Multi-step agent pipeline.

    Defines a graph of operations the agent executes.
    """

    name: str
    nodes: list[PipelineNode] = field(default_factory=list)
    entry_node: str = ""

    def add_node(self, node: PipelineNode) -> "Pipeline":
        """Add a node to the pipeline."""
        self.nodes.append(node)
        if not self.entry_node:
            self.entry_node = node.name
        return self


# =============================================================================
# Controllability Analysis
# =============================================================================


@dataclass
class ControllabilityMetrics:
    """Measures of agent controllability."""

    # Can we steer the agent's behavior?
    rubric_sensitivity: float = 0.0  # How much do rubric weights affect behavior?
    tool_coverage: float = 0.0  # What fraction of desired actions have tools?
    response_latency: float = 0.0  # How quickly does agent respond to changes?


@dataclass
class ObservabilityMetrics:
    """Measures of agent observability."""

    # Can we see what the agent is doing?
    reasoning_transparency: float = 0.0  # How clear is the reasoning?
    state_visibility: float = 0.0  # Can we inspect internal state?
    action_predictability: float = 0.0  # Can we predict next action?


def analyze_controllability(surface: ControlSurface) -> ControllabilityMetrics:
    """Analyze controllability of a control surface."""
    # Tool coverage
    tool_coverage = min(1.0, len(surface.tools) / 10.0)  # Assume 10 tools is good

    # Rubric sensitivity based on biases
    bias_sum = (
        abs(surface.rubric_bias.observe_bias)
        + abs(surface.rubric_bias.orient_bias)
        + abs(surface.rubric_bias.decide_bias)
        + abs(surface.rubric_bias.act_bias)
    )
    rubric_sensitivity = min(1.0, bias_sum)

    return ControllabilityMetrics(
        rubric_sensitivity=rubric_sensitivity,
        tool_coverage=tool_coverage,
        response_latency=0.5,  # Placeholder
    )


def analyze_observability(surface: ControlSurface) -> ObservabilityMetrics:
    """Analyze observability of a control surface."""
    # Sandbox provides better observability
    has_sandbox = 0.3 if surface.sandbox else 0.0

    return ObservabilityMetrics(
        reasoning_transparency=0.7,  # OODA provides good transparency
        state_visibility=0.5 + has_sandbox,
        action_predictability=0.5,
    )


# =============================================================================
# Factory Functions
# =============================================================================


def create_decisive_surface(tools: list[Tool] = None) -> ControlSurface:
    """Create a control surface optimized for fast decisions."""
    surface = ControlSurface(
        name="decisive",
        description="Optimized for speed and confidence",
        tools=tools or [],
    )
    return surface.set_regime(Regime.DECISIVE)


def create_cautious_surface(tools: list[Tool] = None) -> ControlSurface:
    """Create a control surface optimized for safety."""
    surface = ControlSurface(
        name="cautious",
        description="Optimized for thorough analysis",
        tools=tools or [],
    )
    return surface.set_regime(Regime.CAUTIOUS)


def create_coder_surface() -> ControlSurface:
    """Create a control surface for code generation agents."""
    tools = [
        Tool(
            name="write_file",
            description="Write content to a file",
            parameters=[
                ToolParameter(name="path", description="File path"),
                ToolParameter(name="content", description="File content"),
            ],
        ),
        Tool(
            name="read_file",
            description="Read content from a file",
            parameters=[
                ToolParameter(name="path", description="File path"),
            ],
        ),
        Tool(
            name="run_command",
            description="Run a shell command",
            parameters=[
                ToolParameter(name="command", description="Command to run"),
            ],
        ),
    ]

    return ControlSurface(
        name="coder",
        description="Control surface for code generation agents",
        tools=tools,
        sandbox=Sandbox(name="code_sandbox", language="python"),
    )
