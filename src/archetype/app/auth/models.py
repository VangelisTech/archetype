# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import uuid_utils as uuid
from typing import Set, Dict, Any, Literal, List, Optional
from uuid_utils import UUID
from itertools import count
import json
from datetime import datetime

import pyarrow as pa
from pydantic import BaseModel, Field, FieldSerializationInfo, field_serializer

# Global sequence counter for command ordering
_SEQ = count()

# --- Core Identity Models ---

class ActorCtx(BaseModel):
    """
    Base class for all actors. Carries process identity, roles, and organization. 
    Immutable (frozen) so nobody mutates it after enqueue.
    """
    id: UUID = Field(default_factory=uuid.uuid7)
    roles: Set[str] = Field(default_factory=set)
    org: Optional[str] = None
    # UC-related, populated by middleware when bearer is a UC user token
    principal: Optional[str] = None  # user email or subject
    uc_token: Optional[str] = None   # raw UC bearer token for downstream clients

    model_config = {"frozen": True, "arbitrary_types_allowed": True}

# --- Authentication Models ---

class User(BaseModel):
    """
    Represents a user in the system for authentication.
    Used in login flows and tied to ActorCtx.
    """
    id: UUID = Field(default_factory=uuid.uuid7)
    username: str
    email: str
    hashed_password: str  # Never store plain text; use e.g., bcrypt
    roles: Set[str] = Field(default_factory=set)  # e.g., {"admin", "user"}
    org: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    is_active: bool = True

    model_config = {"frozen": True, "arbitrary_types_allowed": True}

    @field_serializer("id")
    def serialize_uuid(self, v: UUID, info: FieldSerializationInfo):
        if info.mode == 'json':
            return str(v)
        return v.bytes

    @field_serializer("created_at")
    def serialize_datetime(self, v: datetime, info: FieldSerializationInfo):
        return v.isoformat()

class Token(BaseModel):
    """
    JWT or access token for authenticated sessions.
    Used in API authentication middleware.
    """
    access_token: str
    token_type: str = "bearer"
    expires_in: int  # seconds until expiration
    refresh_token: Optional[str] = None
    issued_at: datetime = Field(default_factory=datetime.utcnow)

    @field_serializer("issued_at")
    def serialize_datetime(self, v: datetime, info: FieldSerializationInfo):
        return v.isoformat()

# --- Access Control (RBAC) Models ---

class Permission(BaseModel):
    """
    Fine-grained permission definition.
    Can be used to build role permissions dynamically.
    """
    name: str  # e.g., "create_world", "run_simulation", "manage_users"
    resource: Optional[str] = None  # e.g., "world", "simulation", "user"
    action: Optional[str] = None  # e.g., "create", "read", "update", "delete"
    description: Optional[str] = None

    model_config = {"frozen": True}

class Role(BaseModel):
    """
    Defines a role with associated permissions.
    Assigned to users/actors for RBAC in guardrails.
    Example: Used in broker.py to check if actor can enqueue certain ops.
    """
    name: str  # e.g., "admin", "user", "guest", "simulation_runner"
    permissions: Set[str] = Field(default_factory=set)  # e.g., {"create_world", "run_simulation"}
    description: Optional[str] = None
    is_system_role: bool = False  # True for built-in roles that can't be deleted

    model_config = {"frozen": True}


class AuthConfig(BaseModel):
    """Authentication configuration used by API middleware and CLI provider.

    Fields:
    - enabled: turn auth on/off globally (dev convenience)
    - strict_mode: if True, invalid tokens raise 401; otherwise, fallback to admin context
    - provider: name of the provider (e.g., 'uc-google', 'okta', etc.)
    - uc_auth_url/token_url/client_id/client_secret: for UC OAuth device or web flows (optional)
    """

    enabled: bool = False
    strict_mode: bool = False
    provider: Optional[str] = None

    # Optional UC OAuth fields for future use
    uc_auth_url: Optional[str] = None
    uc_token_url: Optional[str] = None
    uc_client_id: Optional[str] = None
    uc_client_secret: Optional[str] = None

    model_config = {"frozen": False}

# --- Guardrail and Budget Models ---

class RateLimit(BaseModel):
    """
    Defines rate limiting rules for guardrails.
    Used in broker.py to prevent abuse (e.g., max commands per minute).
    """
    max_requests: int  # e.g., 100
    per_period_seconds: int  # e.g., 60 (per minute)
    current_count: int = 0  # Runtime counter, reset periodically
    last_reset: datetime = Field(default_factory=datetime.utcnow)
    applies_to: str = "all"  # "all", "user", "org", or specific resource

    def is_exceeded(self) -> bool:
        """Check if rate limit is currently exceeded."""
        now = datetime.utcnow()
        seconds_elapsed = (now - self.last_reset).total_seconds()
        
        if seconds_elapsed >= self.per_period_seconds:
            # Reset period has passed
            return False
        
        return self.current_count >= self.max_requests

    def increment(self):
        """Increment the current count, resetting if period has passed."""
        now = datetime.utcnow()
        seconds_elapsed = (now - self.last_reset).total_seconds()
        
        if seconds_elapsed >= self.per_period_seconds:
            # Reset the counter
            self.current_count = 1
            self.last_reset = now
        else:
            self.current_count += 1

    @field_serializer("last_reset")
    def serialize_datetime(self, v: datetime, info: FieldSerializationInfo):
        return v.isoformat()

class Quota(BaseModel):
    """
    Resource quotas for users/orgs.
    Used in simulation_service.py to limit parallel worlds or steps.
    Example: "max_worlds: 10" prevents unlimited resource consumption.
    """
    max_worlds: int = 10
    max_steps_per_world: int = 10000
    max_concurrent_runs: int = 5
    max_commands_per_hour: int = 1000
    current_usage: Dict[str, int] = Field(default_factory=dict)  # e.g., {"worlds": 3, "runs": 1}

    def check_world_limit(self, requested_worlds: int = 1) -> bool:
        """Check if creating new worlds would exceed quota."""
        current_worlds = self.current_usage.get("worlds", 0)
        return current_worlds + requested_worlds <= self.max_worlds

    def check_run_limit(self, requested_runs: int = 1) -> bool:
        """Check if starting new runs would exceed quota."""
        current_runs = self.current_usage.get("runs", 0)
        return current_runs + requested_runs <= self.max_concurrent_runs

    def increment_usage(self, resource: str, amount: int = 1):
        """Increment usage counter for a resource."""
        self.current_usage[resource] = self.current_usage.get(resource, 0) + amount

    def decrement_usage(self, resource: str, amount: int = 1):
        """Decrement usage counter for a resource."""
        current = self.current_usage.get(resource, 0)
        self.current_usage[resource] = max(0, current - amount)

class Budget(BaseModel):
    """
    Usage-based budgeting (e.g., for cloud costs).
    Tied to org/user; checked in command_service.py before expensive ops.
    """
    total_credits: float  # e.g., 1000.0 (abstract "credits" for computation)
    used_credits: float = 0.0
    cost_per_step: float = 0.01  # Example rate
    cost_per_world: float = 1.0  # Cost to create a world
    cost_per_command: float = 0.001  # Cost per command
    currency: str = "credits"  # For display purposes

    def has_sufficient(self, estimated_cost: float) -> bool:
        """Check if there are sufficient credits for an operation."""
        return self.used_credits + estimated_cost <= self.total_credits

    def estimate_simulation_cost(self, num_worlds: int, steps_per_world: int) -> float:
        """Estimate cost for a simulation."""
        world_cost = num_worlds * self.cost_per_world
        step_cost = num_worlds * steps_per_world * self.cost_per_step
        return world_cost + step_cost

    def charge(self, amount: float) -> bool:
        """Charge credits and return success."""
        if self.has_sufficient(amount):
            self.used_credits += amount
            return True
        return False

    def remaining_credits(self) -> float:
        """Get remaining credits."""
        return max(0.0, self.total_credits - self.used_credits)

# --- Audit and Security Models ---

class SecurityEvent(BaseModel):
    """
    Security event for audit logging.
    Used to track authentication failures, permission denials, etc.
    """
    id: UUID = Field(default_factory=uuid.uuid7)
    event_type: Literal["auth_failure", "permission_denied", "quota_exceeded", "suspicious_activity"]
    actor_id: Optional[UUID] = None
    resource: Optional[str] = None  # e.g., "world_123", "simulation_456"
    action: Optional[str] = None  # e.g., "create_world", "run_simulation"
    details: Dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    severity: Literal["low", "medium", "high", "critical"] = "medium"

    model_config = {"frozen": True, "arbitrary_types_allowed": True}

    @field_serializer("id", "actor_id")
    def serialize_uuids(self, v: Optional[UUID], info: FieldSerializationInfo):
        if v is None:
            return None
        if info.mode == 'json':
            return str(v)
        return v.bytes

    @field_serializer("timestamp")
    def serialize_datetime(self, v: datetime, info: FieldSerializationInfo):
        return v.isoformat()

    @classmethod
    def arrow_schema(cls) -> pa.Schema:
        """Return a canonical Arrow schema for Parquet logging."""
        return pa.schema([
            ("id", pa.binary(16)),
            ("event_type", pa.string()),
            ("actor_id", pa.binary(16)),
            ("resource", pa.string()),
            ("action", pa.string()),
            ("details", pa.string()),  # JSON-encoded
            ("timestamp", pa.timestamp('us')),
            ("severity", pa.string()),
        ])

    def to_arrow(self) -> pa.RecordBatch:
        """Convert to Arrow for efficient logging."""
        return pa.record_batch([
            [self.id.bytes],
            [self.event_type],
            [self.actor_id.bytes if self.actor_id else None],
            [self.resource],
            [self.action],
            [json.dumps(self.details)],
            [self.timestamp],
            [self.severity],
        ], schema=self.arrow_schema())

# --- Usage Examples and Integration Helpers ---

# Predefined system roles for quick setup
SYSTEM_ROLES = {
    "admin": Role(
        name="admin", 
        permissions={"create_world", "run_simulation", "manage_users", "view_all", "delete_any"},
        description="Full system administrator",
        is_system_role=True
    ),
    "user": Role(
        name="user", 
        permissions={"create_world", "run_simulation", "view_own"},
        description="Standard user with simulation access",
        is_system_role=True
    ),
    "guest": Role(
        name="guest", 
        permissions={"view_own"},
        description="Read-only guest access",
        is_system_role=True
    ),
    "simulation_runner": Role(
        name="simulation_runner",
        permissions={"create_world", "run_simulation", "run_parallel"},
        description="Specialized role for running simulations",
        is_system_role=True
    )
}

# Default quotas for different user types
DEFAULT_QUOTAS = {
    "admin": Quota(max_worlds=1000, max_steps_per_world=1000000, max_concurrent_runs=100),
    "user": Quota(max_worlds=10, max_steps_per_world=10000, max_concurrent_runs=5),
    "guest": Quota(max_worlds=1, max_steps_per_world=100, max_concurrent_runs=1),
}