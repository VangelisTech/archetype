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

"""Service-layer error types.

Auth-specific errors live in ``archetype.app.gateway.auth.errors``. This module holds
cross-service exception contracts that gates and transport adapters may depend
on without importing concrete service implementations.
"""


class ConflictError(RuntimeError):
    """A requested operation conflicts with existing service state.

    Concrete services subclass this public contract so transport adapters can
    map conflicts without depending on private implementation modules. The
    internal exception message may contain diagnostic context; adapters expose
    only ``public_detail``.
    """

    public_detail = "Request conflicts with existing state"


class AvailabilityError(RuntimeError):
    """A service dependency is temporarily unable to accept work.

    Concrete services subclass this public contract so transport adapters can
    expose a retryable availability signal without importing implementation
    modules. The internal exception message may contain diagnostic context;
    adapters expose only ``public_detail``.
    """

    public_detail = "Service is temporarily unavailable"


class PayloadRejectedError(RuntimeError):
    """A payload is well-formed but cannot cross a safety boundary.

    Concrete services subclass this public contract so transport adapters can
    reject unsafe content without importing the owning service family or
    exposing internal findings. The internal exception message may contain
    safe diagnostic context; adapters expose only ``public_detail``.
    """

    public_detail = "Payload rejected by safety policy"


class WorldNotFoundError(LookupError):
    """Raised when a gated operation targets a ``world_id`` not in the registry.

    Per ``docs/guide/specification.md`` "Required Hardening Work" item 3,
    submission to an unknown world is rejected at the gate so callers get a
    typed signal instead of a silently orphaned command-ledger entry.
    """

    def __init__(self, world_id) -> None:
        super().__init__(f"World with ID '{world_id}' not found.")
        self.world_id = world_id
