# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Auth error types."""


class GuardrailError(PermissionError):
    """Raised when an ActorCtx lacks permission for a command.

    Includes role context in the message for debugging.
    """

    pass
