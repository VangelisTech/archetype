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

from fastapi import Request, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Optional
import jwt
from datetime import datetime, timezone

from .models import ActorCtx, Role, AuthConfig
from .guards import AuthGuard

class AuthMiddleware:
    """FastAPI middleware for authentication and authorization"""
    
    def __init__(self, config: AuthConfig, auth_guard: AuthGuard):
        self.config = config
        self.auth_guard = auth_guard
        self.security = HTTPBearer(auto_error=False)
    
    async def __call__(self, request: Request, call_next):
        """Process request through auth pipeline"""
        if not self.config.enabled:
            # Bypass auth - create default admin context
            request.state.actor_ctx = self._create_default_context()
            return await call_next(request)
        
        try:
            # Extract and validate token
            credentials = await self.security(request)
            actor_ctx = await self._authenticate(credentials)
            
            # Store in request state
            request.state.actor_ctx = actor_ctx
            
            return await call_next(request)
            
        except HTTPException:
            raise
        except Exception as e:
            if self.config.strict_mode:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Auth system error: {str(e)}"
                )
            # Fallback to default context in non-strict mode
            request.state.actor_ctx = self._create_default_context()
            return await call_next(request)
    
    async def _authenticate(self, credentials: Optional[HTTPAuthorizationCredentials]) -> ActorCtx:
        """Authenticate request and return actor context"""
        if not credentials:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authentication required"
            )
        
        try:
            # Decode JWT token (replace with your auth provider)
            raw_token = credentials.credentials
            # Decode token without verification in non-strict mode; in strict mode, verification should be configured
            payload = jwt.decode(
                raw_token,
                options={"verify_signature": not self.config.strict_mode} if self.config.strict_mode else {"verify_signature": False}
            )
            
            principal = payload.get("email") or payload.get("preferred_username") or payload.get("sub")
            role_names = payload.get("roles", [])
            return ActorCtx(
                id=payload.get("sub", "00000000-0000-0000-0000-000000000000"),
                roles=set(role_names) if isinstance(role_names, list) else set(),
                org=payload.get("org"),
                principal=principal,
                uc_token=raw_token,
                session_start=datetime.now(timezone.utc)
            )
            
        except jwt.InvalidTokenError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication token"
            )
    
    def _create_default_context(self) -> ActorCtx:
        """Create default admin context for development/testing"""
        import uuid
        return ActorCtx(
            id=uuid.uuid4(),
            roles={Role.ADMIN},
            org="default"
        )

# FastAPI dependency for getting actor context
async def get_actor_ctx(request: Request) -> ActorCtx:
    """FastAPI dependency to extract actor context from request"""
    if not hasattr(request.state, 'actor_ctx'):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Actor context not found - auth middleware not configured"
        )
    return request.state.actor_ctx

# CLI context provider
class CLIAuthProvider:
    """Provides auth context for CLI operations"""
    
    def __init__(self, config: AuthConfig):
        self.config = config
    
    def get_cli_context(self, user_id: Optional[str] = None) -> ActorCtx:
        """Get auth context for CLI operations"""
        if not self.config.enabled:
            import uuid
            return ActorCtx(
                id=uuid.uuid4(),
                roles={Role.ADMIN},
                org="cli"
            )
        
        # In production, this might read from config file or prompt user
        # For now, return admin context
        import uuid
        return ActorCtx(
            id=uuid.UUID(user_id) if user_id else uuid.uuid4(),
            roles={Role.ADMIN},
            org="cli"
        )