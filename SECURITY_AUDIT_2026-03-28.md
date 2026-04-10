# Security Audit — Archetype AuthN/AuthZ

**Date:** 2026-03-28
**Auditor:** Claude Opus 4.6 (Red Team review)
**Reviewed by:** Everett (2026-03-28) — corrections incorporated, see Disposition column
**Scope:** `src/archetype/app/auth/`, `src/archetype/api/`, `src/archetype/app/` service layer
**Handoff to:** Implementation agent (fix all, will be reviewed)

---

## Scope boundaries — what this audit does NOT cover

This is a **focused AuthN/AuthZ + API footgun review** of three directories: `app/auth/`, `api/`, and the `app/` service layer. It is not a full product security assessment. The following areas are explicitly out of scope and need separate review passes:

### Out of scope by design

- **`core/`** — Runtime, storage backends, serialization, processors. Likely home of unsafe deserialization, resource exhaustion, and path handling bugs.
- **CLI beyond `--api-key`** — Other flags, config file loading, file path handling.
- **Dependencies** — Lockfile hygiene, known CVEs in transitive deps, typosquatting.
- **Deployment surface** — TLS termination, secrets in env/k8s, reverse proxy headers, `X-Forwarded-*` trust, disabling `/docs` in prod.

### Methodology gaps (not covered even within scope)

- **No threat model** — No enumeration of assets, actors, trust boundaries, or trust assumptions (who can reach the API, who can read disk, single-tenant vs multi-tenant).
- **No systematic IDOR pass** — The `(world_id, ctx)` authorization path was not traced end-to-end for every route. Findings assume guardrails work when `ctx` is present; C3 addresses the structural hole when it's absent.
- **JWT/WorkOS path not red-teamed** — If/when a JWT provider is added, `alg` confusion, `aud`/`iss` validation, clock skew, JWKS fetch failures, and token binding all need dedicated review. Static API keys were the only provider audited.
- **No operator runbook** — No "secure by default" deployment checklist (minimum env vars, disabling OpenAPI docs in prod, required headers, etc.).

### Common attack classes not assessed

- **SSRF / outbound HTTP** — H3 touches `urlparse` for storage URIs; no general SSRF category for user-controlled URLs.
- **DoS beyond rate limits** — Slow requests, deeply nested JSON, expensive queries, unbounded fan-out in batch endpoints. M1 starts this but is not a full DoS section.
- **Secret leakage in access logs** — H2/H5 address error messages and audit fields, but not whether bearer tokens or PII appear in HTTP access logs or framework debug output.
- **Concurrency / logical TOCTOU** — H3 mentions symlink TOCTOU for storage paths. No review of logical races in broker command processing or shared mutable state under concurrent requests.
- **Deserialization safety** — `_hydrate_components` in `command_service.py` reconstructs `Component` objects from user-supplied dicts. Not reviewed for injection or unexpected type instantiation.

---

## How to use this document

Each finding has:

- **ID + Severity** for triage
- **Location** (exact file:line)
- **Problem** (what's wrong)
- **Exploit** (how an attacker uses it)
- **Fix spec** (what to do — be precise, don't deviate)
- **Verification** (how the reviewer confirms it's fixed)

Fix in severity order: Critical → High → Medium. Run `uv run pytest tests/ -v` after each fix to confirm no regressions.

---

## CRITICAL

### C1. SHA-256 for API key secret hashing

**Location:** `src/archetype/app/auth/credentials.py:49-51`

**Problem:** `hashlib.sha256` is a fast hash. API key secrets hashed this way can be brute-forced at billions/sec on GPU if the auth config (env var or file) is leaked.

**Exploit:** Attacker obtains `ARCHETYPE_API_KEYS_JSON` or `ARCHETYPE_AUTH_FILE` → offline brute-force of all `secret_hash` values → full impersonation of any principal.

**Fix spec:**

1. Replace `hash_static_token_secret` with `hashlib.scrypt` (stdlib, no new deps):

   ```python
   import os, hashlib

   def hash_static_token_secret(secret: str, *, salt: bytes | None = None) -> str:
       if salt is None:
           salt = os.urandom(16)
       derived = hashlib.scrypt(secret.encode(), salt=salt, n=16384, r=8, p=1, dklen=32)
       return salt.hex() + ":" + derived.hex()

   def verify_static_token_secret(secret: str, stored: str) -> bool:
       salt_hex, _, hash_hex = stored.partition(":")
       salt = bytes.fromhex(salt_hex)
       candidate = hash_static_token_secret(secret, salt=salt)
       return hmac.compare_digest(candidate, stored)
   ```

2. Update `StaticApiKeyProvider.authenticate_bearer_token` to use `verify_static_token_secret` instead of hashing + comparing directly.
3. Update `ApiKeyRecord.secret_hash` format documentation to note the `salt:hash` format.
4. Update all test fixtures that construct `_hashed_record` to use the new function.
5. Keep `hmac.compare_digest` for the final comparison.

**Disposition:** Adopt with changes (reviewer correction: replace brittle timing test)

**Verification:** Existing auth tests pass. New tests:

1. Same secret with different salts produces different stored hashes.
2. Stored hash format matches `<salt_hex>:<derived_hex>` pattern (regex: `^[0-9a-f]{32}:[0-9a-f]{64}$`).
3. `verify_static_token_secret(correct_secret, stored)` returns `True`; wrong secret returns `False`.
4. Do NOT use timing-based assertions — they are environment-dependent and flaky in CI.

---

### C2. In-process rate limiting bypassed by multi-worker

**Location:** `src/archetype/app/auth/guard.py:122-124` (`_tick_counters`, `_daily_tokens`)

**Problem:** Module-level dicts are per-process. Multiple uvicorn workers = multiplied quotas. Process restart = quota reset.

**Exploit:** Attacker sends requests across workers to get N × quota. Or crash the server to reset daily token budget.

**Disposition:** Adopt with changes (reviewer correction: fix contradictory verification)

**Fix spec:**

1. The rate limiting warning should always emit at startup, since the limiter is always in-process. In `src/archetype/api/app.py` `lifespan`, log at `INFO` level:

   ```text
   logger.info("Rate limiting is in-process only. Not safe for multi-worker deployments.")
   ```

2. Add a `--workers` guard in `cli/main.py` `serve` command: if `workers > 1`, refuse to start with a clear error explaining why.
3. Document in `AGENTS.md` that multi-worker is not safe until rate limits move to a shared backend.
4. (Future) Abstract `_tick_counters` and `_daily_tokens` behind a `RateLimitBackend` protocol so Redis can be swapped in later.

**Verification:**

1. `archetype serve` with default config starts and emits the in-process rate limit info message in logs.
2. If a `--workers` flag is added, `archetype serve --workers 2` exits with a non-zero code and prints a security error.
3. `AGENTS.md` documents the single-worker constraint.

---

### C3. Optional `ctx` makes all guardrails bypassable

**Location:** Every service method signature — `ctx: ActorCtx | None = None`

- `broker.py:67` — `enqueue()`
- `broker.py:105` — `enqueue_bulk()`
- `broker.py:219` — `get_history()`
- `broker.py:234` — `clear()` (NO auth at all)
- `world_service.py:91` — `create_world()`
- `world_service.py:151` — `get_world()`
- `world_service.py:193` — `remove_world()`
- `simulation_service.py:51` — `step()`
- `simulation_service.py:95` — `run()`

**Problem:** Any code path that calls these methods without passing `ctx` silently skips all RBAC, quota, and scope checks.

**Exploit:** A processor, plugin, or future route that forgets `ctx` gets god-mode access.

**Disposition:** Adopt as written (strongest finding — reviewer confirmed)

**Fix spec:**

1. Create `src/archetype/app/auth/system_ctx.py`:

   ```python
   from archetype.app.auth.models import ActorCtx, PrincipalType
   from uuid_utils import UUID

   # Well-known system principal — used for internal operations only
   SYSTEM_ACTOR_ID = UUID("00000000-0000-0000-0000-000000000000")

   SYSTEM_CTX = ActorCtx(
       id=SYSTEM_ACTOR_ID,
       name="system",
       roles={"admin"},
       auth_method="internal",
       principal_type=PrincipalType.MACHINE,
       rate_tier="trusted",
   )
   ```

2. Change all service method signatures from `ctx: ActorCtx | None = None` to `ctx: ActorCtx` (required).
3. Update all internal callers (e.g., `drain_and_apply`, `run_all`, tests) to pass `SYSTEM_CTX` explicitly.
4. Add `guardrail_allow` check: if `ctx.auth_method == "internal"`, skip quota checks but still enforce operation permissions. This makes the bypass explicit and auditable.
5. `broker.clear()` must require `ctx` with `destroy_world` permission.

**Verification:** `grep -r "ctx.*=.*None" src/archetype/app/` returns zero matches in service methods. All tests updated and passing.

---

### C4. No default-deny auth enforcement for new routes

**Location:** `src/archetype/api/app.py:26-43`

**Disposition:** Adopt with changes (reviewer correction: original overstated CSRF risk)

**Problem:** Auth is per-endpoint via `Depends(get_actor_ctx)`. `create_app()` mounts routers and a public root route with no middleware or app-level dependency. Any new route that omits the `Depends(get_actor_ctx)` dependency is silently unauthenticated. This is a default-allow pattern — the safe default is default-deny.

Note: The original audit cited CORS as a CSRF defense, which is incorrect for bearer-token APIs. `Authorization` headers are not automatically attached cross-site the way cookies are, so CSRF is not the primary risk here. The real issue is the missing global auth gate.

**Exploit:** A developer adds a new route, forgets the `Depends(get_actor_ctx)` parameter → endpoint is publicly accessible with no authentication.

**Fix spec:**

1. Add a global FastAPI dependency or middleware that enforces bearer-token presence on all routes by default. Whitelist only explicit public paths (`GET /`, and optionally `/docs`, `/openapi.json`, `/redoc`).

   ```python
   from starlette.middleware.base import BaseHTTPMiddleware
   from starlette.responses import JSONResponse

   UNAUTHENTICATED_PATHS = {"/", "/docs", "/openapi.json", "/redoc"}

   class DefaultDenyAuthMiddleware(BaseHTTPMiddleware):
       async def dispatch(self, request, call_next):
           if request.url.path not in UNAUTHENTICATED_PATHS:
               auth = request.headers.get("authorization")
               if not auth or not auth.lower().startswith("bearer "):
                   return JSONResponse(
                       {"detail": "Missing authentication"},
                       status_code=401,
                       headers={"WWW-Authenticate": "Bearer"},
                   )
           return await call_next(request)
   ```

2. Keep the per-endpoint `Depends(get_actor_ctx)` as the real auth that resolves the `ActorCtx` — the middleware is a safety net that ensures no route is accidentally exposed without any auth at all.
3. CORS middleware is orthogonal to this fix and not required for bearer-token auth security. Add it only if/when browser clients are a supported use case.

**Verification:** `curl localhost:8000/worlds` without `Authorization` header returns 401. `curl localhost:8000/` returns 200 (health check). New test: register a bare route with no `Depends(get_actor_ctx)` → middleware still rejects unauthenticated requests.

---

## HIGH

### H1. Auth config cached forever — revoked keys still work

**Location:** `src/archetype/app/auth/credentials.py` — `load_auth_config()`, `load_auth_provider()`, `_env_workos_payload()`

**Problem:** The auth stack uses unbounded `@lru_cache(maxsize=1)` entries for:

- parsed auth config
- provider construction
- env-derived WorkOS config payload

Disabling a static API key, changing provider selection, or rotating `WORKOS_*` env/config has no effect until process restart (unless `clear_auth_cache()` is called manually).

**Disposition:** Adopt as written

**Fix spec:**

1. Replace `lru_cache` with a TTL-based cache across the full auth resolution path, not just static API key config. Minimal implementation:

   ```python
   import time

   _AUTH_CONFIG_CACHE: tuple[float, AuthConfig] | None = None
   _AUTH_PROVIDER_CACHE: tuple[float, AuthProvider] | None = None
   _ENV_WORKOS_CACHE: tuple[float, dict[str, str]] | None = None
   _AUTH_CONFIG_TTL = 60  # seconds

   def load_auth_config() -> AuthConfig:
       global _AUTH_CONFIG_CACHE
       now = time.monotonic()
       if _AUTH_CONFIG_CACHE and (now - _AUTH_CONFIG_CACHE[0]) < _AUTH_CONFIG_TTL:
           return _AUTH_CONFIG_CACHE[1]
       config = AuthConfig.model_validate(_merge_env_workos(_load_auth_payload()))
       _AUTH_CONFIG_CACHE = (now, config)
       return config
   ```

2. Apply the same TTL pattern to `load_auth_provider()` and `_env_workos_payload()`.
3. `clear_auth_cache()` must invalidate all auth caches, not just the parsed config.
4. TTL configurable via `ARCHETYPE_AUTH_CACHE_TTL_SEC` env var.

**Verification:**

1. Test: load config, change `ARCHETYPE_API_KEYS_JSON`, wait >TTL, re-load → updated config is returned.
2. Test: load provider with `ARCHETYPE_AUTH_PROVIDER=workos`, change `WORKOS_CLIENT_ID` or `WORKOS_API_KEY`, wait >TTL, re-load → rebuilt provider reflects the new values.
3. Test: calling `clear_auth_cache()` invalidates config, provider, and env-derived WorkOS payload immediately.

---

### H2. Error messages leak internal state

**Location:** `src/archetype/app/auth/guard.py` — all `PermissionError` raises; `src/archetype/api/deps.py`; `src/archetype/cli/main.py`

**Problem:** Error messages include actor UUIDs, role sets, exact quota numbers, and world IDs. This is an information disclosure to external callers. The issue affects both:

- API responses, where raw exception strings can be returned to clients
- CLI stderr output, where raw auth/authorization errors are printed directly

**Disposition:** Adopt as written

**Fix spec:**

1. Create generic error messages for external-facing exceptions:

   ```python
   class AuthorizationError(PermissionError):
       """External-facing error with sanitized message."""
       def __init__(self, internal_msg: str):
           self.internal_msg = internal_msg
           super().__init__("Forbidden")
   ```

2. Replace all `raise PermissionError(f"Actor {ctx.id}...")` with `raise AuthorizationError(f"Actor {ctx.id}...")`.
3. In the API layer (`deps.py`, route exception handlers), catch `AuthorizationError` and return only `exc.args[0]` ("Forbidden") to the client. Log `exc.internal_msg` server-side.
4. In the CLI layer, do not echo raw `PermissionError` / `AuthorizationError` strings. Print sanitized user-facing messages (for example `Authentication failed` or `Forbidden`) and log detailed context separately if needed.
5. Keep detailed messages in audit logs.

**Verification:**

1. Hit a forbidden endpoint → response body says `"Forbidden"`, not actor ID or role details.
2. Run a CLI command with insufficient permissions → stderr contains only sanitized text, not actor UUIDs / role sets / quota numbers.
3. Audit log still has the full internal message.

---

### H3. User-controlled `storage_uri` with fragile default root

**Location:** `api/models.py:12`, `guard.py:271`

**Problem:** `ARCHETYPE_STORAGE_ROOT` defaults to `./archetype_data` relative to CWD, which is process-dependent. Symlink TOCTOU possible.

**Disposition:** Adopt as written

**Fix spec:**

1. If `ARCHETYPE_STORAGE_ROOT` is not set, resolve it once at startup to an absolute path and cache it.
2. Add `os.path.realpath()` to the resolved path to follow symlinks at check time (mitigates but doesn't eliminate TOCTOU).
3. Consider: for non-admin callers, ignore user-provided `storage_uri` entirely and always use the default. The `storage_uri` field in `CreateWorldRequest` should only be respected when the actor has `choose_storage_backend` permission.

**Verification:** Test: request with `storage_uri` pointing to `/etc/` → rejected. Request without elevated permissions ignores `storage_uri` field.

---

### H4. `run_all()` has no auth

**Location:** `src/archetype/app/simulation_service.py:132-142`

**Disposition:** Adopt as written (will be subsumed by C3 fix if `ctx` becomes required everywhere)

**Fix spec:**

1. Add `ctx: ActorCtx` as a required parameter.
2. For each world, check `guardrail_allow_operation("run_simulation", ctx, world_id=w.world_id)`.
3. Or: prefix with `_` to make it private (`_run_all`), if it's truly internal-only.

**Verification:** `grep "def run_all" src/` shows either `ctx: ActorCtx` parameter or `_run_all` name.

---

### H5. Audit payload hygiene — uncontrolled schema and user-controlled values

**Location:** `src/archetype/app/audit.py:14-15`

**Disposition:** Adopt with changes (reviewer correction: `json.dumps` prevents raw newline injection, but schema control and log-shaping risks are real)

**Problem:** `audit_event()` accepts arbitrary `**details` and serializes them directly via `json.dumps(..., default=str)`. While `json.dumps` escapes newlines (so raw line-break injection is not possible), the lack of schema control means:

- User-controlled `payload` dicts can inject arbitrarily large or deeply nested data into audit logs, bloating log storage and complicating SIEM parsing.
- The `default=str` serializer will happily stringify internal objects an attacker shouldn't see in logs.
- An attacker can shape audit log content to make forensic analysis harder (log noise/pollution).

**Fix spec:**

1. Define an allow-list of types for audit detail values: `str`, `int`, `float`, `bool`, `None`, `UUID`. Coerce or drop anything else.
2. Truncate string values to a max length (e.g., 1024 chars).
3. At call sites, never pass raw `payload` dicts into audit details — extract only the specific fields needed for the audit record.
4. Replace `default=str` with a strict serializer that raises on unexpected types rather than silently stringifying.

**Verification:** Test: call `audit_event` with a `details` kwarg containing a deeply nested dict and a 10KB string → output is a single JSON line with values truncated and nested objects rejected or flattened. No internal object repr strings in output.

---

## MEDIUM

### M1. No request body size limit

**Location:** `src/archetype/api/app.py`

**Disposition:** Adopt as written

**Fix spec:** Add `app.add_middleware` with a request body size limit, or configure via uvicorn `--limit-request-body`. A `SubmitBatchRequest` with >1000 commands should be rejected before parsing.

Also: add `max_length` to `SubmitBatchRequest.commands`:

```python
commands: list[SubmitCommandRequest] = Field(max_length=MAX_BATCH_COMMANDS)
```

---

### M2. Uncapped `limit` parameter

**Location:** `src/archetype/api/routes/query.py:69`, `src/archetype/api/routes/commands.py:63`

**Disposition:** Adopt as written

**Fix spec:** Cap at server-defined max: `limit = min(limit, 1000)` in the route handler.

---

### M3. No key expiry or rotation

**Location:** `src/archetype/app/auth/models.py` — `ApiKeyRecord`

**Disposition:** Adopt as written

**Fix spec:** Add optional fields:

```python
expires_at: datetime | None = None
last_used_at: datetime | None = None
```

Check `expires_at` in `StaticApiKeyProvider.authenticate_bearer_token`. Update `last_used_at` on successful auth (requires mutable state — defer to next auth backend iteration).

---

### M4. CLI `--api-key` flag leaks to process list

**Location:** `src/archetype/cli/main.py` — all `--api-key` options

**Disposition:** Adopt as written

**Fix spec:** Remove the `--api-key` CLI flag. Only support the `ARCHETYPE_API_KEY` env var. If the flag must stay, emit a deprecation warning: `"Warning: passing API keys via CLI flags exposes them in process listings. Use ARCHETYPE_API_KEY env var instead."`

---

### M5. Broker internal methods have no auth

**Location:** `src/archetype/app/broker.py` — `dequeue`, `dequeue_due`, `ack`, `peek`, `clear`

**Disposition:** Adopt as written (will be partially subsumed by C3 if `clear()` gets `ctx` requirement)

**Fix spec:** These are internal. Prefix with `_` to signal they should not be called from outside the service layer. The `CommandService` and `SimulationService` are the only legitimate callers.

---

## Checklist for implementation agent

| ID | Finding | Disposition | Notes |
|----|---------|-------------|-------|
| C1 | Replace SHA-256 with scrypt for secret hashing | Adopt with changes | No timing tests — use format/behavior assertions |
| C2 | Add multi-worker warning/guard | Adopt with changes | Warning always emits; multi-worker blocked |
| C3 | Make `ctx` required, create `SYSTEM_CTX` | Adopt as written | Highest-value fix — do first |
| C4 | Add default-deny auth middleware | Adopt with changes | No CORS — focus is default-deny gate only |
| H1 | TTL-based auth config/provider/env cache | Adopt as written | Must cover WorkOS env payload too |
| H2 | Sanitize API and CLI error messages | Adopt as written | CLI currently leaks raw error strings too |
| H3 | Harden storage URI defaults | Adopt as written | |
| H4 | Add auth to `run_all()` or make private | Adopt as written | Subsumed by C3 |
| H5 | Audit payload hygiene and schema control | Adopt with changes | Not newline injection — log noise/schema control |
| M1 | Request body size limits | Adopt as written | |
| M2 | Cap `limit` query params | Adopt as written | |
| M3 | Add key expiry fields | Adopt as written | |
| M4 | Deprecate `--api-key` CLI flag | Adopt as written | |
| M5 | Prefix broker internal methods with `_` | Adopt as written | Partially subsumed by C3 |

**Recommended implementation order:** C3 → C4 → C1 → C2 → H2 → H1 → H5 → H3 → H4 → M1 → M2 → M3 → M4 → M5

After all fixes: `uv run pytest tests/ -v` must pass. New tests required for C1, C3, C4, H1, H2, H5.
