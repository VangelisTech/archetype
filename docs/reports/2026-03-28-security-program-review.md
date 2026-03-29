# Archetype Security Program Review

**Date:** 2026-03-28

## Executive Summary

Archetype's security design is improving in the right direction: the API and CLI now share a provider-shaped authentication boundary, the public command channel blocks dangerous control-plane operations, and the authorization model already has the right primitives for roles, scoped actions, and per-world visibility. The biggest remaining structural risk is that service-layer authorization is still optional below the API boundary, which means any internal caller that omits `ctx` silently bypasses RBAC, quota, and scope checks.

This review includes the standing detailed audit in `SECURITY_AUDIT_2026-03-28.md` and adds two things it did not fully cover:
- a design-level assessment of which security decisions are worth preserving versus changing
- a current dependency advisory review for the Python runtime and lockfile

## Included Audit

The standing repository audit is included by reference:
- `SECURITY_AUDIT_2026-03-28.md`

That document remains the source of truth for the concrete AuthN/AuthZ remediation backlog. This report summarizes the higher-level conclusions and dependency risk picture around it.

## Design Decisions Worth Preserving

- Keep the shared provider boundary in `app/auth/credentials.py`. It cleanly separates credential verification from the rest of the authorization model and makes future providers such as WorkOS pluggable.
- Keep `ActorCtx` immutable. Frozen authorization context is a good default and reduces accidental mutation bugs in downstream service code.
- Keep the separation between coarse roles, explicit `allowed_actions`, and `allowed_worlds`. That is the right shape for least-privilege enforcement even though the service layer still needs hardening.
- Keep dangerous control-plane actions out of the public command channel. The explicit blocklist in `CommandService` is a strong defense-in-depth decision.
- Keep WorkOS claim mapping explicit rather than implicit. External role names should not become Archetype roles without a deliberate mapping.

## Highest-Risk Outstanding Design Issues

1. Service-layer auth is still optional.
The highest-value remaining fix is to make `ctx` mandatory below the API boundary and introduce an explicit `SYSTEM_CTX` for trusted internal flows. This is the main structural bypass risk.

2. Static API key secrets are still hashed with fast SHA-256.
The current prefix-plus-hash model is directionally correct, but the hash function should be replaced with a slow salted KDF such as `scrypt`.

3. Auth freshness is still process-lifetime.
The current auth caches do not expire automatically, so key revocation and provider/env changes are not live until restart or manual cache clearing.

4. Deployment safety still depends on operator memory.
Rate limiting is still process-local, and the API still relies on route-by-route auth dependencies instead of a default-deny safety net for future routes.

5. External errors still leak internals.
Raw authorization and authentication error strings are still exposed through API and CLI flows.

## Dependency Security Review

### High-confidence actionable advisories

- `PyJWT`
  - Advisory: `GHSA-752w-5fwx-jx9f` / `CVE-2026-32597`
  - Risk: versions before `2.12.0` accept unknown `crit` header extensions
  - Archetype status: `pyproject.toml` currently allows `PyJWT[crypto]>=2.10`, and `uv.lock` currently resolves `pyjwt 2.10.1`
  - Recommended action: tighten to `PyJWT[crypto]>=2.12.0`

- `starlette`
  - Advisory: `GHSA-7f5h-v6xp-fcq8` / `CVE-2025-62727`
  - Risk: `FileResponse` / `StaticFiles` range-header O(n^2) DoS in `0.39.0` through `0.49.0`
  - Archetype status: `uv.lock` currently resolves `starlette 0.46.2`, which is in the affected range
  - Exploitability note: current Archetype code does not obviously expose `StaticFiles` or `FileResponse`, so this looks more like latent dependency risk than an immediately reachable app bug
  - Recommended action: move to a FastAPI-compatible Starlette resolution at `0.49.1+`

### Lower-priority or context-dependent advisories

- `cryptography`
  - Advisory: `GHSA-r6ph-v2qm-q3c2` / `CVE-2026-26007`
  - Risk: subgroup validation issue for SECT curves
  - Archetype status: `uv.lock` currently resolves `cryptography 46.0.0`
  - Archetype relevance: low for the current visible JWT usage (`RS256`), but still worth picking up via normal dependency refresh

### No credible current advisory found

No high-confidence current advisory was identified for:
- `fastapi`
- `uvicorn`
- `lancedb`
- `daft`
- `pyiceberg`
- `typer`

Absence of a published advisory is not proof of safety. The biggest dependency signal besides known CVEs is that the project's declared constraints and lockfile appear inconsistent around `daft` and `pyiceberg`, which reduces confidence in the current dependency state and complicates automated scanning.

## Recommended Security Implementation Order

1. Make `ctx` mandatory in service-layer APIs and introduce explicit `SYSTEM_CTX`
2. Add a default-deny API auth safety net
3. Replace SHA-256 API key hashing with `scrypt`
4. Sanitize external-facing auth errors in both API and CLI paths
5. Replace permanent auth caches with TTL-based invalidation across config, provider, and WorkOS env payloads
6. Add worker-safety guardrails and documentation for in-process rate limiting
7. Harden audit payload serialization and request/body abuse controls

## Automation Added In This Change

This change set adds a daily GitHub Actions workflow that:
- gathers dependency-audit evidence
- runs a scheduled Claude security review
- writes a daily report artifact
- creates or updates a GitHub issue with the daily security audit

That automation is not a replacement for remediation. Its purpose is to keep the standing audit and dependency advisory picture fresh without relying on memory.
