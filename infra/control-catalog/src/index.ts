// Copyright 2026 Vangelis Technologies Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Archetype remote control catalog (issue #281).
//
// Two Durable Object classes implement the ControlCatalog protocol:
//
// - CatalogDirectoryDO — one instance per storage namespace. Worlds and
//   signatures: the cross-world discovery queries per-world sharding cannot
//   answer. Low write rate by construction (create/fork/first-append only).
// - WorldCommitDO — one instance per world id. Writer fence, tick manifests,
//   command ledger/outbox, and artifact claims: the per-tick hot path. A Durable Object executes
//   requests serially, so the fence is structural — publish is a straight-
//   line transaction with no CAS gymnastics.
//
// Auth: a single bearer token (CATALOG_TOKEN secret). The Python client is
// archetype.app.storage.remote_catalog.RemoteControlCatalog; semantics must match
// SqliteControlCatalog exactly (it is the reference implementation).

export interface Env {
  DIRECTORY: DurableObjectNamespace;
  WORLD: DurableObjectNamespace;
  CATALOG_TOKEN?: string;
}

const JSON_HEADERS = { "content-type": "application/json" };
const CATALOG_PROTOCOL_VERSION = 6;
const ATTEMPT_FINALIZATION_CAPABILITY = "attempt_claim_finalization_v2";
const ATTEMPT_CLAIM_CAPABILITY = "attempt_claim_execution_v2";
const ARTIFACT_SNAPSHOT_CAPABILITY = "artifact_snapshot_decimal_v1";
const ARTIFACT_SERVER_CLOCK_CAPABILITY = "artifact_publication_server_clock_v1";
const FLEET_RECOVERY_CAPABILITY = "fleet_recovery_v1";
const MAX_SIGNED_64_BIT = 9223372036854775807n;
const LEGACY_UNBOUND_MIGRATION = "mission_attempt_legacy_unbound_v8";
const FLEET_RECOVERY_MIGRATION = "fleet_recovery_schema_v1";
const RECOVERY_SWEEP_DOMAIN = "archetype.fleet-recovery-sweep.v1";
const RECOVERY_EXCEPTION_DOMAIN = "archetype.fleet-recovery-exception.v1";
const MAX_RECOVERY_LEASE_MS = 24 * 60 * 60 * 1000;
const MAX_RECOVERY_DELAY_MS = 365 * 24 * 60 * 60 * 1000;
const MAX_RECOVERY_ERROR_CODE_CHARS = 128;
const MAX_RECOVERY_ERROR_DETAIL_CHARS = 4096;
const MAX_ARTIFACT_LEASE_MS = 24 * 60 * 60 * 1000;
const MAX_ARTIFACT_RETRY_WINDOW_MS = 365 * 24 * 60 * 60 * 1000;
const MAX_ARTIFACT_RETRY_DELAY_MS = 365 * 24 * 60 * 60 * 1000;
const ARTIFACT_RETRY_EXPIRED_DETAIL =
  "artifact publication retry window elapsed before upload";
const CATALOG_DIGEST_DOMAIN = "archetype.catalog.v1";
const RECOVERY_SWEEP_STATUSES = new Set(["idle", "leased", "retry_wait", "paused"]);
const RECOVERY_EXCEPTION_STATUSES = new Set(["retry_wait", "dead_letter", "resolved"]);
const WORLD_DISCOVERY_QUERY_FIELDS = new Set(["after_world_id", "limit"]);
type RecoverySweepStatus = "idle" | "leased" | "retry_wait" | "paused";
type RecoveryExceptionStatus = "retry_wait" | "dead_letter" | "resolved";
type RecoverySweepEvent =
  | "create"
  | "lease"
  | "take_over"
  | "renew"
  | "checkpoint"
  | "yield"
  | "fail"
  | "exhaust"
  | "pause"
  | "redrive";
type RecoveryExceptionEvent = "retry" | "dead_letter" | "resolve" | "redrive";
const RECOVERY_SWEEP_TRANSITIONS: ReadonlyMap<string, RecoverySweepStatus> = new Map([
  ["<absent>|create", "idle"],
  ["idle|lease", "leased"],
  ["retry_wait|lease", "leased"],
  ["leased|take_over", "leased"],
  ["leased|renew", "leased"],
  ["leased|checkpoint", "leased"],
  ["leased|yield", "idle"],
  ["leased|fail", "retry_wait"],
  ["leased|exhaust", "paused"],
  ["leased|pause", "paused"],
  ["paused|redrive", "idle"],
]);
const RECOVERY_EXCEPTION_TRANSITIONS: ReadonlyMap<string, RecoveryExceptionStatus> = new Map([
  ["<absent>|retry", "retry_wait"],
  ["<absent>|dead_letter", "dead_letter"],
  ["retry_wait|retry", "retry_wait"],
  ["retry_wait|dead_letter", "dead_letter"],
  ["retry_wait|resolve", "resolved"],
  ["dead_letter|resolve", "resolved"],
  ["dead_letter|redrive", "retry_wait"],
]);
const RECOVERY_ERROR_CODES = new Set([
  "discovery_failed",
  "handler_failed",
  "source_corrupt",
  "policy_rejected",
  "capability_unavailable",
]);
const RECOVERY_KINDS = new Set([
  "mission_model_recovery",
  "mission_finalization",
  "artifact_publication",
  "event_projection",
  "artifact_retention",
  "checkpoint_retention",
  "local_staging_retention",
]);
const RECOVERY_NO_QUERY_FIELDS = new Set<string>();
const RECOVERY_SWEEP_LIST_QUERY_FIELDS = new Set(["status"]);
const RECOVERY_EXCEPTION_ITEM_QUERY_FIELDS = new Set(["kind"]);
const RECOVERY_EXCEPTION_LIST_QUERY_FIELDS = new Set([
  "kind",
  "status",
  "due_only",
  "limit",
]);
const RECOVERY_SWEEP_BODY_FIELDS: ReadonlyMap<string, ReadonlySet<string>> = new Map([
  [
    "ensure-v1",
    new Set([
      "storage_fingerprint",
      "kind",
      "max_consecutive_failures",
      "initial_delay_ms",
    ]),
  ],
  ["lease-v1", new Set(["kind", "claimant", "lease_ms"])],
  ["renew-v1", new Set(["kind", "claimant", "fence_epoch", "lease_ms"])],
  [
    "checkpoint-v1",
    new Set(["kind", "claimant", "fence_epoch", "cursor", "active_subject_key"]),
  ],
  ["yield-v1", new Set(["kind", "claimant", "fence_epoch", "next_delay_ms"])],
  [
    "fail-v1",
    new Set([
      "kind",
      "claimant",
      "fence_epoch",
      "error_code",
      "error_detail",
      "retry_delay_ms",
    ]),
  ],
  [
    "pause-v1",
    new Set(["kind", "claimant", "fence_epoch", "error_code", "error_detail"]),
  ],
  ["redrive-v1", new Set(["kind", "expected_fence_epoch", "delay_ms"])],
]);
const RECOVERY_EXCEPTION_BODY_FIELDS: ReadonlyMap<string, ReadonlySet<string>> = new Map([
  [
    "retry-v1",
    new Set([
      "kind",
      "claimant",
      "fence_epoch",
      "subject_key",
      "authority_key",
      "expected_attempt_count",
      "error_code",
      "error_detail",
      "retry_delay_ms",
      "max_attempts",
      "permanent",
    ]),
  ],
  ["resolve-v1", new Set(["kind", "claimant", "fence_epoch", "exception_key"])],
  [
    "redrive-v1",
    new Set([
      "kind",
      "claimant",
      "fence_epoch",
      "exception_key",
      "expected_attempt_count",
      "retry_delay_ms",
    ]),
  ],
]);
const ATTEMPT_CLAIM_EDGES = new Set([
  "claimed->possibly_submitted",
  "possibly_submitted->provider_acknowledged",
  "provider_acknowledged->finalizing",
  "claimed->settled",
  "possibly_submitted->settled",
  "provider_acknowledged->settled",
  "finalizing->settled",
]);

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), { status, headers: JSON_HEADERS });
}

function conflict(kind: string, message: string): Response {
  return json({ error: kind, message }, 409);
}

function recoveryPending(message: string): Response {
  return json({ error: "recovery_sweep_pending", message }, 423);
}

function recoveryStale(message: string): Response {
  return json({ error: "recovery_sweep_stale", message }, 412);
}

class RecoveryInputError extends Error {}

function recoverySweepTransition(
  source: unknown,
  event: RecoverySweepEvent,
): RecoverySweepStatus {
  if (
    source !== null &&
    (typeof source !== "string" || !RECOVERY_SWEEP_STATUSES.has(source))
  ) {
    throw new Error(`unknown recovery sweep state: ${String(source)}`);
  }
  const sourceLabel = source === null ? "<absent>" : source;
  const target = RECOVERY_SWEEP_TRANSITIONS.get(`${sourceLabel}|${event}`);
  if (!target) {
    throw new Error(
      `illegal recovery sweep transition: ${String(sourceLabel)} via ${event}`,
    );
  }
  return target;
}

function recoveryExceptionTransition(
  source: unknown,
  event: RecoveryExceptionEvent,
): RecoveryExceptionStatus {
  if (
    source !== null &&
    (typeof source !== "string" || !RECOVERY_EXCEPTION_STATUSES.has(source))
  ) {
    throw new Error(`unknown recovery exception state: ${String(source)}`);
  }
  const sourceLabel = source === null ? "<absent>" : source;
  const target = RECOVERY_EXCEPTION_TRANSITIONS.get(`${sourceLabel}|${event}`);
  if (!target) {
    throw new Error(
      `illegal recovery exception transition: ${String(sourceLabel)} via ${event}`,
    );
  }
  return target;
}

function recoverySweepView(
  row: Record<string, unknown> | null,
): Record<string, unknown> | null {
  if (row === null) return null;
  const view = { ...row };
  delete view.redrive_from_fence_epoch;
  delete view.redrive_delay_ms;
  return view;
}

function recoveryExceptionView(row: Record<string, unknown>): Record<string, unknown> {
  const view = { ...row };
  delete view.redrive_from_attempt_count;
  delete view.redrive_retry_delay_ms;
  return view;
}

function recoveryInteger(
  value: unknown,
  field: string,
  minimum: number,
  maximum: number,
): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    throw new RecoveryInputError(`${field} must be an integer`);
  }
  if (value < minimum || value > maximum) {
    throw new RecoveryInputError(
      `${field} must be between ${minimum} and ${maximum}`,
    );
  }
  return value;
}

function recoveryText(
  value: unknown,
  field: string,
  maximum: number,
  allowEmpty = false,
): string {
  if (typeof value !== "string") {
    throw new RecoveryInputError(`${field} must be a string`);
  }
  if ((!allowEmpty && !value.trim()) || value.length > maximum) {
    throw new RecoveryInputError(
      !allowEmpty && !value.trim()
        ? `${field} must not be empty`
        : `${field} exceeds ${maximum} characters`,
    );
  }
  return value;
}

function recoverySha256(value: unknown, field: string): string {
  const digest = recoveryText(value, field, 64);
  if (!/^[0-9a-f]{64}$/.test(digest)) {
    throw new RecoveryInputError(`${field} must be a lowercase SHA-256 digest`);
  }
  return digest;
}

function recoveryKind(value: unknown): string {
  const kind = recoveryText(value, "recovery kind", 128);
  if (!RECOVERY_KINDS.has(kind)) {
    throw new RecoveryInputError(`unsupported recovery kind ${kind}`);
  }
  return kind;
}

function recoveryError(
  code: unknown,
  detail: unknown,
): { errorCode: string; errorDetail: string } {
  const errorCode = recoveryText(
    code,
    "error_code",
    MAX_RECOVERY_ERROR_CODE_CHARS,
  );
  if (!RECOVERY_ERROR_CODES.has(errorCode)) {
    throw new RecoveryInputError(`unsupported recovery error_code ${errorCode}`);
  }
  const errorDetail = recoveryText(
    detail,
    "error_detail",
    MAX_RECOVERY_ERROR_DETAIL_CHARS,
    true,
  );
  return { errorCode, errorDetail };
}

async function recoveryKey(domain: string, ...parts: string[]): Promise<string> {
  const encoded = new TextEncoder().encode(`${domain}\0${parts.join("\0")}`);
  const digest = await crypto.subtle.digest("SHA-256", encoded);
  return Array.from(new Uint8Array(digest), (byte) =>
    byte.toString(16).padStart(2, "0"),
  ).join("");
}

function recoveryObject(value: unknown, field: string): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new RecoveryInputError(`${field} must be an object`);
  }
  return value as Record<string, unknown>;
}

function recoveryExactFields(
  value: Record<string, unknown>,
  allowed: ReadonlySet<string>,
  field: string,
): void {
  for (const key of Object.keys(value)) {
    if (!allowed.has(key)) {
      throw new RecoveryInputError(`${field} contains unsupported field ${key}`);
    }
  }
}

function recoveryExactQuery(url: URL, allowed: ReadonlySet<string>, field: string): void {
  for (const key of url.searchParams.keys()) {
    if (!allowed.has(key)) {
      throw new RecoveryInputError(`${field} contains unsupported parameter ${key}`);
    }
    if (url.searchParams.getAll(key).length !== 1) {
      throw new RecoveryInputError(`${field} contains duplicate parameter ${key}`);
    }
  }
}

function recoveryQueryInteger(
  raw: string | null,
  fallback: number,
  field: string,
  minimum: number,
  maximum: number,
): number {
  if (raw === null) return fallback;
  if (!/^(0|[1-9][0-9]*)$/.test(raw)) {
    throw new RecoveryInputError(`${field} must be canonical decimal text`);
  }
  return recoveryInteger(Number(raw), field, minimum, maximum);
}

function recoveryQueryBoolean(
  raw: string | null,
  fallback: boolean,
  field: string,
): boolean {
  if (raw === null) return fallback;
  if (raw === "1") return true;
  if (raw === "0") return false;
  throw new RecoveryInputError(`${field} must be canonical boolean text (0 or 1)`);
}

function pythonAsciiJsonString(value: string): string {
  return JSON.stringify(value).replace(/[^\x00-\x7e]/gu, (character) => {
    let codePoint = character.codePointAt(0) as number;
    if (codePoint <= 0xffff) return `\\u${codePoint.toString(16).padStart(4, "0")}`;
    codePoint -= 0x10000;
    const high = 0xd800 + (codePoint >> 10);
    const low = 0xdc00 + (codePoint & 0x3ff);
    return `\\u${high.toString(16).padStart(4, "0")}\\u${low
      .toString(16)
      .padStart(4, "0")}`;
  });
}

async function artifactPublicationKey(
  worldId: string,
  runId: string,
  idempotencyKey: string,
): Promise<string> {
  // Match Python json.dumps(sort_keys=True, separators=(",", ":")) including
  // its default ensure_ascii=True behavior for non-ASCII identities.
  const payload =
    `{"domain":${pythonAsciiJsonString(CATALOG_DIGEST_DOMAIN)},` +
    `"idempotency_key":${pythonAsciiJsonString(idempotencyKey)},` +
    `"kind":"artifact-publication",` +
    `"run_id":${pythonAsciiJsonString(runId)},` +
    `"world_id":${pythonAsciiJsonString(worldId)}}`;
  const digest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(payload),
  );
  return Array.from(new Uint8Array(digest), (byte) =>
    byte.toString(16).padStart(2, "0"),
  ).join("");
}

function attemptClaimView(
  row: Record<string, unknown>,
): Record<string, unknown> {
  // The acquisition receipt is durable identity used only for reacquisition
  // checks. Public claim records expose the latest phase receipt instead.
  const view = { ...row };
  delete view.redaction_acquisition_evidence_json;
  return view;
}

function artifactPublicationView(
  row: Record<string, unknown>,
): Record<string, unknown> {
  const view = { ...row };
  view.index_snapshot_id = String(row.index_snapshot_id_text ?? "0");
  delete view.index_snapshot_id_text;
  return view;
}

function artifactPublicationAuthorityError(
  row: Record<string, unknown>,
  publicationKey: string,
): Response | null {
  if (!["PENDING", "UPLOADED", "INDEXED", "EXPIRED"].includes(String(row.status))) {
    return conflict(
      "artifact_publication_conflict",
      `artifact publication ${publicationKey} has invalid durable status`,
    );
  }
  if (
    !Number.isSafeInteger(row.retry_until_ms) ||
    Number(row.retry_until_ms) < 0 ||
    !Number.isSafeInteger(row.attempt_count) ||
    Number(row.attempt_count) < 1 ||
    typeof row.lease_expires_at !== "number" ||
    !Number.isFinite(row.lease_expires_at) ||
    row.lease_expires_at < 0 ||
    row.lease_expires_at > Number.MAX_SAFE_INTEGER
  ) {
    return conflict(
      "artifact_publication_conflict",
      `artifact publication ${publicationKey} has invalid durable clock or counter state`,
    );
  }
  return null;
}

function attemptClaimTransitionReplayMatches(
  row: Record<string, unknown>,
  payload: Record<string, unknown>,
): boolean {
  const expected = String(payload.expected_status ?? "");
  const target = String(payload.target_status ?? "");
  let exact: string[];
  if (target === "finalizing") {
    if (expected !== "provider_acknowledged") return false;
    exact = [
      "redaction_evidence_json",
      "outcome_digest",
      "outcome_json",
      "artifact_request_json",
      "artifact_request_digest",
      "artifact_publication_key",
    ];
  } else if (target === "settled") {
    if (expected !== attemptClaimSettlementSource(row)) return false;
    exact = [
      "redaction_evidence_json",
      "settlement_status",
      "outcome_digest",
      "outcome_json",
      "last_error",
    ];
  } else {
    return false;
  }
  for (const field of exact) {
    if (String(row[field] ?? "") !== String(payload[field] ?? "")) return false;
  }
  // Provider identities and staged artifacts are inherited source evidence.
  // Empty arguments preserve them; any explicitly repeated value is exact.
  for (const field of [
    "provider_session_id",
    "provider_request_id",
    "artifact_request_json",
    "artifact_request_digest",
    "artifact_publication_key",
  ]) {
    const supplied = String(payload[field] ?? "");
    if (supplied && String(row[field] ?? "") !== supplied) return false;
  }
  return true;
}

function attemptClaimSettlementSource(row: Record<string, unknown>): string {
  if (
    row.finalizing_at ||
    row.artifact_request_json ||
    row.artifact_request_digest ||
    row.artifact_publication_key
  ) {
    return "finalizing";
  }
  if (row.acknowledged_at) return "provider_acknowledged";
  if (row.possibly_submitted_at) return "possibly_submitted";
  return "claimed";
}

function legacyClaimRequest(row: Record<string, unknown>): Record<string, unknown> | null {
  try {
    const value = JSON.parse(String(row.request_json ?? "")) as unknown;
    if (value === null || typeof value !== "object" || Array.isArray(value)) return null;
    const request = value as Record<string, unknown>;
    return request.claim_contract_version === undefined ? request : null;
  } catch {
    return null;
  }
}

function isUnboundLegacyIndexedClaim(row: Record<string, unknown>): boolean {
  const request = legacyClaimRequest(row);
  return (
    request?.required_finalization_phase === "indexed" &&
    !String(row.artifact_request_json ?? "") &&
    !String(row.artifact_request_digest ?? "") &&
    !String(row.artifact_publication_key ?? "")
  );
}

function appendCommandEvent(
  sql: SqlStorage,
  command: Record<string, unknown>,
  status: string,
  payloadJson: string,
  occurredAt: string,
): void {
  sql.exec(
    "INSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, command_type, status, actor_id, payload_json, occurred_at) " +
      "VALUES (?, 'command', ?, ?, ?, ?, ?, ?, ?)",
    crypto.randomUUID(),
    command.command_id,
    `command.${status}`,
    command.command_type,
    status,
    command.principal_id ?? null,
    payloadJson,
    occurredAt,
  );
}

function rejectUnsettledCommands(
  sql: SqlStorage,
  reason: string,
  occurredAt: string,
): number {
  const rows = sql
    .exec(
      "SELECT * FROM commands WHERE status IN ('PENDING', 'RETRYABLE', 'LEASED') ORDER BY sequence",
    )
    .toArray() as Array<Record<string, unknown>>;
  for (const row of rows) {
    sql.exec(
      "UPDATE commands SET status = 'REJECTED', lease_owner = NULL, lease_expires_at = NULL, last_error_code = 'world_destroyed', last_error_detail = ?, updated_at = ? WHERE command_id = ?",
      reason.slice(0, 2000),
      occurredAt,
      row.command_id,
    );
    appendCommandEvent(
      sql,
      row,
      "rejected",
      JSON.stringify({ error_code: "world_destroyed" }),
      occurredAt,
    );
  }
  return rows.length;
}

function requireActiveWorld(sql: SqlStorage, worldId: string): void {
  const rows = sql.exec("SELECT status FROM world_state WHERE singleton = 1").toArray();
  if (rows.length === 0 || String(rows[0].status) !== "active") {
    throw new Error(`command_conflict:world ${worldId} is not active`);
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    if (!env.CATALOG_TOKEN) {
      return json(
        { error: "misconfigured", message: "CATALOG_TOKEN is not set" },
        500,
      );
    }
    const auth = request.headers.get("authorization") ?? "";
    if (auth !== `Bearer ${env.CATALOG_TOKEN}`) {
      return json({ error: "unauthorized" }, 401);
    }
    const url = new URL(request.url);
    const parts = url.pathname.split("/").filter(Boolean);
    // /ns/:namespace/... → directory routes
    // /ns/:namespace/w/:worldId/... → per-world commit routes
    if (parts[0] !== "ns" || parts.length < 3) {
      return json({ error: "bad_route", message: url.pathname }, 404);
    }
    const namespace = decodeURIComponent(parts[1]);
    const directory = env.DIRECTORY.get(env.DIRECTORY.idFromName(namespace));

    // World status is mirrored into the per-world authority before command
    // traffic can reach it. Status transitions hit WorldCommitDO first, where
    // they serialize with admission/leasing and atomically reject open work;
    // the directory remains the cross-world discovery index.
    if (parts[2] === "worlds" && parts.length === 3 && request.method === "POST") {
      const record = (await request.clone().json()) as Record<string, unknown>;
      const response = await directory.fetch(request);
      if (!response.ok) return response;
      const result = (await response.clone().json()) as Record<string, unknown>;
      const worldId = String(record.world_id ?? "");
      const status = String(result.status ?? record.status ?? "active");
      const world = env.WORLD.get(env.WORLD.idFromName(`${namespace}:${worldId}`));
      const statusResponse = await world.fetch(
        new Request(`${url.origin}/ns/${namespace}/w/${worldId}/status`, {
          method: "PATCH",
          headers: JSON_HEADERS,
          body: JSON.stringify({ status }),
        }),
      );
      return statusResponse.ok ? response : statusResponse;
    }

    if (parts[2] === "worlds" && parts.length === 4 && request.method === "PATCH") {
      const patch = (await request.clone().json()) as Record<string, unknown>;
      if (typeof patch.status === "string") {
        const worldId = decodeURIComponent(parts[3]);
        const world = env.WORLD.get(env.WORLD.idFromName(`${namespace}:${worldId}`));
        const statusResponse = await world.fetch(
          new Request(`${url.origin}/ns/${namespace}/w/${worldId}/status`, {
            method: "PATCH",
            headers: JSON_HEADERS,
            body: JSON.stringify({ status: patch.status }),
          }),
        );
        if (!statusResponse.ok) return statusResponse;
      }
      return directory.fetch(request);
    }

    if (parts[2] === "w" && parts.length >= 4) {
      const worldId = decodeURIComponent(parts[3]);
      const stub = env.WORLD.get(env.WORLD.idFromName(`${namespace}:${worldId}`));
      // Manifest publication also advances the directory's world head. The
      // SQLite reference does both in one transaction; across two Durable
      // Objects the update is ordered-after-publish and idempotent
      // (MAX(head, tick)), so a crash between the calls self-heals on the
      // next publish. Manifests remain the authority; the directory head is
      // derived data for discovery and artifact-tick selection.
      if (parts[4] === "manifests" && request.method === "POST") {
        const body = (await request.clone().json()) as { tick?: number };
        const response = await stub.fetch(request);
        if (response.ok && typeof body.tick === "number") {
          try {
            const headResponse = await directory.fetch(
              new Request(`${url.origin}/ns/${namespace}/worlds/${worldId}`, {
                method: "PATCH",
                headers: JSON_HEADERS,
                body: JSON.stringify({ tick_head: body.tick }),
              }),
            );
            if (!headResponse.ok) {
              console.error("directory tick_head advance failed", headResponse.status);
            }
          } catch (error) {
            console.error("directory tick_head advance failed", error);
          }
        }
        return response;
      }
      return stub.fetch(request);
    }
    return directory.fetch(request);
  },
};

// ─────────────────────────────────────────────────────────────────────────────
// Directory: worlds + signatures (cross-world discovery)
// ─────────────────────────────────────────────────────────────────────────────

export class CatalogDirectoryDO implements DurableObject {
  private sql: SqlStorage;

  constructor(private state: DurableObjectState) {
    this.sql = state.storage.sql;
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS worlds (
        world_id TEXT PRIMARY KEY, name TEXT, run_id TEXT,
        parent_world_id TEXT, status TEXT NOT NULL,
        tick_head INTEGER NOT NULL DEFAULT 0
      );
      CREATE TABLE IF NOT EXISTS signatures (
        table_id TEXT PRIMARY KEY, component_names TEXT NOT NULL,
        schema_json TEXT NOT NULL, fingerprint TEXT NOT NULL
      );
    `);
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const parts = url.pathname.split("/").filter(Boolean); // ns,:namespace,...
    const route = parts.slice(2);
    const method = request.method;

    if (route[0] === "worlds" && method === "POST") {
      const rec = (await request.json()) as Record<string, unknown>;
      const existing = this.sql
        .exec("SELECT * FROM worlds WHERE world_id = ?", rec.world_id)
        .toArray();
      if (existing.length > 0) {
        const row = existing[0] as Record<string, unknown>;
        if (
          row.name !== (rec.name ?? null) ||
          row.run_id !== (rec.run_id ?? null) ||
          row.parent_world_id !== (rec.parent_world_id ?? null)
        ) {
          return conflict(
            "catalog_conflict",
            `world ${rec.world_id} already registered with different identity`,
          );
        }
        return json({ ok: true, idempotent: true, status: row.status });
      }
      this.sql.exec(
        "INSERT INTO worlds (world_id, name, run_id, parent_world_id, status, tick_head) VALUES (?, ?, ?, ?, ?, ?)",
        rec.world_id,
        rec.name ?? null,
        rec.run_id ?? null,
        rec.parent_world_id ?? null,
        rec.status ?? "active",
        rec.tick_head ?? 0,
      );
      return json({ ok: true, status: rec.status ?? "active" });
    }

    if (route[0] === "worlds" && route.length === 1 && method === "GET") {
      try {
        recoveryExactQuery(
          url,
          WORLD_DISCOVERY_QUERY_FIELDS,
          "world discovery query",
        );
      } catch (error) {
        if (error instanceof RecoveryInputError) {
          return json({ error: "invalid", message: error.message }, 400);
        }
        throw error;
      }
      if (
        url.searchParams.has("after_world_id") ||
        url.searchParams.has("limit")
      ) {
        const afterWorldId = url.searchParams.get("after_world_id") ?? "";
        let rawLimit: number;
        try {
          rawLimit = recoveryQueryInteger(
            url.searchParams.get("limit"),
            1000,
            "world discovery page limit",
            1,
            10_000,
          );
        } catch (error) {
          if (error instanceof RecoveryInputError) {
            return json({ error: "invalid", message: error.message }, 400);
          }
          throw error;
        }
        return json(
          this.sql
            .exec(
              "SELECT * FROM worlds WHERE world_id > ? ORDER BY world_id LIMIT ?",
              afterWorldId,
              rawLimit,
            )
            .toArray(),
        );
      }
      return json(this.sql.exec("SELECT * FROM worlds ORDER BY world_id").toArray());
    }

    if (route[0] === "worlds" && route.length === 2) {
      const worldId = decodeURIComponent(route[1]);
      if (method === "GET") {
        const rows = this.sql.exec("SELECT * FROM worlds WHERE world_id = ?", worldId).toArray();
        return rows.length ? json(rows[0]) : json({ error: "not_found" }, 404);
      }
      if (method === "PATCH") {
        const patch = (await request.json()) as Record<string, unknown>;
        if (typeof patch.status === "string") {
          this.sql.exec("UPDATE worlds SET status = ? WHERE world_id = ?", patch.status, worldId);
        }
        if (typeof patch.run_id === "string") {
          this.sql.exec("UPDATE worlds SET run_id = ? WHERE world_id = ?", patch.run_id, worldId);
        }
        if (typeof patch.tick_head === "number") {
          this.sql.exec(
            "UPDATE worlds SET tick_head = MAX(tick_head, ?) WHERE world_id = ?",
            patch.tick_head,
            worldId,
          );
        }
        return json({ ok: true });
      }
    }

    if (route[0] === "signatures" && method === "POST") {
      const rec = (await request.json()) as Record<string, unknown>;
      const existing = this.sql
        .exec("SELECT fingerprint FROM signatures WHERE table_id = ?", rec.table_id)
        .toArray();
      if (existing.length > 0) {
        if ((existing[0] as Record<string, unknown>).fingerprint !== rec.fingerprint) {
          return conflict(
            "catalog_conflict",
            `signature ${rec.table_id} already registered with a different fingerprint`,
          );
        }
        return json({ ok: true, idempotent: true });
      }
      this.sql.exec(
        "INSERT INTO signatures (table_id, component_names, schema_json, fingerprint) VALUES (?, ?, ?, ?)",
        rec.table_id,
        JSON.stringify(rec.component_names ?? []),
        rec.schema_json,
        rec.fingerprint,
      );
      return json({ ok: true });
    }

    if (route[0] === "signatures" && method === "GET") {
      return json(this.sql.exec("SELECT * FROM signatures ORDER BY table_id").toArray());
    }

    return json({ error: "bad_route" }, 404);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-world commit authority: fence + manifests + claims (the hot path)
// ─────────────────────────────────────────────────────────────────────────────

export class WorldCommitDO implements DurableObject {
  private sql: SqlStorage;

  constructor(private state: DurableObjectState) {
    this.sql = state.storage.sql;
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS fence (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        epoch INTEGER NOT NULL, holder TEXT NOT NULL, acquired_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS world_state (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        status TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS manifests (
        run_id TEXT NOT NULL, tick INTEGER NOT NULL,
        commit_token TEXT NOT NULL, writer_epoch INTEGER NOT NULL,
        tables_json TEXT NOT NULL, created_at TEXT NOT NULL,
        PRIMARY KEY (run_id, tick)
      );
      CREATE TABLE IF NOT EXISTS claims (
        scope_key TEXT PRIMARY KEY, run_id TEXT NOT NULL,
        producer TEXT NOT NULL, external_id TEXT NOT NULL,
        payload_digest TEXT NOT NULL, status TEXT NOT NULL,
        commit_token TEXT NOT NULL, tick INTEGER NOT NULL,
        artifact_entity_id INTEGER NOT NULL DEFAULT 0, table_id TEXT,
        claimant TEXT NOT NULL, lease_expires_at REAL NOT NULL,
        fence_epoch INTEGER NOT NULL, created_at TEXT NOT NULL, completed_at TEXT
      );
      CREATE TABLE IF NOT EXISTS mission_attempt_claims (
        claim_key TEXT PRIMARY KEY, run_id TEXT NOT NULL,
        mission_id TEXT NOT NULL, task_id TEXT NOT NULL, attempt_id TEXT NOT NULL,
        idempotency_key TEXT NOT NULL, request_fingerprint TEXT NOT NULL,
        request_json TEXT NOT NULL, redaction_policy_id TEXT NOT NULL DEFAULT '',
        redaction_acquisition_evidence_json TEXT NOT NULL DEFAULT '',
        redaction_evidence_json TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL, provider TEXT NOT NULL,
        provider_request_fingerprint TEXT NOT NULL,
        supports_idempotent_replay INTEGER NOT NULL,
        supports_session_resume INTEGER NOT NULL,
        provider_idempotency_key TEXT NOT NULL,
        claimant TEXT NOT NULL, lease_expires_at REAL NOT NULL,
        fence_epoch INTEGER NOT NULL, execution_nonce TEXT NOT NULL DEFAULT '',
        execution_consumed_at TEXT, provider_session_id TEXT NOT NULL DEFAULT '',
        provider_request_id TEXT NOT NULL DEFAULT '',
        settlement_status TEXT NOT NULL DEFAULT '',
        outcome_digest TEXT NOT NULL DEFAULT '', outcome_json TEXT NOT NULL DEFAULT '',
        artifact_request_json TEXT NOT NULL DEFAULT '',
        artifact_request_digest TEXT NOT NULL DEFAULT '',
        artifact_publication_key TEXT NOT NULL DEFAULT '',
        legacy_unbound_eligible INTEGER NOT NULL DEFAULT 0,
        last_error TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
        possibly_submitted_at TEXT, acknowledged_at TEXT,
        finalizing_at TEXT, settled_at TEXT
      );
      CREATE INDEX IF NOT EXISTS mission_attempt_claims_due
      ON mission_attempt_claims (status, lease_expires_at);
      -- World identity is implicit in this per-world Durable Object.
      CREATE UNIQUE INDEX IF NOT EXISTS mission_attempt_claims_identity
      ON mission_attempt_claims (mission_id, task_id, attempt_id);
      CREATE TABLE IF NOT EXISTS artifact_publications (
        publication_key TEXT PRIMARY KEY, run_id TEXT NOT NULL,
        attempt_id TEXT NOT NULL, idempotency_key TEXT NOT NULL,
        request_digest TEXT NOT NULL, status TEXT NOT NULL,
        request_json TEXT NOT NULL, records_json TEXT NOT NULL DEFAULT '[]',
        claimant TEXT NOT NULL, lease_expires_at REAL NOT NULL,
        retry_until_ms INTEGER NOT NULL, attempt_count INTEGER NOT NULL DEFAULT 1,
        index_snapshot_id INTEGER NOT NULL DEFAULT 0,
        index_snapshot_id_text TEXT NOT NULL DEFAULT '0',
        manifest_uri TEXT NOT NULL DEFAULT '', last_error TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL, completed_at TEXT
      );
      CREATE INDEX IF NOT EXISTS artifact_publications_due
      ON artifact_publications (status, lease_expires_at);
      CREATE TABLE IF NOT EXISTS fleet_recovery_sweeps (
        sweep_key TEXT PRIMARY KEY,
        storage_fingerprint TEXT NOT NULL,
        world_id TEXT NOT NULL,
        kind TEXT NOT NULL,
        status TEXT NOT NULL,
        cursor TEXT NOT NULL DEFAULT '',
        cycle INTEGER NOT NULL DEFAULT 0,
        claimant TEXT NOT NULL DEFAULT '',
        lease_expires_at_ms INTEGER NOT NULL DEFAULT 0,
        fence_epoch INTEGER NOT NULL DEFAULT 0,
        active_subject_key TEXT NOT NULL DEFAULT '',
        consecutive_failures INTEGER NOT NULL DEFAULT 0,
        max_consecutive_failures INTEGER NOT NULL,
        next_due_at_ms INTEGER NOT NULL,
        last_error_code TEXT NOT NULL DEFAULT '',
        last_error_detail TEXT NOT NULL DEFAULT '',
        redrive_from_fence_epoch INTEGER,
        redrive_delay_ms INTEGER,
        created_at_ms INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL,
        paused_at_ms INTEGER,
        UNIQUE (world_id, kind)
      );
      CREATE INDEX IF NOT EXISTS fleet_recovery_sweeps_due
      ON fleet_recovery_sweeps (
        world_id, status, next_due_at_ms, lease_expires_at_ms, kind, sweep_key
      );
      CREATE TABLE IF NOT EXISTS fleet_recovery_exceptions (
        exception_key TEXT PRIMARY KEY,
        sweep_key TEXT NOT NULL,
        storage_fingerprint TEXT NOT NULL,
        world_id TEXT NOT NULL,
        kind TEXT NOT NULL,
        subject_key TEXT NOT NULL,
        authority_key TEXT NOT NULL,
        status TEXT NOT NULL,
        attempt_count INTEGER NOT NULL DEFAULT 0,
        max_attempts INTEGER NOT NULL,
        retry_at_ms INTEGER NOT NULL,
        last_error_code TEXT NOT NULL DEFAULT '',
        last_error_detail TEXT NOT NULL DEFAULT '',
        redrive_from_attempt_count INTEGER,
        redrive_retry_delay_ms INTEGER,
        created_at_ms INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL,
        resolved_at_ms INTEGER,
        dead_lettered_at_ms INTEGER,
        UNIQUE (sweep_key, subject_key),
        FOREIGN KEY (sweep_key) REFERENCES fleet_recovery_sweeps (sweep_key)
      );
      CREATE INDEX IF NOT EXISTS fleet_recovery_exceptions_due
      ON fleet_recovery_exceptions (
        world_id, kind, status, retry_at_ms, exception_key
      );
      CREATE TABLE IF NOT EXISTS catalog_meta (
        key TEXT PRIMARY KEY, value TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS commands (
        sequence INTEGER PRIMARY KEY AUTOINCREMENT,
        command_id TEXT NOT NULL UNIQUE,
        scheduled_tick INTEGER NOT NULL, priority INTEGER NOT NULL,
        command_type TEXT NOT NULL, payload_json TEXT NOT NULL,
        payload_digest TEXT NOT NULL, version INTEGER NOT NULL,
        principal_id TEXT, origin TEXT NOT NULL, reserved_entity_id INTEGER,
        status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
        max_attempts INTEGER NOT NULL, lease_owner TEXT, lease_expires_at REAL,
        last_error_code TEXT, last_error_detail TEXT,
        accepted_at TEXT NOT NULL, updated_at TEXT NOT NULL,
        applied_tick INTEGER, commit_token TEXT
      );
      CREATE INDEX IF NOT EXISTS commands_due_idx
        ON commands (status, scheduled_tick, priority, sequence);
      CREATE TABLE IF NOT EXISTS outbox (
        sequence INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT NOT NULL UNIQUE, aggregate_type TEXT NOT NULL,
        aggregate_id TEXT NOT NULL, event_type TEXT NOT NULL,
        command_type TEXT NOT NULL, status TEXT NOT NULL,
        actor_id TEXT, payload_json TEXT NOT NULL,
        occurred_at TEXT NOT NULL, projected_at TEXT
      );
      CREATE INDEX IF NOT EXISTS outbox_pending_idx
        ON outbox (projected_at, sequence);
    `);
    const claimColumns = this.sql
      .exec("PRAGMA table_info(claims)")
      .toArray()
      .map((row) => String(row.name));
    if (claimColumns.includes("fact_entity_id") && !claimColumns.includes("artifact_entity_id")) {
      this.sql.exec(
        "ALTER TABLE claims RENAME COLUMN fact_entity_id TO artifact_entity_id",
      );
    }
    this.state.storage.transactionSync(() => {
      const attemptClaimColumns = this.sql
        .exec("PRAGMA table_info(mission_attempt_claims)")
        .toArray()
        .map((row) => String(row.name));
      if (!attemptClaimColumns.includes("outcome_json")) {
        this.sql.exec(
          "ALTER TABLE mission_attempt_claims ADD COLUMN outcome_json TEXT NOT NULL DEFAULT ''",
        );
      }
      if (!attemptClaimColumns.includes("execution_nonce")) {
        this.sql.exec(
          "ALTER TABLE mission_attempt_claims ADD COLUMN " +
            "execution_nonce TEXT NOT NULL DEFAULT ''",
        );
      }
      if (!attemptClaimColumns.includes("execution_consumed_at")) {
        this.sql.exec(
          "ALTER TABLE mission_attempt_claims ADD COLUMN execution_consumed_at TEXT",
        );
      }
      if (!attemptClaimColumns.includes("redaction_policy_id")) {
        this.sql.exec(
          "ALTER TABLE mission_attempt_claims ADD COLUMN " +
            "redaction_policy_id TEXT NOT NULL DEFAULT ''",
        );
      }
      if (!attemptClaimColumns.includes("redaction_acquisition_evidence_json")) {
        this.sql.exec(
          "ALTER TABLE mission_attempt_claims ADD COLUMN " +
            "redaction_acquisition_evidence_json TEXT NOT NULL DEFAULT ''",
        );
      }
      if (!attemptClaimColumns.includes("redaction_evidence_json")) {
        this.sql.exec(
          "ALTER TABLE mission_attempt_claims ADD COLUMN " +
            "redaction_evidence_json TEXT NOT NULL DEFAULT ''",
        );
      }
      if (!attemptClaimColumns.includes("artifact_request_json")) {
        this.sql.exec(
          "ALTER TABLE mission_attempt_claims ADD COLUMN " +
            "artifact_request_json TEXT NOT NULL DEFAULT ''",
        );
      }
      if (!attemptClaimColumns.includes("artifact_request_digest")) {
        this.sql.exec(
          "ALTER TABLE mission_attempt_claims ADD COLUMN " +
            "artifact_request_digest TEXT NOT NULL DEFAULT ''",
        );
      }
      if (!attemptClaimColumns.includes("artifact_publication_key")) {
        this.sql.exec(
          "ALTER TABLE mission_attempt_claims ADD COLUMN " +
            "artifact_publication_key TEXT NOT NULL DEFAULT ''",
        );
      }
      if (!attemptClaimColumns.includes("finalizing_at")) {
        this.sql.exec("ALTER TABLE mission_attempt_claims ADD COLUMN finalizing_at TEXT");
      }
      if (!attemptClaimColumns.includes("legacy_unbound_eligible")) {
        this.sql.exec(
          "ALTER TABLE mission_attempt_claims ADD COLUMN " +
            "legacy_unbound_eligible INTEGER NOT NULL DEFAULT 0",
        );
      }
      const legacyMigration = this.sql
        .exec("SELECT value FROM catalog_meta WHERE key = ?", LEGACY_UNBOUND_MIGRATION)
        .toArray();
      if (!legacyMigration.length) {
        const candidates = this.sql
          .exec(
            "SELECT * FROM mission_attempt_claims WHERE status = 'settled' " +
              "AND legacy_unbound_eligible = 0 " +
              "AND artifact_request_json = '' AND artifact_request_digest = '' " +
              "AND artifact_publication_key = ''",
          )
          .toArray();
        for (const candidate of candidates) {
          const row = candidate as Record<string, unknown>;
          if (isUnboundLegacyIndexedClaim(row)) {
            this.sql.exec(
              "UPDATE mission_attempt_claims SET legacy_unbound_eligible = 1 " +
                "WHERE claim_key = ?",
              row.claim_key,
            );
          }
        }
        this.sql.exec(
          "INSERT INTO catalog_meta (key, value) VALUES (?, 'complete')",
          LEGACY_UNBOUND_MIGRATION,
        );
      }
      const artifactPublicationColumns = this.sql
        .exec("PRAGMA table_info(artifact_publications)")
        .toArray()
        .map((row) => String(row.name));
      if (!artifactPublicationColumns.includes("index_snapshot_id_text")) {
        this.sql.exec(
          "ALTER TABLE artifact_publications ADD COLUMN " +
            "index_snapshot_id_text TEXT NOT NULL DEFAULT '0'",
        );
      }
      // Backfill only legacy values that were provably JS-safe. Larger values
      // may already have been rounded by the old JSON-number protocol; leaving
      // their text receipt at "0" makes new INDEXED reads fail closed.
      this.sql.exec(
        "UPDATE artifact_publications SET index_snapshot_id_text = CAST(index_snapshot_id AS TEXT) " +
          "WHERE index_snapshot_id_text = '0' " +
          "AND index_snapshot_id BETWEEN 1 AND 9007199254740991",
      );
      // Recovery coordination first shipped as schema v9. Prerelease v9
      // Durable Objects may already have these tables without the internal
      // lost-response receipts, so column presence—not only the migration
      // marker—is the forward-compatibility oracle.
      const recoverySweepColumns = this.sql
        .exec("PRAGMA table_info(fleet_recovery_sweeps)")
        .toArray()
        .map((row) => String(row.name));
      if (!recoverySweepColumns.includes("redrive_from_fence_epoch")) {
        this.sql.exec(
          "ALTER TABLE fleet_recovery_sweeps " +
            "ADD COLUMN redrive_from_fence_epoch INTEGER",
        );
      }
      if (!recoverySweepColumns.includes("redrive_delay_ms")) {
        this.sql.exec(
          "ALTER TABLE fleet_recovery_sweeps ADD COLUMN redrive_delay_ms INTEGER",
        );
      }
      const recoveryExceptionColumns = this.sql
        .exec("PRAGMA table_info(fleet_recovery_exceptions)")
        .toArray()
        .map((row) => String(row.name));
      if (!recoveryExceptionColumns.includes("redrive_from_attempt_count")) {
        this.sql.exec(
          "ALTER TABLE fleet_recovery_exceptions " +
            "ADD COLUMN redrive_from_attempt_count INTEGER",
        );
      }
      if (!recoveryExceptionColumns.includes("redrive_retry_delay_ms")) {
        this.sql.exec(
          "ALTER TABLE fleet_recovery_exceptions " +
            "ADD COLUMN redrive_retry_delay_ms INTEGER",
        );
      }
      this.sql.exec(
        "INSERT OR IGNORE INTO catalog_meta (key, value) VALUES (?, 'complete')",
        FLEET_RECOVERY_MIGRATION,
      );
    });
  }

  private recoverySweep(
    worldId: string,
    kind: string,
  ): Record<string, unknown> | null {
    const rows = this.sql
      .exec(
        "SELECT * FROM fleet_recovery_sweeps WHERE world_id = ? AND kind = ?",
        worldId,
        kind,
      )
      .toArray();
    return rows.length ? (rows[0] as Record<string, unknown>) : null;
  }

  private liveRecoverySweep(
    worldId: string,
    kind: string,
    claimant: string,
    fenceEpoch: number,
    nowMs: number,
  ): { row?: Record<string, unknown>; error?: Response } {
    const row = this.recoverySweep(worldId, kind);
    if (!row) {
      return {
        error: conflict(
          "recovery_sweep_conflict",
          `recovery sweep ${worldId}/${kind} is not registered`,
        ),
      };
    }
    if (
      row.status !== "leased" ||
      row.claimant !== claimant ||
      Number(row.fence_epoch) !== fenceEpoch ||
      Number(row.lease_expires_at_ms) <= nowMs
    ) {
      return {
        error: recoveryStale(
          `recovery sweep ${worldId}/${kind} is not live at fence ${fenceEpoch}`,
        ),
      };
    }
    return { row };
  }

  private recoverArtifactPublication(
    publicationKey: string,
    claimant: string,
    leaseMs: number,
    nowMs: number,
  ): Response {
    const rows = this.sql
      .exec(
        "SELECT * FROM artifact_publications WHERE publication_key = ?",
        publicationKey,
      )
      .toArray();
    if (!rows.length) {
      return json({ outcome: "obsolete", publication: null });
    }
    const row = rows[0] as Record<string, unknown>;
    const authorityError = artifactPublicationAuthorityError(row, publicationKey);
    if (authorityError) return authorityError;
    if (row.status === "INDEXED") {
      return json({ outcome: "duplicate", publication: artifactPublicationView(row) });
    }
    if (row.status === "EXPIRED") {
      return json({ outcome: "expired", publication: artifactPublicationView(row) });
    }
    if (row.status !== "PENDING" && row.status !== "UPLOADED") {
      return conflict(
        "artifact_publication_conflict",
        `artifact publication ${publicationKey} has invalid status ${row.status}`,
      );
    }
    const nowSec = nowMs / 1000;
    const nowText = new Date(nowMs).toISOString();
    if (row.status === "PENDING" && Number(row.retry_until_ms) <= nowMs) {
      const expiredWrite = this.sql.exec(
        "UPDATE artifact_publications SET status = 'EXPIRED', lease_expires_at = 0, " +
          "last_error = ?, updated_at = ?, completed_at = ? " +
          "WHERE publication_key = ? AND status = 'PENDING'",
        ARTIFACT_RETRY_EXPIRED_DETAIL,
        nowText,
        nowText,
        publicationKey,
      );
      if (expiredWrite.rowsWritten < 1) {
        return conflict(
          "artifact_publication_conflict",
          `artifact publication ${publicationKey} changed before expiry`,
        );
      }
      const expired = this.sql
        .exec(
          "SELECT * FROM artifact_publications WHERE publication_key = ?",
          publicationKey,
        )
        .toArray();
      return json({
        outcome: "expired",
        publication: artifactPublicationView(expired[0] as Record<string, unknown>),
      });
    }
    if (Number(row.lease_expires_at) > nowSec) {
      if (row.claimant === claimant) {
        const renewed = this.sql.exec(
          "UPDATE artifact_publications SET lease_expires_at = ?, updated_at = ? " +
            "WHERE publication_key = ? AND status = ? AND claimant = ?",
          (nowMs + leaseMs) / 1000,
          nowText,
          publicationKey,
          row.status,
          claimant,
        );
        if (renewed.rowsWritten < 1) {
          return json(
            {
              error: "artifact_publication_pending",
              message: `artifact publication ${publicationKey} changed before renewal`,
            },
            423,
          );
        }
        const owned = this.sql
          .exec(
            "SELECT * FROM artifact_publications WHERE publication_key = ?",
            publicationKey,
          )
          .toArray();
        return json({
          outcome: "owned",
          publication: artifactPublicationView(owned[0] as Record<string, unknown>),
        });
      }
      return json(
        { error: "artifact_publication_pending", message: "a live publication lease exists" },
        423,
      );
    }
    if (Number(row.attempt_count) >= Number.MAX_SAFE_INTEGER) {
      return conflict(
        "artifact_publication_conflict",
        `artifact publication ${publicationKey} exhausted its portable attempt counter`,
      );
    }
    const updated = this.sql.exec(
      "UPDATE artifact_publications SET claimant = ?, lease_expires_at = ?, " +
        "attempt_count = attempt_count + 1, updated_at = ? " +
        "WHERE publication_key = ? AND status = ? AND lease_expires_at <= ?",
      claimant,
      (nowMs + leaseMs) / 1000,
      nowText,
      publicationKey,
      row.status,
      nowSec,
    );
    if (updated.rowsWritten < 1) {
      return json(
        {
          error: "artifact_publication_pending",
          message: `artifact publication ${publicationKey} changed before recovery`,
        },
        423,
      );
    }
    const recovered = this.sql
      .exec(
        "SELECT * FROM artifact_publications WHERE publication_key = ?",
        publicationKey,
      )
      .toArray();
    return json({
      outcome: "recovered",
      publication: artifactPublicationView(recovered[0] as Record<string, unknown>),
    });
  }

  private async handleArtifactServerClock(
    request: Request,
    url: URL,
    route: string[],
    worldId: string,
  ): Promise<Response> {
    const method = request.method;

    if (route.length === 2 && route[1] === "acquire-v3" && method === "POST") {
      const p = recoveryObject(await request.json(), "artifact acquisition body");
      recoveryExactFields(
        p,
        new Set([
          "publication_key",
          "run_id",
          "attempt_id",
          "idempotency_key",
          "request_digest",
          "request_json",
          "claimant",
          "retry_window_ms",
          "retry_not_after_ms",
          "lease_ms",
        ]),
        "artifact acquisition body",
      );
      const publicationKey = recoverySha256(p.publication_key, "publication_key");
      const runId = recoveryText(p.run_id, "run_id", 4096);
      const attemptId = recoveryText(p.attempt_id, "attempt_id", 4096);
      const idempotencyKey = recoveryText(p.idempotency_key, "idempotency_key", 4096);
      const requestDigest = recoveryText(p.request_digest, "request_digest", 4096);
      const requestJson = recoveryText(p.request_json, "request_json", 16 * 1024 * 1024);
      const claimant = recoveryText(p.claimant, "artifact publication claimant", 1024);
      const retryWindowMs = recoveryInteger(
        p.retry_window_ms,
        "artifact retry_window_ms",
        0,
        MAX_ARTIFACT_RETRY_WINDOW_MS,
      );
      const leaseMs = recoveryInteger(
        p.lease_ms,
        "artifact lease_ms",
        1,
        MAX_ARTIFACT_LEASE_MS,
      );
      let retryNotAfterMs: number | null = null;
      if (Object.prototype.hasOwnProperty.call(p, "retry_not_after_ms")) {
        retryNotAfterMs = recoveryInteger(
          p.retry_not_after_ms,
          "artifact retry_not_after_ms",
          0,
          Number.MAX_SAFE_INTEGER,
        );
      }
      const expectedPublicationKey = await artifactPublicationKey(
        worldId,
        runId,
        idempotencyKey,
      );
      if (publicationKey !== expectedPublicationKey) {
        throw new RecoveryInputError(
          "publication_key does not match world_id/run_id/idempotency_key",
        );
      }
      const existing = this.sql
        .exec(
          "SELECT * FROM artifact_publications WHERE publication_key = ?",
          publicationKey,
        )
        .toArray();
      if (existing.length) {
        const row = existing[0] as Record<string, unknown>;
        if (row.request_digest !== requestDigest) {
          return conflict(
            "artifact_publication_conflict",
            `${idempotencyKey} was reused with a different publication request`,
          );
        }
        return this.recoverArtifactPublication(
          publicationKey,
          claimant,
          leaseMs,
          Date.now(),
        );
      }
      const nowMs = Date.now();
      const retryUntilMs = retryNotAfterMs === null
        ? nowMs + retryWindowMs
        : Math.min(nowMs + retryWindowMs, retryNotAfterMs);
      const initiallyExpired = retryUntilMs <= nowMs;
      const nowText = new Date(nowMs).toISOString();
      this.sql.exec(
        "INSERT INTO artifact_publications (publication_key, run_id, attempt_id, " +
          "idempotency_key, request_digest, status, request_json, records_json, claimant, " +
          "lease_expires_at, retry_until_ms, attempt_count, last_error, created_at, " +
          "updated_at, completed_at) VALUES (?, ?, ?, ?, ?, ?, ?, '[]', ?, ?, ?, 1, " +
          "?, ?, ?, ?)",
        publicationKey,
        runId,
        attemptId,
        idempotencyKey,
        requestDigest,
        initiallyExpired ? "EXPIRED" : "PENDING",
        requestJson,
        claimant,
        initiallyExpired ? 0 : (nowMs + leaseMs) / 1000,
        retryUntilMs,
        initiallyExpired ? ARTIFACT_RETRY_EXPIRED_DETAIL : "",
        nowText,
        nowText,
        initiallyExpired ? nowText : null,
      );
      const created = this.sql
        .exec(
          "SELECT * FROM artifact_publications WHERE publication_key = ?",
          publicationKey,
        )
        .toArray();
      return json({
        outcome: initiallyExpired ? "expired" : "acquired",
        publication: artifactPublicationView(created[0] as Record<string, unknown>),
      });
    }

    if (route.length === 2 && route[1] === "due-v1" && method === "GET") {
      if (url.searchParams.has("due")) {
        throw new RecoveryInputError("artifact due listing does not accept a caller clock");
      }
      recoveryExactQuery(
        url,
        new Set(["limit", "after_publication_key"]),
        "artifact due listing",
      );
      const limit = recoveryQueryInteger(
        url.searchParams.get("limit"),
        100,
        "artifact publication page limit",
        1,
        10_000,
      );
      const rawCursor = url.searchParams.get("after_publication_key") ?? "";
      const cursor = rawCursor === ""
        ? ""
        : recoverySha256(rawCursor, "after_publication_key");
      const dueRows = this.sql
        .exec(
          "SELECT publication_key FROM artifact_publications " +
            "WHERE status IN ('PENDING', 'UPLOADED') " +
            "AND lease_expires_at <= ? AND publication_key > ? " +
            "ORDER BY publication_key LIMIT ?",
          Date.now() / 1000,
          cursor,
          limit,
        )
        .toArray();
      return json(
        dueRows.map((row) => ({
          publication_key: String((row as Record<string, unknown>).publication_key),
        })),
      );
    }

    if (route.length === 3 && route[2] === "recover-v1" && method === "POST") {
      const publicationKey = recoverySha256(route[1], "publication_key");
      const p = recoveryObject(await request.json(), "artifact recovery body");
      recoveryExactFields(
        p,
        new Set(["claimant", "lease_ms"]),
        "artifact recovery body",
      );
      const claimant = recoveryText(p.claimant, "artifact publication claimant", 1024);
      const leaseMs = recoveryInteger(
        p.lease_ms,
        "artifact lease_ms",
        1,
        MAX_ARTIFACT_LEASE_MS,
      );
      return this.recoverArtifactPublication(publicationKey, claimant, leaseMs, Date.now());
    }

    if (route.length === 3 && route[2] === "fail-v3" && method === "POST") {
      const publicationKey = recoverySha256(route[1], "publication_key");
      const p = recoveryObject(await request.json(), "artifact failure body");
      recoveryExactFields(
        p,
        new Set(["claimant", "error", "retry_delay_ms"]),
        "artifact failure body",
      );
      const claimant = recoveryText(p.claimant, "artifact publication claimant", 1024);
      const error = recoveryText(p.error, "artifact publication error", 8000, true);
      const retryDelayMs = recoveryInteger(
        p.retry_delay_ms,
        "artifact retry_delay_ms",
        0,
        MAX_ARTIFACT_RETRY_DELAY_MS,
      );
      const rows = this.sql
        .exec(
          "SELECT * FROM artifact_publications WHERE publication_key = ?",
          publicationKey,
        )
        .toArray();
      if (!rows.length) return json({ ok: true });
      const row = rows[0] as Record<string, unknown>;
      const authorityError = artifactPublicationAuthorityError(row, publicationKey);
      if (authorityError) return authorityError;
      if (row.status === "INDEXED" || row.status === "EXPIRED") return json({ ok: true });
      if (row.claimant !== claimant) {
        return json(
          {
            error: "artifact_publication_pending",
            message: `artifact publication ${publicationKey} was taken over`,
          },
          423,
        );
      }
      const nowMs = Date.now();
      if (row.status === "PENDING" && Number(row.retry_until_ms) <= nowMs) {
        const nowText = new Date(nowMs).toISOString();
        this.sql.exec(
          "UPDATE artifact_publications SET status = 'EXPIRED', lease_expires_at = 0, " +
            "last_error = ?, updated_at = ?, completed_at = ? " +
            "WHERE publication_key = ? AND status = 'PENDING'",
          ARTIFACT_RETRY_EXPIRED_DETAIL,
          nowText,
          nowText,
          publicationKey,
        );
        return json({ ok: true });
      }
      if (Number(row.lease_expires_at) <= nowMs / 1000) {
        return json(
          {
            error: "artifact_publication_pending",
            message: `artifact publication ${publicationKey} lease expired before failure`,
          },
          423,
        );
      }
      this.sql.exec(
        "UPDATE artifact_publications SET last_error = ?, lease_expires_at = ?, " +
          "updated_at = ? WHERE publication_key = ?",
        error,
        (nowMs + retryDelayMs) / 1000,
        new Date(nowMs).toISOString(),
        publicationKey,
      );
      return json({ ok: true });
    }

    return json({ error: "bad_route" }, 404);
  }

  private async handleRecovery(
    request: Request,
    url: URL,
    route: string[],
    worldId: string,
  ): Promise<Response> {
    if (route[1] === "sweeps") {
      return this.handleRecoverySweeps(request, url, route, worldId);
    }
    if (route[1] === "exceptions") {
      return this.handleRecoveryExceptions(request, url, route, worldId);
    }
    return json({ error: "bad_route" }, 404);
  }

  private async handleRecoverySweeps(
    request: Request,
    url: URL,
    route: string[],
    worldId: string,
  ): Promise<Response> {
    const method = request.method;

    if (route.length === 2 && method === "GET") {
      recoveryExactQuery(
        url,
        RECOVERY_SWEEP_LIST_QUERY_FIELDS,
        "recovery sweep listing query",
      );
      const status = url.searchParams.get("status");
      if (status !== null && !RECOVERY_SWEEP_STATUSES.has(status)) {
        throw new RecoveryInputError(`unsupported recovery sweep status ${status}`);
      }
      const rows = status === null
        ? this.sql
            .exec(
              "SELECT * FROM fleet_recovery_sweeps WHERE world_id = ? " +
                "ORDER BY kind, sweep_key",
              worldId,
            )
            .toArray()
        : this.sql
            .exec(
              "SELECT * FROM fleet_recovery_sweeps WHERE world_id = ? AND status = ? " +
                "ORDER BY kind, sweep_key",
              worldId,
              status,
            )
            .toArray();
      if (rows.length > RECOVERY_KINDS.size) {
        throw new Error("recovery sweep list exceeds the closed kind set");
      }
      return json(
        rows.map((row) => recoverySweepView(row as Record<string, unknown>)),
      );
    }

    if (route.length !== 3 || method !== "POST") {
      return json({ error: "bad_route" }, 404);
    }

    const operation = route[2];
    const allowedFields = RECOVERY_SWEEP_BODY_FIELDS.get(operation);
    if (!allowedFields) {
      return json({ error: "bad_route" }, 404);
    }
    recoveryExactQuery(
      url,
      RECOVERY_NO_QUERY_FIELDS,
      `recovery sweep ${operation} query`,
    );
    const bodyField = `recovery sweep ${operation} body`;
    const p = recoveryObject(await request.json(), bodyField);
    recoveryExactFields(p, allowedFields, bodyField);

    if (operation === "ensure-v1") {
      const storageFingerprint = recoverySha256(
        p.storage_fingerprint,
        "storage_fingerprint",
      );
      const kind = recoveryKind(p.kind);
      const maxFailures = recoveryInteger(
        p.max_consecutive_failures,
        "max_consecutive_failures",
        1,
        1_000_000,
      );
      const initialDelay = recoveryInteger(
        p.initial_delay_ms ?? 0,
        "initial_delay_ms",
        0,
        MAX_RECOVERY_DELAY_MS,
      );
      const sweepKey = await recoveryKey(
        RECOVERY_SWEEP_DOMAIN,
        storageFingerprint,
        worldId,
        kind,
      );
      const world = this.sql
        .exec("SELECT 1 FROM world_state WHERE singleton = 1")
        .toArray();
      if (!world.length) {
        return conflict(
          "recovery_sweep_conflict",
          `world ${worldId} is not registered in this catalog`,
        );
      }
      const existing = this.recoverySweep(worldId, kind);
      if (existing) {
        if (
          existing.sweep_key !== sweepKey ||
          existing.storage_fingerprint !== storageFingerprint ||
          Number(existing.max_consecutive_failures) !== maxFailures
        ) {
          return conflict(
            "recovery_sweep_conflict",
            `recovery sweep ${worldId}/${kind} has different immutable policy`,
          );
        }
        return json(recoverySweepView(existing));
      }
      const nowMs = Date.now();
      const initialStatus = recoverySweepTransition(null, "create");
      try {
        this.sql.exec(
          "INSERT INTO fleet_recovery_sweeps " +
            "(sweep_key, storage_fingerprint, world_id, kind, status, " +
            "max_consecutive_failures, next_due_at_ms, created_at_ms, updated_at_ms) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
          sweepKey,
          storageFingerprint,
          worldId,
          kind,
          initialStatus,
          maxFailures,
          nowMs + initialDelay,
          nowMs,
          nowMs,
        );
      } catch (error) {
        const collision = this.recoverySweep(worldId, kind);
        if (collision) {
          return conflict(
            "recovery_sweep_conflict",
            `recovery sweep ${worldId}/${kind} has different immutable policy`,
          );
        }
        throw error;
      }
      return json(recoverySweepView(this.recoverySweep(worldId, kind)));
    }

    if (operation === "lease-v1") {
      const kind = recoveryKind(p.kind);
      const claimant = recoveryText(p.claimant, "recovery claimant", 1024);
      const leaseMs = recoveryInteger(
        p.lease_ms,
        "recovery lease_ms",
        1,
        MAX_RECOVERY_LEASE_MS,
      );
      const existing = this.recoverySweep(worldId, kind);
      if (!existing) {
        return conflict(
          "recovery_sweep_conflict",
          `recovery sweep ${worldId}/${kind} is not registered`,
        );
      }
      const nowMs = Date.now();
      if (existing.status === "paused") {
        return json({ outcome: "paused", sweep: recoverySweepView(existing) });
      }
      if (
        existing.status === "leased" &&
        Number(existing.lease_expires_at_ms) > nowMs
      ) {
        if (existing.claimant === claimant) {
          return json({ outcome: "owned", sweep: recoverySweepView(existing) });
        }
        return recoveryPending(
          `recovery sweep ${worldId}/${kind} is leased by another worker`,
        );
      }
      if (
        (existing.status === "idle" || existing.status === "retry_wait") &&
        Number(existing.next_due_at_ms) > nowMs
      ) {
        return json({ outcome: "not_due", sweep: recoverySweepView(existing) });
      }
      if (!["idle", "retry_wait", "leased"].includes(String(existing.status))) {
        return conflict(
          "recovery_sweep_conflict",
          `recovery sweep ${worldId}/${kind} has invalid status ${existing.status}`,
        );
      }
      const currentFence = Number(existing.fence_epoch);
      const currentCycle = Number(existing.cycle);
      if (
        !Number.isSafeInteger(currentFence) ||
        !Number.isSafeInteger(currentCycle) ||
        currentFence < 0 ||
        currentCycle < 0 ||
        currentFence >= Number.MAX_SAFE_INTEGER ||
        currentCycle >= Number.MAX_SAFE_INTEGER
      ) {
        return conflict(
          "recovery_sweep_conflict",
          `recovery sweep ${worldId}/${kind} exhausted its portable counter`,
        );
      }
      const outcome = existing.status === "leased" ? "recovered" : "acquired";
      const event: RecoverySweepEvent =
        existing.status === "leased" ? "take_over" : "lease";
      const targetStatus = recoverySweepTransition(existing.status, event);
      const updated = this.sql.exec(
        "UPDATE fleet_recovery_sweeps SET status = ?, claimant = ?, " +
          "lease_expires_at_ms = ?, fence_epoch = fence_epoch + 1, cycle = cycle + 1, " +
          "active_subject_key = ?, updated_at_ms = ?, paused_at_ms = NULL, " +
          "redrive_from_fence_epoch = NULL, redrive_delay_ms = NULL " +
          "WHERE sweep_key = ? AND fence_epoch = ? AND status = ?",
        targetStatus,
        claimant,
        nowMs + leaseMs,
        existing.active_subject_key,
        nowMs,
        existing.sweep_key,
        existing.fence_epoch,
        existing.status,
      );
      if (updated.rowsWritten < 1) {
        return recoveryStale(
          `recovery sweep ${worldId}/${kind} changed before lease acquisition`,
        );
      }
      return json({
        outcome,
        sweep: recoverySweepView(this.recoverySweep(worldId, kind)),
      });
    }

    const kind = recoveryKind(p.kind);
    const claimant = operation === "redrive-v1"
      ? ""
      : recoveryText(p.claimant, "recovery claimant", 1024);

    if (operation === "renew-v1") {
      const fenceEpoch = recoveryInteger(
        p.fence_epoch,
        "fence_epoch",
        0,
        Number.MAX_SAFE_INTEGER,
      );
      const leaseMs = recoveryInteger(
        p.lease_ms,
        "recovery lease_ms",
        1,
        MAX_RECOVERY_LEASE_MS,
      );
      const nowMs = Date.now();
      const live = this.liveRecoverySweep(
        worldId,
        kind,
        claimant,
        fenceEpoch,
        nowMs,
      );
      if (live.error) return live.error;
      const sourceStatus = live.row!.status;
      const targetStatus = recoverySweepTransition(sourceStatus, "renew");
      const updated = this.sql.exec(
        "UPDATE fleet_recovery_sweeps SET status = ?, lease_expires_at_ms = ?, " +
          "updated_at_ms = ?, redrive_from_fence_epoch = NULL, " +
          "redrive_delay_ms = NULL WHERE sweep_key = ? AND status = ? AND claimant = ? " +
          "AND fence_epoch = ? " +
          "AND lease_expires_at_ms > ?",
        targetStatus,
        nowMs + leaseMs,
        nowMs,
        live.row!.sweep_key,
        sourceStatus,
        claimant,
        fenceEpoch,
        nowMs,
      );
      if (updated.rowsWritten < 1) {
        return recoveryStale(
          `recovery sweep ${worldId}/${kind} changed before renewal`,
        );
      }
      return json(recoverySweepView(this.recoverySweep(worldId, kind)));
    }

    if (operation === "checkpoint-v1") {
      const fenceEpoch = recoveryInteger(
        p.fence_epoch,
        "fence_epoch",
        0,
        Number.MAX_SAFE_INTEGER,
      );
      const cursor = p.cursor === undefined || p.cursor === ""
        ? ""
        : recoverySha256(p.cursor, "recovery cursor");
      const activeSubject = p.active_subject_key === undefined || p.active_subject_key === ""
        ? ""
        : recoverySha256(p.active_subject_key, "active_subject_key");
      const nowMs = Date.now();
      const live = this.liveRecoverySweep(
        worldId,
        kind,
        claimant,
        fenceEpoch,
        nowMs,
      );
      if (live.error) return live.error;
      const sourceStatus = live.row!.status;
      const targetStatus = recoverySweepTransition(sourceStatus, "checkpoint");
      const updated = this.sql.exec(
        "UPDATE fleet_recovery_sweeps SET status = ?, cursor = ?, active_subject_key = ?, " +
          "updated_at_ms = ?, redrive_from_fence_epoch = NULL, " +
          "redrive_delay_ms = NULL WHERE sweep_key = ? AND status = ? AND claimant = ? " +
          "AND fence_epoch = ? AND lease_expires_at_ms > ?",
        targetStatus,
        cursor,
        activeSubject,
        nowMs,
        live.row!.sweep_key,
        sourceStatus,
        claimant,
        fenceEpoch,
        nowMs,
      );
      if (updated.rowsWritten < 1) {
        return recoveryStale(
          `recovery sweep ${worldId}/${kind} changed before checkpoint`,
        );
      }
      return json(recoverySweepView(this.recoverySweep(worldId, kind)));
    }

    if (operation === "yield-v1") {
      const fenceEpoch = recoveryInteger(
        p.fence_epoch,
        "fence_epoch",
        0,
        Number.MAX_SAFE_INTEGER,
      );
      const nextDelay = recoveryInteger(
        p.next_delay_ms,
        "next_delay_ms",
        0,
        MAX_RECOVERY_DELAY_MS,
      );
      const existing = this.recoverySweep(worldId, kind);
      if (
        existing?.status === "idle" &&
        existing.claimant === claimant &&
        Number(existing.fence_epoch) === fenceEpoch &&
        Number(existing.next_due_at_ms) - Number(existing.updated_at_ms) === nextDelay
      ) {
        return json(recoverySweepView(existing));
      }
      const nowMs = Date.now();
      const live = this.liveRecoverySweep(
        worldId,
        kind,
        claimant,
        fenceEpoch,
        nowMs,
      );
      if (live.error) return live.error;
      const sourceStatus = live.row!.status;
      const targetStatus = recoverySweepTransition(sourceStatus, "yield");
      const updated = this.sql.exec(
        "UPDATE fleet_recovery_sweeps SET status = ?, lease_expires_at_ms = 0, " +
          "active_subject_key = '', consecutive_failures = 0, next_due_at_ms = ?, " +
          "last_error_code = '', last_error_detail = '', updated_at_ms = ?, " +
          "paused_at_ms = NULL, redrive_from_fence_epoch = NULL, " +
          "redrive_delay_ms = NULL WHERE sweep_key = ? AND status = ? " +
          "AND claimant = ? AND fence_epoch = ? AND lease_expires_at_ms > ?",
        targetStatus,
        nowMs + nextDelay,
        nowMs,
        live.row!.sweep_key,
        sourceStatus,
        claimant,
        fenceEpoch,
        nowMs,
      );
      if (updated.rowsWritten < 1) {
        return recoveryStale(
          `recovery sweep ${worldId}/${kind} changed before yield`,
        );
      }
      return json(recoverySweepView(this.recoverySweep(worldId, kind)));
    }

    if (operation === "fail-v1") {
      const fenceEpoch = recoveryInteger(
        p.fence_epoch,
        "fence_epoch",
        0,
        Number.MAX_SAFE_INTEGER,
      );
      const { errorCode, errorDetail } = recoveryError(
        p.error_code,
        p.error_detail,
      );
      const retryDelay = recoveryInteger(
        p.retry_delay_ms,
        "retry_delay_ms",
        0,
        MAX_RECOVERY_DELAY_MS,
      );
      const existing = this.recoverySweep(worldId, kind);
      if (
        existing &&
        (existing.status === "retry_wait" || existing.status === "paused") &&
        existing.claimant === claimant &&
        Number(existing.fence_epoch) === fenceEpoch &&
        existing.last_error_code === errorCode &&
        existing.last_error_detail === errorDetail &&
        Number(existing.next_due_at_ms) - Number(existing.updated_at_ms) === retryDelay
      ) {
        return json(recoverySweepView(existing));
      }
      const nowMs = Date.now();
      const live = this.liveRecoverySweep(
        worldId,
        kind,
        claimant,
        fenceEpoch,
        nowMs,
      );
      if (live.error) return live.error;
      const failures = Number(live.row!.consecutive_failures) + 1;
      const paused = failures >= Number(live.row!.max_consecutive_failures);
      const sourceStatus = live.row!.status;
      const event: RecoverySweepEvent = paused ? "exhaust" : "fail";
      const targetStatus = recoverySweepTransition(sourceStatus, event);
      const updated = this.sql.exec(
        "UPDATE fleet_recovery_sweeps SET status = ?, lease_expires_at_ms = 0, " +
          "consecutive_failures = ?, next_due_at_ms = ?, last_error_code = ?, " +
          "last_error_detail = ?, updated_at_ms = ?, paused_at_ms = ?, " +
          "redrive_from_fence_epoch = NULL, redrive_delay_ms = NULL " +
          "WHERE sweep_key = ? AND status = ? AND claimant = ? AND fence_epoch = ? " +
          "AND lease_expires_at_ms > ?",
        targetStatus,
        failures,
        nowMs + retryDelay,
        errorCode,
        errorDetail,
        nowMs,
        paused ? nowMs : null,
        live.row!.sweep_key,
        sourceStatus,
        claimant,
        fenceEpoch,
        nowMs,
      );
      if (updated.rowsWritten < 1) {
        return recoveryStale(
          `recovery sweep ${worldId}/${kind} changed before failure recording`,
        );
      }
      return json(recoverySweepView(this.recoverySweep(worldId, kind)));
    }

    if (operation === "pause-v1") {
      const fenceEpoch = recoveryInteger(
        p.fence_epoch,
        "fence_epoch",
        0,
        Number.MAX_SAFE_INTEGER,
      );
      const { errorCode, errorDetail } = recoveryError(
        p.error_code,
        p.error_detail,
      );
      const existing = this.recoverySweep(worldId, kind);
      if (
        existing?.status === "paused" &&
        existing.claimant === claimant &&
        Number(existing.fence_epoch) === fenceEpoch &&
        existing.last_error_code === errorCode &&
        existing.last_error_detail === errorDetail
      ) {
        return json(recoverySweepView(existing));
      }
      const nowMs = Date.now();
      const live = this.liveRecoverySweep(
        worldId,
        kind,
        claimant,
        fenceEpoch,
        nowMs,
      );
      if (live.error) return live.error;
      const sourceStatus = live.row!.status;
      const targetStatus = recoverySweepTransition(sourceStatus, "pause");
      const updated = this.sql.exec(
        "UPDATE fleet_recovery_sweeps SET status = ?, lease_expires_at_ms = 0, " +
          "last_error_code = ?, last_error_detail = ?, updated_at_ms = ?, " +
          "paused_at_ms = ?, redrive_from_fence_epoch = NULL, " +
          "redrive_delay_ms = NULL WHERE sweep_key = ? AND status = ? " +
          "AND claimant = ? AND fence_epoch = ? AND lease_expires_at_ms > ?",
        targetStatus,
        errorCode,
        errorDetail,
        nowMs,
        nowMs,
        live.row!.sweep_key,
        sourceStatus,
        claimant,
        fenceEpoch,
        nowMs,
      );
      if (updated.rowsWritten < 1) {
        return recoveryStale(
          `recovery sweep ${worldId}/${kind} changed before pause`,
        );
      }
      return json(recoverySweepView(this.recoverySweep(worldId, kind)));
    }

    if (operation === "redrive-v1") {
      const expectedFence = recoveryInteger(
        p.expected_fence_epoch,
        "expected_fence_epoch",
        0,
        Number.MAX_SAFE_INTEGER - 1,
      );
      const delay = recoveryInteger(
        p.delay_ms ?? 0,
        "delay_ms",
        0,
        MAX_RECOVERY_DELAY_MS,
      );
      const row = this.recoverySweep(worldId, kind);
      if (!row) {
        return conflict(
          "recovery_sweep_conflict",
          `recovery sweep ${worldId}/${kind} is not registered`,
        );
      }
      if (
        row.status === "idle" &&
        Number(row.fence_epoch) === expectedFence + 1 &&
        row.redrive_from_fence_epoch !== null &&
        Number(row.redrive_from_fence_epoch) === expectedFence &&
        row.redrive_delay_ms !== null &&
        Number(row.redrive_delay_ms) === delay
      ) {
        return json(recoverySweepView(row));
      }
      if (
        row.status !== "paused" ||
        Number(row.fence_epoch) !== expectedFence
      ) {
        return recoveryStale(
          `recovery sweep ${worldId}/${kind} is not paused at the expected fence`,
        );
      }
      const nowMs = Date.now();
      const sourceStatus = row.status;
      const targetStatus = recoverySweepTransition(sourceStatus, "redrive");
      const updated = this.sql.exec(
        "UPDATE fleet_recovery_sweeps SET status = ?, claimant = '', " +
          "lease_expires_at_ms = 0, fence_epoch = fence_epoch + 1, " +
          "consecutive_failures = 0, next_due_at_ms = ?, " +
          "last_error_code = '', last_error_detail = '', updated_at_ms = ?, " +
          "paused_at_ms = NULL, redrive_from_fence_epoch = ?, redrive_delay_ms = ? " +
          "WHERE sweep_key = ? AND status = ? " +
          "AND fence_epoch = ?",
        targetStatus,
        nowMs + delay,
        nowMs,
        expectedFence,
        delay,
        row.sweep_key,
        sourceStatus,
        expectedFence,
      );
      if (updated.rowsWritten < 1) {
        return recoveryStale(
          `recovery sweep ${worldId}/${kind} changed before redrive`,
        );
      }
      return json(recoverySweepView(this.recoverySweep(worldId, kind)));
    }

    return json({ error: "bad_route" }, 404);
  }

  private async handleRecoveryExceptions(
    request: Request,
    url: URL,
    route: string[],
    worldId: string,
  ): Promise<Response> {
    const method = request.method;

    if (method === "GET" && route.length === 3) {
      recoveryExactQuery(
        url,
        RECOVERY_EXCEPTION_ITEM_QUERY_FIELDS,
        "recovery exception lookup query",
      );
      const exceptionKey = recoverySha256(route[2], "exception_key");
      const kind = recoveryKind(url.searchParams.get("kind"));
      const rows = this.sql
        .exec(
          "SELECT * FROM fleet_recovery_exceptions " +
            "WHERE world_id = ? AND kind = ? AND exception_key = ?",
          worldId,
          kind,
          exceptionKey,
        )
        .toArray();
      return rows.length
        ? json(recoveryExceptionView(rows[0] as Record<string, unknown>))
        : json({ error: "not_found" }, 404);
    }

    if (method === "GET" && route.length === 2) {
      recoveryExactQuery(
        url,
        RECOVERY_EXCEPTION_LIST_QUERY_FIELDS,
        "recovery exception listing query",
      );
      const kind = url.searchParams.get("kind");
      const validatedKind = kind === null ? null : recoveryKind(kind);
      const status = url.searchParams.get("status");
      if (status !== null && !RECOVERY_EXCEPTION_STATUSES.has(status)) {
        throw new RecoveryInputError(
          `unsupported recovery exception status ${status}`,
        );
      }
      const dueOnly = recoveryQueryBoolean(
        url.searchParams.get("due_only"),
        false,
        "due_only",
      );
      if (dueOnly && status !== null && status !== "retry_wait") {
        throw new RecoveryInputError(
          "due_only recovery exceptions must have retry_wait status",
        );
      }
      const limit = recoveryQueryInteger(
        url.searchParams.get("limit"),
        100,
        "recovery exception limit",
        1,
        10_000,
      );
      const where = ["world_id = ?"];
      const values: unknown[] = [worldId];
      if (validatedKind !== null) {
        where.push("kind = ?");
        values.push(validatedKind);
      }
      if (status !== null) {
        where.push("status = ?");
        values.push(status);
      }
      if (dueOnly) {
        where.push("status = 'retry_wait'");
        where.push("retry_at_ms <= ?");
        values.push(Date.now());
      }
      values.push(limit);
      return json(
        this.sql
          .exec(
            "SELECT * FROM fleet_recovery_exceptions WHERE " +
              where.join(" AND ") +
              " ORDER BY retry_at_ms, exception_key LIMIT ?",
            ...values,
          )
          .toArray()
          .map((row) => recoveryExceptionView(row as Record<string, unknown>)),
      );
    }

    if (route.length !== 3 || method !== "POST") {
      return json({ error: "bad_route" }, 404);
    }

    const operation = route[2];
    const allowedFields = RECOVERY_EXCEPTION_BODY_FIELDS.get(operation);
    if (!allowedFields) {
      return json({ error: "bad_route" }, 404);
    }
    recoveryExactQuery(
      url,
      RECOVERY_NO_QUERY_FIELDS,
      `recovery exception ${operation} query`,
    );
    const bodyField = `recovery exception ${operation} body`;
    const p = recoveryObject(await request.json(), bodyField);
    recoveryExactFields(p, allowedFields, bodyField);
    const kind = recoveryKind(p.kind);
    const claimant = recoveryText(p.claimant, "recovery claimant", 1024);
    const fenceEpoch = recoveryInteger(
      p.fence_epoch,
      "fence_epoch",
      0,
      Number.MAX_SAFE_INTEGER,
    );

    if (operation === "retry-v1") {
      const subjectKey = recoverySha256(p.subject_key, "subject_key");
      const authorityKey = recoverySha256(p.authority_key, "authority_key");
      const expectedAttempt = recoveryInteger(
        p.expected_attempt_count,
        "expected_attempt_count",
        0,
        Number.MAX_SAFE_INTEGER - 1,
      );
      const maxAttempts = recoveryInteger(
        p.max_attempts,
        "max_attempts",
        1,
        1_000_000,
      );
      const retryDelay = recoveryInteger(
        p.retry_delay_ms,
        "retry_delay_ms",
        0,
        MAX_RECOVERY_DELAY_MS,
      );
      const { errorCode, errorDetail } = recoveryError(
        p.error_code,
        p.error_detail,
      );
      if (p.permanent !== undefined && typeof p.permanent !== "boolean") {
        throw new RecoveryInputError("permanent must be a boolean");
      }
      const permanent = Boolean(p.permanent ?? false);
      // Hashing yields to the event loop. Read only immutable identity before
      // that await, then establish lease authority afterwards so takeover
      // cannot leave a stale owner writing an exception.
      const identity = this.recoverySweep(worldId, kind);
      if (!identity) {
        return conflict(
          "recovery_sweep_conflict",
          `recovery sweep ${worldId}/${kind} is not registered`,
        );
      }
      const exceptionKey = await recoveryKey(
        RECOVERY_EXCEPTION_DOMAIN,
        String(identity.sweep_key),
        subjectKey,
      );
      const nowMs = Date.now();
      const live = this.liveRecoverySweep(
        worldId,
        kind,
        claimant,
        fenceEpoch,
        nowMs,
      );
      if (live.error) return live.error;
      const sweep = live.row!;
      if (sweep.sweep_key !== identity.sweep_key) {
        return recoveryStale(
          `recovery sweep ${worldId}/${kind} changed before retry recording`,
        );
      }
      const rows = this.sql
        .exec(
          "SELECT * FROM fleet_recovery_exceptions WHERE exception_key = ?",
          exceptionKey,
        )
        .toArray();
      const existing = rows.length
        ? (rows[0] as Record<string, unknown>)
        : null;
      const nextAttempt = expectedAttempt + 1;
      const event: RecoveryExceptionEvent =
        permanent || nextAttempt >= maxAttempts ? "dead_letter" : "retry";
      let targetStatus = recoveryExceptionTransition(null, event);
      const retryAtMs = nowMs + retryDelay;
      if (existing) {
        if (
          existing.sweep_key !== sweep.sweep_key ||
          existing.storage_fingerprint !== sweep.storage_fingerprint ||
          existing.world_id !== worldId ||
          existing.kind !== kind ||
          existing.subject_key !== subjectKey ||
          existing.authority_key !== authorityKey ||
          Number(existing.max_attempts) !== maxAttempts
        ) {
          return conflict(
            "recovery_exception_conflict",
            `recovery exception ${exceptionKey} has different immutable content`,
          );
        }
        if (
          Number(existing.attempt_count) === nextAttempt &&
          existing.status === targetStatus &&
          existing.last_error_code === errorCode &&
          existing.last_error_detail === errorDetail &&
          existing.redrive_from_attempt_count === null &&
          existing.redrive_retry_delay_ms === null &&
          Number(existing.retry_at_ms) - Number(existing.updated_at_ms) === retryDelay
        ) {
          return json(recoveryExceptionView(existing));
        }
        const sourceStatus = existing.status;
        try {
          targetStatus = recoveryExceptionTransition(sourceStatus, event);
        } catch {
          return conflict(
            "recovery_exception_conflict",
            `recovery exception ${exceptionKey} must be redriven before retry`,
          );
        }
        if (Number(existing.attempt_count) !== expectedAttempt) {
          return conflict(
            "recovery_exception_conflict",
            `recovery exception ${exceptionKey} attempt count changed`,
          );
        }
        const updated = this.sql.exec(
          "UPDATE fleet_recovery_exceptions SET status = ?, attempt_count = ?, " +
            "retry_at_ms = ?, last_error_code = ?, last_error_detail = ?, " +
            "updated_at_ms = ?, resolved_at_ms = NULL, dead_lettered_at_ms = ?, " +
            "redrive_from_attempt_count = NULL, redrive_retry_delay_ms = NULL " +
            "WHERE exception_key = ? AND status = ? AND attempt_count = ?",
          targetStatus,
          nextAttempt,
          retryAtMs,
          errorCode,
          errorDetail,
          nowMs,
          targetStatus === "dead_letter" ? nowMs : null,
          exceptionKey,
          sourceStatus,
          expectedAttempt,
        );
        if (updated.rowsWritten < 1) {
          return conflict(
            "recovery_exception_conflict",
            `recovery exception ${exceptionKey} changed before retry recording`,
          );
        }
      } else {
        if (expectedAttempt !== 0) {
          return conflict(
            "recovery_exception_conflict",
            `recovery exception ${exceptionKey} has not been recorded`,
          );
        }
        this.sql.exec(
          "INSERT INTO fleet_recovery_exceptions " +
            "(exception_key, sweep_key, storage_fingerprint, world_id, kind, " +
            "subject_key, authority_key, status, attempt_count, max_attempts, " +
            "retry_at_ms, last_error_code, last_error_detail, created_at_ms, " +
            "updated_at_ms, dead_lettered_at_ms) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
          exceptionKey,
          sweep.sweep_key,
          sweep.storage_fingerprint,
          worldId,
          kind,
          subjectKey,
          authorityKey,
          targetStatus,
          nextAttempt,
          maxAttempts,
          retryAtMs,
          errorCode,
          errorDetail,
          nowMs,
          nowMs,
          targetStatus === "dead_letter" ? nowMs : null,
        );
      }
      const result = this.sql
        .exec(
          "SELECT * FROM fleet_recovery_exceptions WHERE exception_key = ?",
          exceptionKey,
        )
        .toArray();
      return json(recoveryExceptionView(result[0] as Record<string, unknown>));
    }

    const exceptionKey = recoverySha256(p.exception_key, "exception_key");
    const nowMs = Date.now();
    const live = this.liveRecoverySweep(
      worldId,
      kind,
      claimant,
      fenceEpoch,
      nowMs,
    );
    if (live.error) return live.error;
    const rows = this.sql
      .exec(
        "SELECT * FROM fleet_recovery_exceptions WHERE exception_key = ?",
        exceptionKey,
      )
      .toArray();
    const row = rows.length ? (rows[0] as Record<string, unknown>) : null;
    if (!row || row.sweep_key !== live.row!.sweep_key) {
      return conflict(
        "recovery_exception_conflict",
        `recovery exception ${exceptionKey} is not part of ${worldId}/${kind}`,
      );
    }

    if (operation === "resolve-v1") {
      if (row.status === "resolved") return json(recoveryExceptionView(row));
      const sourceStatus = row.status;
      let targetStatus: RecoveryExceptionStatus;
      try {
        targetStatus = recoveryExceptionTransition(sourceStatus, "resolve");
      } catch {
        return conflict(
          "recovery_exception_conflict",
          `recovery exception ${exceptionKey} has invalid status ${row.status}`,
        );
      }
      const updated = this.sql.exec(
        "UPDATE fleet_recovery_exceptions SET status = ?, updated_at_ms = ?, " +
          "resolved_at_ms = ?, redrive_from_attempt_count = NULL, " +
          "redrive_retry_delay_ms = NULL WHERE exception_key = ? AND status = ?",
        targetStatus,
        nowMs,
        nowMs,
        exceptionKey,
        sourceStatus,
      );
      if (updated.rowsWritten < 1) {
        return conflict(
          "recovery_exception_conflict",
          `recovery exception ${exceptionKey} changed before resolution`,
        );
      }
      return json(
        recoveryExceptionView(
          this.sql
            .exec(
              "SELECT * FROM fleet_recovery_exceptions WHERE exception_key = ?",
              exceptionKey,
            )
            .toArray()[0] as Record<string, unknown>,
        ),
      );
    }

    if (operation === "redrive-v1") {
      const expectedAttempt = recoveryInteger(
        p.expected_attempt_count,
        "expected_attempt_count",
        0,
        Number.MAX_SAFE_INTEGER,
      );
      const retryDelay = recoveryInteger(
        p.retry_delay_ms ?? 0,
        "retry_delay_ms",
        0,
        MAX_RECOVERY_DELAY_MS,
      );
      if (Number(row.attempt_count) !== expectedAttempt) {
        return conflict(
          "recovery_exception_conflict",
          `recovery exception ${exceptionKey} attempt count changed`,
        );
      }
      if (
        row.status === "retry_wait" &&
        row.redrive_from_attempt_count !== null &&
        Number(row.redrive_from_attempt_count) === expectedAttempt &&
        row.redrive_retry_delay_ms !== null &&
        Number(row.redrive_retry_delay_ms) === retryDelay
      ) {
        return json(recoveryExceptionView(row));
      }
      const sourceStatus = row.status;
      let targetStatus: RecoveryExceptionStatus;
      try {
        targetStatus = recoveryExceptionTransition(sourceStatus, "redrive");
      } catch {
        return conflict(
          "recovery_exception_conflict",
          `recovery exception ${exceptionKey} is not dead-lettered`,
        );
      }
      const updated = this.sql.exec(
        "UPDATE fleet_recovery_exceptions SET status = ?, retry_at_ms = ?, " +
          "updated_at_ms = ?, resolved_at_ms = NULL, dead_lettered_at_ms = NULL, " +
          "redrive_from_attempt_count = ?, redrive_retry_delay_ms = ? " +
          "WHERE exception_key = ? AND status = ? AND attempt_count = ?",
        targetStatus,
        nowMs + retryDelay,
        nowMs,
        expectedAttempt,
        retryDelay,
        exceptionKey,
        sourceStatus,
        expectedAttempt,
      );
      if (updated.rowsWritten < 1) {
        return conflict(
          "recovery_exception_conflict",
          `recovery exception ${exceptionKey} changed before redrive`,
        );
      }
      return json(
        recoveryExceptionView(
          this.sql
            .exec(
              "SELECT * FROM fleet_recovery_exceptions WHERE exception_key = ?",
              exceptionKey,
            )
            .toArray()[0] as Record<string, unknown>,
        ),
      );
    }

    return json({ error: "bad_route" }, 404);
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const parts = url.pathname.split("/").filter(Boolean); // ns,:namespace,w,:world,...
    const route = parts.slice(4);
    const method = request.method;
    const now = new Date().toISOString();
    const worldId = decodeURIComponent(parts[3]);

    const artifactServerClockRoute =
      route[0] === "artifact-publications" &&
      (
        (route.length === 2 && ["acquire-v3", "due-v1"].includes(route[1])) ||
        (route.length === 3 && ["recover-v1", "fail-v3"].includes(route[2]))
      );
    if (artifactServerClockRoute) {
      try {
        return await this.handleArtifactServerClock(request, url, route, worldId);
      } catch (error) {
        if (error instanceof RecoveryInputError) {
          return json({ error: "invalid", message: error.message }, 400);
        }
        throw error;
      }
    }

    const legacyArtifactClockRoute =
      route[0] === "artifact-publications" &&
      (
        (route.length === 1 && method === "GET") ||
        (route.length === 2 && ["acquire", "acquire-v2"].includes(route[1])) ||
        (route.length === 3 && ["fail", "fail-v2"].includes(route[2]))
      );
    if (legacyArtifactClockRoute) {
      if (method === "POST") await request.arrayBuffer();
      return json(
        {
          error: "upgrade_required",
          message: "artifact publication clock authority requires the v3/v1 routes",
        },
        426,
      );
    }

    if (route[0] === "recovery") {
      try {
        return await this.handleRecovery(request, url, route, worldId);
      } catch (error) {
        if (error instanceof RecoveryInputError) {
          return json({ error: "invalid", message: error.message }, 400);
        }
        throw error;
      }
    }

    if (
      route[0] === "attempt-claims" &&
      (route[1] === "acquire" || route[1] === "acquire-v2") &&
      method === "POST"
    ) {
      const p = (await request.json()) as Record<string, unknown>;
      const nowSec = Date.now() / 1000;
      const lease = Number(p.lease_seconds ?? 900);
      if (lease < 0) {
        return json({ error: "invalid", message: "lease_seconds must be non-negative" }, 400);
      }
      const redactionPolicyId = String(p.redaction_policy_id ?? "");
      const redactionEvidence = String(p.redaction_evidence_json ?? "");
      if (!redactionPolicyId.trim()) {
        return json(
          { error: "invalid", message: "redaction_policy_id must not be empty" },
          400,
        );
      }
      if (!redactionEvidence.trim()) {
        return json(
          { error: "invalid", message: "redaction_evidence_json must not be empty" },
          400,
        );
      }
      const existing = this.sql
        .exec("SELECT * FROM mission_attempt_claims WHERE claim_key = ?", p.claim_key)
        .toArray();
      if (existing.length > 0) {
        const row = existing[0] as Record<string, unknown>;
        const immutableMatches =
          row.run_id === p.run_id &&
          row.mission_id === p.mission_id &&
          row.task_id === p.task_id &&
          row.attempt_id === p.attempt_id &&
          row.idempotency_key === p.idempotency_key &&
          row.request_fingerprint === p.request_fingerprint &&
          row.request_json === p.request_json &&
          row.redaction_policy_id === redactionPolicyId &&
          row.redaction_acquisition_evidence_json === redactionEvidence &&
          row.provider === p.provider &&
          row.provider_request_fingerprint === p.provider_request_fingerprint &&
          Boolean(row.supports_idempotent_replay) === Boolean(p.supports_idempotent_replay) &&
          Boolean(row.supports_session_resume) === Boolean(p.supports_session_resume) &&
          row.provider_idempotency_key === (p.provider_idempotency_key ?? "");
        if (!immutableMatches) {
          return conflict(
            "attempt_claim_conflict",
            `attempt claim ${p.claim_key} was reused with different immutable input`,
          );
        }
        if (row.status === "settled") {
          return json({ outcome: "duplicate", claim: attemptClaimView(row) });
        }
        if (Number(row.lease_expires_at) > nowSec) {
          if (row.claimant === p.claimant) {
            return json({ outcome: "owned", claim: attemptClaimView(row) });
          }
          return json(
            { error: "attempt_claim_pending", message: "a live attempt lease exists" },
            423,
          );
        }
        this.sql.exec(
          "UPDATE mission_attempt_claims SET claimant = ?, lease_expires_at = ?, " +
            "fence_epoch = fence_epoch + 1, updated_at = ? WHERE claim_key = ?",
          p.claimant,
          nowSec + lease,
          now,
          p.claim_key,
        );
        const recovered = this.sql
          .exec("SELECT * FROM mission_attempt_claims WHERE claim_key = ?", p.claim_key)
          .toArray();
        return json({
          outcome: "recovered",
          claim: attemptClaimView(recovered[0] as Record<string, unknown>),
        });
      }
      const identity = this.sql
        .exec(
          "SELECT claim_key FROM mission_attempt_claims " +
            "WHERE mission_id = ? AND task_id = ? AND attempt_id = ?",
          p.mission_id,
          p.task_id,
          p.attempt_id,
        )
        .toArray();
      if (identity.length > 0) {
        return conflict(
          "attempt_claim_conflict",
          `attempt identity ${p.mission_id}/${p.task_id}/${p.attempt_id} ` +
            `already belongs to claim ${identity[0].claim_key}`,
        );
      }
      try {
        this.sql.exec(
          "INSERT INTO mission_attempt_claims (claim_key, run_id, mission_id, task_id, " +
            "attempt_id, idempotency_key, request_fingerprint, request_json, " +
            "redaction_policy_id, redaction_acquisition_evidence_json, " +
            "redaction_evidence_json, status, " +
            "provider, provider_request_fingerprint, supports_idempotent_replay, " +
            "supports_session_resume, provider_idempotency_key, claimant, lease_expires_at, " +
            "fence_epoch, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
            "'claimed', ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
          p.claim_key,
          p.run_id,
          p.mission_id,
          p.task_id,
          p.attempt_id,
          p.idempotency_key,
          p.request_fingerprint,
          p.request_json,
          redactionPolicyId,
          redactionEvidence,
          redactionEvidence,
          p.provider,
          p.provider_request_fingerprint,
          p.supports_idempotent_replay ? 1 : 0,
          p.supports_session_resume ? 1 : 0,
          p.provider_idempotency_key ?? "",
          p.claimant,
          nowSec + lease,
          now,
          now,
        );
      } catch (error) {
        const collision = this.sql
          .exec(
            "SELECT claim_key FROM mission_attempt_claims " +
              "WHERE mission_id = ? AND task_id = ? AND attempt_id = ?",
            p.mission_id,
            p.task_id,
            p.attempt_id,
          )
          .toArray();
        if (collision.length > 0) {
          return conflict(
            "attempt_claim_conflict",
            `attempt identity ${p.mission_id}/${p.task_id}/${p.attempt_id} ` +
              `already belongs to claim ${collision[0].claim_key}`,
          );
        }
        throw error;
      }
      const created = this.sql
        .exec("SELECT * FROM mission_attempt_claims WHERE claim_key = ?", p.claim_key)
        .toArray();
      return json({
        outcome: "acquired",
        claim: attemptClaimView(created[0] as Record<string, unknown>),
      });
    }

    if (route[0] === "attempt-claims" && route.length === 1 && method === "GET") {
      const due = Number(url.searchParams.get("due") ?? Date.now() / 1000);
      const limit = Math.max(0, Number(url.searchParams.get("limit") ?? 100));
      return json(
        this.sql
          .exec(
            "SELECT * FROM mission_attempt_claims WHERE status != 'settled' " +
              "AND lease_expires_at <= ? ORDER BY lease_expires_at, claim_key LIMIT ?",
            due,
            limit,
          )
          .toArray()
          .map((row) => attemptClaimView(row as Record<string, unknown>)),
      );
    }

    if (route[0] === "attempt-claims" && route.length >= 2) {
      const claimKey = route[1];
      if (route.length === 2 && method === "GET") {
        const rows = this.sql
          .exec("SELECT * FROM mission_attempt_claims WHERE claim_key = ?", claimKey)
          .toArray();
        return rows.length
          ? json(attemptClaimView(rows[0] as Record<string, unknown>))
          : json({ error: "not_found" }, 404);
      }

      if (route.length === 3 && route[2] === "renew" && method === "POST") {
        const p = (await request.json()) as Record<string, unknown>;
        const lease = Number(p.lease_seconds ?? 0);
        if (lease <= 0) {
          return json({ error: "invalid", message: "lease_seconds must be positive" }, 400);
        }
        const rows = this.sql
          .exec("SELECT * FROM mission_attempt_claims WHERE claim_key = ?", claimKey)
          .toArray();
        if (!rows.length) {
          return conflict("attempt_claim_conflict", `no attempt claim ${claimKey} exists`);
        }
        const row = rows[0] as Record<string, unknown>;
        const nowSec = Date.now() / 1000;
        if (
          row.claimant !== p.claimant ||
          Number(row.fence_epoch) !== Number(p.fence_epoch) ||
          row.status === "settled" ||
          Number(row.lease_expires_at) <= nowSec
        ) {
          return json(
            { error: "attempt_claim_stale", message: "attempt claim fence is stale" },
            412,
          );
        }
        const renewed = this.sql.exec(
          "UPDATE mission_attempt_claims SET lease_expires_at = ?, updated_at = ? " +
            "WHERE claim_key = ? AND claimant = ? AND fence_epoch = ? " +
            "AND status != 'settled' AND lease_expires_at > ?",
          nowSec + lease,
          now,
          claimKey,
          p.claimant,
          p.fence_epoch,
          nowSec,
        );
        if (renewed.rowsWritten < 1) {
          return json(
            { error: "attempt_claim_stale", message: "attempt claim lease expired" },
            412,
          );
        }
        const updated = this.sql
          .exec("SELECT * FROM mission_attempt_claims WHERE claim_key = ?", claimKey)
          .toArray();
        return json(attemptClaimView(updated[0] as Record<string, unknown>));
      }

      if (
        route.length === 3 &&
        (route[2] === "consume" || route[2] === "consume-v2") &&
        method === "POST"
      ) {
        const p = (await request.json()) as Record<string, unknown>;
        const executionNonce = String(p.execution_nonce ?? "");
        if (!executionNonce) {
          return json(
            { error: "invalid", message: "attempt execution nonce must not be empty" },
            400,
          );
        }
        const consumedAt = new Date().toISOString();
        const consumed = this.sql.exec(
          "UPDATE mission_attempt_claims SET execution_consumed_at = ?, updated_at = ? " +
            "WHERE claim_key = ? AND status = 'possibly_submitted' AND claimant = ? " +
            "AND fence_epoch = ? AND execution_nonce = ? " +
            "AND execution_consumed_at IS NULL AND lease_expires_at > ?",
          consumedAt,
          consumedAt,
          claimKey,
          p.claimant,
          p.fence_epoch,
          executionNonce,
          Date.now() / 1000,
        );
        if (consumed.rowsWritten < 1) {
          return json(
            {
              error: "attempt_claim_stale",
              message: `attempt execution grant ${claimKey} is stale or already consumed`,
            },
            412,
          );
        }
        const updated = this.sql
          .exec("SELECT * FROM mission_attempt_claims WHERE claim_key = ?", claimKey)
          .toArray();
        return json(attemptClaimView(updated[0] as Record<string, unknown>));
      }

      if (
        route.length === 3 &&
        (route[2] === "transition" || route[2] === "transition-v2") &&
        method === "POST"
      ) {
        const p = (await request.json()) as Record<string, unknown>;
        const target = String(p.target_status ?? "");
        const expected = String(p.expected_status ?? "");
        if (!ATTEMPT_CLAIM_EDGES.has(`${expected}->${target}`)) {
          return json(
            {
              error: "invalid",
              message: `illegal attempt claim transition: ${expected} to ${target}`,
            },
            400,
          );
        }
        const executionNonce = String(p.execution_nonce ?? "");
        if (target === "possibly_submitted" && !executionNonce) {
          return json(
            { error: "invalid", message: "arming submission requires an execution nonce" },
            400,
          );
        }
        if (executionNonce && target !== "possibly_submitted") {
          return json(
            {
              error: "invalid",
              message: "execution nonce may only be recorded while arming submission",
            },
            400,
          );
        }
        const redactionEvidence = String(p.redaction_evidence_json ?? "");
        if (redactionEvidence && !redactionEvidence.trim()) {
          return json(
            { error: "invalid", message: "redaction evidence update must not be blank" },
            400,
          );
        }
        const artifactRequestJson = String(p.artifact_request_json ?? "");
        const artifactRequestDigest = String(p.artifact_request_digest ?? "");
        const artifactPublicationKey = String(p.artifact_publication_key ?? "");
        const settlementStatus = String(p.settlement_status ?? "");
        const outcomeDigest = String(p.outcome_digest ?? "");
        const outcomeJson = String(p.outcome_json ?? "");
        const providerSessionId = String(p.provider_session_id ?? "");
        const providerRequestId = String(p.provider_request_id ?? "");
        const lastError = String(p.last_error ?? "");
        const stagedArtifact = [
          artifactRequestJson,
          artifactRequestDigest,
          artifactPublicationKey,
        ];
        const terminalEvidence = [
          settlementStatus,
          outcomeDigest,
          outcomeJson,
          ...stagedArtifact,
          lastError,
        ];
        if (target === "possibly_submitted" || target === "provider_acknowledged") {
          if (terminalEvidence.some((value) => value)) {
            return json(
              {
                error: "invalid",
                message: "non-terminal transition may not record terminal evidence",
              },
              400,
            );
          }
          if (target === "provider_acknowledged") {
            if (!redactionEvidence.trim()) {
              return json(
                {
                  error: "invalid",
                  message: "provider acknowledgement requires redaction evidence",
                },
                400,
              );
            }
            if (!providerSessionId.trim() && !providerRequestId.trim()) {
              return json(
                {
                  error: "invalid",
                  message: "provider acknowledgement requires a provider identity",
                },
                400,
              );
            }
          } else if (providerSessionId || providerRequestId) {
            return json(
              {
                error: "invalid",
                message: "provider identity may only be recorded during provider acknowledgement",
              },
              400,
            );
          }
        } else if (providerSessionId || providerRequestId) {
          return json(
            {
              error: "invalid",
              message: "provider identity may only be recorded during provider acknowledgement",
            },
            400,
          );
        }
        if (target === "finalizing") {
          if (!redactionEvidence.trim()) {
            return json(
              { error: "invalid", message: "entering finalizing requires redaction evidence" },
              400,
            );
          }
          if (settlementStatus || lastError) {
            return json(
              {
                error: "invalid",
                message: "terminal settlement evidence may only be recorded while settling",
              },
              400,
            );
          }
          if (!stagedArtifact.every((value) => value.trim())) {
            return json(
              {
                error: "invalid",
                message: "entering finalizing requires a complete artifact request",
              },
              400,
            );
          }
          if (!outcomeDigest.trim() || !outcomeJson.trim()) {
            return json(
              {
                error: "invalid",
                message: "entering finalizing requires a complete durable outcome",
              },
              400,
            );
          }
        } else if (target === "settled") {
          const missing = [
            ["redaction_evidence_json", redactionEvidence],
            ["settlement_status", settlementStatus],
            ["outcome_digest", outcomeDigest],
            ["outcome_json", outcomeJson],
          ]
            .filter(([, value]) => !value.trim())
            .map(([name]) => name);
          if (missing.length) {
            return json(
              {
                error: "invalid",
                message: `entering settled requires complete terminal evidence: ${missing.join(", ")}`,
              },
              400,
            );
          }
          if (
            stagedArtifact.some((value) => value) &&
            !stagedArtifact.every((value) => value.trim())
          ) {
            return json(
              {
                error: "invalid",
                message: "settlement artifact evidence must be complete when supplied",
              },
              400,
            );
          }
          if (stagedArtifact.some((value) => value) && expected !== "finalizing") {
            return json(
              {
                error: "invalid",
                message: "artifact request evidence may only be recorded while finalizing",
              },
              400,
            );
          }
        } else if (stagedArtifact.some((value) => value)) {
          return json(
            {
              error: "invalid",
              message: "artifact request evidence may only be recorded while finalizing",
            },
            400,
          );
        }
        const rows = this.sql
          .exec("SELECT * FROM mission_attempt_claims WHERE claim_key = ?", claimKey)
          .toArray();
        if (!rows.length) {
          return conflict("attempt_claim_conflict", `no attempt claim ${claimKey} exists`);
        }
        const row = rows[0] as Record<string, unknown>;
        const nowSec = Date.now() / 1000;
        if (
          row.claimant !== p.claimant ||
          Number(row.fence_epoch) !== Number(p.fence_epoch)
        ) {
          return json(
            { error: "attempt_claim_stale", message: "attempt claim fence is stale" },
            412,
          );
        }
        if (
          row.status !== "settled" &&
          target === "settled" &&
          settlementStatus === "accepted" &&
          isUnboundLegacyIndexedClaim(row)
        ) {
          return conflict(
            "attempt_claim_conflict",
            `legacy indexed attempt claim ${claimKey} must bind artifact authority before settlement`,
          );
        }
        const replayMatches = attemptClaimTransitionReplayMatches(row, p);
        if (row.status === "settled") {
          if (target === "settled" && replayMatches) {
            return json(attemptClaimView(row));
          }
          return json(
            { error: "attempt_claim_stale", message: "settled attempt claim cannot be mutated" },
            412,
          );
        }
        if (Number(row.lease_expires_at) <= nowSec) {
          return json(
            { error: "attempt_claim_stale", message: "attempt claim lease expired" },
            412,
          );
        }
        if (row.status !== expected) {
          if (row.status === "finalizing" && target === "finalizing" && replayMatches) {
            return json(attemptClaimView(row));
          }
          return conflict(
            "attempt_claim_conflict",
            `attempt claim ${claimKey} is ${row.status}, expected ${expected}`,
          );
        }
        if (expected === "finalizing" && target === "settled") {
          if (
            !String(row.artifact_request_json ?? "") ||
            !String(row.artifact_request_digest ?? "") ||
            !String(row.artifact_publication_key ?? "")
          ) {
            return conflict(
              "attempt_claim_conflict",
              `attempt claim ${claimKey} has incomplete finalization evidence`,
            );
          }
          if (
            outcomeDigest === String(row.outcome_digest ?? "") ||
            outcomeJson === String(row.outcome_json ?? "")
          ) {
            return conflict(
              "attempt_claim_conflict",
              `attempt claim ${claimKey} settlement must replace provisional outcome`,
            );
          }
        }
        for (const field of [
          "artifact_request_json",
          "artifact_request_digest",
          "artifact_publication_key",
        ]) {
          const supplied = String(p[field] ?? "");
          const recorded = String(row[field] ?? "");
          if (supplied && recorded && supplied !== recorded) {
            return conflict(
              "attempt_claim_conflict",
              `attempt claim ${claimKey} immutable ${field} changed`,
            );
          }
        }
        const possiblySubmittedAt = target === "possibly_submitted" ? now : null;
        const acknowledgedAt = target === "provider_acknowledged" ? now : null;
        const finalizingAt = target === "finalizing" ? now : null;
        const settledAt = target === "settled" ? now : null;
        const transition = this.sql.exec(
          "UPDATE mission_attempt_claims SET status = ?, " +
            "execution_nonce = CASE WHEN ? = '' THEN execution_nonce ELSE ? END, " +
            "redaction_evidence_json = CASE WHEN ? = '' " +
            "THEN redaction_evidence_json ELSE ? END, " +
            "provider_session_id = CASE WHEN ? = '' THEN provider_session_id ELSE ? END, " +
            "provider_request_id = CASE WHEN ? = '' THEN provider_request_id ELSE ? END, " +
            "settlement_status = CASE WHEN ? = '' THEN settlement_status ELSE ? END, " +
            "outcome_digest = CASE WHEN ? = '' THEN outcome_digest ELSE ? END, " +
            "outcome_json = CASE WHEN ? = '' THEN outcome_json ELSE ? END, " +
            "artifact_request_json = CASE WHEN ? = '' THEN artifact_request_json ELSE ? END, " +
            "artifact_request_digest = CASE WHEN ? = '' " +
            "THEN artifact_request_digest ELSE ? END, " +
            "artifact_publication_key = CASE WHEN ? = '' " +
            "THEN artifact_publication_key ELSE ? END, " +
            "last_error = CASE WHEN ? = 'settled' THEN ? " +
            "WHEN ? = '' THEN last_error ELSE ? END, updated_at = ?, " +
            "possibly_submitted_at = COALESCE(possibly_submitted_at, ?), " +
            "acknowledged_at = COALESCE(acknowledged_at, ?), " +
            "finalizing_at = COALESCE(finalizing_at, ?), " +
            "settled_at = COALESCE(settled_at, ?) WHERE claim_key = ? " +
            "AND status = ? AND status != 'settled' AND claimant = ? " +
            "AND fence_epoch = ? AND lease_expires_at > ?",
          target,
          executionNonce,
          executionNonce,
          redactionEvidence,
          redactionEvidence,
          p.provider_session_id ?? "",
          p.provider_session_id ?? "",
          p.provider_request_id ?? "",
          p.provider_request_id ?? "",
          p.settlement_status ?? "",
          p.settlement_status ?? "",
          p.outcome_digest ?? "",
          p.outcome_digest ?? "",
          p.outcome_json ?? "",
          p.outcome_json ?? "",
          artifactRequestJson,
          artifactRequestJson,
          artifactRequestDigest,
          artifactRequestDigest,
          artifactPublicationKey,
          artifactPublicationKey,
          target,
          p.last_error ?? "",
          p.last_error ?? "",
          p.last_error ?? "",
          now,
          possiblySubmittedAt,
          acknowledgedAt,
          finalizingAt,
          settledAt,
          claimKey,
          expected,
          p.claimant,
          p.fence_epoch,
          nowSec,
        );
        // SqlStorage counts index maintenance as writes, so one logical row
        // may report more than one write when unique indexes are present.
        if (transition.rowsWritten < 1) {
          return conflict(
            "attempt_claim_conflict",
            `attempt claim ${claimKey} changed during transition from ` +
              `${p.expected_status} to ${p.target_status}`,
          );
        }
        const updated = this.sql
          .exec("SELECT * FROM mission_attempt_claims WHERE claim_key = ?", claimKey)
          .toArray();
        return json(attemptClaimView(updated[0] as Record<string, unknown>));
      }
    }

    if (
      route[0] === "artifact-publications" &&
      (route[1] === "acquire" || route[1] === "acquire-v2") &&
      method === "POST"
    ) {
      const p = (await request.json()) as Record<string, unknown>;
      const nowSec = Date.now() / 1000;
      const lease = Number(p.lease_seconds ?? 900);
      const existing = this.sql
        .exec("SELECT * FROM artifact_publications WHERE publication_key = ?", p.publication_key)
        .toArray();
      if (existing.length > 0) {
        const row = existing[0] as Record<string, unknown>;
        if (row.request_digest !== p.request_digest) {
          return conflict(
            "artifact_publication_conflict",
            `${p.idempotency_key} was reused with a different publication request`,
          );
        }
        if (row.status === "INDEXED") {
          return json({ outcome: "duplicate", publication: artifactPublicationView(row) });
        }
        if (row.status === "EXPIRED") {
          return json({ outcome: "expired", publication: artifactPublicationView(row) });
        }
        if (Number(row.lease_expires_at) > nowSec) {
          return json(
            { error: "artifact_publication_pending", message: "a live publication lease exists" },
            423,
          );
        }
        this.sql.exec(
          "UPDATE artifact_publications SET claimant = ?, lease_expires_at = ?, " +
            "attempt_count = attempt_count + 1, updated_at = ? WHERE publication_key = ?",
          p.claimant,
          nowSec + lease,
          now,
          p.publication_key,
        );
        const updated = this.sql
          .exec("SELECT * FROM artifact_publications WHERE publication_key = ?", p.publication_key)
          .toArray();
        return json({
          outcome: "recovered",
          publication: artifactPublicationView(updated[0] as Record<string, unknown>),
        });
      }
      this.sql.exec(
        "INSERT INTO artifact_publications (publication_key, run_id, attempt_id, " +
          "idempotency_key, request_digest, status, request_json, records_json, claimant, " +
          "lease_expires_at, retry_until_ms, attempt_count, created_at, updated_at) " +
          "VALUES (?, ?, ?, ?, ?, 'PENDING', ?, '[]', ?, ?, ?, 1, ?, ?)",
        p.publication_key,
        p.run_id,
        p.attempt_id,
        p.idempotency_key,
        p.request_digest,
        p.request_json,
        p.claimant,
        nowSec + lease,
        p.retry_until_ms,
        now,
        now,
      );
      const created = this.sql
        .exec("SELECT * FROM artifact_publications WHERE publication_key = ?", p.publication_key)
        .toArray();
      return json({
        outcome: "acquired",
        publication: artifactPublicationView(created[0] as Record<string, unknown>),
      });
    }

    if (route[0] === "artifact-publications" && route.length >= 2) {
      const key = route[1];
      const rows = this.sql
        .exec("SELECT * FROM artifact_publications WHERE publication_key = ?", key)
        .toArray();
      if (!rows.length) {
        // Match SqliteControlCatalog's per-operation missing-row contracts.
        // Failure release is deliberately idempotent; every other lifecycle
        // mutation requires a publication to exist.
        if (route.length === 3 && method === "POST") {
          // A Durable Object must consume the incoming body before returning;
          // otherwise workerd reports an uncaught stream error after sending
          // this response and a subsequent request can receive a spurious 503.
          await request.arrayBuffer();
          if (route[2] === "fail" || route[2] === "fail-v2") return json({ ok: true });
          if (
            [
              "renew",
              "renew-v2",
              "uploads",
              "uploads-v2",
              "complete",
              "complete-v2",
              "expire",
              "expire-v2",
            ].includes(route[2])
          ) {
            return conflict(
              "artifact_publication_conflict",
              `no artifact publication recorded for ${key}`,
            );
          }
        }
        return json({ error: "not_found" }, 404);
      }
      const row = rows[0] as Record<string, unknown>;
      const authorityError = artifactPublicationAuthorityError(row, key);
      if (authorityError) return authorityError;

      if (route.length === 2 && method === "GET") return json(artifactPublicationView(row));

      if ((route[2] === "renew" || route[2] === "renew-v2") && method === "POST") {
        let claimant: string;
        let leaseSeconds: number;
        try {
          const body = recoveryObject(await request.json(), "artifact renewal body");
          if (route[2] === "renew-v2") {
            recoveryExactFields(
              body,
              new Set(["claimant", "lease_seconds"]),
              "artifact renewal body",
            );
          }
          claimant = recoveryText(
            body.claimant,
            "artifact publication claimant",
            1024,
          );
          if (
            typeof body.lease_seconds !== "number" ||
            !Number.isFinite(body.lease_seconds) ||
            body.lease_seconds <= 0 ||
            body.lease_seconds > MAX_ARTIFACT_LEASE_MS / 1000
          ) {
            throw new RecoveryInputError(
              "artifact lease_seconds must be a finite positive bounded number",
            );
          }
          leaseSeconds = body.lease_seconds;
        } catch (error) {
          if (error instanceof RecoveryInputError) {
            return json({ error: "invalid", message: error.message }, 400);
          }
          throw error;
        }
        if (row.status === "INDEXED" || row.status === "EXPIRED") {
          return json(artifactPublicationView(row));
        }
        if (row.claimant !== claimant) {
          return json(
            { error: "artifact_publication_pending", message: "publication was taken over" },
            423,
          );
        }
        const nowMs = Date.now();
        const nowText = new Date(nowMs).toISOString();
        if (row.status === "PENDING" && Number(row.retry_until_ms) <= nowMs) {
          this.sql.exec(
            "UPDATE artifact_publications SET status = 'EXPIRED', lease_expires_at = 0, " +
              "last_error = ?, updated_at = ?, completed_at = ? " +
              "WHERE publication_key = ? AND status = 'PENDING'",
            ARTIFACT_RETRY_EXPIRED_DETAIL,
            nowText,
            nowText,
            key,
          );
          const expired = this.sql
            .exec("SELECT * FROM artifact_publications WHERE publication_key = ?", key)
            .toArray();
          return json(artifactPublicationView(expired[0] as Record<string, unknown>));
        }
        if (Number(row.lease_expires_at) <= nowMs / 1000) {
          return json(
            {
              error: "artifact_publication_pending",
              message: "publication lease expired before renewal",
            },
            423,
          );
        }
        this.sql.exec(
          "UPDATE artifact_publications SET lease_expires_at = ?, updated_at = ? " +
            "WHERE publication_key = ?",
          nowMs / 1000 + leaseSeconds,
          nowText,
          key,
        );
        const updated = this.sql
          .exec("SELECT * FROM artifact_publications WHERE publication_key = ?", key)
          .toArray();
        return json(artifactPublicationView(updated[0] as Record<string, unknown>));
      }

      if ((route[2] === "uploads" || route[2] === "uploads-v2") && method === "POST") {
        let claimant: string;
        let recordsJson: string;
        let manifestUri: string;
        try {
          const body = recoveryObject(await request.json(), "artifact uploads body");
          if (route[2] === "uploads-v2") {
            recoveryExactFields(
              body,
              new Set(["claimant", "records_json", "manifest_uri"]),
              "artifact uploads body",
            );
          }
          claimant = recoveryText(
            body.claimant,
            "artifact publication claimant",
            1024,
          );
          recordsJson = recoveryText(
            body.records_json,
            "artifact records_json",
            16 * 1024 * 1024,
            true,
          );
          manifestUri = recoveryText(
            body.manifest_uri,
            "artifact manifest_uri",
            16 * 1024,
            true,
          );
        } catch (error) {
          if (error instanceof RecoveryInputError) {
            return json({ error: "invalid", message: error.message }, 400);
          }
          throw error;
        }
        if (row.status === "INDEXED") return json({ ok: true, idempotent: true });
        if (row.status === "EXPIRED") {
          return conflict("artifact_publication_expired", `publication ${key} expired`);
        }
        if (row.claimant !== claimant) {
          return json(
            { error: "artifact_publication_pending", message: "publication was taken over" },
            423,
          );
        }
        if (row.status === "UPLOADED") {
          if (row.records_json === recordsJson && row.manifest_uri === manifestUri) {
            return json({ ok: true, idempotent: true });
          }
          return conflict("artifact_publication_conflict", "different uploads already recorded");
        }
        const nowMs = Date.now();
        const nowText = new Date(nowMs).toISOString();
        if (Number(row.retry_until_ms) <= nowMs) {
          this.sql.exec(
            "UPDATE artifact_publications SET status = 'EXPIRED', lease_expires_at = 0, " +
              "last_error = ?, updated_at = ?, completed_at = ? " +
              "WHERE publication_key = ? AND status = 'PENDING'",
            ARTIFACT_RETRY_EXPIRED_DETAIL,
            nowText,
            nowText,
            key,
          );
          return conflict("artifact_publication_expired", `publication ${key} expired`);
        }
        if (Number(row.lease_expires_at) <= nowMs / 1000) {
          return json(
            {
              error: "artifact_publication_pending",
              message: "publication lease expired before uploads",
            },
            423,
          );
        }
        this.sql.exec(
          "UPDATE artifact_publications SET status = 'UPLOADED', records_json = ?, " +
            "manifest_uri = ?, last_error = '', updated_at = ? WHERE publication_key = ?",
          recordsJson,
          manifestUri,
          nowText,
          key,
        );
        return json({ ok: true });
      }

      if ((route[2] === "complete" || route[2] === "complete-v2") && method === "POST") {
        let body: Record<string, unknown>;
        let claimant: string;
        try {
          body = recoveryObject(await request.json(), "artifact completion body");
          if (route[2] === "complete-v2") {
            recoveryExactFields(
              body,
              new Set(["claimant", "index_snapshot_id"]),
              "artifact completion body",
            );
          }
          claimant = recoveryText(
            body.claimant,
            "artifact publication claimant",
            1024,
          );
        } catch (error) {
          if (error instanceof RecoveryInputError) {
            return json({ error: "invalid", message: error.message }, 400);
          }
          throw error;
        }
        let snapshotId: string;
        if (route[2] === "complete-v2") {
          const supplied = body.index_snapshot_id;
          if (
            typeof supplied !== "string" ||
            !/^[1-9][0-9]{0,18}$/.test(supplied) ||
            BigInt(supplied) > MAX_SIGNED_64_BIT
          ) {
            return json(
              {
                error: "invalid",
                message:
                  "index_snapshot_id must be canonical positive signed-64-bit decimal text",
              },
              400,
            );
          }
          snapshotId = supplied;
        } else {
          const supplied = body.index_snapshot_id;
          if (
            typeof supplied !== "number" ||
            !Number.isSafeInteger(supplied) ||
            supplied <= 0
          ) {
            return json(
              {
                error: "invalid",
                message: "legacy index_snapshot_id must be a positive safe integer",
              },
              400,
            );
          }
          snapshotId = String(supplied);
        }
        if (row.status === "INDEXED") {
          if (String(row.index_snapshot_id_text ?? "0") === snapshotId) {
            return json({ ok: true, idempotent: true });
          }
          return conflict(
            "artifact_publication_conflict",
            `publication ${key} was indexed at snapshot ${row.index_snapshot_id_text}, ` +
              `not ${snapshotId}`,
          );
        }
        if (row.status !== "UPLOADED") {
          return conflict(
            "artifact_publication_conflict",
            `publication ${key} cannot move from ${row.status} to INDEXED`,
          );
        }
        if (row.claimant !== claimant) {
          return json(
            { error: "artifact_publication_pending", message: "publication was taken over" },
            423,
          );
        }
        const nowMs = Date.now();
        if (Number(row.lease_expires_at) <= nowMs / 1000) {
          return json(
            {
              error: "artifact_publication_pending",
              message: "publication lease expired before completion",
            },
            423,
          );
        }
        const completedAt = new Date(nowMs).toISOString();
        this.sql.exec(
          "UPDATE artifact_publications SET status = 'INDEXED', index_snapshot_id_text = ?, " +
            "last_error = '', updated_at = ?, completed_at = ? WHERE publication_key = ?",
          snapshotId,
          completedAt,
          completedAt,
          key,
        );
        return json({ ok: true });
      }

      if ((route[2] === "fail" || route[2] === "fail-v2") && method === "POST") {
        const body = (await request.json()) as {
          claimant: string;
          error: string;
          retry_at: number;
        };
        if (row.status === "INDEXED" || row.status === "EXPIRED") return json({ ok: true });
        if (row.claimant !== body.claimant) {
          return json(
            { error: "artifact_publication_pending", message: "publication was taken over" },
            423,
          );
        }
        this.sql.exec(
          "UPDATE artifact_publications SET last_error = ?, lease_expires_at = ?, " +
            "updated_at = ? WHERE publication_key = ?",
          String(body.error).slice(0, 8000),
          body.retry_at,
          now,
          key,
        );
        return json({ ok: true });
      }

      if ((route[2] === "expire" || route[2] === "expire-v2") && method === "POST") {
        let claimant: string;
        let errorDetail: string;
        try {
          const body = recoveryObject(await request.json(), "artifact expiry body");
          if (route[2] === "expire-v2") {
            recoveryExactFields(
              body,
              new Set(["claimant", "error"]),
              "artifact expiry body",
            );
          }
          claimant = recoveryText(
            body.claimant,
            "artifact publication claimant",
            1024,
          );
          errorDetail = recoveryText(
            body.error,
            "artifact publication error",
            8000,
            true,
          );
        } catch (error) {
          if (error instanceof RecoveryInputError) {
            return json({ error: "invalid", message: error.message }, 400);
          }
          throw error;
        }
        if (row.status === "INDEXED" || row.status === "EXPIRED") return json({ ok: true });
        if (row.status === "UPLOADED") {
          return conflict(
            "artifact_publication_conflict",
            "uploaded publications must be indexed, not expired",
          );
        }
        if (row.claimant !== claimant) {
          return json(
            { error: "artifact_publication_pending", message: "publication was taken over" },
            423,
          );
        }
        const nowMs = Date.now();
        if (Number(row.lease_expires_at) <= nowMs / 1000) {
          return json(
            {
              error: "artifact_publication_pending",
              message: "publication lease expired before expiry",
            },
            423,
          );
        }
        const completedAt = new Date(nowMs).toISOString();
        this.sql.exec(
          "UPDATE artifact_publications SET status = 'EXPIRED', last_error = ?, " +
            "updated_at = ?, completed_at = ? WHERE publication_key = ?",
          errorDetail,
          completedAt,
          completedAt,
          key,
        );
        return json({ ok: true });
      }
    }

    if (route[0] === "status" && route.length === 1) {
      if (method === "GET") {
        try {
          recoveryExactQuery(
            url,
            RECOVERY_NO_QUERY_FIELDS,
            "world protocol status query",
          );
        } catch (error) {
          if (error instanceof RecoveryInputError) {
            return json({ error: "invalid", message: error.message }, 400);
          }
          throw error;
        }
        const rows = this.sql
          .exec("SELECT status FROM world_state WHERE singleton = 1")
          .toArray();
        const protocol = {
          catalog_protocol_version: CATALOG_PROTOCOL_VERSION,
          capabilities: [
            ATTEMPT_FINALIZATION_CAPABILITY,
            ATTEMPT_CLAIM_CAPABILITY,
            ARTIFACT_SNAPSHOT_CAPABILITY,
            ARTIFACT_SERVER_CLOCK_CAPABILITY,
            FLEET_RECOVERY_CAPABILITY,
          ],
        };
        return rows.length
          ? json({ ...rows[0], ...protocol })
          : json({ error: "not_found", ...protocol }, 404);
      }
      if (method === "PATCH") {
        const body = (await request.json()) as { status?: unknown };
        if (body.status !== "active" && body.status !== "destroyed") {
          return json(
            { error: "invalid_request", message: "status must be active or destroyed" },
            422,
          );
        }
        const result = this.state.storage.transactionSync(() => {
          this.sql.exec(
            "INSERT INTO world_state (singleton, status) VALUES (1, ?) " +
              "ON CONFLICT(singleton) DO UPDATE SET status = excluded.status",
            body.status,
          );
          const cancelled =
            body.status === "active"
              ? 0
              : rejectUnsettledCommands(
                  this.sql,
                  `world transitioned to ${body.status}`,
                  now,
                );
          return { ok: true, status: body.status, cancelled };
        });
        return json(result);
      }
    }

    if (route[0] === "commands" && route[1] === "admit" && method === "POST") {
      const body = (await request.json()) as { admissions?: Array<Record<string, unknown>> };
      const admissions = body.admissions ?? [];
      if (admissions.length === 0) return json([]);
      try {
        const records = this.state.storage.transactionSync(() => {
          requireActiveWorld(this.sql, worldId);
          const seen = new Map<string, string>();
          for (const admission of admissions) {
            const commandId = String(admission.command_id);
            const digest = String(admission.payload_digest);
            const prior = seen.get(commandId);
            if (prior !== undefined && prior !== digest) {
              throw new Error(`command_conflict:command ${commandId} appears twice with different content`);
            }
            seen.set(commandId, digest);
            const existing = this.sql
              .exec("SELECT payload_digest FROM commands WHERE command_id = ?", commandId)
              .toArray();
            if (existing.length && (existing[0] as Record<string, unknown>).payload_digest !== digest) {
              throw new Error(`command_conflict:command ${commandId} already exists with different content`);
            }
          }
          for (const admission of admissions) {
            const commandId = String(admission.command_id);
            const existing = this.sql
              .exec("SELECT 1 FROM commands WHERE command_id = ?", commandId)
              .toArray();
            if (existing.length) continue;
            this.sql.exec(
              "INSERT INTO commands (command_id, scheduled_tick, priority, command_type, payload_json, payload_digest, version, principal_id, origin, reserved_entity_id, status, attempts, max_attempts, accepted_at, updated_at) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', 0, ?, ?, ?)",
              commandId,
              admission.scheduled_tick,
              admission.priority,
              admission.command_type,
              admission.payload_json,
              admission.payload_digest,
              admission.version,
              admission.principal_id ?? null,
              admission.origin,
              admission.reserved_entity_id ?? null,
              admission.max_attempts ?? 3,
              now,
              now,
            );
            appendCommandEvent(
              this.sql,
              admission,
              "queued",
              JSON.stringify({
                origin: admission.origin,
                scheduled_tick: admission.scheduled_tick,
                priority: admission.priority,
              }),
              now,
            );
          }
          return admissions.map((admission) =>
            this.sql
              .exec("SELECT * FROM commands WHERE command_id = ?", admission.command_id)
              .toArray()[0],
          );
        });
        return json(records);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        if (message.startsWith("command_conflict:")) {
          return conflict("command_conflict", message.slice("command_conflict:".length));
        }
        throw error;
      }
    }

    if (route[0] === "commands" && route[1] === "lease" && method === "POST") {
      const body = (await request.json()) as Record<string, unknown>;
      const tick = Number(body.tick ?? 0);
      const owner = String(body.owner ?? "");
      const leaseSeconds = Number(body.lease_seconds ?? 30);
      const limit = Number(body.limit ?? 50000);
      if (!owner || leaseSeconds <= 0 || limit < 1) {
        return json({ error: "invalid_request", message: "invalid lease parameters" }, 422);
      }
      const nowSec = Date.now() / 1000;
      try {
        const records = this.state.storage.transactionSync(() => {
          requireActiveWorld(this.sql, worldId);
          const rows = this.sql
            .exec(
              "SELECT * FROM commands WHERE scheduled_tick <= ? AND (status IN ('PENDING', 'RETRYABLE') OR (status = 'LEASED' AND (lease_owner = ? OR lease_expires_at <= ?))) ORDER BY scheduled_tick, priority, sequence LIMIT ?",
              tick,
              owner,
              nowSec,
              limit,
            )
            .toArray() as Array<Record<string, unknown>>;
          return rows.map((row) => {
            const sameLive =
              row.status === "LEASED" &&
              row.lease_owner === owner &&
              Number(row.lease_expires_at ?? 0) > nowSec;
            const attempts = Number(row.attempts) + (sameLive ? 0 : 1);
            this.sql.exec(
              "UPDATE commands SET status = 'LEASED', attempts = ?, lease_owner = ?, lease_expires_at = ?, updated_at = ? WHERE command_id = ?",
              attempts,
              owner,
              nowSec + leaseSeconds,
              now,
              row.command_id,
            );
            return this.sql
              .exec("SELECT * FROM commands WHERE command_id = ?", row.command_id)
              .toArray()[0];
          });
        });
        return json(records);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        if (message.startsWith("command_conflict:")) {
          return conflict("command_conflict", message.slice("command_conflict:".length));
        }
        throw error;
      }
    }

    if (route[0] === "commands" && route[1] === "release" && method === "POST") {
      const body = (await request.json()) as { command_ids?: string[]; owner?: string };
      this.state.storage.transactionSync(() => {
        for (const commandId of body.command_ids ?? []) {
          this.sql.exec(
            "UPDATE commands SET status = 'PENDING', attempts = MAX(attempts - 1, 0), lease_owner = NULL, lease_expires_at = NULL, updated_at = ? WHERE command_id = ? AND status = 'LEASED' AND lease_owner = ?",
            now,
            commandId,
            body.owner ?? "",
          );
        }
      });
      return json({ ok: true });
    }

    if (route[0] === "commands" && route.length === 3 && route[2] === "fail" && method === "POST") {
      const body = (await request.json()) as Record<string, unknown>;
      const status = String(body.status ?? "");
      if (!["RETRYABLE", "REJECTED", "DEAD_LETTER"].includes(status)) {
        return json({ error: "invalid_request", message: "invalid command status" }, 422);
      }
      try {
        const record = this.state.storage.transactionSync(() => {
          const rows = this.sql
            .exec("SELECT * FROM commands WHERE command_id = ?", route[1])
            .toArray();
          if (!rows.length) throw new Error(`command_conflict:unknown command ${route[1]}`);
          const row = rows[0] as Record<string, unknown>;
          if (["REJECTED", "DEAD_LETTER"].includes(String(row.status))) return row;
          if (row.status !== "LEASED" || row.lease_owner !== body.owner) {
            throw new Error(`command_conflict:command ${route[1]} is not leased by ${body.owner}`);
          }
          this.sql.exec(
            "UPDATE commands SET status = ?, lease_owner = NULL, lease_expires_at = NULL, last_error_code = ?, last_error_detail = ?, updated_at = ? WHERE command_id = ?",
            status,
            body.error_code ?? "Error",
            String(body.error_detail ?? "").slice(0, 2000),
            now,
            route[1],
          );
          appendCommandEvent(
            this.sql,
            row,
            status.toLowerCase(),
            JSON.stringify({
              error_code: body.error_code ?? "Error",
              error_detail: String(body.error_detail ?? "").slice(0, 500),
            }),
            now,
          );
          return this.sql.exec("SELECT * FROM commands WHERE command_id = ?", route[1]).toArray()[0];
        });
        return json(record);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        if (message.startsWith("command_conflict:")) {
          return conflict("command_conflict", message.slice("command_conflict:".length));
        }
        throw error;
      }
    }

    if (route[0] === "commands" && route.length === 1 && method === "GET") {
      const status = url.searchParams.get("status");
      const limit = Math.max(0, Number(url.searchParams.get("limit") ?? 100));
      const rows = status
        ? this.sql
            .exec("SELECT * FROM commands WHERE status = ? ORDER BY sequence DESC LIMIT ?", status, limit)
            .toArray()
        : this.sql.exec("SELECT * FROM commands ORDER BY sequence DESC LIMIT ?", limit).toArray();
      return json(rows.reverse());
    }

    if (route[0] === "commands" && route[1] === "pending-count" && method === "GET") {
      const rows = this.sql
        .exec("SELECT COUNT(*) AS count FROM commands WHERE status IN ('PENDING', 'RETRYABLE', 'LEASED')")
        .toArray();
      return json({ count: Number((rows[0] as Record<string, unknown>).count) });
    }

    if (route[0] === "commands" && route[1] === "max-reserved" && method === "GET") {
      const rows = this.sql.exec("SELECT MAX(reserved_entity_id) AS entity_id FROM commands").toArray();
      return json({ entity_id: (rows[0] as Record<string, unknown>).entity_id ?? null });
    }

    if (route[0] === "commands" && route[1] === "cancel" && method === "POST") {
      const body = (await request.json()) as { reason?: string };
      const count = this.state.storage.transactionSync(() =>
        rejectUnsettledCommands(
          this.sql,
          String(body.reason ?? "world destroyed"),
          now,
        ),
      );
      return json({ count });
    }

    if (route[0] === "outbox" && route.length === 1 && method === "GET") {
      const limit = Math.max(1, Number(url.searchParams.get("limit") ?? 1000));
      return json(
        this.sql
          .exec("SELECT * FROM outbox WHERE projected_at IS NULL ORDER BY sequence LIMIT ?", limit)
          .toArray(),
      );
    }

    if (route[0] === "outbox" && route[1] === "project" && method === "POST") {
      const body = (await request.json()) as { event_ids?: string[] };
      this.state.storage.transactionSync(() => {
        for (const eventId of body.event_ids ?? []) {
          this.sql.exec(
            "UPDATE outbox SET projected_at = COALESCE(projected_at, ?) WHERE event_id = ?",
            now,
            eventId,
          );
        }
      });
      return json({ ok: true });
    }

    if (route[0] === "outbox" && route[1] === "progress" && method === "GET") {
      const rows = this.sql
        .exec("SELECT COALESCE(MAX(CASE WHEN projected_at IS NOT NULL THEN sequence END), 0) AS watermark, SUM(CASE WHEN projected_at IS NULL THEN 1 ELSE 0 END) AS pending FROM outbox")
        .toArray();
      const row = rows[0] as Record<string, unknown>;
      return json({ watermark: Number(row.watermark ?? 0), pending: Number(row.pending ?? 0) });
    }

    if (route[0] === "fence" && method === "POST") {
      const body = (await request.json()) as { holder?: string };
      const rows = this.sql.exec("SELECT epoch FROM fence WHERE singleton = 1").toArray();
      const epoch = (rows.length ? Number((rows[0] as Record<string, unknown>).epoch) : 0) + 1;
      this.sql.exec(
        "INSERT INTO fence (singleton, epoch, holder, acquired_at) VALUES (1, ?, ?, ?) " +
          "ON CONFLICT(singleton) DO UPDATE SET epoch = excluded.epoch, holder = excluded.holder, acquired_at = excluded.acquired_at",
        epoch,
        body.holder ?? "unknown",
        now,
      );
      return json({ epoch });
    }

    if (route[0] === "fence" && method === "GET") {
      const rows = this.sql.exec("SELECT epoch FROM fence WHERE singleton = 1").toArray();
      return json({ epoch: rows.length ? Number((rows[0] as Record<string, unknown>).epoch) : null });
    }

    if (route[0] === "manifest-head" && method === "GET") {
      const run = url.searchParams.get("run") ?? "";
      const rows = this.sql
        .exec("SELECT MAX(tick) AS tick FROM manifests WHERE run_id = ?", run)
        .toArray();
      const tick = rows.length ? (rows[0] as Record<string, unknown>).tick : null;
      return json({ tick: tick === null ? null : Number(tick) });
    }

    if (route[0] === "manifests" && method === "POST") {
      const m = (await request.json()) as Record<string, unknown>;
      const commandIds = (m.command_ids as string[] | undefined) ?? [];
      try {
        this.state.storage.transactionSync(() => {
          const fence = this.sql.exec("SELECT epoch FROM fence WHERE singleton = 1").toArray();
          const live = fence.length ? Number((fence[0] as Record<string, unknown>).epoch) : null;
          if (live === null || live !== Number(m.writer_epoch)) {
            throw new Error(`stale_writer:epoch ${m.writer_epoch} is not the live fence (${live})`);
          }
          const existing = this.sql
            .exec("SELECT commit_token FROM manifests WHERE run_id = ? AND tick = ?", m.run_id, m.tick)
            .toArray();
          if (existing.length > 0) {
            if ((existing[0] as Record<string, unknown>).commit_token !== m.commit_token) {
              throw new Error(`catalog_conflict:tick ${m.tick} already published by a different attempt`);
            }
          } else {
            this.sql.exec(
              "INSERT INTO manifests (run_id, tick, commit_token, writer_epoch, tables_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
              m.run_id,
              m.tick,
              m.commit_token,
              m.writer_epoch,
              JSON.stringify([...((m.table_ids as string[] | undefined) ?? [])].sort()),
              now,
            );
          }
          for (const commandId of commandIds) {
            const rows = this.sql
              .exec("SELECT * FROM commands WHERE command_id = ?", commandId)
              .toArray();
            if (!rows.length) {
              throw new Error(`command_conflict:tick ${m.tick} attempted to settle unknown command ${commandId}`);
            }
            const command = rows[0] as Record<string, unknown>;
            if (command.status === "APPLIED") {
              if (Number(command.applied_tick) !== Number(m.tick) || command.commit_token !== m.commit_token) {
                throw new Error(`command_conflict:command ${commandId} was applied by a different tick commit`);
              }
              continue;
            }
            if (command.status !== "LEASED" || command.lease_owner !== m.lease_owner) {
              throw new Error(`command_conflict:command ${commandId} is not leased by ${m.lease_owner}`);
            }
            this.sql.exec(
              "UPDATE commands SET status = 'APPLIED', lease_owner = NULL, lease_expires_at = NULL, updated_at = ?, applied_tick = ?, commit_token = ? WHERE command_id = ?",
              now,
              m.tick,
              m.commit_token,
              commandId,
            );
            appendCommandEvent(
              this.sql,
              command,
              "applied",
              JSON.stringify({ tick: m.tick, commit_token: m.commit_token }),
              now,
            );
          }
        });
        return json({ ok: true });
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        const [kind, ...detail] = message.split(":");
        if (kind === "stale_writer") return json({ error: kind, message: detail.join(":") }, 412);
        if (kind === "catalog_conflict" || kind === "command_conflict") {
          return conflict(kind, detail.join(":"));
        }
        throw error;
      }
    }

    if (route[0] === "manifests" && method === "GET") {
      const run = url.searchParams.get("run");
      const rows = run
        ? this.sql.exec("SELECT * FROM manifests WHERE run_id = ? ORDER BY tick", run).toArray()
        : this.sql.exec("SELECT * FROM manifests ORDER BY run_id, tick").toArray();
      return json(rows);
    }

    if (route[0] === "visible" && method === "GET") {
      const run = url.searchParams.get("run") ?? "";
      const ticksParam = url.searchParams.get("ticks");
      const anyManifest = this.sql
        .exec("SELECT 1 FROM manifests WHERE run_id = ? LIMIT 1", run)
        .toArray();
      const anyClaim = this.sql
        .exec("SELECT 1 FROM claims WHERE run_id = ? LIMIT 1", run)
        .toArray();
      const fence = this.sql.exec("SELECT 1 FROM fence WHERE singleton = 1").toArray();
      if (!anyManifest.length && !anyClaim.length) {
        return json({ visible: fence.length ? {} : null });
      }
      const ticks =
        ticksParam === null
          ? null
          : ticksParam === ""
            ? []
            : ticksParam.split(",").map((t) => parseInt(t, 10));
      const visible: Record<string, string[]> = {};
      const add = (tick: number, token: string) => {
        if (ticks && !ticks.includes(tick)) return;
        (visible[String(tick)] ??= []).push(token);
      };
      if (!anyManifest.length && !fence.length) {
        for (const tick of ticks ?? [0]) add(tick, "");
      }
      for (const row of this.sql
        .exec("SELECT tick, commit_token FROM manifests WHERE run_id = ?", run)
        .toArray() as Array<Record<string, unknown>>) {
        add(Number(row.tick), String(row.commit_token));
      }
      for (const row of this.sql
        .exec("SELECT tick, commit_token FROM claims WHERE run_id = ? AND status = 'COMPLETE'", run)
        .toArray() as Array<Record<string, unknown>>) {
        add(Number(row.tick), String(row.commit_token));
      }
      return json({ visible });
    }

    if (route[0] === "claims" && route[1] === "acquire" && method === "POST") {
      const c = (await request.json()) as Record<string, unknown>;
      const nowSec = Date.now() / 1000;
      const lease = Number(c.lease_seconds ?? 30);
      const existing = this.sql
        .exec("SELECT * FROM claims WHERE scope_key = ?", c.scope_key)
        .toArray();
      if (existing.length > 0) {
        const row = existing[0] as Record<string, unknown>;
        if (row.payload_digest !== c.payload_digest) {
          return conflict("claim_conflict", `${c.external_id} submitted with a different digest`);
        }
        if (row.status === "COMPLETE") {
          return json({ outcome: "duplicate", claim: row });
        }
        if (Number(row.lease_expires_at) > nowSec) {
          return json({ error: "claim_pending", message: "a live lease holds this claim" }, 423);
        }
        this.sql.exec(
          "UPDATE claims SET claimant = ?, lease_expires_at = ? WHERE scope_key = ?",
          c.claimant,
          nowSec + lease,
          c.scope_key,
        );
        const updated = this.sql.exec("SELECT * FROM claims WHERE scope_key = ?", c.scope_key).toArray();
        return json({ outcome: "recovered", claim: updated[0] });
      }
      const fence = this.sql.exec("SELECT epoch FROM fence WHERE singleton = 1").toArray();
      const epoch = fence.length ? Number((fence[0] as Record<string, unknown>).epoch) : 0;
      const token = `artifact-${String(c.scope_key).slice(0, 32)}`;
      this.sql.exec(
        "INSERT INTO claims (scope_key, run_id, producer, external_id, payload_digest, status, commit_token, tick, artifact_entity_id, table_id, claimant, lease_expires_at, fence_epoch, created_at) " +
          "VALUES (?, ?, ?, ?, ?, 'PENDING', ?, ?, 0, NULL, ?, ?, ?, ?)",
        c.scope_key,
        c.run_id,
        c.producer,
        c.external_id,
        c.payload_digest,
        token,
        c.tick ?? 0,
        c.claimant,
        nowSec + lease,
        epoch,
        now,
      );
      const rowid = this.sql
        .exec("SELECT rowid FROM claims WHERE scope_key = ?", c.scope_key)
        .toArray();
      const artifactEid = -(100_000 + Number((rowid[0] as Record<string, unknown>).rowid));
      this.sql.exec(
        "UPDATE claims SET artifact_entity_id = ? WHERE scope_key = ?",
        artifactEid,
        c.scope_key,
      );
      const created = this.sql.exec("SELECT * FROM claims WHERE scope_key = ?", c.scope_key).toArray();
      return json({ outcome: "acquired", claim: created[0] });
    }

    if (route[0] === "claims" && route.length === 3 && route[2] === "table" && method === "POST") {
      const body = (await request.json()) as { table_id: string };
      this.sql.exec(
        "UPDATE claims SET table_id = ? WHERE scope_key = ? AND status = 'PENDING'",
        body.table_id,
        route[1],
      );
      return json({ ok: true });
    }

    if (route[0] === "claims" && route.length === 3 && route[2] === "rearm" && method === "POST") {
      const body = (await request.json()) as { claimant: string; commit_token: string };
      const rows = this.sql
        .exec("SELECT * FROM claims WHERE scope_key = ?", route[1])
        .toArray();
      if (!rows.length) return conflict("claim_conflict", `no claim for scope ${route[1]}`);
      const row = rows[0] as Record<string, unknown>;
      if (row.status !== "PENDING") {
        return conflict("claim_conflict", `claim ${route[1]} is already ${row.status}`);
      }
      if (row.claimant !== body.claimant) {
        return json({ error: "claim_pending", message: "claim is held by another claimant" }, 423);
      }
      if (row.commit_token === body.commit_token) {
        return conflict("claim_conflict", "re-arm requires a fresh commit token");
      }
      this.sql.exec(
        "UPDATE claims SET commit_token = ?, table_id = NULL WHERE scope_key = ?",
        body.commit_token,
        route[1],
      );
      const updated = this.sql.exec("SELECT * FROM claims WHERE scope_key = ?", route[1]).toArray();
      return json(updated[0]);
    }

    if (route[0] === "claims" && route.length === 3 && route[2] === "complete" && method === "POST") {
      const body = (await request.json()) as { claimant: string; table_id: string };
      const rows = this.sql
        .exec("SELECT status, claimant FROM claims WHERE scope_key = ?", route[1])
        .toArray();
      if (!rows.length) return conflict("claim_conflict", `no claim for scope ${route[1]}`);
      const row = rows[0] as Record<string, unknown>;
      if (row.status === "COMPLETE") return json({ ok: true, idempotent: true });
      if (row.claimant !== body.claimant) {
        return json({ error: "claim_pending", message: "claim was taken over" }, 423);
      }
      this.sql.exec(
        "UPDATE claims SET status = 'COMPLETE', table_id = ?, completed_at = ? WHERE scope_key = ?",
        body.table_id,
        now,
        route[1],
      );
      return json({ ok: true });
    }

    if (route[0] === "claims" && route.length === 2 && method === "GET") {
      const rows = this.sql.exec("SELECT * FROM claims WHERE scope_key = ?", route[1]).toArray();
      return rows.length ? json(rows[0]) : json({ error: "not_found" }, 404);
    }

    return json({ error: "bad_route" }, 404);
  }
}
