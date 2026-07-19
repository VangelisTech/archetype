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

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), { status, headers: JSON_HEADERS });
}

function conflict(kind: string, message: string): Response {
  return json({ error: kind, message }, 409);
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
    const namespace = parts[1];
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
        const worldId = parts[3];
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
      const worldId = parts[3];
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
      return json(this.sql.exec("SELECT * FROM worlds ORDER BY world_id").toArray());
    }

    if (route[0] === "worlds" && route.length === 2) {
      const worldId = route[1];
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
        last_error TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
        possibly_submitted_at TEXT, acknowledged_at TEXT, settled_at TEXT
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
        manifest_uri TEXT NOT NULL DEFAULT '', last_error TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL, completed_at TEXT
      );
      CREATE INDEX IF NOT EXISTS artifact_publications_due
      ON artifact_publications (status, lease_expires_at);
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
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const parts = url.pathname.split("/").filter(Boolean); // ns,:namespace,w,:world,...
    const route = parts.slice(4);
    const method = request.method;
    const now = new Date().toISOString();

    if (route[0] === "attempt-claims" && route[1] === "acquire" && method === "POST") {
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

      if (route.length === 3 && route[2] === "consume" && method === "POST") {
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

      if (route.length === 3 && route[2] === "transition" && method === "POST") {
        const p = (await request.json()) as Record<string, unknown>;
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
        if (row.status === "settled") {
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
        if (row.status !== p.expected_status) {
          return conflict(
            "attempt_claim_conflict",
            `attempt claim ${claimKey} is ${row.status}, expected ${p.expected_status}`,
          );
        }
        const target = String(p.target_status ?? "");
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
        const possiblySubmittedAt = target === "possibly_submitted" ? now : null;
        const acknowledgedAt = target === "provider_acknowledged" ? now : null;
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
            "last_error = CASE WHEN ? = '' THEN last_error ELSE ? END, updated_at = ?, " +
            "possibly_submitted_at = COALESCE(possibly_submitted_at, ?), " +
            "acknowledged_at = COALESCE(acknowledged_at, ?), " +
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
          p.last_error ?? "",
          p.last_error ?? "",
          now,
          possiblySubmittedAt,
          acknowledgedAt,
          settledAt,
          claimKey,
          p.expected_status,
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

    if (route[0] === "artifact-publications" && route[1] === "acquire" && method === "POST") {
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
          return json({ outcome: "duplicate", publication: row });
        }
        if (row.status === "EXPIRED") {
          return json({ outcome: "expired", publication: row });
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
        return json({ outcome: "recovered", publication: updated[0] });
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
      return json({ outcome: "acquired", publication: created[0] });
    }

    if (route[0] === "artifact-publications" && route.length === 1 && method === "GET") {
      const due = Number(url.searchParams.get("due") ?? Date.now() / 1000);
      const limit = Math.max(0, Number(url.searchParams.get("limit") ?? 100));
      return json(
        this.sql
          .exec(
            "SELECT * FROM artifact_publications WHERE status IN ('PENDING', 'UPLOADED') " +
              "AND lease_expires_at <= ? ORDER BY lease_expires_at, publication_key LIMIT ?",
            due,
            limit,
          )
          .toArray(),
      );
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
          if (route[2] === "fail") return json({ ok: true });
          if (["renew", "uploads", "complete", "expire"].includes(route[2])) {
            return conflict(
              "artifact_publication_conflict",
              `no artifact publication recorded for ${key}`,
            );
          }
        }
        return json({ error: "not_found" }, 404);
      }
      const row = rows[0] as Record<string, unknown>;

      if (route.length === 2 && method === "GET") return json(row);

      if (route[2] === "renew" && method === "POST") {
        const body = (await request.json()) as { claimant: string; lease_seconds: number };
        if (row.status === "INDEXED" || row.status === "EXPIRED") return json(row);
        if (row.claimant !== body.claimant) {
          return json(
            { error: "artifact_publication_pending", message: "publication was taken over" },
            423,
          );
        }
        this.sql.exec(
          "UPDATE artifact_publications SET lease_expires_at = ?, updated_at = ? " +
            "WHERE publication_key = ?",
          Date.now() / 1000 + Number(body.lease_seconds),
          now,
          key,
        );
        const updated = this.sql
          .exec("SELECT * FROM artifact_publications WHERE publication_key = ?", key)
          .toArray();
        return json(updated[0]);
      }

      if (route[2] === "uploads" && method === "POST") {
        const body = (await request.json()) as {
          claimant: string;
          records_json: string;
          manifest_uri: string;
        };
        if (row.status === "INDEXED") return json({ ok: true, idempotent: true });
        if (row.status === "EXPIRED") {
          return conflict("artifact_publication_expired", `publication ${key} expired`);
        }
        if (row.claimant !== body.claimant) {
          return json(
            { error: "artifact_publication_pending", message: "publication was taken over" },
            423,
          );
        }
        if (row.status === "UPLOADED") {
          if (row.records_json === body.records_json && row.manifest_uri === body.manifest_uri) {
            return json({ ok: true, idempotent: true });
          }
          return conflict("artifact_publication_conflict", "different uploads already recorded");
        }
        this.sql.exec(
          "UPDATE artifact_publications SET status = 'UPLOADED', records_json = ?, " +
            "manifest_uri = ?, last_error = '', updated_at = ? WHERE publication_key = ?",
          body.records_json,
          body.manifest_uri,
          now,
          key,
        );
        return json({ ok: true });
      }

      if (route[2] === "complete" && method === "POST") {
        const body = (await request.json()) as { claimant: string; index_snapshot_id: number };
        if (row.status === "INDEXED") return json({ ok: true, idempotent: true });
        if (row.status !== "UPLOADED") {
          return conflict(
            "artifact_publication_conflict",
            `publication ${key} cannot move from ${row.status} to INDEXED`,
          );
        }
        if (row.claimant !== body.claimant) {
          return json(
            { error: "artifact_publication_pending", message: "publication was taken over" },
            423,
          );
        }
        this.sql.exec(
          "UPDATE artifact_publications SET status = 'INDEXED', index_snapshot_id = ?, " +
            "last_error = '', updated_at = ?, completed_at = ? WHERE publication_key = ?",
          body.index_snapshot_id,
          now,
          now,
          key,
        );
        return json({ ok: true });
      }

      if (route[2] === "fail" && method === "POST") {
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

      if (route[2] === "expire" && method === "POST") {
        const body = (await request.json()) as { claimant: string; error: string };
        if (row.status === "INDEXED" || row.status === "EXPIRED") return json({ ok: true });
        if (row.status === "UPLOADED") {
          return conflict(
            "artifact_publication_conflict",
            "uploaded publications must be indexed, not expired",
          );
        }
        if (row.claimant !== body.claimant) {
          return json(
            { error: "artifact_publication_pending", message: "publication was taken over" },
            423,
          );
        }
        this.sql.exec(
          "UPDATE artifact_publications SET status = 'EXPIRED', last_error = ?, " +
            "updated_at = ?, completed_at = ? WHERE publication_key = ?",
          String(body.error).slice(0, 8000),
          now,
          now,
          key,
        );
        return json({ ok: true });
      }
    }

    if (route[0] === "status" && route.length === 1) {
      if (method === "GET") {
        const rows = this.sql
          .exec("SELECT status FROM world_state WHERE singleton = 1")
          .toArray();
        return rows.length ? json(rows[0]) : json({ error: "not_found" }, 404);
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
          requireActiveWorld(this.sql, parts[3]);
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
          requireActiveWorld(this.sql, parts[3]);
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
