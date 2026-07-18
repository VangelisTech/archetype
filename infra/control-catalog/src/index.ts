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
          const directory = env.DIRECTORY.get(env.DIRECTORY.idFromName(namespace));
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
    const stub = env.DIRECTORY.get(env.DIRECTORY.idFromName(namespace));
    return stub.fetch(request);
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
        return json({ ok: true, idempotent: true });
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
      return json({ ok: true });
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
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const parts = url.pathname.split("/").filter(Boolean); // ns,:namespace,w,:world,...
    const route = parts.slice(4);
    const method = request.method;
    const now = new Date().toISOString();

    if (route[0] === "commands" && route[1] === "admit" && method === "POST") {
      const body = (await request.json()) as { admissions?: Array<Record<string, unknown>> };
      const admissions = body.admissions ?? [];
      try {
        const records = this.state.storage.transactionSync(() => {
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
      const records = this.state.storage.transactionSync(() => {
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
      const count = this.state.storage.transactionSync(() => {
        const rows = this.sql
          .exec("SELECT * FROM commands WHERE status IN ('PENDING', 'RETRYABLE', 'LEASED') ORDER BY sequence")
          .toArray() as Array<Record<string, unknown>>;
        for (const row of rows) {
          this.sql.exec(
            "UPDATE commands SET status = 'REJECTED', lease_owner = NULL, lease_expires_at = NULL, last_error_code = 'world_destroyed', last_error_detail = ?, updated_at = ? WHERE command_id = ?",
            String(body.reason ?? "world destroyed").slice(0, 2000),
            now,
            row.command_id,
          );
          appendCommandEvent(this.sql, row, "rejected", JSON.stringify({ error_code: "world_destroyed" }), now);
        }
        return rows.length;
      });
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
