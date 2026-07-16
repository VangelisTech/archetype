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
//   and fact claims: the per-tick hot path. A Durable Object executes
//   requests serially, so the fence is structural — publish is a straight-
//   line transaction with no CAS gymnastics.
//
// Auth: a single bearer token (CATALOG_TOKEN secret). The Python client is
// archetype.app._remote_catalog.RemoteControlCatalog; semantics must match
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
      // derived data for discovery and fact-tick selection.
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
        fact_entity_id INTEGER NOT NULL DEFAULT 0, table_id TEXT,
        claimant TEXT NOT NULL, lease_expires_at REAL NOT NULL,
        fence_epoch INTEGER NOT NULL, created_at TEXT NOT NULL, completed_at TEXT
      );
    `);
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const parts = url.pathname.split("/").filter(Boolean); // ns,:namespace,w,:world,...
    const route = parts.slice(4);
    const method = request.method;
    const now = new Date().toISOString();

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
      const fence = this.sql.exec("SELECT epoch FROM fence WHERE singleton = 1").toArray();
      const live = fence.length ? Number((fence[0] as Record<string, unknown>).epoch) : null;
      if (live === null || live !== Number(m.writer_epoch)) {
        return json(
          { error: "stale_writer", message: `epoch ${m.writer_epoch} is not the live fence (${live})` },
          412,
        );
      }
      const existing = this.sql
        .exec("SELECT commit_token FROM manifests WHERE run_id = ? AND tick = ?", m.run_id, m.tick)
        .toArray();
      if (existing.length > 0) {
        if ((existing[0] as Record<string, unknown>).commit_token === m.commit_token) {
          return json({ ok: true, idempotent: true });
        }
        return conflict("catalog_conflict", `tick ${m.tick} already published by a different attempt`);
      }
      this.sql.exec(
        "INSERT INTO manifests (run_id, tick, commit_token, writer_epoch, tables_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        m.run_id,
        m.tick,
        m.commit_token,
        m.writer_epoch,
        JSON.stringify([...((m.table_ids as string[] | undefined) ?? [])].sort()),
        now,
      );
      return json({ ok: true });
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
        .exec("SELECT 1 FROM claims WHERE run_id = ? AND status = 'COMPLETE' LIMIT 1", run)
        .toArray();
      if (!anyManifest.length && !anyClaim.length) {
        const fence = this.sql.exec("SELECT 1 FROM fence WHERE singleton = 1").toArray();
        return json({ visible: fence.length ? {} : null });
      }
      const ticks = ticksParam ? ticksParam.split(",").map((t) => parseInt(t, 10)) : null;
      const visible: Record<string, string[]> = {};
      const add = (tick: number, token: string) => {
        if (ticks && !ticks.includes(tick)) return;
        (visible[String(tick)] ??= []).push(token);
      };
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
      const token = `fact-${String(c.scope_key).slice(0, 32)}`;
      this.sql.exec(
        "INSERT INTO claims (scope_key, run_id, producer, external_id, payload_digest, status, commit_token, tick, fact_entity_id, table_id, claimant, lease_expires_at, fence_epoch, created_at) " +
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
      const factEid = -(100_000 + Number((rowid[0] as Record<string, unknown>).rowid));
      this.sql.exec("UPDATE claims SET fact_entity_id = ? WHERE scope_key = ?", factEid, c.scope_key);
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
