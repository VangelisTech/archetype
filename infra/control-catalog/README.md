# archetype-control-catalog

The remote control catalog (issue #281): Cloudflare Durable Objects serving
Archetype's discovery, fencing, manifests, durable commands, evaluation
execution leases, and transactional outbox across hosts. The
Python client is `archetype.storage.catalog.remote.RemoteControlCatalog`;
`SqliteControlCatalog` remains the reference implementation and the default.

## Layout

- `CatalogDirectoryDO` — one per storage identity: worlds + signatures
  (cross-world discovery; low write rate).
- `WorldCommitDO` — one per world: writer fence, tick manifests, durable
  command leases and settlement, evaluation leases, and transactional outbox
  (the serialized control path; serialized execution makes publish a
  straight-line transaction).

## Deploy

```bash
npx wrangler deploy                          # needs CLOUDFLARE_API_TOKEN or wrangler login
python -c "import secrets; print(secrets.token_urlsafe(32))" | npx wrangler secret put CATALOG_TOKEN
```

## Select from Archetype

```bash
export ARCHETYPE_CONTROL_CATALOG_URL=https://archetype-control-catalog.<account>.workers.dev
export ARCHETYPE_CONTROL_CATALOG_TOKEN=<the secret>
```

Every coordinated world in that process now fences, publishes manifests, and
settles durable commands through the worker. The Worker returns a configuration error when
`CATALOG_TOKEN` is absent, and the Python host fails during catalog setup
when the URL is configured without the matching token. Parity with the
SQLite reference is enforced by `tests/storage/test_remote_catalog_parity.py`
(runs the worker under `wrangler dev`); the owner-facing end-to-end proof
is `scripts/validate_r2_substrate.py`.

Catalog protocol v8 adds immutable world `writer_mode`. Existing directory rows
migrate in place and default to `resumable`; physical evidence registers as
`cleanup_only`. The Python client performs an authenticated `/protocol`
preflight and then uses the versioned `POST /protocol/v8/worlds` route for every
cleanup-only write. Both an older outer Worker and an older resident Directory
Durable Object reject that route before SQL: the v8 outer Worker rewrites the
public route to the Directory-only
`https://catalog-directory.internal/ns/<namespace>/_gateway/v8/worlds` route,
and the v8 Directory accepts versioned registration only on that internal host
and path. A v7 outer Worker therefore cannot pass the public route to a v8
Directory, and a v8 outer Worker cannot pass the internal route to a v7
Directory. Deploy the v8 Worker before a host that can create those worlds.

After the Directory write succeeds, the outer Worker mirrors status into the
per-world `WorldCommitDO` and only then adds
`gateway_protocol_version: 8` to the response. The client never falls back to
unversioned registration and requires the response to confirm catalog protocol
v8, gateway protocol v8, and the exact writer marker. If any confirmation fails
after a response, fail-closed retirement completes despite caller cancellation
and preserves both the retirement outcome and cancellation provenance.
