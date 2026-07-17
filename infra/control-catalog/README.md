# archetype-control-catalog

The remote control catalog (issue #281): Cloudflare Durable Objects serving
Archetype's discovery, fencing, manifests, claims, and artifact-publication
reconciliation across hosts. The
Python client is `archetype.app._remote_catalog.RemoteControlCatalog`;
`SqliteControlCatalog` remains the reference implementation and the default.

## Layout

- `CatalogDirectoryDO` — one per storage identity: worlds + signatures
  (cross-world discovery; low write rate).
- `WorldCommitDO` — one per world: writer fence, tick manifests, fact
  claims, and resumable artifact-publication leases (the per-world hot path;
  serialized execution makes each state transition a straight-line
  transaction).

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

Every coordinated world in that process now fences, publishes, and claims
through the worker. The Worker returns a configuration error when
`CATALOG_TOKEN` is absent, and the Python host fails during catalog setup
when the URL is configured without the matching token. Parity with the
SQLite reference is enforced by `tests/app/test_remote_catalog_parity.py`
(runs the worker under `wrangler dev`); the owner-facing end-to-end proof
is `scripts/validate_r2_substrate.py`.
