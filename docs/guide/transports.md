# HTTP, MCP and CLI

Install matching `archetype-transports==0.7.0` or `archetype-ecs[transports]`.
`archetype serve` uses the same native runtime and owns its lifetime. Configure
`ARCHETYPE_PRINCIPALS_PATH` with a real principal directory and
`ARCHETYPE_RESOURCES_PATH` with immutable resource declarations and exact grants.
It opens no public listener without explicit configuration. Synthetic test
credentials are test-only; role labels and actor arguments are not credentials.

Both interfaces accept the closed envelope:

```json
{"version":1,"resource":"experiment","operation":"history","arguments":{"offset":"0","limit":"32"}}
```

HTTP sends JSON to `POST /invoke` with `Authorization: Bearer TOKEN` and
`Content-Type: application/json`. MCP uses the official SDK at `/mcp`; its
`simulation` tool takes that same envelope as the `request_json` string. Grants
are checked before resource/binding lookup. Integers/control values use decimal
strings; Bool is a boolean; finite Float64 uses canonical hexadecimal bits.
Unknown keys, duplicate JSON fields, overflow, coercion and unsupported operations
fail closed. Native identities, programs, checkpoint bytes and diagnostics stay
private. A client cancellation retains underlying supervision and capacity.

`archetype invoke request.json` sends the exact request bytes over HTTP.
`archetype world status/start/stop/history NAME` are thin conveniences for those
same operations. `ARCHETYPE_URL` selects the server and `ARCHETYPE_TOKEN` supplies
the credential. The CLI validates response identity/version and caps reads at
16 KiB. It never constructs a local live manager for ordinary commands.

Request limits are 64 KiB and response limits 16 KiB. Pages allow 1–32 rows;
large facts can require a smaller page. Failure envelopes retain bounded codes
and explicit `not_dispatched` or `unknown` outcomes. Mutation errors never imply
safe replay. See the [operation reference](../reference/native-operations.md).

Operator composition may call `archetype.api.create_app(config=ServerConfig(...),
verifier=...)` with an existing real identity verifier implementing `configured`
and `authenticate(credential)`. It must return a verified principal with explicit
capabilities. The same immutable resource grants still apply before lookup. This
seam lets authenticated hosts retain their issuer and tenant mapping without
exposing trusted runtime commands to clients.
