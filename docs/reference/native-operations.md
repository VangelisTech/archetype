# Native operations

Generated from the closed version 1 request decoder and capability map.

HTTP uses `POST /invoke`; MCP exposes the same operations through the installed server. CLI `invoke` sends the same JSON request. All paths authenticate and authorize the complete resource grant set before native lookup.

```json
{"version":1,"operation":"history","resource":"experiment","arguments":{"offset":"0","limit":"32"}}
```

Requests contain exactly `version`, `operation`, `resource`, and `arguments`. Duplicate or unknown fields fail validation. Requests are bounded to 64 KiB and responses to 16 KiB. Pages contain at most 32 rows. Native Int64 and counters use canonical decimal strings; Float64 cells use finite IEEE-754 bit strings with positive zero; Bool cells use JSON booleans. Artifact metadata may independently contain null values.

| Operation | Required argument fields | Capability |
|---|---|---|
| `admission_status` | `generation`, `admission_key` | `simulation:read` |
| `admit` | `generation`, `revision`, `admission_key`, `expected_head`, `changes` | `simulation:submit` |
| `artifact_upload` | `context_id`, `exact_cut`, `artifact_id`, `logical_path`, `content_base64` | `artifacts:publish` |
| `confirm` | `boundary`, `tick`, `expected_parent` | `simulation:confirm` |
| `context_artifacts` | `context_id`, `exact_cut`, `all`, `offset`, `limit` | `artifacts:read` |
| `create` | `request_key`, `label`, `program` | `simulation:create` |
| `fork` | `source_resource`, `receipt`, `request_key`, `expected_generation` | `simulation:fork` |
| `history` | `offset`, `limit` | `simulation:read` |
| `program_compose` | `request_key`, `description`, `composition` | `programs:create` |
| `program_create` | `request_key`, `description`, `definition` | `programs:create` |
| `program_describe` | None | `programs:read` |
| `program_resolve` | None | `programs:read` |
| `publish` | `boundary` | `simulation:publish` |
| `publish_context` | `source_resource` | `artifacts:publish` |
| `read` | `receipt`, `component`, `offset`, `limit` | `simulation:read` |
| `read_context` | None | `artifacts:read` |
| `reconcile` | `boundary`, `tick`, `expected_parent` | `simulation:publish` |
| `resolve` | None | `simulation:read` |
| `restore` | `receipt`, `expected_generation` | `simulation:restore` |
| `start` | None | `simulation:control` |
| `status` | None | `simulation:read` |
| `stop` | None | `simulation:control` |

Composition additionally requires read grants on every referenced program. Create requires a read grant on its pinned program. Fork and hosted context publication require the same capability on their source resource. Context scopes and all native paths are configured by the operator; clients cannot select physical storage or compiler paths.

Inline artifact uploads contain base64 bytes, a canonical UUIDv7, and a relative logical path. Decoded content is limited to 32 KiB. Larger local batches use the Python artifact preparation and publication workflow described in [Artifacts](../guide/artifacts.md).

Errors report a bounded public code and outcome. `not_dispatched` means admission did not occur; `unknown` requires checking the recorded admission identity before retry. Cancelling a caller does not release ownership of admitted native work.
