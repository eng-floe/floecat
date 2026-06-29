# Trace Diagnostic Events

Floecat emits compact diagnostic events on existing spans so Jaeger stays readable. These events
are not durable metrics and should not be used for alerting. They answer "what did this request do?"
inside a single sampled trace.

Use this convention:

- Keep one generic RPC event, `floecat.rpc.summary`, for cross-cutting request, repository, and
  storage facts.
- Keep domain events, such as `floecat.get_user_objects.summary`, for operation-specific phase
  timings and result counters.
- Prefer summary fields over extra spans for cheap in-process phases. Enable detailed store spans
  only when the generic summary says the bottleneck is inside repository/store work.

## Profiles and knobs

| Profile / property | Default | Purpose |
|---|---:|---|
| `telemetry.diagnostics.enabled` | `true` | Emits compact diagnostic events on current trace spans. |
| `telemetry.store-spans.enabled` | `false` | Emits detailed repository/store child spans. Keep off unless drilling into store internals. |
| `%dev.telemetry.strict` / `%test.telemetry.strict` | `true` | Fails fast on telemetry contract violations in local/test runs. |
| `telemetry.strict` | `false` | Production default: invalid optional telemetry is dropped/counts, not fatal. |
| `telemetry-otlp` profile | off | Enables OTLP export to the local collector on `localhost:4317`. |
| `telemetry-debug` profile | off | Enables `telemetry.store-spans.enabled=true`; use alongside `telemetry-otlp` for store-call traces. |
| `telemetry-prof` profile | off | Enables profiling endpoints and policy-triggered capture support. |

Typical local trace session:

```bash
QUARKUS_PROFILE=telemetry-otlp mvn -pl service quarkus:dev
```

For repository/store drill-down:

```bash
QUARKUS_PROFILE=telemetry-otlp,telemetry-debug mvn -pl service quarkus:dev
```

## Generic RPC summary

Event: `floecat.rpc.summary`

Emitted by the gRPC server interceptor for every RPC. This is the generic place for transport
status and cross-cutting repository/store activity. Domain code should not duplicate these fields
in its own summary event.

| Field | Meaning |
|---|---|
| `status` | gRPC status code name, for example `OK` or `INVALID_ARGUMENT`. |
| `success` | `true` when the RPC closed with `Status.OK`. |
| `cancelled` | `true` when the server listener observed cancellation. |
| `completed` | `true` when the request completed normally, or the RPC closed `OK`. |
| `request_messages` | Number of request messages observed by the server listener. |
| `response_messages` | Number of response messages sent by the server call. |
| `duration_ms` | Wall-clock RPC duration measured by the interceptor. |
| `store_operations` | Number of repository/store observations recorded during this RPC. |
| `store_errors` | Number of recorded repository/store observations that closed with error. |
| `store_ms` | Sum of recorded repository/store observation durations. |
| `repo_gets` | Count of repository operations classified as point reads. |
| `repo_lists` | Count of repository operations classified as list/prefix scans. |
| `repo_counts` | Count of repository count operations. |
| `repo_writes` | Count of repository create/update/delete/put operations. |
| `fallbacks` | Count of generic fallback paths taken during the RPC. |
| `repo_<resource>_<operation>_count` | Per-repository operation count, for example `repo_table_get_by_key_count`. |
| `repo_<resource>_<operation>_ms` | Total time for that repository operation family. |
| `current_snapshot_source` | `pointer` when the dedicated current-snapshot pointer resolved the snapshot; `fallback` when the code had to scan latest-by-time. Fallback is read-only: it diagnoses missing/stale pointer state but does not repair state during query reads. |
| `snapshot_pin_source` | Where the scan snapshot came from. Today `query_context` means it was already pinned during planning/begin-query. |
| `storage_authority_source` | Source for storage authority selection. Today `load` means the resolver listed authorities from the repository. Future values may include `cache` or `direct`. |
| `table_load_ms` | Time spent loading table metadata during scan initialization. |
| `snapshot_load_ms` | Time spent loading snapshot metadata during scan initialization. |
| `scan_init_ms` | End-to-end scan initialization time inside Floecat, including table/snapshot load and `TableInfo` build. |
| `storage_authority_ms` | Time spent loading storage authorities for server-side file IO properties. |

How to read it:

- If `repo_lists > 0` on a warm point lookup, the request is still doing scan-shaped repository work.
- If `current_snapshot_source=fallback`, the table did not use the O(1) current-snapshot pointer path. Repair should happen through metadata ingestion/reconcile paths, not as a side effect of the read.
- If `storage_authority_source=load` and `storage_authority_ms` is material, storage authority lookup is a candidate for caching or direct lookup.
- If `store_ms` is small but `duration_ms` is large, look at the domain summary or caller-side spans.

## Domain summaries

### `floecat.get_user_objects.summary`

Emitted by `GetUserObjects`. This is the main planner metadata lookup summary for user relations.

| Field | Meaning |
|---|---|
| `query_id` / `correlation_id` | Query and request correlation identifiers. |
| `candidates` | Number of table-reference candidates requested. |
| `chunks` | Number of resolution chunks emitted. |
| `found` / `not_found` | Candidate resolution result counts. |
| `total_ms` | End-to-end service time for the operation body. |
| `resolve_ms` | Total candidate resolution phase. |
| `normalize_ms` | Input/reference normalization time. |
| `select_relation_ms` | Time choosing the selected relation/input for each candidate. |
| `default_catalog_ms` | Time spent resolving default catalog state. |
| `name_resolve_ms` | Time spent resolving names to graph nodes. |
| `node_resolve_ms` | Time spent resolving graph nodes to relation metadata. |
| `base_inject_ms` | Time spent injecting base-table information for view-related outputs. |
| `pin_collect_ms` | Time spent collecting snapshot pins. |
| `pin_commit_ms` | Time spent committing snapshot pins to query context. |
| `pin_ms` | Total pinning time as a convenience field. |
| `relation_build_ms` | Time building returned relation metadata. |
| `decoration_ms` | Total decoration time. |
| `stats_lookup_ms` | Time spent loading stats/decorator inputs. |
| `decorate_relation_ms` / `decorate_view_ms` | Relation/view decoration time. |
| `decorate_columns_ms` | Total column decoration time. |
| `decorate_column_invoke_ms` | Time invoking column decorators. |
| `decorate_complete_ms` | Final decoration completion/assembly time. |
| `hint_persist_ms` | Time persisting engine hints/decorator outputs. |
| `scheduling_ms` | Time not attributed to measured sub-phases, usually scheduling or queueing around the work. |
| `decorator_warm_hits` | Number of decoration results served from warm decorator state. |
| `default_catalog_lookups` | Number of default-catalog lookups. |
| `name_cache_hits` / `name_cache_misses` | Request-local name-resolution cache behavior. |
| `node_cache_hits` / `node_cache_misses` | Request-local node-resolution cache behavior. |
| `name_cache_entries` / `node_cache_entries` / `relation_cache_entries` | Final request-local cache sizes. |
| `outcome` | `completed`, `failed`, or cancellation-specific outcome where applicable. |

### `floecat.flight.summary`

Emitted by the Arrow Flight system-table stream worker.

| Field | Meaning |
|---|---|
| `query_id` / `correlation_id` | Query and request correlation identifiers. |
| `system_table` | System table being streamed. |
| `outcome` | `completed`, `cancelled`, or `failed`. |
| `cancelled` | Whether stream cancellation was observed. |
| `rows` | Rows serialized into Arrow batches. |
| `batches` | Arrow record batches produced. |
| `empty_stream` | `true` when the stream completed without a data batch. |
| `total_ms` | End-to-end worker stream time. |
| `allocator_ms` | Time creating the Arrow allocator. |
| `build_plan_ms` | Time building the scanner/serialization plan. |
| `serialize_ms` | Time serializing rows into Arrow batches. |
| `sink_copy_ms` | Time copying completed vectors/batches into the Flight sink path. |
| `sink_put_next_ms` | Time spent in Flight `putNext`. |
| `sink_complete_ms` | Time spent completing the Flight stream. |
| `cleanup_ms` | Cleanup time after streaming. |
| `scheduling_ms` | Residual time outside measured sub-phases. |

### `floecat.system_scan.summary`

Emitted by `ScanSystemTable`.

| Field | Meaning |
|---|---|
| `query_id` / `correlation_id` | Query and request correlation identifiers. |
| `table_id` | Requested system-table resource ID. |
| `format` | Response format selected by the request, such as Arrow vs row chunks. |
| `schema_columns` | Number of columns in the scanner schema. |
| `required_columns` | Number of projected columns requested. |
| `predicates` | Number of predicates passed to the scanner. |
| `arrow_requested` | Whether the Arrow path was requested. |
| `arrow_plan_ms` | Time planning Arrow output. Present on Arrow path. |
| `arrow_sink_ms` | Time sinking Arrow output. Present on Arrow path. |
| `row_scan_ms` | Time scanning/filtering/projecting row output. Present on row path. |
| `rows_emitted` | Row chunks emitted on row path. |
| `error` | Exception class name on failure. |
| `outcome` | `completed`, `cancelled`, or `failed`. |
| `total_ms` | End-to-end service time for the system scan. |

### `floecat.get_system_objects.summary`

Emitted by `GetSystemObjects`.

| Field | Meaning |
|---|---|
| `engine_kind` / `engine_version` | Engine identity from the request context. |
| `registry_bytes` | Serialized registry payload size. |
| `response_bytes` | Serialized response size. |
| `error` | Exception class name on failure. |
| `outcome` | `completed` or `failed`. |
| `total_ms` | End-to-end service time. |

### Query input and describe summaries

Events:

- `floecat.query_input_metadata.summary`
- `floecat.describe_inputs.summary`

These summarize input resolution, schema assembly, expansion metadata, obligations, and snapshot pins.

| Field | Meaning |
|---|---|
| `query_id` / `correlation_id` | Query and request correlation identifiers, when available. |
| `inputs` | Number of requested inputs. |
| `resolved_inputs` | Number of inputs resolved to relation metadata. |
| `snapshot_pins` | Number of snapshot pins produced. |
| `schemas` | Number of schemas returned by `DescribeInputs`. |
| `expansion_bytes` | Serialized expansion-map size. |
| `obligations` | Governance obligations produced. |
| `obligation_bytes` | Serialized governance obligation payload size. |
| `error` | Exception class name on failure. |
| `outcome` | `completed` or `failed`. |
| `total_ms` | End-to-end service time. |

### Planner stats summaries

Events:

- `floecat.planner_target_stats.summary`
- `floecat.planner_constraints.summary`

These summarize planner stats/constraint bundle RPCs.

| Field | Meaning |
|---|---|
| `query_id` / `correlation_id` | Query and request correlation identifiers. |
| `include_constraints` | Whether constraints were requested with target stats. |
| `requested_tables_raw` | Table entries in the raw request. |
| `requested_tables` | Normalized requested table count. |
| `requested_targets` | Requested stats target count. |
| `returned_targets` / `returned_tables` | Returned stats/constraint counts. |
| `not_found_targets` / `not_found_tables` | Missing target/table counts. |
| `error_targets` / `error_tables` | Per-target/table errors returned. |
| `max_results_per_chunk` | Server chunking limit. |
| `chunks` | Response chunks emitted on streaming paths. |
| `messages` | Response messages emitted on streaming paths. |
| `stream_ms` | Time spent streaming after setup. |
| `error` | Exception class name on failure. |
| `outcome` | `completed`, `cancelled`, or `failed`. |
| `total_ms` | End-to-end service time. |

### `floecat.metagraph.name_resolver.summary`

Emitted by name-resolution helpers around metagraph lookups.

| Field | Meaning |
|---|---|
| `correlation_id` / `account_id` | Request/account identifiers. |
| `found` | Whether a point lookup found a result. |
| `result_count` | Number of results for list-style lookups. |
| `error` | Exception class name on failure. |
| `outcome` | `completed` or `failed`. |
| `total_ms` | End-to-end resolver call time. |

## Design notes

- Summary event fields are intentionally low-cardinality. Do not add table names, namespace names,
  SQL strings, object URIs, or user-provided text.
- IDs such as `query_id`, `correlation_id`, and `table_id` are acceptable because they are trace
  diagnostics, not metric tags.
- If a field is generic across RPCs, add it to `floecat.rpc.summary` via the shared aggregator.
- If a field only makes sense inside one operation, add it to that operation's domain summary.
- If a phase performs network or storage I/O and frequently dominates latency, prefer a real span
  or the debug store-span path. For cheap CPU-only phases, prefer a timing field on the summary.
