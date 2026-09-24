# Generic Engine RPC Contract

The external catalog surface an engine adapter binds against. Relation reads are kind-neutral, so a
client lists, resolves, and describes tables and views without choosing between them first.
Mutation stays resource-specific because table and view specs differ.

## Generic Engine Needs

| Engine need | Floecat endpoint | Notes |
|-------------|------------------|-------|
| List catalogs | `CatalogService.ListCatalogs` | |
| List schemas/namespaces | `NamespaceService.ListNamespaces` | |
| List tables and views together | `RelationService.ListRelations` | Metadata/topology only by default. Does not require a queryable snapshot. |
| Resolve a table or view by name | `RelationService.ResolveRelations` | Batch, one result per logical reference, with search-path candidates tried in order. |
| Describe a table or view by id | `RelationService.GetRelation` | Set `include_schema` when the caller needs columns. |
| Pin query inputs / snapshot-aware schema | `QuerySchemaService.DescribeInputs`, `UserObjectsService.GetUserObjects` | Query-context APIs. Pinned/current/as-of behavior belongs here. |
| Create/update/delete tables | `TableService` | |
| Create/update/delete views | `ViewService` | |
| Fetch functions/types/operators/casts/collations/aggregates | `SqlCatalogService.GetSqlObjectsRegistry` | Serves the `SystemObjectsRegistry` envelope. |

## Engine Surface And Management Surface

`RelationService`, `SqlCatalogService`, `CatalogService.ListCatalogs` and
`NamespaceService.ListNamespaces` are the engine surface. An adapter binds against these and needs
nothing else to browse, resolve and describe.

`CatalogService.GetCatalog`, catalog mutations, `NamespaceService.GetNamespace`, namespace
mutations, `TableService`, `ViewService`, `DirectoryService`, `SnapshotService`,
`TableStatisticsService`, `TableIndexService` and `TableConstraintsService` are the management
surface. They carry
per-resource fields an engine does not bind against — creation timestamps, descriptions, catalog
and namespace ids, write specs — and the CLI and control plane use them. `Relation` does not carry
those fields, which is why the two surfaces stay separate rather than one folding into the other.
In particular, `TableService.ListTables/GetTable` and `ViewService.ListViews/GetView` remain
management APIs for callers that need the typed resource records; engine adapters use
`RelationService.ListRelations/GetRelation` instead. The CLI's typed view listing is deliberate
because it prints management metadata such as `created_at`.

`SnapshotService.GetSnapshotSchema` answers the schema of one table as of one snapshot. Generic
relation callers use `RelationService.GetRelation(include_schema=true)` for the current schema, and
query planners use `QuerySchemaService.DescribeInputs` or `UserObjectsService.GetUserObjects` when
pinned snapshot behavior is required.

## Adapter Recipe

For catalog browsing and name binding, an engine adapter can use this sequence:

1. Call `CatalogService.ListCatalogs`.
2. Call `NamespaceService.ListNamespaces` for the selected catalog or namespace. Walk pages until
   the opaque token is empty; set `recursive` when the engine wants the whole namespace tree.
3. Call `RelationService.ListRelations` for a namespace or catalog. Leave `include_schema`,
   `include_status`, and `include_total` off unless the engine needs those costs. Consume
   `ListRelationsResponse.results` in order; each entry is either a relation or a row-level error.
4. Call `RelationService.ResolveRelations` when binding ordered name candidates from a search path,
   or `RelationService.GetRelation` when the resource id is already known.
5. Start the query lifecycle and use `QuerySchemaService.DescribeInputs` or
   `UserObjectsService.GetUserObjects` only when planning requires pinned snapshot state.

`ResolveRelations` and `GetUserObjects` intentionally accept different candidate shapes. The former
is a catalog bind: send one `RelationReference` per logical input and put ordered `NameRef`
candidates in its `candidates` field. The latter is a query bind: send one `QueryInput` per logical
input to `GetUserObjectsRequest.tables`; its candidates can carry ids, names, and snapshot
overrides, and successful inputs become pinned. The `tables` field is historical naming for relation
inputs, not a requirement that the engine support tables only. In short: use `NameRef` candidates
for unpinned name resolution, and `QueryInput` candidates when requesting the pinned planner bundle.

## Relation RPCs

`ListRelationsRequest`
- `namespace_id` or `catalog_id`: the scope, exactly as on `ListNamespacesRequest`. A catalog scope
  starts at its top-level namespaces.
- `recursive`: also lists namespaces below the scope. Nested namespaces are otherwise invisible to
  a listing, so an adapter walking a hierarchy sets this.
- `kinds`: filter over `RK_TABLE` and `RK_VIEW`; empty means both. Any other kind is rejected with
  `INVALID_ARGUMENT`.
- `include_schema`: hydrates current relation metadata, including `Relation.schema`, properties and
  kind-specific details. When false, the relation is identity-only: details and properties are
  absent, and `resource_id.kind` is the table/view discriminator.
- `include_status`: populates `Relation.status`, at the cost of one current-snapshot read per
  relation.
- `include_total`: populates `page.total_size`, at the cost of one count per namespace in scope on
  the first page. The three `include_*` flags are the things that cost; none is on by default.
- `page`: ordinary `PageRequest`. Namespaces page in qualified-path order, tables before views
  within each. The token is opaque and bound to the account, scope, recursive flag, kind filter,
  response flags, and listing semantics that minted it; replaying it under a different request
  scope is rejected. It keysets on the namespace path, so a namespace removed between pages does
  not void it, and carries the total so only the first page counts.

`ListRelationsResponse`
- `results` is the canonical ordered page. It contains one entry for every discovered relation, in
  source order; each entry is either a hydrated relation or a row-level error. Errors identify the
  relation and carry the structured `Error`, so clients cannot silently turn a partial page into a
  complete catalog.
- Errors consume page space, so a client must advance the opaque page token even when a page has no
  successful relations.
- Request, cancellation and backend failures still fail the whole RPC, and so does authorization
  on the request: read is checked per relation kind the request can return, before the walk, so a
  caller without `table.read` gets a status rather than a page of errors. `RelationListResult.error`
  is only for a failure isolated to one relation, including one the caller may not read.
- An adapter that cannot represent partial catalog results must fail its own listing when it sees a
  row error. An adapter with a warning-capable UX, such as the CLI, may continue only after showing
  the affected names and preserving the continuation token.

`ResolveRelationsRequest`
- `references`: one entry per logical reference, each carrying the names to try in order — the same
  shape `query.TableReferenceCandidate` uses for planning. A caller expands its search path into
  candidates instead of sending the cross product of names and namespaces and re-applying
  precedence to the results, so the batch stays one entry per SQL table reference.
- Matching is exact, for a builtin relation as much as a user one, so `orders` and `ORDERS`
  are different names and only the stored spelling resolves. A caller that accepts either
  spelling folds case itself and sends the folded name.
- The total candidate count is bounded by `floecat.relation.resolve.max-names`. This is a bind-time
  batch, not an enumeration path; use `ListRelations` to walk a catalog.
- `include_schema`: uses the same hydration semantics as `ListRelations`; without it, details and
  properties are absent and callers classify the relation from `resource_id.kind`.
- `include_status`: populates `Relation.status`.

`ResolveRelationsResponse`
- One `ResolveRelationResult` per reference, in request order.
- `resolved_name` is the candidate that won; the first candidate that resolves wins.
- A reference where no candidate resolves carries an `Error` in its result rather than failing the
  RPC, and so does a candidate that resolves but cannot be read for a reason that belongs to that
  relation. The `Error` is the one the failure carries, with its code, message key and params.
- `MC_NOT_FOUND` is the only result error that means absence. Adapters may return their native
  "not found" value for it. `MC_PERMISSION_DENIED`, `MC_INVALID_ARGUMENT`, `MC_INTERNAL` and all
  other codes mean the relation was unreadable or the response was invalid; adapters must surface
  or propagate them instead of treating the relation as missing. Core clients can use the shared
  `RelationResults.requireResolved` helper so this distinction is the default behavior.
- A failure that belongs to the request rather than a relation, such as an unavailable backend,
  fails the whole call. A broken backend is not reported as every relation being missing.

`GetRelationRequest`
- `relation_id`: `RK_TABLE` or `RK_VIEW`.
- `include_schema`, `include_status`: as above. An identity-only response never sets the details
  oneof, so an empty `table` or `view` payload is not used to mean "not loaded".

`Relation`
- Common fields: `resource_id`, `name`, `display_name`, `origin`, `schema`, `status`, `properties`.
- The kind is always `resource_id.kind`; the `details` oneof carries the kind-specific payload only
  when hydration was requested.
- `origin` is `query.Origin`, the same builtin-vs-user distinction SQL objects use. Listings merge
  both kinds, so a client that treats them differently reads this rather than the name.
- `name` is the fully qualified `NameRef`; `display_name` is the leaf.
- `table` details carry `UpstreamRef` (format and partition keys live on it) and `schema_json`.
- `view` details carry `ViewSqlDefinition`, base relation names and the creation search path.

`GetUserObjects` returns canonical recursive `types.LogicalType` values in `ColumnInfo`. The SQL
object registry keeps `NameRef` type symbols for engine-defined functions, operators, and builtin
types because those names may not have a Floecat logical-type equivalent.

`RelationStatus.queryability`
- `Q_QUERYABLE` for system relations and for views, which have no relation-local snapshot of their
  own. This does not assert that every base relation of a view is currently pinnable; the query
  path evaluates dependency readiness.
- For a user table, `Q_QUERYABLE` with `current_snapshot_id` once it has a committed current
  snapshot, otherwise `Q_NOT_QUERYABLE_NO_SNAPSHOT`. That pointer read reports the committed
  selection and does not pin.
- `Q_UNSPECIFIED` means the request did not ask; `Q_UNKNOWN` means it asked and `reason` says why
  the answer is undecidable.

## Code Hooks

- Protos: `core/proto/src/main/proto/floecat/catalog/relation.proto` and
  `core/proto/src/main/proto/floecat/query/sql_catalog.proto`. The whole surface is mapped by
  audience in `core/proto/README.md`.
- `CatalogGraphView.resolveNames(...)` is the graph-level hook behind relation resolution.
- `CatalogSurfaceRelations` drives `CatalogSurfaceRelationPager` over the same table and view page
  sources the typed listings use, so both return the same rows under the same account scoping.
- Recursive and catalog-wide relation listing currently obtains the namespace references eagerly
  from the graph view before walking relation pages. This is an internal scalability trade-off,
  not a client contract requirement; a paged namespace-reference SPI can be introduced later
  without changing the RPC.
- `RelationServiceImpl` is the gRPC entrypoint for generic relation reads.
- `SqlCatalogServiceImpl` is the gRPC entrypoint for SQL object registry reads.
- Engine clients should use the shared `RelationResults` helper. Its `results` view preserves wire
  order; `relations()` and `errors()` are convenience projections. Use the strict completion check
  when the host API cannot return warnings or partial-list status.

## Pinned Behavior

Listing and relation resolution do not pin snapshots and do not require a queryable snapshot. They
answer catalog topology questions.

Pinning is query-context specific:
- `QuerySchemaService.DescribeInputs` resolves schemas under the active query context.
- `UserObjectsService.GetUserObjects` streams planner bundles with relation, schema, stats, and
  constraint data.
- `CatalogGraphView.tablePinFor(...)` is the lower-level graph hook for table pins.

`GetUserObjects` keeps query-level failures as stream failures: an unknown or inactive query,
authorization failure, cancellation, malformed request, or unavailable backend cannot produce a
usable planning result. A relation-scoped pin, schema, or decoration failure is emitted as that
input's `RelationResolution.ERROR`; other inputs in the same bundle continue and successful pins
are committed together. This is the planning equivalent of `ResolveRelations`' per-reference
errors, while preserving the snapshot pin as the unit of consistency for every successful input.

The separation keeps generic catalog browsing cheap and confines the snapshot contract to the
planner path.

## Legacy Pin Projections

`query.pinning.RelationPinSet` and `TablePin` are the canonical server-side query pin model.
`query.lifecycle.SnapshotPin` and `SnapshotSet` remain narrow wire projections for the legacy query
descriptor and lifecycle payloads. They are populated from canonical pins and do not define the
generic engine read contract; obligations, schema resolution, statistics lookup, and reconciliation
use canonical pins directly.
