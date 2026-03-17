# Minimal Iceberg REST Catalog Feasibility

This note evaluates whether Floecat should add a sibling subproject with a much smaller Iceberg REST catalog implementation rather than continuing to grow [`protocol-gateway/iceberg-rest`](/Users/markcusack/development/floe/metacat/protocol-gateway/iceberg-rest).

## Short answer

Yes. A sibling module is feasible, and it can still support meaningful existing coverage if the scope is cut aggressively.

The current module is broad:

- `135` production classes under [`protocol-gateway/iceberg-rest/src/main/java`](/Users/markcusack/development/floe/metacat/protocol-gateway/iceberg-rest/src/main/java)
- `44` unit-style tests under [`protocol-gateway/iceberg-rest/src/test/java`](/Users/markcusack/development/floe/metacat/protocol-gateway/iceberg-rest/src/test/java)
- `2` end-to-end integration test classes under [`protocol-gateway/iceberg-rest/src/it/java`](/Users/markcusack/development/floe/metacat/protocol-gateway/iceberg-rest/src/it/java)

The largest complexity drivers are not the core REST catalog contract. They are:

- Delta compatibility and metadata synthesis
- View support
- Scan planning, task fan-out, and credential vending
- Staged create flows
- Post-commit side effects and outbox handling
- OIDC-specific gateway behavior

## What the current code actually exposes

The advertised endpoint list in [`ConfigResource.java`](/Users/markcusack/development/floe/metacat/protocol-gateway/iceberg-rest/src/main/java/ai/floedb/floecat/gateway/iceberg/rest/resources/system/ConfigResource.java#L42) includes far more than the smallest useful REST catalog:

- namespace CRUD
- table CRUD, register, rename, commit
- multi-table transaction commit
- table planning, task fetch, credentials, metrics
- view CRUD, register, rename
- OAuth token endpoint stub

The main resource layer already shows the split:

- [`TableResource.java`](/Users/markcusack/development/floe/metacat/protocol-gateway/iceberg-rest/src/main/java/ai/floedb/floecat/gateway/iceberg/rest/resources/table/TableResource.java) handles core table paths plus `plan`, `tasks`, `credentials`, and `metrics`
- [`TableAdminResource.java`](/Users/markcusack/development/floe/metacat/protocol-gateway/iceberg-rest/src/main/java/ai/floedb/floecat/gateway/iceberg/rest/resources/table/TableAdminResource.java) handles rename and `/transactions/commit`

That split is useful for a minimal rewrite because the HTTP surface is already separable from much of the extra behavior.

## Minimum useful scope

A new sibling module should target the subset needed for real catalog interoperability and current smoke value:

- `GET /v1/config`
- namespace list/create/get/delete
- table list/create/get/delete/head
- table commit at `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}`
- table register
- `POST /v1/{prefix}/tables/rename`
- `POST /v1/{prefix}/transactions/commit`
- `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics` as a strict no-op validator if clients send it

Everything below should stay out of v1 of the minimal module:

- views
- `/plan`, `/tasks`, `/credentials`
- Delta compatibility
- staged create unless a real client proves it is required
- OIDC-specific behavior
- commit side effects that are not required for core catalog correctness

## Why this still has value

That subset is enough to preserve the important behavior exercised by current engine-facing paths:

- config discovery
- namespace lifecycle
- table load/list/register
- create/commit/delete/rename
- atomic transaction commit semantics

It also aligns with the current smoke setup more than the full module surface. The compose smoke wiring in [`tools/compose-smoke.sh`](/Users/markcusack/development/floe/metacat/tools/compose-smoke.sh) and [`docker/trino/catalog/floecat.properties`](/Users/markcusack/development/floe/metacat/docker/trino/catalog/floecat.properties) is centered on:

- attaching Trino and DuckDB to the REST catalog
- creating tables
- CTAS
- insert/delete/update/merge-like mutation flows
- rename
- namespace create/drop
- time travel via snapshot history

Those flows depend mainly on namespace, table load, commit, rename, and transaction commit. They do not depend on views, planning endpoints, or Delta translation.

## Useful existing tests to preserve

The current test suite is too large to use wholesale as the acceptance target for a minimal rewrite. A better approach is to define a retained slice.

Recommended retained tests:

- [`SystemResourceTest.java`](/Users/markcusack/development/floe/metacat/protocol-gateway/iceberg-rest/src/test/java/ai/floedb/floecat/gateway/iceberg/rest/resources/system/SystemResourceTest.java)
  - keep config response coverage
  - drop the OAuth endpoint expectation if the minimal module does not expose it
- [`NamespaceResourceTest.java`](/Users/markcusack/development/floe/metacat/protocol-gateway/iceberg-rest/src/test/java/ai/floedb/floecat/gateway/iceberg/rest/resources/namespace/NamespaceResourceTest.java)
  - keep list/create/get/delete
  - drop property mutation if not needed immediately
- [`TableResourceTest.java`](/Users/markcusack/development/floe/metacat/protocol-gateway/iceberg-rest/src/test/java/ai/floedb/floecat/gateway/iceberg/rest/resources/table/TableResourceTest.java)
  - keep list/get/create/delete/head/rename/metrics-no-op coverage
  - drop `plan`, `tasks`, credential loading, staging-specific tests
- [`TransactionCommitResourceTest.java`](/Users/markcusack/development/floe/metacat/protocol-gateway/iceberg-rest/src/test/java/ai/floedb/floecat/gateway/iceberg/rest/resources/table/TransactionCommitResourceTest.java)
  - keep as core acceptance for atomic commit wiring
- [`TableCommitResourceTest.java`](/Users/markcusack/development/floe/metacat/protocol-gateway/iceberg-rest/src/test/java/ai/floedb/floecat/gateway/iceberg/rest/resources/table/TableCommitResourceTest.java)
  - keep only if the minimal module keeps the single-table commit endpoint shape
- [`IcebergRestFixtureIT.java`](/Users/markcusack/development/floe/metacat/protocol-gateway/iceberg-rest/src/it/java/ai/floedb/floecat/gateway/iceberg/rest/IcebergRestFixtureIT.java)
  - keep namespace lifecycle
  - keep register-table flow
  - keep create/commit flows that materialize metadata
  - keep transaction commit atomicity/idempotency
  - drop view and stage-create scenarios initially

Tests that should not block the minimal module:

- all `services.compat.*` tests
- all `services.view.*` tests
- all `services.planning.*` tests
- OIDC integration tests
- staging repository tests unless stage-create is explicitly included

## Suggested sibling module shape

Add a sibling module under [`protocol-gateway/pom.xml`](/Users/markcusack/development/floe/metacat/protocol-gateway/pom.xml):

- `protocol-gateway/iceberg-rest-minimal`

Suggested package shape:

```text
protocol-gateway/iceberg-rest-minimal/
  src/main/java/.../rest/minimal/
    api/
    resources/
    support/
```

The goal should be a deliberately flat design:

- `ConfigResource`
- `NamespaceResource`
- `TableResource`
- `TableAdminResource`
- `CatalogContextResolver`
- `GrpcCatalogClient` or a very small set of direct gRPC adapters
- `CommitTranslator`
- `MetadataMapper`

Avoid recreating the current service taxonomy unless a class earns its existence.

## Design constraints for the minimal implementation

The new module should optimize for directness:

- translate HTTP request DTOs directly into gRPC requests
- keep resource methods thin but not ceremonial
- prefer a few concrete helpers over many service layers
- keep post-commit behavior out of scope unless required for correctness
- treat unsupported features as truly unsupported, not half-implemented

The minimal module should also advertise only the endpoints it really implements. That means its `GET /v1/config` response must not claim support for views, planning, or other omitted paths.

## Main risk

The main risk is not technical viability. It is choosing the wrong acceptance boundary.

If the minimal module is forced to preserve:

- Delta compatibility
- staged create
- full view support
- planning/task consumption
- OIDC-specific gateway behavior

then it will quickly converge back toward the current design.

The only credible way to keep it minimal is to make the reduced feature set explicit and to gate retained tests accordingly.

## Recommended rollout

1. Add `protocol-gateway/iceberg-rest-minimal` as a sibling module.
2. Start with config, namespace CRUD, table CRUD/load/register, rename, transaction commit, and metrics no-op.
3. Create a dedicated retained test suite for the minimal module instead of reusing all existing tests unchanged.
4. Point one smoke target at the minimal module.
5. Add features back only when a client or smoke flow proves they are required.

## Conclusion

Creating a minimal sibling implementation is practical and likely worthwhile.

The current module is carrying multiple orthogonal concerns. A reduced module can stay small if it is treated as:

- a protocol adapter for the core REST catalog contract
- not the home for Delta compatibility
- not the home for views and planning unless those are explicitly needed
- not the place for every backend side effect

If this path is taken, the key engineering decision is to preserve only the tests that protect external catalog behavior, not the tests that lock in the current internal decomposition.
