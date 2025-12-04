# Iceberg REST Restructuring Plan

This document captures the target package layout for the `protocol-gateway/iceberg-rest`
module and the incremental steps required to reach it.

## Target Package Layout

```
ai.floedb.metacat.gateway.iceberg.rest
├── api
│   ├── dto          // response DTOs (LoadTableResultDto, CommitTableResponseDto, etc.)
│   ├── request      // TableRequests, PlanRequests, SnapshotRequests, ViewRequests, TaskRequests
│   ├── metadata     // TableMetadataView, ViewMetadataView, helper view classes
│   └── error        // IcebergError, IcebergErrorResponse
├── resources
│   ├── table        // TableResource, TableAdminResource
│   ├── namespace    // NamespaceResource
│   ├── view         // ViewResource, ViewAdminResource
│   ├── snapshot     // SnapshotResource, PartitionSpecResource, SchemaResource, StatsResource
│   └── system       // ConfigResource
├── services
│   ├── catalog      // TableGatewaySupport, StageCommitProcessor, StageCommitException
│   ├── metadata     // MetadataMirrorService, MetadataMirrorException
│   ├── staging      // StagedTableService, StagedTableRepository, StagedTableEntry, StageState, StagedTableKey
│   ├── resolution   // NameResolution, NamespacePaths
│   └── tenant       // TenantContext
├── support
│   ├── mapper       // TableResponseMapper (to be broken up), ViewResponseMapper, future mappers
│   ├── filter       // TenantHeaderFilter
│   ├── serialization// NamespaceListDeserializer
│   └── util         // helper types that do not fit the above buckets
└── config           // CatalogConfigDto, MetricsRequests, etc. that describe module configuration
```

## Migration Steps

1. **Introduce package directories**  
   - Create `api`, `resources`, `services`, `support`, and `config` directories under
     `ai/floedb/metacat/gateway/iceberg/rest`.
   - Move one representative file at a time (e.g., `TableRequests` → `api/request`),
     update its `package` declaration, and fix imports.
   - Commit in small batches per area (e.g., move all request DTOs together).

2. **DTO Consolidation**  
   - Group load/commit/view DTOs in `api/dto`.  
   - Retire legacy DTOs that are no longer part of the REST contract (e.g., `TableDto`,
     `NamespaceDto`) or move them if still needed internally.  
   - Ensure `README.md` references the new canonical DTO package.

3. **Resource Packaging**  
   - Move the JAX-RS resource classes into the `resources` subpackages listed above.
   - Co-locate unit tests with the new package (folder moves only; no test logic changes).

4. **Service Packaging**  
   - Move `TableGatewaySupport`, `MetadataMirrorService`, `StageCommitProcessor`, starch
     related helpers, and the staging repository into `services/...`.  
   - Extract interfaces where helpful (e.g., metadata mirroring interface) so resources
     depend on abstractions rather than concrete classes.

5. **Support & Utilities**  
   - Move response mappers, Jackson helpers, filters, and minor DTOs that pertain strictly
     to support logic into `support/...`.
   - Use this step to start breaking down `TableResponseMapper` (see below).

6. **Build & Verification**  
   - After each batch, run `mvn -pl protocol-gateway/iceberg-rest -DskipITs -DskipTests clean compile`.
   - Update Quarkus `application.properties` or CDI configuration only if package names are
     referenced in configuration.

## TableResponseMapper Refactor Outline

1. **Extract Composition Helpers**  
   - `SchemaMapper` for schema/spec/write order normalization.  
   - `SnapshotMapper` for snapshot, statistics, metadata-log rendering.

2. **Separate API Use Cases**  
   - `TableLoadMapper` builds `LoadTableResultDto`.  
   - `TableCommitMapper` builds `CommitTableResponseDto`.  
   - `StageResponseMapper` builds `StageCreateResponseDto`.

3. **Shared Metadata Utilities**  
   - Create `TableMetadataBuilder` that applies metadata-location, property defaults,
     and location resolution used during both load and commit responses.

4. **Unit Tests**  
   - Introduce focused tests per mapper class to validate schema/spec serialization,
     property merging, and snapshot filtering without spinning up REST resources.

Completing these steps will leave the module with clear layering and smaller, easier to
maintain mappers, ready for future enhancements.
