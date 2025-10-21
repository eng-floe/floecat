package ai.floedb.metacat.service.catalog.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.catalog.rpc.CreateCatalogRequest;
import ai.floedb.metacat.catalog.rpc.CreateCatalogResponse;
import ai.floedb.metacat.catalog.rpc.CreateNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.CreateNamespaceResponse;
import ai.floedb.metacat.catalog.rpc.CreateSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.CreateSnapshotResponse;
import ai.floedb.metacat.catalog.rpc.CreateTableRequest;
import ai.floedb.metacat.catalog.rpc.CreateTableResponse;
import ai.floedb.metacat.catalog.rpc.DeleteCatalogRequest;
import ai.floedb.metacat.catalog.rpc.DeleteCatalogResponse;
import ai.floedb.metacat.catalog.rpc.DeleteNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.DeleteNamespaceResponse;
import ai.floedb.metacat.catalog.rpc.DeleteSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.DeleteSnapshotResponse;
import ai.floedb.metacat.catalog.rpc.DeleteTableRequest;
import ai.floedb.metacat.catalog.rpc.DeleteTableResponse;
import ai.floedb.metacat.catalog.rpc.MoveTableRequest;
import ai.floedb.metacat.catalog.rpc.MoveTableResponse;
import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.catalog.rpc.ResourceMutation;
import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.UpdateCatalogRequest;
import ai.floedb.metacat.catalog.rpc.UpdateCatalogResponse;
import ai.floedb.metacat.catalog.rpc.UpdateTableSchemaRequest;
import ai.floedb.metacat.catalog.rpc.UpdateTableSchemaResponse;
import ai.floedb.metacat.catalog.rpc.RenameNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.RenameNamespaceResponse;
import ai.floedb.metacat.catalog.rpc.RenameTableRequest;
import ai.floedb.metacat.catalog.rpc.RenameTableResponse;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.catalog.util.MutationOps;
import ai.floedb.metacat.service.common.BaseServiceImpl;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.impl.CatalogRepository;
import ai.floedb.metacat.service.repo.impl.NamespaceRepository;
import ai.floedb.metacat.service.repo.impl.SnapshotRepository;
import ai.floedb.metacat.service.repo.impl.TableRepository;
import ai.floedb.metacat.service.repo.util.BaseRepository;
import ai.floedb.metacat.service.repo.util.Keys;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import ai.floedb.metacat.service.storage.IdempotencyStore;
import ai.floedb.metacat.service.storage.PointerStore;
import ai.floedb.metacat.service.storage.util.IdempotencyGuard;

@GrpcService
public class ResourceMutationImpl extends BaseServiceImpl implements ResourceMutation {
  @Inject CatalogRepository catalogs;
  @Inject NamespaceRepository namespaces;
  @Inject TableRepository tables;
  @Inject SnapshotRepository snapshots;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject PointerStore ptr;
  @Inject IdempotencyStore idempotencyStore;

  @Override
  public Uni<CreateCatalogResponse> createCatalog(CreateCatalogRequest request) {
    return mapFailures(runWithRetry(() -> {
      final var principalContext = principal.get();
      final var correlationId = principalContext.getCorrelationId();
      final var tenantId = principalContext.getTenantId();

      authz.require(principalContext, "catalog.write");

      final var idempotencyKey = request.hasIdempotency()
          ? request.getIdempotency().getKey() : "";

      final byte[] fingerprint = request.getSpec().toBuilder()
          .clearDescription().build().toByteArray();

      var tsNow = nowTs();

      var catalogProto = MutationOps.createProto(
          tenantId,
          "CreateCatalog",
          idempotencyKey,
          () -> fingerprint,
          () -> {
            String catalogUuid =
                !idempotencyKey.isBlank()
                    ? deterministicUuid(tenantId, "catalog", idempotencyKey)
                    : UUID.randomUUID().toString();

            var catalogId = ResourceId.newBuilder()
                .setTenantId(tenantId)
                .setId(catalogUuid)
                .setKind(ResourceKind.RK_CATALOG)
                .build();

            var built = Catalog.newBuilder()
                .setResourceId(catalogId)
                .setDisplayName(mustNonEmpty(
                    request.getSpec().getDisplayName(), "display_name", correlationId))
                .setDescription(request.getSpec().getDescription())
                .setCreatedAt(tsNow)
                .build();

            try {
              catalogs.create(built);
            } catch (BaseRepository.NameConflictException e) {
              if (!idempotencyKey.isBlank()) {
                var existing = catalogs.getByName(tenantId, built.getDisplayName());
                if (existing.isPresent()) {
                  return new IdempotencyGuard.CreateResult<>(
                      existing.get(), existing.get().getResourceId());
                }
              }
              throw GrpcErrors.conflict(
                  correlationId, "catalog.already_exists",
                      Map.of("display_name", built.getDisplayName()));
            }

            return new IdempotencyGuard.CreateResult<>(built, catalogId);
          },
          (catalog) -> catalogs.metaFor(catalog.getResourceId(), tsNow),
          idempotencyStore,
          tsNow,
          IDEMPOTENCY_TTL_SECONDS,
          this::correlationId,
          Catalog::parseFrom
      );

      return CreateCatalogResponse.newBuilder()
          .setCatalog(catalogProto.body)
          .setMeta(catalogProto.meta)
          .build();
    }), correlationId());
  }

  @Override
  public Uni<UpdateCatalogResponse> updateCatalog(UpdateCatalogRequest request) {
    return mapFailures(runWithRetry(() -> {
      var principalContext = principal.get();
      var correlationId = principalContext.getCorrelationId();

      authz.require(principalContext, "catalog.write");

      var tsNow = nowTs();

      var catalogId = request.getCatalogId();
      ensureKind(catalogId, ResourceKind.RK_CATALOG, "catalog_id", correlationId);

      var prev = catalogs.getById(catalogId)
          .orElseThrow(() -> GrpcErrors.notFound(
                correlationId, "catalog", Map.of("id", catalogId.getId())));

      var desiredName = mustNonEmpty(
          request.getSpec().getDisplayName(), "display_name", correlationId);
      var desiredDescription = request.getSpec().getDescription();

      var meta = catalogs.metaFor(catalogId, tsNow);
      long expectedVersion = meta.getPointerVersion();
      enforcePreconditions(correlationId, meta, request.getPrecondition());

      if (desiredName.equals(prev.getDisplayName())
          && Objects.equals(desiredDescription, prev.getDescription())) {
        return UpdateCatalogResponse.newBuilder().setCatalog(prev).setMeta(meta).build();
      }

      if (!desiredName.equals(prev.getDisplayName())) {
        try {
          catalogs.rename(principalContext.getTenantId(), catalogId, desiredName, expectedVersion);
        } catch (BaseRepository.NameConflictException nce) {
          throw GrpcErrors.conflict(correlationId, "catalog.already_exists",
              Map.of("display_name", desiredName));
        } catch (BaseRepository.PreconditionFailedException pfe) {
          var nowMeta = catalogs.metaFor(catalogId, tsNow);
          throw GrpcErrors.preconditionFailed(correlationId, "version_mismatch",
                Map.of("expected", Long.toString(expectedVersion),
                    "actual", Long.toString(nowMeta.getPointerVersion())));
        }

        if (!Objects.equals(desiredDescription, prev.getDescription())) {
          meta = catalogs.metaFor(catalogId, tsNow);
          expectedVersion = meta.getPointerVersion();
          var renamedObj = catalogs.getById(catalogId).orElse(prev);
          var withDesc = renamedObj.toBuilder().setDescription(desiredDescription).build();

          try {
            catalogs.update(withDesc, meta.getPointerVersion());
          } catch (BaseRepository.PreconditionFailedException pfe) {
            var nowMeta = catalogs.metaFor(catalogId, tsNow);
            throw GrpcErrors.preconditionFailed(correlationId, "version_mismatch",
                Map.of("expected", Long.toString(expectedVersion),
                    "actual", Long.toString(nowMeta.getPointerVersion())));
          }

        }
      } else {
        meta = catalogs.metaFor(catalogId, tsNow);
        expectedVersion = meta.getPointerVersion();
        var updated = prev.toBuilder().setDescription(desiredDescription).build();

        try {
          catalogs.update(updated, meta.getPointerVersion());
        } catch (BaseRepository.PreconditionFailedException pfe) {
          var nowMeta = catalogs.metaFor(catalogId, tsNow);
          throw GrpcErrors.preconditionFailed(correlationId, "version_mismatch",
              Map.of("expected", Long.toString(expectedVersion),
                  "actual", Long.toString(nowMeta.getPointerVersion())));
        }
      }

      return UpdateCatalogResponse.newBuilder()
          .setCatalog(catalogs.getById(catalogId).orElse(prev))
          .setMeta(catalogs.metaFor(catalogId, tsNow))
          .build();
    }), correlationId());
  }

  @Override
  public Uni<DeleteCatalogResponse> deleteCatalog(DeleteCatalogRequest request) {
    return mapFailures(runWithRetry(() -> {
      var principalContext = principal.get();
      var correlationId = principalContext.getCorrelationId();

      authz.require(principalContext, "catalog.write");

      var tsNow = nowTs();

      var catalogId = request.getCatalogId();
      ensureKind(catalogId, ResourceKind.RK_CATALOG, "catalog_id", correlationId);

      var key = Keys.catPtr(catalogId.getTenantId(), catalogId.getId());
      if (ptr.get(key).isEmpty()) {
        catalogs.delete(catalogId);
        var safe = catalogs.metaForSafe(catalogId, tsNow);
        return DeleteCatalogResponse.newBuilder().setMeta(safe).build();
      }

      if (request.getRequireEmpty() && namespaces.countUnderCatalog(catalogId) > 0) {
        var currentNamespace = catalogs.getById(catalogId).orElse(null);
        var displayName = (currentNamespace != null && !currentNamespace.getDisplayName().isBlank())
            ? currentNamespace.getDisplayName() : catalogId.getId();
        throw GrpcErrors.conflict(
            correlationId, "catalog.not_empty", Map.of("display_name", displayName));
      }

      var meta = catalogs.metaFor(catalogId, tsNow);
      long expectedVersion = meta.getPointerVersion();
      enforcePreconditions(correlationId, meta, request.getPrecondition());

      try {
        catalogs.deleteWithPrecondition(catalogId, expectedVersion);
      } catch (BaseRepository.PreconditionFailedException pfe) {
        var nowMeta = catalogs.metaFor(catalogId, tsNow);
        throw GrpcErrors.preconditionFailed(correlationId, "version_mismatch",
            Map.of("expected", Long.toString(expectedVersion),
                "actual", Long.toString(nowMeta.getPointerVersion())));
      } catch (BaseRepository.NotFoundException nfe) {
        throw GrpcErrors.notFound(correlationId, "catalog",
            Map.of("id", catalogId.getId()));
      }

      return DeleteCatalogResponse.newBuilder().setMeta(meta).build();
    }), correlationId());
  }

  @Override
  public Uni<CreateNamespaceResponse> createNamespace(CreateNamespaceRequest request) {
    return mapFailures(runWithRetry(() -> {
      var principalContext = principal.get();
      var correlationId = principalContext.getCorrelationId();

      authz.require(principalContext, "namespace.write");

      var tsNow = nowTs();

      var tenantId = principalContext.getTenantId();
      var idempotencyKey = request.hasIdempotency() ? request.getIdempotency().getKey() : "";
      byte[] fingerprint = request.getSpec().toBuilder().clearDescription().build().toByteArray();

      var namespaceProto = MutationOps.createProto(
          tenantId,
          "CreateNamespace",
          idempotencyKey,
          () -> fingerprint,
          () -> {
            String namespaceUuid =
                !idempotencyKey.isBlank()
                    ? deterministicUuid(tenantId, "namespace", idempotencyKey)
                    : UUID.randomUUID().toString();

            var namespaceId = ResourceId.newBuilder()
                .setTenantId(tenantId)
                .setId(namespaceUuid)
                .setKind(ResourceKind.RK_NAMESPACE)
                .build();

            if (catalogs.getById(request.getSpec().getCatalogId()).isEmpty()) {
              throw GrpcErrors.notFound(correlationId, "catalog",
                  Map.of("id", request.getSpec().getCatalogId().getId()));
            }

            var built = Namespace.newBuilder()
                .setResourceId(namespaceId)
                .setDisplayName(mustNonEmpty(
                    request.getSpec().getDisplayName(), "display_name", correlationId))
                .addAllParents(request.getSpec().getPathList())
                .setDescription(request.getSpec().getDescription())
                .setCreatedAt(tsNow)
                .build();

            try {
              namespaces.create(built, request.getSpec().getCatalogId());
            } catch (BaseRepository.NameConflictException e) {
              if (!idempotencyKey.isBlank()) {
                var fullPath = new ArrayList<>(request.getSpec().getPathList());
                fullPath.add(request.getSpec().getDisplayName());
                var existingId = namespaces.getByPath(
                    tenantId, request.getSpec().getCatalogId(), fullPath);
                if (existingId.isPresent()) {
                  var existing = namespaces.get(request.getSpec().getCatalogId(), existingId.get());
                  if (existing.isPresent()) {
                    return new IdempotencyGuard.CreateResult<>(existing.get(), existingId.get());
                  }
                }
              }
              var pretty = prettyNamespacePath(
                  request.getSpec().getPathList(), request.getSpec().getDisplayName());

              throw GrpcErrors.conflict(
                  correlationId, "namespace.already_exists",
                  Map.of("catalog", request.getSpec().getCatalogId().getId(),
                        "path", pretty));
            }

            return new IdempotencyGuard.CreateResult<>(built, namespaceId);
          },
          (namespace) -> namespaces.metaFor(
              request.getSpec().getCatalogId(), namespace.getResourceId(), tsNow),
          idempotencyStore,
          tsNow,
          IDEMPOTENCY_TTL_SECONDS,
          this::correlationId,
          Namespace::parseFrom
      );

      return CreateNamespaceResponse.newBuilder()
          .setNamespace(namespaceProto.body)
          .setMeta(namespaceProto.meta)
          .build();
    }), correlationId());
  }

  @Override
  public Uni<RenameNamespaceResponse> renameNamespace(RenameNamespaceRequest request) {
    return mapFailures(runWithRetry(() -> {
      var principalContext = principal.get();
      var correlationId = principalContext.getCorrelationId();

      authz.require(principalContext, "namespace.write");

      var tsNow = nowTs();

      var namespaceId = request.getNamespaceId();
      ensureKind(namespaceId, ResourceKind.RK_NAMESPACE, "namespace_id", correlationId);

      var tenantId = principalContext.getTenantId();

      var currentCatalogId = namespaces.findOwnerCatalog(tenantId, namespaceId.getId())
          .orElseThrow(() -> GrpcErrors.notFound(
              correlationId, "namespace", Map.of("id", namespaceId.getId())));

      Namespace currentNamespace = namespaces.get(currentCatalogId, namespaceId).orElseThrow(() ->
          GrpcErrors.notFound(correlationId, "namespace", Map.of("id", namespaceId.getId())));

      var pathProvided = !request.getNewPathList().isEmpty();
      var newLeaf = pathProvided
          ? request.getNewPath(request.getNewPathCount() - 1)
          : (request.getNewDisplayName().isBlank()
              ? currentNamespace.getDisplayName() : request.getNewDisplayName());

      var currentParents = currentNamespace.getParentsList();
      var newParents = pathProvided
          ? List.copyOf(request.getNewPathList().subList(0, request.getNewPathCount() - 1))
          : currentParents;

      var targetCatalogId = (request.hasNewCatalogId()
          && !request.getNewCatalogId().getId().isBlank())
              ? request.getNewCatalogId()
              : currentCatalogId;

      if (catalogs.getById(targetCatalogId).isEmpty()) {
        throw GrpcErrors.notFound(correlationId, "catalog", Map.of("id", targetCatalogId.getId()));
      }

      var sameCatalog = targetCatalogId.getId().equals(currentCatalogId.getId());
      var sameLeaf = newLeaf.equals(currentNamespace.getDisplayName());
      var sameParents = Objects.equals(currentParents, newParents);

      var meta = namespaces.metaFor(currentCatalogId, namespaceId, tsNow);
      long expectedVersion = meta.getPointerVersion();
      enforcePreconditions(correlationId, meta, request.getPrecondition());

      if (sameCatalog && sameLeaf && sameParents) {
        return RenameNamespaceResponse.newBuilder()
            .setNamespace(currentNamespace).setMeta(meta).build();
      }

      var updated = currentNamespace.toBuilder().setDisplayName(newLeaf)
          .clearParents().addAllParents(newParents).build();

      try {
        namespaces.renameOrMove(
            updated,
            currentCatalogId,
            currentParents,
            currentNamespace.getDisplayName(),
            targetCatalogId,
            newParents,
            expectedVersion);
      } catch (BaseRepository.NameConflictException e) {
        var parts = new ArrayList<>(newParents);
        parts.add(newLeaf);
        var pretty = String.join("/", parts);
        throw GrpcErrors.conflict(
            correlationId, "namespace.already_exists",
                Map.of("catalog", targetCatalogId.getId(),
                    "path", pretty));
      } catch (BaseRepository.PreconditionFailedException pfe) {
        var nowMeta = namespaces.metaFor(currentCatalogId, namespaceId, tsNow);
        throw GrpcErrors.preconditionFailed(
            correlationId, "version_mismatch",
            Map.of("expected", Long.toString(expectedVersion),
                "actual", Long.toString(nowMeta.getPointerVersion())));
      }

      var outMeta = namespaces.metaFor(targetCatalogId, namespaceId, tsNow);
      return RenameNamespaceResponse.newBuilder()
          .setNamespace(updated)
          .setMeta(outMeta).build();
    }), correlationId());
  }

  @Override
  public Uni<DeleteNamespaceResponse> deleteNamespace(DeleteNamespaceRequest request) {
    return mapFailures(runWithRetry(() -> {
      var principalContext = principal.get();
      var correlationId = principalContext.getCorrelationId();

      authz.require(principalContext, "namespace.write");

      var tsNow = nowTs();

      final var tenantId = principalContext.getTenantId();
      final var namespaceId = request.getNamespaceId();
      ensureKind(namespaceId, ResourceKind.RK_NAMESPACE, "namespace_id", correlationId);

      var catalogId = namespaces.findOwnerCatalog(tenantId, namespaceId.getId())
          .orElse(null);

      if (catalogId == null) {
        var safe = namespaces.metaForSafe(ResourceId.newBuilder()
            .setTenantId(tenantId)
            .setId("_unknown_")
            .setKind(ResourceKind.RK_CATALOG)
            .build(), namespaceId, tsNow);
        return DeleteNamespaceResponse.newBuilder().setMeta(safe).build();
      }

      if (request.getRequireEmpty() && tables.countUnderNamespace(catalogId, namespaceId) > 0) {
        var currentNamespace = namespaces.get(catalogId, namespaceId).orElse(null);
        String displayName = (currentNamespace != null)
            ? prettyNamespacePath(
                currentNamespace.getParentsList(), currentNamespace.getDisplayName())
            : namespaceId.getId();
        throw GrpcErrors.conflict(
            correlationId, "namespace.not_empty", Map.of("display_name", displayName));
      }

      var meta = namespaces.metaFor(catalogId, namespaceId, tsNow);
      long expectedVersion = meta.getPointerVersion();
      enforcePreconditions(correlationId, meta, request.getPrecondition());

      try {
        namespaces.deleteWithPrecondition(catalogId, namespaceId, expectedVersion);
      } catch (BaseRepository.PreconditionFailedException pfe) {
        var nowMeta = namespaces.metaFor(catalogId, namespaceId, tsNow);
        throw GrpcErrors.preconditionFailed(correlationId, "version_mismatch",
            Map.of("expected", Long.toString(expectedVersion),
                "actual", Long.toString(nowMeta.getPointerVersion())));
      } catch (BaseRepository.NotFoundException nfe) {
        throw GrpcErrors.notFound(correlationId,
            "namespace", Map.of("id", namespaceId.getId()));
      }

      return DeleteNamespaceResponse.newBuilder().setMeta(meta).build();
    }), correlationId());
  }

  @Override
  public Uni<CreateTableResponse> createTable(CreateTableRequest request) {
    return mapFailures(runWithRetry(() -> {
      var principalContext = principal.get();
      var correlationId = principalContext.getCorrelationId();

      authz.require(principalContext, "table.write");

      var tsNow = nowTs();

      if (catalogs.getById(request.getSpec().getCatalogId()).isEmpty()) {
        throw GrpcErrors.notFound(correlationId, "catalog",
            Map.of("id", request.getSpec().getCatalogId().getId()));
      }
      if (namespaces.get(
          request.getSpec().getCatalogId(), request.getSpec().getNamespaceId()).isEmpty()) {
        throw GrpcErrors.notFound(correlationId, "namespace",
            Map.of("id", request.getSpec().getNamespaceId().getId()));
      }

      var idempotencyKey = request.hasIdempotency() ? request.getIdempotency().getKey() : "";
      byte[] fingerprint = request.getSpec().toBuilder().clearDescription().build().toByteArray();

      var tenantId = principalContext.getTenantId();
      var tableProto = MutationOps.createProto(
          tenantId,
          "CreateTable",
          idempotencyKey,
          () -> fingerprint,
          () -> {
            String tableUuid =
                !idempotencyKey.isBlank()
                    ? deterministicUuid(tenantId, "table", idempotencyKey)
                    : UUID.randomUUID().toString();

            var tableId = ResourceId.newBuilder()
                .setTenantId(tenantId)
                .setId(tableUuid)
                .setKind(ResourceKind.RK_TABLE)
                .build();

            var table = Table.newBuilder()
                .setResourceId(tableId)
                .setDisplayName(mustNonEmpty(
                    request.getSpec().getDisplayName(), "display_name", correlationId))
                .setDescription(request.getSpec().getDescription())
                .setFormat(request.getSpec().getFormat())
                .setCatalogId(request.getSpec().getCatalogId())
                .setNamespaceId(request.getSpec().getNamespaceId())
                .setRootUri(mustNonEmpty(
                      request.getSpec().getRootUri(), "root_uri", correlationId))
                .setSchemaJson(
                    mustNonEmpty(request.getSpec().getSchemaJson(), "schema_json", correlationId))
                .setCreatedAt(tsNow)
                .build();

            try {
              tables.create(table);
            }  catch (BaseRepository.NameConflictException e) {
              if (!idempotencyKey.isBlank()) {
                return tables
                    .getByName(request.getSpec().getCatalogId(),
                              request.getSpec().getNamespaceId(),
                              table.getDisplayName())
                    .map(ex -> new IdempotencyGuard.CreateResult<>(ex, ex.getResourceId()))
                    .orElseThrow(() -> GrpcErrors.conflict(
                        correlationId, "table.already_exists",
                            Map.of("display_name", table.getDisplayName())));
              }
              throw GrpcErrors.conflict(correlationId, "table.already_exists",
                  Map.of("display_name", table.getDisplayName()));
            }

            return new IdempotencyGuard.CreateResult<>(table, tableId);
          },
          (table) -> tables.metaFor(table.getResourceId(), tsNow),
          idempotencyStore,
          tsNow,
          IDEMPOTENCY_TTL_SECONDS,
          this::correlationId,
          Table::parseFrom
      );

      return CreateTableResponse.newBuilder()
          .setTable(tableProto.body).setMeta(tableProto.meta).build();
    }), correlationId());
  }

  @Override
  public Uni<UpdateTableSchemaResponse> updateTableSchema(UpdateTableSchemaRequest request) {
    return mapFailures(runWithRetry(() -> {
      var principalContext = principal.get();
      var correlationId = principalContext.getCorrelationId();

      authz.require(principalContext, "table.write");

      var tsNow = nowTs();

      var tableId = request.getTableId();
      ensureKind(tableId, ResourceKind.RK_TABLE, "table_id", correlationId);

      var currentNamespace = tables.get(tableId).orElseThrow(() ->
          GrpcErrors.notFound(correlationId, "table", Map.of("id", tableId.getId())));

      var updated = currentNamespace.toBuilder()
          .setSchemaJson(request.getSchemaJson())
          .build();

      if (updated.equals(currentNamespace)) {
        var metaNoop = tables.metaFor(tableId, tsNow);
        enforcePreconditions(correlationId, metaNoop, request.getPrecondition());
        return UpdateTableSchemaResponse.newBuilder()
            .setTable(currentNamespace)
            .setMeta(metaNoop)
            .build();
      }

      var meta = tables.metaFor(tableId, tsNow);
      long expectedVersion = meta.getPointerVersion();
      enforcePreconditions(correlationId, meta, request.getPrecondition());

      try {
        tables.update(updated, expectedVersion);
      } catch (BaseRepository.PreconditionFailedException pfe) {
        var nowMeta = tables.metaFor(tableId, tsNow);
        throw GrpcErrors.preconditionFailed(correlationId, "version_mismatch",
            Map.of("expected", Long.toString(expectedVersion),
                "actual", Long.toString(nowMeta.getPointerVersion())));
      }

      var outMeta = tables.metaFor(tableId, tsNow);

      return UpdateTableSchemaResponse.newBuilder()
              .setTable(updated)
              .setMeta(outMeta)
              .build();
    }), correlationId());
  }

  @Override
  public Uni<RenameTableResponse> renameTable(RenameTableRequest request) {
    return mapFailures(runWithRetry(() -> {
      var principalContext = principal.get();
      var correlationId = principalContext.getCorrelationId();

      authz.require(principalContext, "table.write");

      var tsNow = nowTs();

      var tableId = request.getTableId();
      ensureKind(tableId, ResourceKind.RK_TABLE, "table_id", correlationId);

      var currentNamespace = tables.get(tableId).orElseThrow(() ->
          GrpcErrors.notFound(correlationId, "table", Map.of("id", tableId.getId())));

      var newName = mustNonEmpty(request.getNewDisplayName(), "display_name", correlationId);
      if (newName.equals(currentNamespace.getDisplayName())) {
        var metaNoop = tables.metaFor(tableId, tsNow);
        enforcePreconditions(correlationId, metaNoop, request.getPrecondition());

        return RenameTableResponse.newBuilder()
            .setTable(currentNamespace).setMeta(metaNoop).build();
      }

      var meta = tables.metaFor(tableId, tsNow);
      long expectedVersion = meta.getPointerVersion();
      enforcePreconditions(correlationId, meta, request.getPrecondition());

      try {
        tables.rename(tableId, newName, expectedVersion);
      } catch (BaseRepository.NameConflictException nce) {
        throw GrpcErrors.conflict(correlationId, "table.already_exists",
            Map.of("display_name", newName));
      } catch (BaseRepository.PreconditionFailedException pfe) {
        var nowMeta = tables.metaFor(tableId, tsNow);
        throw GrpcErrors.preconditionFailed(correlationId, "version_mismatch",
            Map.of("expected", Long.toString(expectedVersion),
                "actual", Long.toString(nowMeta.getPointerVersion())));
      }

      var outMeta = tables.metaFor(tableId, tsNow);
      var updated = tables.get(tableId).orElse(currentNamespace);

      return RenameTableResponse.newBuilder().setTable(updated).setMeta(outMeta).build();
    }), correlationId());
  }

  @Override
  public Uni<MoveTableResponse> moveTable(MoveTableRequest request) {
    return mapFailures(runWithRetry(() -> {
      final var principalContext = principal.get();
      var correlationId = principalContext.getCorrelationId();

      authz.require(principalContext, "table.write");

      final var tableId = request.getTableId();
      ensureKind(tableId, ResourceKind.RK_TABLE, "table_id", correlationId);

      final var tenantId = principalContext.getTenantId();

      final var currentNamespace = tables.get(tableId).orElseThrow(() ->
          GrpcErrors.notFound(correlationId, "table", Map.of("id", tableId.getId())));

      if (!request.hasNewNamespaceId() || request.getNewNamespaceId().getId().isBlank()) {
        throw GrpcErrors.invalidArgument(correlationId, null, Map.of("field", "new_namespace_id"));
      }

      final var newNamespaceId = request.getNewNamespaceId();
      ensureKind(newNamespaceId, ResourceKind.RK_NAMESPACE, "new_namespace_id", correlationId);

      final var tsNow = nowTs();
      final var newCatalogId = namespaces.findOwnerCatalog(tenantId, newNamespaceId.getId())
          .orElseThrow(() -> GrpcErrors.notFound(
              correlationId, "catalog",
                  Map.of("reason", "namespace_owner_missing",
                      "namespace_id", newNamespaceId.getId())));

      if (namespaces.get(newCatalogId, newNamespaceId).isEmpty()) {
        throw GrpcErrors.notFound(correlationId, "namespace", Map.of("id", newNamespaceId.getId()));
      }

      final var targetName =
          (request.getNewDisplayName() != null && !request.getNewDisplayName().isBlank())
              ? request.getNewDisplayName()
              : currentNamespace.getDisplayName();

      final boolean sameNs = currentNamespace
          .getNamespaceId().getId().equals(newNamespaceId.getId());
      final boolean sameName = currentNamespace.getDisplayName().equals(targetName);
      if (sameNs && sameName) {
        final var metaNoop = tables.metaFor(tableId, tsNow);
        enforcePreconditions(correlationId, metaNoop, request.getPrecondition());

        return MoveTableResponse.newBuilder().setTable(currentNamespace).setMeta(metaNoop).build();
      }

      final var updated = currentNamespace.toBuilder()
          .setDisplayName(targetName)
          .setCatalogId(newCatalogId)
          .setNamespaceId(newNamespaceId)
          .build();

      var meta = tables.metaFor(tableId, tsNow);
      long expectedVersion = meta.getPointerVersion();
      enforcePreconditions(correlationId, meta, request.getPrecondition());

      try {
        tables.move(updated, currentNamespace.getDisplayName(),
            currentNamespace.getCatalogId(), currentNamespace.getNamespaceId(),
                newCatalogId, newNamespaceId, expectedVersion);
      } catch (BaseRepository.NameConflictException nce) {
        throw GrpcErrors.conflict(correlationId, "table.already_exists",
            Map.of("display_name", targetName));
      } catch (BaseRepository.PreconditionFailedException pfe) {
        var nowMeta = tables.metaFor(tableId, tsNow);
        throw GrpcErrors.preconditionFailed(correlationId, "version_mismatch",
            Map.of("expected", Long.toString(expectedVersion),
                "actual", Long.toString(nowMeta.getPointerVersion())));
      }

      final var outMeta = tables.metaFor(tableId, tsNow);

      return MoveTableResponse.newBuilder().setTable(updated).setMeta(outMeta).build();
    }), correlationId());
  }

  @Override
  public Uni<DeleteTableResponse> deleteTable(DeleteTableRequest request) {
    return mapFailures(runWithRetry(() -> {
      var principalContext = principal.get();
      var correlationId = principalContext.getCorrelationId();

      authz.require(principalContext, "table.write");

      var tsNow = nowTs();

      var tableId = request.getTableId();
      ensureKind(tableId, ResourceKind.RK_TABLE, "table_id", correlationId);

      var tenantId = tableId.getTenantId();
      var cannonicalKey = Keys.tblCanonicalPtr(tenantId, tableId.getId());
      var cannonicalPtr = ptr.get(cannonicalKey);

      if (cannonicalPtr.isEmpty()) {
        tables.delete(tableId);
        var safe = tables.metaForSafe(tableId, tsNow);

        return DeleteTableResponse.newBuilder().setMeta(safe).build();
      }

      var meta = tables.metaFor(tableId, tsNow);
      long expectedVersion = meta.getPointerVersion();
      enforcePreconditions(correlationId, meta, request.getPrecondition());

      try {
        tables.deleteWithPrecondition(tableId, expectedVersion);
      } catch (BaseRepository.PreconditionFailedException pfe) {
        var nowMeta = tables.metaFor(tableId, tsNow);
        throw GrpcErrors.preconditionFailed(correlationId, "version_mismatch",
            Map.of("expected", Long.toString(expectedVersion),
                "actual", Long.toString(nowMeta.getPointerVersion())));
      } catch (BaseRepository.NotFoundException nfe) {
        throw GrpcErrors.notFound(correlationId, "table", Map.of("id", tableId.getId()));
      }

      return DeleteTableResponse.newBuilder().setMeta(meta).build();
    }), correlationId());
  }

  @Override
  public Uni<CreateSnapshotResponse> createSnapshot(CreateSnapshotRequest request) {
    return mapFailures(runWithRetry(() -> {
      var principalContext = principal.get();
      var correlationId = principalContext.getCorrelationId();

      authz.require(principalContext, "table.write");

      var tsNow = nowTs();

      var tenantId = principalContext.getTenantId();
      var idempotencyKey = request.hasIdempotency() ? request.getIdempotency().getKey() : "";

      byte[] fingerprint = request.getSpec().toBuilder().build().toByteArray();

      var snapshotProto = MutationOps.createProto(
          tenantId,
          "CreateSnapshot",
          idempotencyKey,
          () -> fingerprint,
          () -> {
            var snap = Snapshot.newBuilder()
                .setTableId(request.getSpec().getTableId())
                .setSnapshotId(request.getSpec().getSnapshotId())
                .setIngestedAt(tsNow)
                .setUpstreamCreatedAt(request.getSpec().getUpstreamCreatedAt())
                .setParentSnapshotId(request.getSpec().getParentSnapshotId())
                .build();
            try {
              snapshots.create(snap);
            } catch (BaseRepository.NameConflictException e) {
              if (!idempotencyKey.isBlank()) {
                var existing = snapshots.get(
                    request.getSpec().getTableId(), request.getSpec().getSnapshotId());
                if (existing.isPresent()) {
                  return new IdempotencyGuard.CreateResult<>(
                      existing.get(), existing.get().getTableId());
                }
              }
              throw GrpcErrors.conflict(
                  correlationId, "snapshot.already_exists",
                  Map.of("id", Long.toString(request.getSpec().getSnapshotId())));
            }

            return new IdempotencyGuard.CreateResult<>(snap, snap.getTableId());
          },
          (snapshot) -> snapshots.metaFor(snapshot.getTableId(), snapshot.getSnapshotId(), tsNow),
          idempotencyStore,
          tsNow,
          IDEMPOTENCY_TTL_SECONDS,
          this::correlationId,
          Snapshot::parseFrom
      );

      return CreateSnapshotResponse.newBuilder().setMeta(snapshotProto.meta).build();
    }), correlationId());
  }

  @Override
  public Uni<DeleteSnapshotResponse> deleteSnapshot(DeleteSnapshotRequest request) {
    return mapFailures(runWithRetry(() -> {
      var principalContext = principal.get();
      var correlationId = principalContext.getCorrelationId();

      authz.require(principalContext, "table.write");

      var tsNow = nowTs();

      var tableId = request.getTableId();
      long snapshotId = request.getSnapshotId();
      ensureKind(tableId, ResourceKind.RK_TABLE, "table_id", correlationId);

      var meta = snapshots.metaFor(tableId, snapshotId, tsNow);
      long expectedVersion = meta.getPointerVersion();
      enforcePreconditions(correlationId, meta, request.getPrecondition());

      try {
        snapshots.deleteWithPrecondition(tableId, snapshotId, expectedVersion);
      } catch (BaseRepository.PreconditionFailedException pfe) {
        var nowMeta = snapshots.metaFor(tableId, snapshotId, tsNow);
        throw GrpcErrors.preconditionFailed(correlationId, "version_mismatch",
            Map.of("expected", Long.toString(expectedVersion),
                "actual", Long.toString(nowMeta.getPointerVersion())));
      } catch (BaseRepository.NotFoundException nfe) {
        throw GrpcErrors.notFound(correlationId, "snapshot",
            Map.of("id", Long.toString(snapshotId)));
      }

      return DeleteSnapshotResponse.newBuilder().setMeta(meta).build();
    }), correlationId());
  }
}
