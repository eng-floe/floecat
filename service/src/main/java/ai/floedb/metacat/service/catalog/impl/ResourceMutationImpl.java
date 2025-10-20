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
  public Uni<CreateCatalogResponse> createCatalog(CreateCatalogRequest req) {
    return mapFailures(runWithRetry(() -> {
      var p = principal.get();
      var corr = p.getCorrelationId();
      authz.require(p, "catalog.write");

      final var tenant = p.getTenantId();
      final var idemKey = req.hasIdempotency() ? req.getIdempotency().getKey() : "";
      final byte[] fp = req.getSpec().toBuilder().clearDescription().build().toByteArray();
      var tsNow = nowTs();

      var out = MutationOps.createProto(
          tenant,
          "CreateCatalog",
          idemKey,
          () -> fp,
          () -> {
            final String catalogUuid =
              !idemKey.isBlank()
                ? deterministicUuid(tenant, "catalog", idemKey)
                : UUID.randomUUID().toString();

            var catalogId = ResourceId.newBuilder()
                .setTenantId(tenant)
                .setId(catalogUuid)
                .setKind(ResourceKind.RK_CATALOG)
                .build();

            var built = Catalog.newBuilder()
                .setResourceId(catalogId)
                .setDisplayName(mustNonEmpty(req.getSpec().getDisplayName(), "display_name", corr))
                .setDescription(req.getSpec().getDescription())
                .setCreatedAt(tsNow)
                .build();

            try {
              catalogs.create(built);
            } catch (BaseRepository.NameConflictException e) {
              if (!idemKey.isBlank()) {
                var existing = catalogs.getByName(tenant, built.getDisplayName());
                if (existing.isPresent()) {
                  return new IdempotencyGuard.CreateResult<>(existing.get(), existing.get().getResourceId());
                }
              }
              throw GrpcErrors.conflict(
                  corr, "catalog.already_exists",
                  Map.of("display_name", built.getDisplayName()));
            }

            return new IdempotencyGuard.CreateResult<>(built, catalogId);
          },
          (c) -> catalogs.metaFor(c.getResourceId(), tsNow),
          idempotencyStore,
          tsNow,
          IDEMPOTENCY_TTL_SECONDS,
          this::corrId,
          Catalog::parseFrom
      );

      return CreateCatalogResponse.newBuilder()
          .setCatalog(out.body)
          .setMeta(out.meta)
          .build();
    }), corrId());
  }

  @Override
  public Uni<UpdateCatalogResponse> updateCatalog(UpdateCatalogRequest req) {
    return mapFailures(runWithRetry(() -> {
      var p = principal.get();
      var corr = p.getCorrelationId();
      authz.require(p, "catalog.write");

      var tsNow = nowTs();

      var catalogId = req.getCatalogId();
      ensureKind(catalogId, ResourceKind.RK_CATALOG, "catalog_id", corr);

      var prev = catalogs.getById(catalogId)
          .orElseThrow(() -> GrpcErrors.notFound(
                corr, "catalog", Map.of("id", catalogId.getId())));

      var desiredName = mustNonEmpty(req.getSpec().getDisplayName(), "display_name", corr);
      var desiredDesc = req.getSpec().getDescription();

      var meta = catalogs.metaFor(catalogId, tsNow);
      long expectedVersion = meta.getPointerVersion();
      enforcePreconditions(corr, meta, req.getPrecondition());

      if (desiredName.equals(prev.getDisplayName()) &&
          Objects.equals(desiredDesc, prev.getDescription())) {
        return UpdateCatalogResponse.newBuilder().setCatalog(prev).setMeta(meta).build();
      }

      if (!desiredName.equals(prev.getDisplayName())) {
        try {
          catalogs.rename(p.getTenantId(), catalogId, desiredName, expectedVersion);
        } catch (BaseRepository.NameConflictException nce) {
          throw GrpcErrors.conflict(corr, "catalog.already_exists",
              Map.of("display_name", desiredName));
        } catch (BaseRepository.PreconditionFailedException pfe) {
          var nowMeta = catalogs.metaFor(catalogId, tsNow);
          throw GrpcErrors.preconditionFailed(corr, "version_mismatch",
                Map.of("expected", Long.toString(expectedVersion),
                    "actual", Long.toString(nowMeta.getPointerVersion())));
        }

        if (!Objects.equals(desiredDesc, prev.getDescription())) {
          meta = catalogs.metaFor(catalogId, tsNow);
          expectedVersion = meta.getPointerVersion();
          var renamedObj = catalogs.getById(catalogId).orElse(prev);
          var withDesc = renamedObj.toBuilder().setDescription(desiredDesc).build();

          try {
            catalogs.update(withDesc, meta.getPointerVersion());
          } catch (BaseRepository.PreconditionFailedException pfe) {
            var nowMeta = catalogs.metaFor(catalogId, tsNow);
            throw GrpcErrors.preconditionFailed(corr, "version_mismatch",
                Map.of("expected", Long.toString(expectedVersion),
                    "actual", Long.toString(nowMeta.getPointerVersion())));
          }

        }
      } else {
        meta = catalogs.metaFor(catalogId, tsNow);
        expectedVersion = meta.getPointerVersion();
        var updated = prev.toBuilder().setDescription(desiredDesc).build();

        try {
          catalogs.update(updated, meta.getPointerVersion());
        } catch (BaseRepository.PreconditionFailedException pfe) {
          var nowMeta = catalogs.metaFor(catalogId, tsNow);
          throw GrpcErrors.preconditionFailed(corr, "version_mismatch",
              Map.of("expected", Long.toString(expectedVersion),
                  "actual", Long.toString(nowMeta.getPointerVersion())));
        }
      }

      return UpdateCatalogResponse.newBuilder()
          .setCatalog(catalogs.getById(catalogId).orElse(prev))
          .setMeta(catalogs.metaFor(catalogId, tsNow))
          .build();
    }), corrId());
  }

  @Override
  public Uni<DeleteCatalogResponse> deleteCatalog(DeleteCatalogRequest req) {
    return mapFailures(runWithRetry(() -> {
      var p = principal.get();
      var corr = p.getCorrelationId();
      authz.require(p, "catalog.write");

      var tsNow = nowTs();

      var catalogId = req.getCatalogId();
      ensureKind(catalogId, ResourceKind.RK_CATALOG, "catalog_id", corr);

      var key = Keys.catPtr(catalogId.getTenantId(), catalogId.getId());
      if (ptr.get(key).isEmpty()) {
        catalogs.delete(catalogId);
        var safe = catalogs.metaForSafe(catalogId, tsNow);
        return DeleteCatalogResponse.newBuilder().setMeta(safe).build();
      }

      if (req.getRequireEmpty() && namespaces.countUnderCatalog(catalogId) > 0) {
        var cur = catalogs.getById(catalogId).orElse(null);
        var displayName = (cur != null && !cur.getDisplayName().isBlank())
            ? cur.getDisplayName() : catalogId.getId();
        throw GrpcErrors.conflict(corr, "catalog.not_empty", Map.of("display_name", displayName));
      }

      var meta = catalogs.metaFor(catalogId, tsNow);
      long expectedVersion = meta.getPointerVersion();
      enforcePreconditions(corr, meta, req.getPrecondition());

      try {
        catalogs.deleteWithPrecondition(catalogId, expectedVersion);
      } catch (BaseRepository.PreconditionFailedException pfe) {
        var nowMeta = catalogs.metaFor(catalogId, tsNow);
        throw GrpcErrors.preconditionFailed(corr, "version_mismatch",
            Map.of("expected", Long.toString(expectedVersion),
                "actual", Long.toString(nowMeta.getPointerVersion())));
      } catch (BaseRepository.NotFoundException nfe) {
        throw GrpcErrors.notFound(corr,
            "catalog", Map.of("id", catalogId.getId()));
      }

      return DeleteCatalogResponse.newBuilder().setMeta(meta).build();
    }), corrId());
  }

  @Override
  public Uni<CreateNamespaceResponse> createNamespace(CreateNamespaceRequest req) {
    return mapFailures(runWithRetry(() -> {
      var p = principal.get();
      var corr = p.getCorrelationId();
      authz.require(p, "namespace.write");

      var tsNow = nowTs();

      var tenant = p.getTenantId();
      var idemKey = req.hasIdempotency() ? req.getIdempotency().getKey() : "";
      byte[] fp = req.getSpec().toBuilder().clearDescription().build().toByteArray();

      var out = MutationOps.createProto(
          tenant,
          "CreateNamespace",
          idemKey,
          () -> fp,
          () -> {
            final String namespaceUuid =
              !idemKey.isBlank()
                ? deterministicUuid(tenant, "namespace", idemKey)
                : UUID.randomUUID().toString();

            var namespaceId = ResourceId.newBuilder()
                .setTenantId(tenant)
                .setId(namespaceUuid)
                .setKind(ResourceKind.RK_NAMESPACE)
                .build();

            if (catalogs.getById(req.getSpec().getCatalogId()).isEmpty()) {
              throw GrpcErrors.notFound(corr, "catalog",
                  Map.of("id", req.getSpec().getCatalogId().getId()));
            }

            var built = Namespace.newBuilder()
                .setResourceId(namespaceId)
                .setDisplayName(mustNonEmpty(
                    req.getSpec().getDisplayName(), "display_name", corr))
                .addAllParents(req.getSpec().getPathList())
                .setDescription(req.getSpec().getDescription())
                .setCreatedAt(tsNow)
                .build();

            try {
              namespaces.create(built, req.getSpec().getCatalogId());
            } catch (BaseRepository.NameConflictException e) {
              if (!idemKey.isBlank()) {
                var fullPath = new ArrayList<>(req.getSpec().getPathList());
                fullPath.add(req.getSpec().getDisplayName());
                var existingId = namespaces.getByPath(tenant, req.getSpec().getCatalogId(), fullPath);
                if (existingId.isPresent()) {
                  var existing = namespaces.get(req.getSpec().getCatalogId(), existingId.get());
                  if (existing.isPresent()) {
                    return new IdempotencyGuard.CreateResult<>(existing.get(), existingId.get());
                  }
                }
              }
              var pretty = prettyNamespacePath(req.getSpec().getPathList(), req.getSpec().getDisplayName());
              throw GrpcErrors.conflict(
                  corr, "namespace.already_exists",
                  Map.of("catalog", req.getSpec().getCatalogId().getId(),
                        "path", pretty));
            }

            return new IdempotencyGuard.CreateResult<>(built, namespaceId);
          },
          (n) -> namespaces.metaFor(req.getSpec().getCatalogId(), n.getResourceId(), tsNow),
          idempotencyStore,
          tsNow,
          IDEMPOTENCY_TTL_SECONDS,
          this::corrId,
          Namespace::parseFrom
      );

      return CreateNamespaceResponse.newBuilder()
          .setNamespace(out.body)
          .setMeta(out.meta)
          .build();
    }), corrId());
  }

  @Override
  public Uni<RenameNamespaceResponse> renameNamespace(RenameNamespaceRequest req) {
    return mapFailures(runWithRetry(() -> {
      var p = principal.get();
      var corr = p.getCorrelationId();
      authz.require(p, "namespace.write");

      var tsNow = nowTs();

      var nsId = req.getNamespaceId();
      ensureKind(nsId, ResourceKind.RK_NAMESPACE, "namespace_id", corr);

      var tenant = p.getTenantId();

      var curCatalogId = namespaces.findOwnerCatalog(tenant, nsId.getId())
          .orElseThrow(() -> GrpcErrors.notFound(corr, "namespace", Map.of("id", nsId.getId())));

      Namespace cur = namespaces.get(curCatalogId, nsId).orElseThrow(() ->
          GrpcErrors.notFound(corr, "namespace", Map.of("id", nsId.getId())));

      var pathProvided = !req.getNewPathList().isEmpty();
      var newLeaf = pathProvided
          ? req.getNewPath(req.getNewPathCount() - 1)
          : (req.getNewDisplayName().isBlank() ? cur.getDisplayName() : req.getNewDisplayName());

      var curParents = cur.getParentsList();
      var newParents = pathProvided
          ? List.copyOf(req.getNewPathList().subList(0, req.getNewPathCount() - 1))
          : curParents;

      var targetCatalogId = (req.hasNewCatalogId() && !req.getNewCatalogId().getId().isBlank())
          ? req.getNewCatalogId()
          : curCatalogId;

      if (catalogs.getById(targetCatalogId).isEmpty()) {
        throw GrpcErrors.notFound(corr, "catalog", Map.of("id", targetCatalogId.getId()));
      }

      var sameCatalog = targetCatalogId.getId().equals(curCatalogId.getId());
      var sameLeaf = newLeaf.equals(cur.getDisplayName());
      var sameParents = Objects.equals(curParents, newParents);

      var meta = namespaces.metaFor(curCatalogId, nsId, tsNow);
      long expectedVersion = meta.getPointerVersion();
      enforcePreconditions(corr, meta, req.getPrecondition());

      if (sameCatalog && sameLeaf && sameParents) {
        return RenameNamespaceResponse.newBuilder().setNamespace(cur).setMeta(meta).build();
      }

      var updated = cur.toBuilder().setDisplayName(newLeaf).clearParents().addAllParents(newParents).build();

      try {
        namespaces.renameOrMove(
            updated,
            curCatalogId,
            curParents,
            cur.getDisplayName(),
            targetCatalogId,
            newParents,
            expectedVersion);
      } catch (BaseRepository.NameConflictException e) {
        var parts = new ArrayList<>(newParents);
        parts.add(newLeaf);
        var pretty = String.join("/", parts);
        throw GrpcErrors.conflict(
            corr, "namespace.already_exists",
                Map.of("catalog", targetCatalogId.getId(),
                    "path", pretty));
      } catch (BaseRepository.PreconditionFailedException pfe) {
        var nowMeta = namespaces.metaFor(curCatalogId, nsId, tsNow);
        throw GrpcErrors.preconditionFailed(
            corr, "version_mismatch",
            Map.of("expected", Long.toString(expectedVersion),
                "actual", Long.toString(nowMeta.getPointerVersion())));
      }

      var outMeta = namespaces.metaFor(targetCatalogId, nsId, tsNow);
      return RenameNamespaceResponse.newBuilder()
          .setNamespace(updated)
          .setMeta(outMeta).build();
    }), corrId());
  }

  @Override
  public Uni<DeleteNamespaceResponse> deleteNamespace(DeleteNamespaceRequest req) {
    return mapFailures(runWithRetry(() -> {
      var p = principal.get();
      var corr = p.getCorrelationId();
      authz.require(p, "namespace.write");

      var tsNow = nowTs();

      final var tenantId = p.getTenantId();
      final var namespaceId = req.getNamespaceId();
      ensureKind(namespaceId, ResourceKind.RK_NAMESPACE, "namespace_id", corr);

      var catId = namespaces.findOwnerCatalog(tenantId, namespaceId.getId())
          .orElse(null);

      if (catId == null) {
        var safe = namespaces.metaForSafe(ResourceId.newBuilder()
            .setTenantId(tenantId)
            .setId("_unknown_")
            .setKind(ResourceKind.RK_CATALOG)
            .build(), namespaceId, tsNow);
        return DeleteNamespaceResponse.newBuilder().setMeta(safe).build();
      }

      if (req.getRequireEmpty() && tables.countUnderNamespace(catId, namespaceId) > 0) {
        var cur = namespaces.get(catId, namespaceId).orElse(null);
        final String display = (cur != null)
            ? prettyNamespacePath(cur.getParentsList(), cur.getDisplayName())
            : namespaceId.getId();
        throw GrpcErrors.conflict(
            corr, "namespace.not_empty", Map.of("display_name", display));
      }

      var meta = namespaces.metaFor(catId, namespaceId, tsNow);
      long expectedVersion = meta.getPointerVersion();
      enforcePreconditions(corr, meta, req.getPrecondition());

      try {
        namespaces.deleteWithPrecondition(catId, namespaceId, expectedVersion);
      } catch (BaseRepository.PreconditionFailedException pfe) {
        var nowMeta = namespaces.metaFor(catId, namespaceId, tsNow);
        throw GrpcErrors.preconditionFailed(corr, "version_mismatch",
            Map.of("expected", Long.toString(expectedVersion),
                "actual", Long.toString(nowMeta.getPointerVersion())));
      } catch (BaseRepository.NotFoundException nfe) {
        throw GrpcErrors.notFound(corr,
            "namespace", Map.of("id", namespaceId.getId()));
      }

      return DeleteNamespaceResponse.newBuilder().setMeta(meta).build();
    }), corrId());
  }

  @Override
  public Uni<CreateTableResponse> createTable(CreateTableRequest req) {
    return mapFailures(runWithRetry(() -> {
      var p = principal.get();
      var corr = p.getCorrelationId();
      authz.require(p, "table.write");

      var tsNow = nowTs();

      var tenant = p.getTenantId();
      var idemKey = req.hasIdempotency() ? req.getIdempotency().getKey() : "";

      if (catalogs.getById(req.getSpec().getCatalogId()).isEmpty()) {
        throw GrpcErrors.notFound(corr, "catalog",
            Map.of("id", req.getSpec().getCatalogId().getId()));
      }
      if (namespaces.get(req.getSpec().getCatalogId(), req.getSpec().getNamespaceId()).isEmpty()) {
        throw GrpcErrors.notFound(corr, "namespace",
            Map.of("id", req.getSpec().getNamespaceId().getId()));
      }

      byte[] fp = req.getSpec().toBuilder().clearDescription().build().toByteArray();

      var out = MutationOps.createProto(
          tenant,
          "CreateTable",
          idemKey,
          () -> fp,
          () -> {
            final String tableUuid =
              !idemKey.isBlank()
                ? deterministicUuid(tenant, "table", idemKey)
                : UUID.randomUUID().toString();

            var tableId = ResourceId.newBuilder()
                .setTenantId(tenant)
                .setId(tableUuid)
                .setKind(ResourceKind.RK_TABLE)
                .build();

            var td = Table.newBuilder()
                .setResourceId(tableId)
                .setDisplayName(mustNonEmpty(
                    req.getSpec().getDisplayName(), "display_name", corr))
                .setDescription(req.getSpec().getDescription())
                .setFormat(req.getSpec().getFormat())
                .setCatalogId(req.getSpec().getCatalogId())
                .setNamespaceId(req.getSpec().getNamespaceId())
                .setRootUri(mustNonEmpty(
                      req.getSpec().getRootUri(), "root_uri", corr))
                .setSchemaJson(
                    mustNonEmpty(req.getSpec().getSchemaJson(), "schema_json", corr))
                .setCreatedAt(tsNow)
                .build();

            try {
              tables.create(td);
            }  catch (BaseRepository.NameConflictException e) {
              if (!idemKey.isBlank()) {
                return tables
                    .getByName(req.getSpec().getCatalogId(),
                              req.getSpec().getNamespaceId(),
                              td.getDisplayName())
                    .map(ex -> new IdempotencyGuard.CreateResult<>(ex, ex.getResourceId()))
                    .orElseThrow(() -> GrpcErrors.conflict(
                        corr, "table.already_exists", Map.of("display_name", td.getDisplayName())));
              }
              throw GrpcErrors.conflict(corr, "table.already_exists",
                  Map.of("display_name", td.getDisplayName()));
            }

            return new IdempotencyGuard.CreateResult<>(td, tableId);
          },
          (t) -> tables.metaFor(t.getResourceId(), tsNow),
          idempotencyStore,
          tsNow,
          IDEMPOTENCY_TTL_SECONDS,
          this::corrId,
          Table::parseFrom
      );

      return CreateTableResponse.newBuilder().setTable(out.body).setMeta(out.meta).build();
    }), corrId());
  }

  @Override
  public Uni<UpdateTableSchemaResponse> updateTableSchema(UpdateTableSchemaRequest req) {
    return mapFailures(runWithRetry(() -> {
      var p = principal.get();
      var corr = p.getCorrelationId();
      authz.require(p, "table.write");

      var tsNow = nowTs();

      var tableId = req.getTableId();
      ensureKind(tableId, ResourceKind.RK_TABLE, "table_id", corr);

      var cur = tables.get(tableId).orElseThrow(() ->
          GrpcErrors.notFound(corr, "table", Map.of("id", tableId.getId())));

      var updated = cur.toBuilder()
          .setSchemaJson(req.getSchemaJson())
          .build();

      if (updated.equals(cur)) {
        var metaNoop = tables.metaFor(tableId, tsNow);
        enforcePreconditions(corr, metaNoop, req.getPrecondition());
        return UpdateTableSchemaResponse.newBuilder()
            .setTable(cur)
            .setMeta(metaNoop)
            .build();
      }

      var meta = tables.metaFor(tableId, tsNow);
      long expectedVersion = meta.getPointerVersion();
      enforcePreconditions(corr, meta, req.getPrecondition());

      try {
        tables.update(updated, expectedVersion);
      } catch (BaseRepository.PreconditionFailedException pfe) {
        var nowMeta = tables.metaFor(tableId, tsNow);
        throw GrpcErrors.preconditionFailed(corr, "version_mismatch",
            Map.of("expected", Long.toString(expectedVersion),
                "actual", Long.toString(nowMeta.getPointerVersion())));
      }

      var outMeta = tables.metaFor(tableId, tsNow);
      return UpdateTableSchemaResponse.newBuilder()
              .setTable(updated)
              .setMeta(outMeta)
              .build();
      }), corrId()
    );
  }

  @Override
  public Uni<RenameTableResponse> renameTable(RenameTableRequest req) {
    return mapFailures(runWithRetry(() -> {
      var p = principal.get();
      var corr = p.getCorrelationId();
      authz.require(p, "table.write");

      var tsNow = nowTs();

      var tableId = req.getTableId();
      ensureKind(tableId, ResourceKind.RK_TABLE, "table_id", corr);

      var cur = tables.get(tableId).orElseThrow(() ->
          GrpcErrors.notFound(corr, "table", Map.of("id", tableId.getId())));

      var newName = mustNonEmpty(req.getNewDisplayName(), "display_name", corr);
      if (newName.equals(cur.getDisplayName())) {
        var metaNoop = tables.metaFor(tableId, tsNow);
        enforcePreconditions(corr, metaNoop, req.getPrecondition());
        return RenameTableResponse.newBuilder().setTable(cur).setMeta(metaNoop).build();
      }

      var meta = tables.metaFor(tableId, tsNow);
      long expectedVersion = meta.getPointerVersion();
      enforcePreconditions(corr, meta, req.getPrecondition());

      try {
        tables.rename(tableId, newName, expectedVersion);
      } catch (BaseRepository.NameConflictException nce) {
        throw GrpcErrors.conflict(corr, "table.already_exists",
            Map.of("display_name", newName));
      } catch (BaseRepository.PreconditionFailedException pfe) {
        var nowMeta = tables.metaFor(tableId, tsNow);
        throw GrpcErrors.preconditionFailed(corr, "version_mismatch",
            Map.of("expected", Long.toString(expectedVersion),
                "actual", Long.toString(nowMeta.getPointerVersion())));
      }

      var outMeta = tables.metaFor(tableId, tsNow);
      var updated = tables.get(tableId).orElse(cur);
      return RenameTableResponse.newBuilder().setTable(updated).setMeta(outMeta).build();
    }), corrId());
  }

  @Override
  public Uni<MoveTableResponse> moveTable(MoveTableRequest req) {
    return mapFailures(runWithRetry(() -> {
      final var p = principal.get();
      var corr = p.getCorrelationId();
      authz.require(p, "table.write");

      var tsNow = nowTs();

      final var tableId = req.getTableId();
      ensureKind(tableId, ResourceKind.RK_TABLE, "table_id", corr);

      final var tenant = p.getTenantId();

      final var cur = tables.get(tableId).orElseThrow(() ->
          GrpcErrors.notFound(corr, "table", Map.of("id", tableId.getId())));

      if (!req.hasNewNamespaceId() || req.getNewNamespaceId().getId().isBlank()) {
        throw GrpcErrors.invalidArgument(corr, null, Map.of("field", "new_namespace_id"));
      }

      final var newNsId = req.getNewNamespaceId();
      ensureKind(newNsId, ResourceKind.RK_NAMESPACE, "new_namespace_id", corr);

      final var newCatId = namespaces.findOwnerCatalog(tenant, newNsId.getId())
          .orElseThrow(() -> GrpcErrors.notFound(
              corr, "catalog", Map.of("reason", "namespace_owner_missing", "namespace_id", newNsId.getId())));

      if (namespaces.get(newCatId, newNsId).isEmpty()) {
        throw GrpcErrors.notFound(corr, "namespace", Map.of("id", newNsId.getId()));
      }

      final var targetName =
          (req.getNewDisplayName() != null && !req.getNewDisplayName().isBlank())
              ? req.getNewDisplayName()
              : cur.getDisplayName();

      final boolean sameNs = cur.getNamespaceId().getId().equals(newNsId.getId());
      final boolean sameName = cur.getDisplayName().equals(targetName);
      if (sameNs && sameName) {
        final var metaNoop = tables.metaFor(tableId, tsNow);
        enforcePreconditions(corr, metaNoop, req.getPrecondition());
        return MoveTableResponse.newBuilder().setTable(cur).setMeta(metaNoop).build();
      }

      final var updated = cur.toBuilder()
          .setDisplayName(targetName)
          .setCatalogId(newCatId)
          .setNamespaceId(newNsId)
          .build();

      var meta = tables.metaFor(tableId, tsNow);
      long expectedVersion = meta.getPointerVersion();
      enforcePreconditions(corr, meta, req.getPrecondition());

      try {
        tables.move(updated, cur.getDisplayName(), cur.getCatalogId(), cur.getNamespaceId(),
            newCatId, newNsId, expectedVersion);
      } catch (BaseRepository.NameConflictException nce) {
        throw GrpcErrors.conflict(corr, "table.already_exists",
            Map.of("display_name", targetName));
      } catch (BaseRepository.PreconditionFailedException pfe) {
        var nowMeta = tables.metaFor(tableId, tsNow);
        throw GrpcErrors.preconditionFailed(corr, "version_mismatch",
            Map.of("expected", Long.toString(expectedVersion),
                "actual", Long.toString(nowMeta.getPointerVersion())));
      }

      final var outMeta = tables.metaFor(tableId, tsNow);
      return MoveTableResponse.newBuilder().setTable(updated).setMeta(outMeta).build();
    }), corrId());
  }

  @Override
  public Uni<DeleteTableResponse> deleteTable(DeleteTableRequest req) {
    return mapFailures(runWithRetry(() -> {
      var p = principal.get();
      var corr = p.getCorrelationId();
      authz.require(p, "table.write");

      var tsNow = nowTs();

      var tableId = req.getTableId();
      ensureKind(tableId, ResourceKind.RK_TABLE, "table_id", corr);

      var tenant = tableId.getTenantId();
      var canonKey = Keys.tblCanonicalPtr(tenant, tableId.getId());
      var canonPtr = ptr.get(canonKey);

      if (canonPtr.isEmpty()) {
        tables.delete(tableId);
        var safe = tables.metaForSafe(tableId, tsNow);
        return DeleteTableResponse.newBuilder().setMeta(safe).build();
      }

      var meta = tables.metaFor(tableId, tsNow);
      long expectedVersion = meta.getPointerVersion();
      enforcePreconditions(corr, meta, req.getPrecondition());

      try {
        tables.deleteWithPrecondition(tableId, expectedVersion);
        } catch (BaseRepository.PreconditionFailedException pfe) {
          var nowMeta = tables.metaFor(tableId, tsNow);
          throw GrpcErrors.preconditionFailed(corr, "version_mismatch",
              Map.of("expected", Long.toString(expectedVersion),
                  "actual", Long.toString(nowMeta.getPointerVersion())));
        } catch (BaseRepository.NotFoundException nfe) {
          throw GrpcErrors.notFound(corr, "table", Map.of("id", tableId.getId()));
        }

      return DeleteTableResponse.newBuilder().setMeta(meta).build();
    }), corrId());
  }

  @Override
  public Uni<CreateSnapshotResponse> createSnapshot(CreateSnapshotRequest req) {
    return mapFailures(runWithRetry(() -> {
      var p = principal.get();
      var corr = p.getCorrelationId();
      authz.require(p, "table.write");

      var tsNow = nowTs();

      var tenant = p.getTenantId();
      var idemKey = req.hasIdempotency() ? req.getIdempotency().getKey() : "";

      byte[] fp = req.getSpec().toBuilder().build().toByteArray();

      var out = MutationOps.createProto(
          tenant,
          "CreateSnapshot",
          idemKey,
          () -> fp,
          () -> {
            var snap = Snapshot.newBuilder()
                .setTableId(req.getSpec().getTableId())
                .setSnapshotId(req.getSpec().getSnapshotId())
                .setIngestedAt(tsNow)
                .setUpstreamCreatedAt(req.getSpec().getUpstreamCreatedAt())
                .setParentSnapshotId(req.getSpec().getParentSnapshotId())
                .build();
            try {
              snapshots.create(snap);
            } catch (BaseRepository.NameConflictException e) {
              if (!idemKey.isBlank()) {
                var existing = snapshots.get(req.getSpec().getTableId(), req.getSpec().getSnapshotId());
                if (existing.isPresent()) {
                  return new IdempotencyGuard.CreateResult<>(existing.get(), existing.get().getTableId());
                }
              }
              throw GrpcErrors.conflict(
                  corr, "snapshot.already_exists",
                  Map.of("id", Long.toString(req.getSpec().getSnapshotId())));
            }

            return new IdempotencyGuard.CreateResult<>(snap, snap.getTableId());
          },
          (s) -> snapshots.metaFor(s.getTableId(), s.getSnapshotId(), tsNow),
          idempotencyStore,
          tsNow,
          IDEMPOTENCY_TTL_SECONDS,
          this::corrId,
          Snapshot::parseFrom
      );

      return CreateSnapshotResponse.newBuilder().setMeta(out.meta).build();
    }), corrId());
  }

  @Override
  public Uni<DeleteSnapshotResponse> deleteSnapshot(DeleteSnapshotRequest req) {
    return mapFailures(runWithRetry(() -> {
      var p = principal.get();
      var corr = p.getCorrelationId();
      authz.require(p, "table.write");

      var tsNow = nowTs();
      
      var tableId = req.getTableId();
      long snapshotId = req.getSnapshotId();
      ensureKind(tableId, ResourceKind.RK_TABLE, "table_id", corr);

      var meta = snapshots.metaFor(tableId, snapshotId, tsNow);
      long expectedVersion = meta.getPointerVersion();
      enforcePreconditions(corr, meta, req.getPrecondition());

      try {
        snapshots.deleteWithPrecondition(tableId, snapshotId, expectedVersion);
      } catch (BaseRepository.PreconditionFailedException pfe) {
        var nowMeta = snapshots.metaFor(tableId, snapshotId, tsNow);
        throw GrpcErrors.preconditionFailed(corr, "version_mismatch",
            Map.of("expected", Long.toString(expectedVersion),
                "actual", Long.toString(nowMeta.getPointerVersion())));
      } catch (BaseRepository.NotFoundException nfe) {
        throw GrpcErrors.notFound(corr, "snapshot",
            Map.of("id", Long.toString(snapshotId)));
      }

      return DeleteSnapshotResponse.newBuilder().setMeta(meta).build();
    }), corrId());
  }
}
