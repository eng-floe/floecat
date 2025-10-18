package ai.floedb.metacat.service.catalog.impl;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import com.google.protobuf.util.Timestamps;
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
import ai.floedb.metacat.catalog.rpc.MutationMeta;
import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.catalog.rpc.Precondition;
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
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.impl.CatalogRepository;
import ai.floedb.metacat.service.repo.impl.NamespaceRepository;
import ai.floedb.metacat.service.repo.impl.SnapshotRepository;
import ai.floedb.metacat.service.repo.impl.TableRepository;
import ai.floedb.metacat.service.repo.util.Keys;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import ai.floedb.metacat.service.storage.IdempotencyStore;
import ai.floedb.metacat.service.storage.PointerStore;
import ai.floedb.metacat.service.storage.util.IdempotencyGuard;

@GrpcService
public class ResourceMutationImpl implements ResourceMutation {

  @Inject CatalogRepository catalogs;
  @Inject NamespaceRepository namespaces;
  @Inject TableRepository tables;
  @Inject SnapshotRepository snapshots;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject PointerStore ptr;
  @Inject IdempotencyStore idempotencyStore;

  private final Clock clock;

  public ResourceMutationImpl() {
    this.clock = Clock.systemUTC();
  }

  @Override
  public Uni<CreateCatalogResponse> createCatalog(CreateCatalogRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.write");

    final var tenant = p.getTenantId();
    final var idemKey = req.hasIdempotency() ? req.getIdempotency().getKey() : "";

    final var nowTs = Timestamps.fromMillis(clock.millis());
    final byte[] fp = req.getSpec().toBuilder().clearDescription().build().toByteArray();

    var out = MutationOps.createProto(
        tenant,
        "CreateCatalog",
        idemKey,
        () -> fp,
        () -> {
          var catalogId = ResourceId.newBuilder()
              .setTenantId(tenant)
              .setId(UUID.randomUUID().toString())
              .setKind(ResourceKind.RK_CATALOG)
              .build();

          var built = Catalog.newBuilder()
              .setResourceId(catalogId)
              .setDisplayName(mustNonEmpty(req.getSpec().getDisplayName(), "display_name"))
              .setDescription(req.getSpec().getDescription())
              .setCreatedAt(nowTs)
              .build();

          try {
            catalogs.create(built);
          } catch (IllegalStateException e) {
            throw GrpcErrors.conflict(
                corrId(), "catalog.already_exists",
                java.util.Map.of("display_name", built.getDisplayName()));
          }

          return new IdempotencyGuard.CreateResult<>(built, catalogId);
        },
        (c) -> catalogs.metaFor(c.getResourceId(), nowTs),
        idempotencyStore,
        nowTs,
        86_400L,
        this::corrId,
        Catalog::parseFrom
    );

    return Uni.createFrom().item(
        CreateCatalogResponse.newBuilder()
            .setCatalog(out.body)
            .setMeta(out.meta)
            .build()
    );
  }

  @Override
  public Uni<UpdateCatalogResponse> updateCatalog(UpdateCatalogRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.write");

    var catalogId = req.getCatalogId();
    ensureKind(catalogId, ResourceKind.RK_CATALOG, "UpdateCatalog");

    var corr = corrId();

    var prev = catalogs.getById(catalogId)
        .orElseThrow(() -> GrpcErrors.notFound(
              corr, "catalog", Map.of("id", catalogId.getId())));

    var desiredName = mustNonEmpty(req.getSpec().getDisplayName(), "display_name");
    var desiredDesc = req.getSpec().getDescription();

    final var nowTs = Timestamps.fromMillis(clock.millis());
    var curMeta = catalogs.metaFor(catalogId, nowTs);
    enforcePreconditions(corr, curMeta, req.getPrecondition());

    if (desiredName.equals(prev.getDisplayName()) &&
        Objects.equals(desiredDesc, prev.getDescription())) {
      return Uni.createFrom().item(
          UpdateCatalogResponse.newBuilder().setCatalog(prev).setMeta(curMeta).build());
    }

    if (!desiredName.equals(prev.getDisplayName())) {
      final boolean renamed;
      try {
        renamed = catalogs.rename(p.getTenantId(), catalogId, desiredName, curMeta.getPointerVersion());
      } catch (IllegalStateException e) {
        throw GrpcErrors.conflict(corr, "catalog.already_exists",
            Map.of("display_name", desiredName));
      }
      if (!renamed) {
        var nowMeta = catalogs.metaFor(catalogId, nowTs);
        throw GrpcErrors.preconditionFailed(corr, "version_mismatch",
            Map.of("expected", Long.toString(curMeta.getPointerVersion()),
                "actual",   Long.toString(nowMeta.getPointerVersion())));
      }
      if (!Objects.equals(desiredDesc, prev.getDescription())) {
        var afterMeta = catalogs.metaFor(catalogId, nowTs);
        var renamedObj = catalogs.getById(catalogId).orElse(prev);
        var withDesc = renamedObj.toBuilder().setDescription(desiredDesc).build();
        if (!catalogs.update(withDesc, afterMeta.getPointerVersion())) {
          var nowMeta = catalogs.metaFor(catalogId, nowTs);
          throw GrpcErrors.preconditionFailed(corr, "version_mismatch",
              Map.of("expected", Long.toString(afterMeta.getPointerVersion()),
                  "actual",   Long.toString(nowMeta.getPointerVersion())));
        }
      }
    } else {
      var updated = prev.toBuilder().setDescription(desiredDesc).build();
      if (!catalogs.update(updated, curMeta.getPointerVersion())) {
        var nowMeta = catalogs.metaFor(catalogId, nowTs);
        throw GrpcErrors.preconditionFailed(corr, "version_mismatch",
            Map.of("expected", Long.toString(curMeta.getPointerVersion()),
                "actual",   Long.toString(nowMeta.getPointerVersion())));
      }
    }

    return Uni.createFrom().item(
        UpdateCatalogResponse.newBuilder()
            .setCatalog(catalogs.getById(catalogId).orElse(prev))
            .setMeta(catalogs.metaFor(catalogId, nowTs))
            .build());
  }

  @Override
  public Uni<DeleteCatalogResponse> deleteCatalog(DeleteCatalogRequest req) {
    authz.require(principal.get(), "catalog.write");

    var catalogId = req.getCatalogId();
    ensureKind(catalogId, ResourceKind.RK_CATALOG, "DeleteCatalog");

    var corr = corrId();
    final var nowTs = Timestamps.fromMillis(clock.millis());

    var key = Keys.catPtr(catalogId.getTenantId(), catalogId.getId());
    if (ptr.get(key).isEmpty()) {
      catalogs.delete(catalogId);
      var safe = catalogs.metaForSafe(catalogId, nowTs);
      return Uni.createFrom().item(DeleteCatalogResponse.newBuilder().setMeta(safe).build());
    }

    if (req.getRequireEmpty() && namespaces.countUnderCatalog(catalogId) > 0) {
      var cur = catalogs.getById(catalogId).orElse(null);
      var displayName = (cur != null && !cur.getDisplayName().isBlank())
          ? cur.getDisplayName() : catalogId.getId();
      throw GrpcErrors.conflict(corr, "catalog.not_empty", Map.of("display_name", displayName));
    }

    var meta = catalogs.metaFor(catalogId, nowTs);
    enforcePreconditions(corr, meta, req.getPrecondition());

    if (!catalogs.deleteWithPrecondition(catalogId, meta.getPointerVersion())) {
      var cur = catalogs.metaForSafe(catalogId, nowTs);
      throw GrpcErrors.preconditionFailed(
          corr, "version_mismatch",
          Map.of("expected", Long.toString(meta.getPointerVersion()),
                "actual", Long.toString(cur.getPointerVersion())));
    }
    return Uni.createFrom().item(DeleteCatalogResponse.newBuilder().setMeta(meta).build());
  }

  @Override
  public Uni<CreateNamespaceResponse> createNamespace(CreateNamespaceRequest req) {
    var p = principal.get();
    authz.require(p, "namespace.write");

    var tenant = p.getTenantId();
    var idemKey = req.hasIdempotency() ? req.getIdempotency().getKey() : "";

    var nowTs = Timestamps.fromMillis(clock.millis());
    byte[] fp = req.getSpec().toBuilder().clearDescription().build().toByteArray();

    var out = MutationOps.createProto(
        tenant,
        "CreateNamespace",
        idemKey,
        () -> fp,
        () -> {
          var namespaceId = ResourceId.newBuilder()
              .setTenantId(tenant)
              .setId(UUID.randomUUID().toString())
              .setKind(ResourceKind.RK_NAMESPACE)
              .build();

          if (catalogs.getById(req.getSpec().getCatalogId()).isEmpty()) {
            throw GrpcErrors.notFound(corrId(), "catalog",
                Map.of("id", req.getSpec().getCatalogId().getId()));
          }

          var built = Namespace.newBuilder()
              .setResourceId(namespaceId)
              .setDisplayName(mustNonEmpty(req.getSpec().getDisplayName(), "display_name"))
              .addAllParents(req.getSpec().getPathList())
              .setDescription(req.getSpec().getDescription())
              .setCreatedAt(nowTs)
              .build();

          try {
            namespaces.create(built, req.getSpec().getCatalogId());
          } catch (IllegalStateException e) {
            {
              var parts = new java.util.ArrayList<>(req.getSpec().getPathList());
              parts.add(req.getSpec().getDisplayName());
              var pretty = String.join("/", parts);
              throw GrpcErrors.conflict(corrId(), "namespace.already_exists",
                  Map.of("catalog", req.getSpec().getCatalogId().getId(), "path", pretty));
            }
          }

          return new IdempotencyGuard.CreateResult<>(built, namespaceId);
        },
        (n) -> namespaces.metaFor(req.getSpec().getCatalogId(), n.getResourceId(), nowTs),
        idempotencyStore,
        nowTs,
        86_400L,
        this::corrId,
        Namespace::parseFrom
    );

    return Uni.createFrom().item(
        CreateNamespaceResponse.newBuilder()
            .setNamespace(out.body)
            .setMeta(out.meta)
            .build());
  }

  @Override
  public Uni<RenameNamespaceResponse> renameNamespace(RenameNamespaceRequest req) {
    var p = principal.get();
    authz.require(p, "namespace.write");

    var nsId = req.getNamespaceId();
    ensureKind(nsId, ResourceKind.RK_NAMESPACE, "RenameNamespace");

    var corr = corrId();
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

    final var nowTs = Timestamps.fromMillis(clock.millis());
    var curMeta = namespaces.metaFor(curCatalogId, nsId, nowTs);
    enforcePreconditions(corr, curMeta, req.getPrecondition());

    if (sameCatalog && sameLeaf && sameParents) {
      return Uni.createFrom().item(
          RenameNamespaceResponse.newBuilder().setNamespace(cur).setMeta(curMeta).build());
    }

    var updated = cur.toBuilder().setDisplayName(newLeaf).clearParents().addAllParents(newParents).build();

    boolean ok = namespaces.renameOrMove(
        updated,
        curCatalogId,
        curParents,
        cur.getDisplayName(),
        targetCatalogId,
        newParents,
        curMeta.getPointerVersion());

    if (!ok) {
      var nowMeta = namespaces.metaFor(targetCatalogId, nsId, nowTs);
      throw GrpcErrors.preconditionFailed(corr, "version_mismatch",
          Map.of("expected", Long.toString(curMeta.getPointerVersion()),
                "actual", Long.toString(nowMeta.getPointerVersion())));
    }

    var outMeta = namespaces.metaFor(targetCatalogId, nsId, nowTs);
    return Uni.createFrom().item(
        RenameNamespaceResponse.newBuilder().setNamespace(updated).setMeta(outMeta).build());
  }

  @Override
  public Uni<DeleteNamespaceResponse> deleteNamespace(DeleteNamespaceRequest req) {
    var p = principal.get();
    authz.require(p, "namespace.write");

    final var tenantId = p.getTenantId();
    final var namespaceId = req.getNamespaceId();
    ensureKind(namespaceId, ResourceKind.RK_NAMESPACE, "DeleteNamespace");

    final var corr = corrId();
    final var nowTs = Timestamps.fromMillis(clock.millis());

    var catId = namespaces.findOwnerCatalog(tenantId, namespaceId.getId())
        .orElse(null);

    if (catId == null) {
      var safe = namespaces.metaForSafe(ResourceId.newBuilder()
          .setTenantId(tenantId)
          .setId("_unknown_")
          .setKind(ResourceKind.RK_CATALOG)
          .build(), namespaceId, nowTs);
      return Uni.createFrom().item(DeleteNamespaceResponse.newBuilder().setMeta(safe).build());
    }

    if (req.getRequireEmpty() && tables.countUnderNamespace(catId, namespaceId) > 0) {
      var cur = namespaces.get(catId, namespaceId).orElse(null);
      final String display;
      if (cur != null) {
        var parts = new ArrayList<>(cur.getParentsList());
        parts.add(cur.getDisplayName());
        display = String.join("/", parts);
      } else {
        display = namespaceId.getId();
      }
      throw GrpcErrors.conflict(
          corr, "namespace.not_empty", Map.of("display_name", display));
    }

    var meta = namespaces.metaFor(catId, namespaceId, nowTs);
    enforcePreconditions(corr, meta, req.getPrecondition());

    boolean ok = namespaces.deleteWithPrecondition(catId, namespaceId, meta.getPointerVersion());
    if (!ok) {
      var cur = namespaces.metaFor(catId, namespaceId, nowTs);
      throw GrpcErrors.preconditionFailed(
          corr, "version_mismatch",
          Map.of("expected", Long.toString(meta.getPointerVersion()),
                "actual", Long.toString(cur.getPointerVersion())));
    }

    return Uni.createFrom().item(DeleteNamespaceResponse.newBuilder().setMeta(meta).build());
  }

  @Override
  public Uni<CreateTableResponse> createTable(CreateTableRequest req) {
    var p = principal.get();
    authz.require(p, "table.write");

    var tenant = p.getTenantId();
    var idemKey = req.hasIdempotency() ? req.getIdempotency().getKey() : "";

    if (catalogs.getById(req.getSpec().getCatalogId()).isEmpty()) {
      throw GrpcErrors.notFound(corrId(), "catalog",
          Map.of("id", req.getSpec().getCatalogId().getId()));
    }
    if (namespaces.get(req.getSpec().getCatalogId(), req.getSpec().getNamespaceId()).isEmpty()) {
      throw GrpcErrors.notFound(corrId(), "namespace",
          Map.of("id", req.getSpec().getNamespaceId().getId()));
    }

    var nowTs = Timestamps.fromMillis(clock.millis());
    byte[] fp = req.getSpec().toBuilder().clearDescription().build().toByteArray();

    var out = MutationOps.createProto(
        tenant,
        "CreateTable",
        idemKey,
        () -> fp,
        () -> {
          var tableId = ResourceId.newBuilder()
              .setTenantId(tenant)
              .setId(UUID.randomUUID().toString())
              .setKind(ResourceKind.RK_TABLE)
              .build();

          var td = Table.newBuilder()
              .setResourceId(tableId)
              .setDisplayName(mustNonEmpty(req.getSpec().getDisplayName(), "display_name"))
              .setDescription(req.getSpec().getDescription())
              .setCatalogId(req.getSpec().getCatalogId())
              .setNamespaceId(req.getSpec().getNamespaceId())
              .setRootUri(mustNonEmpty(req.getSpec().getRootUri(), "root_uri"))
              .setSchemaJson(mustNonEmpty(req.getSpec().getSchemaJson(), "schema_json"))
              .setCreatedAt(nowTs)
              .build();

          try {
            tables.create(td);
          } catch (IllegalStateException e) {
            throw GrpcErrors.conflict(corrId(), "table.already_exists",
                Map.of("name", td.getDisplayName()));
          }
          return new IdempotencyGuard.CreateResult<>(td, tableId);
        },
        (t) -> tables.metaFor(t.getResourceId(), nowTs),
        idempotencyStore,
        nowTs,
        86_400L,
        this::corrId,
        Table::parseFrom
    );

    return Uni.createFrom().item(
        CreateTableResponse.newBuilder().setTable(out.body).setMeta(out.meta).build());
  }

  @Override
  public Uni<UpdateTableSchemaResponse> updateTableSchema(UpdateTableSchemaRequest req) {
    var p = principal.get();
    authz.require(p, "table.write");

    var tableId = req.getTableId();
    ensureKind(tableId, ResourceKind.RK_TABLE, "UpdateTableSchema");

    var corr = corrId();
    final var nowTs = Timestamps.fromMillis(clock.millis());

    var cur = tables.get(tableId).orElseThrow(() ->
        GrpcErrors.notFound(corr, "table", Map.of("id", tableId.getId())));

    var updated = cur.toBuilder()
        .setSchemaJson(req.getSchemaJson())
        .build();

    if (updated.equals(cur)) {
      var metaNoop = tables.metaFor(tableId, nowTs);
      enforcePreconditions(corr, metaNoop, req.getPrecondition());
      return Uni.createFrom().item(
          UpdateTableSchemaResponse.newBuilder()
              .setTable(cur)
              .setMeta(metaNoop)
              .build());
    }

    var meta = tables.metaFor(tableId, nowTs);
    enforcePreconditions(corr, meta, req.getPrecondition());

    boolean ok = tables.update(updated, meta.getPointerVersion());
    if (!ok) {
      var now = tables.metaFor(tableId, nowTs);
      throw GrpcErrors.preconditionFailed(
          corr, "version_mismatch",
          Map.of("expected", Long.toString(meta.getPointerVersion()),
                "actual", Long.toString(now.getPointerVersion())));
    }

    var outMeta = tables.metaFor(tableId, nowTs);
    return Uni.createFrom().item(
        UpdateTableSchemaResponse.newBuilder()
            .setTable(updated)
            .setMeta(outMeta)
            .build());
  }

  @Override
  public Uni<RenameTableResponse> renameTable(RenameTableRequest req) {
    var p = principal.get();
    authz.require(p, "table.write");

    var tableId = req.getTableId();
    ensureKind(tableId, ResourceKind.RK_TABLE, "RenameTable");

    var corr = corrId();
    final var nowTs = Timestamps.fromMillis(clock.millis());

    var cur = tables.get(tableId).orElseThrow(() ->
        GrpcErrors.notFound(corr, "table", Map.of("id", tableId.getId())));

    var newName = mustNonEmpty(req.getNewDisplayName(), "display_name");
    if (newName.equals(cur.getDisplayName())) {
      var metaNoop = tables.metaFor(tableId, nowTs);
      enforcePreconditions(corr, metaNoop, req.getPrecondition());
      return Uni.createFrom().item(
          RenameTableResponse.newBuilder().setTable(cur).setMeta(metaNoop).build());
    }

    var meta = tables.metaFor(tableId, nowTs);
    enforcePreconditions(corr, meta, req.getPrecondition());

    boolean ok;
    try {
      ok = tables.rename(tableId, newName, meta.getPointerVersion());
    } catch (IllegalStateException e) {
      throw GrpcErrors.conflict(corr, "table.already_exists", Map.of("name", newName));
    }
    if (!ok) {
      var now = tables.metaFor(tableId, nowTs);
      throw GrpcErrors.preconditionFailed(
          corr, "version_mismatch",
          Map.of("expected", Long.toString(meta.getPointerVersion()),
                "actual", Long.toString(now.getPointerVersion())));
    }

    var outMeta = tables.metaFor(tableId, nowTs);
    var updated = tables.get(tableId).orElse(cur);
    return Uni.createFrom().item(
        RenameTableResponse.newBuilder().setTable(updated).setMeta(outMeta).build());
  }

  @Override
  public Uni<MoveTableResponse> moveTable(MoveTableRequest req) {
    final var p = principal.get();
    authz.require(p, "table.write");

    final var tableId = req.getTableId();
    ensureKind(tableId, ResourceKind.RK_TABLE, "MoveTable");

    final var corr = corrId();
    final var tenant = p.getTenantId();
    final var nowTs = Timestamps.fromMillis(clock.millis());

    final var cur = tables.get(tableId).orElseThrow(() ->
        GrpcErrors.notFound(corr, "table", Map.of("id", tableId.getId())));

    if (!req.hasNewNamespaceId() || req.getNewNamespaceId().getId().isBlank()) {
      throw GrpcErrors.invalidArgument(corr, null, Map.of("field", "new_namespace_id"));
    }

    final var newNsId = req.getNewNamespaceId();
    ensureKind(newNsId, ResourceKind.RK_NAMESPACE, "MoveTable.new_namespace_id");

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
      final var metaNoop = tables.metaFor(tableId, nowTs);
      enforcePreconditions(corr, metaNoop, req.getPrecondition());
      return Uni.createFrom().item(
          MoveTableResponse.newBuilder().setTable(cur).setMeta(metaNoop).build());
    }

    final var updated = cur.toBuilder()
        .setDisplayName(targetName)
        .setCatalogId(newCatId)
        .setNamespaceId(newNsId)
        .build();

    final var meta = tables.metaFor(tableId, nowTs);
    enforcePreconditions(corr, meta, req.getPrecondition());

    final boolean ok;
    try {
      ok = tables.move(updated, cur.getCatalogId(), cur.getNamespaceId(),
          newCatId, newNsId, meta.getPointerVersion());
    } catch (IllegalStateException e) {
      throw GrpcErrors.conflict(
          corr, "table.already_exists",
          Map.of("name", targetName));
    }

    if (!ok) {
      final var now = tables.metaFor(tableId, nowTs);
      if (now.getPointerVersion() != meta.getPointerVersion()) {
        throw GrpcErrors.preconditionFailed(
            corr, "version_mismatch",
            Map.of("expected", Long.toString(meta.getPointerVersion()),
                  "actual", Long.toString(now.getPointerVersion())));
      }
      throw GrpcErrors.preconditionFailed(
          corr, "move_conflict",
          Map.of("reason", "target_namespace_busy_or_old_namespace_cas_failed"));
    }

    final var outMeta = tables.metaFor(tableId, nowTs);
    return Uni.createFrom().item(
        MoveTableResponse.newBuilder().setTable(updated).setMeta(outMeta).build());
  }

  @Override
  public Uni<DeleteTableResponse> deleteTable(DeleteTableRequest req) {
    var p = principal.get();
    authz.require(p, "table.write");

    var tableId = req.getTableId();
    ensureKind(tableId, ResourceKind.RK_TABLE, "DeleteTable");

    var corr = corrId();
    var tenant = tableId.getTenantId();
    var canonKey = Keys.tblCanonicalPtr(tenant, tableId.getId());
    var canonPtr = ptr.get(canonKey);
    final var nowTs = Timestamps.fromMillis(clock.millis());

    if (canonPtr.isEmpty()) {
      tables.delete(tableId);
      var safe = tables.metaForSafe(tableId, nowTs);
      return Uni.createFrom().item(DeleteTableResponse.newBuilder().setMeta(safe).build());
    }

    var meta = tables.metaFor(tableId, nowTs);
    enforcePreconditions(corr, meta, req.getPrecondition());

    boolean ok = tables.deleteWithPrecondition(tableId, meta.getPointerVersion());
    if (!ok) {
      var nowPtr = ptr.get(canonKey).orElse(null);
      var actual = (nowPtr == null) ? 0L : nowPtr.getVersion();
      throw GrpcErrors.preconditionFailed(corr, "version_mismatch",
          java.util.Map.of("expected", Long.toString(meta.getPointerVersion()),
              "actual", Long.toString(actual)));
    }

    return Uni.createFrom().item(DeleteTableResponse.newBuilder().setMeta(meta).build());
  }

  @Override
  public Uni<CreateSnapshotResponse> createSnapshot(CreateSnapshotRequest req) {
    var p = principal.get();
    authz.require(p, "table.write");

    var tenant = p.getTenantId();
    var idemKey = req.hasIdempotency() ? req.getIdempotency().getKey() : "";

    var nowTs = Timestamps.fromMillis(clock.millis());
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
              .setIngestedAt(nowTs)
              .setUpstreamCreatedAt(req.getSpec().getUpstreamCreatedAt())
              .setParentSnapshotId(req.getSpec().getParentSnapshotId())
              .build();
          try {
            snapshots.create(snap);
          } catch (IllegalStateException e) {
            throw GrpcErrors.conflict(corrId(), "snapshot.already_exists",
                Map.of("name", Long.toString(snap.getSnapshotId())));
          }
          return new IdempotencyGuard.CreateResult<>(snap, snap.getTableId());
        },
        (s) -> snapshots.metaFor(s.getTableId(), s.getSnapshotId(), nowTs),
        idempotencyStore,
        nowTs,
        86_400L,
        this::corrId,
        Snapshot::parseFrom
    );

    return Uni.createFrom().item(
        CreateSnapshotResponse.newBuilder().setMeta(out.meta).build());
  }

  @Override
  public Uni<DeleteSnapshotResponse> deleteSnapshot(DeleteSnapshotRequest req) {
    var p = principal.get();
    authz.require(p, "table.write");

    var tableId = req.getTableId();
    long snapshotId = req.getSnapshotId();
    ensureKind(tableId, ResourceKind.RK_TABLE, "DeleteSnapshot");

    var corr = corrId();
    var tenant = tableId.getTenantId();
    var snapKey = Keys.snapPtrById(tenant, tableId.getId(), req.getSnapshotId());
    final var nowTs = Timestamps.fromMillis(clock.millis());

    var meta = snapshots.metaFor(tableId, snapshotId, nowTs);
    enforcePreconditions(corr, meta, req.getPrecondition());

    boolean ok = snapshots.deleteWithPrecondition(tableId, snapshotId, meta.getPointerVersion());
    if (!ok) {
      var nowPtr = ptr.get(snapKey).orElse(null);
      var actual = (nowPtr == null) ? 0L : nowPtr.getVersion();
      throw GrpcErrors.preconditionFailed(corr, "version_mismatch",
          java.util.Map.of("expected", Long.toString(meta.getPointerVersion()),
              "actual", Long.toString(actual)));
    }

    return Uni.createFrom().item(
        DeleteSnapshotResponse.newBuilder().setMeta(meta).build());
  }

  private void ensureKind(ResourceId rid, ResourceKind want, String op) {
    if (rid == null)
      throw GrpcErrors.invalidArgument(corrId(), null, Map.of("field", op));
    if (rid.getKind() != want)
      throw GrpcErrors.invalidArgument(corrId(), null, Map.of("field", op));
  }

  private String mustNonEmpty(String v, String name) {
    if (v == null || v.isBlank())
      throw GrpcErrors.invalidArgument(corrId(), null, Map.of("field", name));
    return v;
  }

  private String corrId() {
    var pctx = principal != null ? principal.get() : null;
    return pctx != null ? pctx.getCorrelationId() : "";
  }

  private void enforcePreconditions(
      String corrId,
      MutationMeta cur,
      Precondition pc) {
    if (pc == null) return;

    boolean checkVer = pc.getExpectedVersion() > 0;
    boolean checkTag = pc.getExpectedEtag() != null && !pc.getExpectedEtag().isBlank();

    if (!checkVer && !checkTag) {
      return;
    }

    if (checkVer && cur.getPointerVersion() != pc.getExpectedVersion()) {
      throw GrpcErrors.preconditionFailed(
        corrId, "version_mismatch",
        Map.of("expected", Long.toString(pc.getExpectedVersion()),
            "actual", Long.toString(cur.getPointerVersion())));
    }
    if (checkTag && !cur.getEtag().equals(pc.getExpectedEtag())) {
      throw GrpcErrors.preconditionFailed(
        corrId, "etag_mismatch",
        Map.of("expected", pc.getExpectedEtag(),
            "actual", cur.getEtag()));
    }
  }
}
