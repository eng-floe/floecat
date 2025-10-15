package ai.floedb.metacat.service.catalog.impl;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;

import com.google.protobuf.util.Timestamps;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.catalog.rpc.CreateCatalogRequest;
import ai.floedb.metacat.catalog.rpc.CreateCatalogResponse;
import ai.floedb.metacat.catalog.rpc.CreateNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.CreateNamespaceResponse;
import ai.floedb.metacat.catalog.rpc.CreateTableRequest;
import ai.floedb.metacat.catalog.rpc.CreateTableResponse;
import ai.floedb.metacat.catalog.rpc.DeleteCatalogRequest;
import ai.floedb.metacat.catalog.rpc.DeleteCatalogResponse;
import ai.floedb.metacat.catalog.rpc.DeleteNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.DeleteNamespaceResponse;
import ai.floedb.metacat.catalog.rpc.DeleteTableRequest;
import ai.floedb.metacat.catalog.rpc.DeleteTableResponse;
import ai.floedb.metacat.catalog.rpc.MoveTableRequest;
import ai.floedb.metacat.catalog.rpc.MoveTableResponse;
import ai.floedb.metacat.catalog.rpc.MutationMeta;
import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.catalog.rpc.Precondition;
import ai.floedb.metacat.catalog.rpc.ResourceMutation;
import ai.floedb.metacat.catalog.rpc.TableDescriptor;
import ai.floedb.metacat.catalog.rpc.UpdateCatalogRequest;
import ai.floedb.metacat.catalog.rpc.UpdateCatalogResponse;
import ai.floedb.metacat.catalog.rpc.UpdateTableSchemaRequest;
import ai.floedb.metacat.catalog.rpc.UpdateTableSchemaResponse;
import ai.floedb.metacat.catalog.rpc.RenameNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.RenameNamespaceResponse;
import ai.floedb.metacat.catalog.rpc.RenameTableRequest;
import ai.floedb.metacat.catalog.rpc.RenameTableResponse;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.Pointer;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.catalog.util.MutationOps;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.impl.CatalogRepository;
import ai.floedb.metacat.service.repo.impl.NameIndexRepository;
import ai.floedb.metacat.service.repo.impl.NamespaceRepository;
import ai.floedb.metacat.service.repo.impl.TableRepository;
import ai.floedb.metacat.service.repo.util.Keys;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.IdempotencyStore;
import ai.floedb.metacat.service.storage.PointerStore;
import ai.floedb.metacat.service.storage.util.IdempotencyGuard;

@GrpcService
public class ResourceMutationImpl implements ResourceMutation {

  @Inject CatalogRepository catalogs;
  @Inject NamespaceRepository namespaces;
  @Inject TableRepository tables;
  @Inject NameIndexRepository nameIndex;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject PointerStore ptr;
  @Inject BlobStore blobs;
  @Inject IdempotencyStore idempotencyStore;

  private final Clock clock;

  public ResourceMutationImpl() {
    this.clock = Clock.systemUTC();
  }

  @Override
  public Uni<CreateCatalogResponse> createCatalog(CreateCatalogRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.write");

    var tenant = p.getTenantId();
    var idemKey = req.hasIdempotency() ? req.getIdempotency().getKey() : "";

    if (idemKey.isBlank()) {
      var existing = nameIndex.getCatalogByName(tenant, req.getSpec().getDisplayName());
      if (existing.isPresent()) {
        throw GrpcErrors.conflict(corrId(), "catalog.already_exists",
            Map.of("display_name", req.getSpec().getDisplayName()));
      }
    }

    var nowTs = Timestamps.fromMillis(clock.millis());
    byte[] fp = req.getSpec().toBuilder().clearDescription().build().toByteArray();

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

        catalogs.put(built);

        return new IdempotencyGuard.CreateResult<>(built, catalogId);
      },
      (c) -> catalogs.metaFor(c.getResourceId()),
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

    var tenantId = p.getTenantId();
    var corr = corrId();

    var prev = catalogs.get(catalogId)
      .orElseThrow(() -> GrpcErrors.notFound(
          corr,
          "catalog",
          Map.of("id", catalogId.getId())
      ));

    var newName = mustNonEmpty(req.getSpec().getDisplayName(), "display_name");
    if (!newName.isBlank() && !newName.equals(prev.getDisplayName())) {
      var exists = nameIndex.getCatalogByName(tenantId, newName);
      if (exists.isPresent() && !exists.get().getResourceId().getId().equals(catalogId.getId())) {
        throw GrpcErrors.conflict(corr, "catalog.already_exists",
            Map.of("display_name", newName));
      }
    }

    var curMeta = catalogs.metaFor(catalogId);
    var updated = prev.toBuilder()
        .setDisplayName(req.getSpec().getDisplayName())
        .setDescription(req.getSpec().getDescription())
        .build();

    if (updated.equals(prev)) {
      enforcePreconditions(corr, curMeta, req.getPrecondition());
      return Uni.createFrom().item(
          UpdateCatalogResponse.newBuilder()
              .setCatalog(prev)
              .setMeta(curMeta)
              .build());
    }

    enforcePreconditions(corr, curMeta, req.getPrecondition());

    boolean ok = catalogs.update(updated, curMeta.getPointerVersion());
    if (!ok) {
      var nowMeta = catalogs.metaFor(catalogId);
      throw GrpcErrors.preconditionFailed(corr, "version_mismatch",
        Map.of("expected", Long.toString(curMeta.getPointerVersion()),
            "actual", Long.toString(nowMeta.getPointerVersion())));
    }

    return Uni.createFrom().item(
        UpdateCatalogResponse.newBuilder()
            .setCatalog(updated)
            .setMeta(catalogs.metaFor(catalogId))
            .build()
    );
  }

  @Override
  public Uni<DeleteCatalogResponse> deleteCatalog(DeleteCatalogRequest req) {
    authz.require(principal.get(), "catalog.write");

    var catalogId = req.getCatalogId();
    ensureKind(catalogId, ResourceKind.RK_CATALOG, "DeleteCatalog");

    var corr = corrId();

    var key = Keys.catPtr(catalogId.getTenantId(), catalogId.getId());
    var ptrOpt = ptr.get(key);
    if (ptrOpt.isEmpty()) {
      catalogs.delete(catalogId);
      var safe = catalogs.metaForSafe(catalogId);
      return Uni.createFrom().item(DeleteCatalogResponse.newBuilder().setMeta(safe).build());
    }

    if (req.getRequireEmpty() && namespaces.count(catalogId) > 0) {
      var display = nameIndex.getCatalogById(principal.get().getTenantId(), catalogId.getId())
          .map(NameRef::getCatalog).orElse(catalogId.getId());
      throw GrpcErrors.conflict(corr, "catalog.not_empty", Map.of("display_name", display));
    }

    var meta = catalogs.metaFor(catalogId);
    enforcePreconditions(corr, meta, req.getPrecondition());

    boolean ok = catalogs.deleteWithPrecondition(catalogId, meta.getPointerVersion());
    if (!ok) {
      var cur = catalogs.metaForSafe(catalogId);
      throw GrpcErrors.preconditionFailed(
          corr, "version_mismatch",
          Map.of("expected", Long.toString(meta.getPointerVersion()),
                "actual",   Long.toString(cur.getPointerVersion())));
    }
    return Uni.createFrom().item(DeleteCatalogResponse.newBuilder().setMeta(meta).build());
  }

  @Override
  public Uni<CreateNamespaceResponse> createNamespace(CreateNamespaceRequest req) {
    var p = principal.get();
    authz.require(p, "namespace.write");

    var tenant = p.getTenantId();
    var parents = req.getSpec().getPathList();
    var idemKey = req.hasIdempotency() ? req.getIdempotency().getKey() : "";

    if (idemKey.isBlank()) {
      var full = new ArrayList<>(parents);
      full.add(req.getSpec().getDisplayName());
      var nsExists = nameIndex.getNamespaceByPath(tenant, req.getSpec().getCatalogId().getId(), full);
      if (nsExists.isPresent()) {
        throw GrpcErrors.conflict(corrId(), "namespace.already_exists",
            Map.of("catalog", req.getSpec().getCatalogId().getId(),
                "path", String.join("/", full)));
      }
    }

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

        var built = Namespace.newBuilder()
            .setResourceId(namespaceId)
            .setDisplayName(req.getSpec().getDisplayName())
            .addAllParents(parents)
            .setDescription(req.getSpec().getDescription())
            .setCreatedAt(nowTs)
            .build();

        namespaces.put(built, req.getSpec().getCatalogId());

        return new IdempotencyGuard.CreateResult<>(built, namespaceId);
      },
      (n) -> namespaces.metaFor(req.getSpec().getCatalogId(), n.getResourceId()),
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
            .build()
    );
  }

  @Override
  public Uni<RenameNamespaceResponse> renameNamespace(RenameNamespaceRequest req) {
    var p = principal.get();
    authz.require(p, "namespace.write");

    var nsId = req.getNamespaceId();
    ensureKind(nsId, ResourceKind.RK_NAMESPACE, "RenameNamespace");

    var tenant = p.getTenantId();
    var corr = corrId();

    Namespace cur = namespaces.get(nsId).orElseThrow(() ->
        GrpcErrors.notFound(corr, "namespace", Map.of("id", nsId.getId())));

    var curNameRef = nameIndex.getNamespaceById(tenant, nsId.getId()).orElseThrow(() ->
        GrpcErrors.notFound(corr, "namespace", Map.of("id", nsId.getId())));

    var curCatalogName = curNameRef.getCatalog();
    var curCatalogId = nameIndex.getCatalogByName(tenant, curCatalogName)
        .map(NameRef::getResourceId)
        .orElseThrow(() -> GrpcErrors.notFound(corr, "catalog", Map.of("id", curCatalogName)));

    var targetCatalogId = req.hasNewCatalogId() && !req.getNewCatalogId().getId().isBlank()
        ? req.getNewCatalogId()
        : curCatalogId;

    nameIndex.getCatalogById(tenant, targetCatalogId.getId())
        .orElseThrow(() -> GrpcErrors.notFound(
            corr, "catalog", Map.of("id", targetCatalogId.getId())));

    var curParents = ((Supplier<List<String>>) () -> {
      var c = new ArrayList<>(curNameRef.getPathList());
      if (!c.isEmpty()) {
        c.remove(c.size() - 1);
      }
      return c;
    }).get();

    final boolean pathProvided = !req.getNewPathList().isEmpty();

    final String newLeaf;
    if (pathProvided) {
      newLeaf = req.getNewPath(req.getNewPathCount() - 1);
    } else {
      if (req.getNewDisplayName().isBlank()) {
        newLeaf = cur.getDisplayName();
      } else {
        newLeaf = req.getNewDisplayName();
      }
    }

    final List<String> newParents = pathProvided
        ? List.copyOf(req.getNewPathList().subList(0, req.getNewPathCount() - 1))
        : curParents;

    boolean sameCatalog = targetCatalogId.getId().equals(curCatalogId.getId());
    boolean sameLeaf = newLeaf.equals(cur.getDisplayName());
    boolean sameParents = Objects.equals(curParents, newParents);

    var curMeta = namespaces.metaFor(curCatalogId, nsId);
    enforcePreconditions(corr, curMeta, req.getPrecondition());

    if (sameCatalog && sameLeaf && sameParents) {
      return Uni.createFrom().item(
          RenameNamespaceResponse.newBuilder().setNamespace(cur).setMeta(curMeta).build());
      }

    {
      var targetFull = new ArrayList<>(newParents);
      targetFull.add(newLeaf);
      var exists = nameIndex.getNamespaceByPath(tenant, targetCatalogId.getId(), targetFull);
      if (exists.isPresent() && !exists.get().getResourceId().getId().equals(nsId.getId())) {
        var pretty = NameIndexRepository.joinPath(targetFull);
        throw GrpcErrors.conflict(corr, "namespace.already_exists",
            Map.of("display_name", pretty));
      }
    }

    var updated = cur.toBuilder().setDisplayName(newLeaf).addAllParents(newParents).build();

    boolean ok = namespaces.renameWithPrecondition(
        updated, 
        curCatalogId,
        curParents,
        targetCatalogId, 
        newParents,
        curMeta.getPointerVersion());

    if (!ok) {
      var nowMeta = namespaces.metaFor(targetCatalogId, nsId);
      throw GrpcErrors.preconditionFailed(corr, "version_mismatch",
        Map.of("expected", Long.toString(curMeta.getPointerVersion()),
            "actual", Long.toString(nowMeta.getPointerVersion())));
    }

    var outMeta = namespaces.metaFor(targetCatalogId, nsId);

    return Uni.createFrom().item(
        RenameNamespaceResponse.newBuilder()
            .setNamespace(updated)
            .setMeta(outMeta)
            .build());
  }

  @Override
  public Uni<DeleteNamespaceResponse> deleteNamespace(DeleteNamespaceRequest req) {
    var p = principal.get();
    authz.require(p, "namespace.write");

    final var tenantId = p.getTenantId();
    final var namespaceId = req.getNamespaceId();
    ensureKind(namespaceId, ResourceKind.RK_NAMESPACE, "DeleteNamespace");

    final var corr = corrId();

    if (req.getRequireEmpty() && tables.count(namespaceId) > 0) {
      var nameRefOpt = nameIndex.getNamespaceById(tenantId, namespaceId.getId());
      var display = nameRefOpt
          .map(nr -> NameIndexRepository.joinPath(nr.getPathList()))
          .orElse(namespaceId.getId());
      throw GrpcErrors.conflict(corr, "namespace.not_empty",
          Map.of("display_name", display));
    }

    NameRef nsNameRef = nameIndex.getNamespaceById(tenantId, namespaceId.getId())
        .orElseThrow(() -> GrpcErrors.notFound(
            corr, "namespace", Map.of("id", namespaceId.getId())));
    NameRef catNameRef = nameIndex.getCatalogByName(tenantId, nsNameRef.getCatalog())
        .orElseThrow(() -> GrpcErrors.notFound(
            corr, "catalog", Map.of("id", nsNameRef.getCatalog())));

    var ptrKey = Keys.nsPtr(tenantId, catNameRef.getResourceId().getId(), namespaceId.getId());
    if (ptr.get(ptrKey).isEmpty()) {
      namespaces.delete(catNameRef.getResourceId(), namespaceId);
      var safe = namespaces.metaForSafe(catNameRef.getResourceId(), namespaceId);
      return Uni.createFrom().item(DeleteNamespaceResponse.newBuilder().setMeta(safe).build());
    }

    var meta = namespaces.metaFor(catNameRef.getResourceId(), namespaceId);
    enforcePreconditions(corr, meta, req.getPrecondition());

    boolean ok = namespaces.deleteWithPrecondition(
        catNameRef.getResourceId(), namespaceId, meta.getPointerVersion());

    if (!ok) {
      var cur = namespaces.metaFor(catNameRef.getResourceId(), namespaceId);
      throw GrpcErrors.preconditionFailed(
          corr, "version_mismatch",
          Map.of("expected", Long.toString(meta.getPointerVersion()),
                "actual",   Long.toString(cur.getPointerVersion())));
    }

    return Uni.createFrom().item(DeleteNamespaceResponse.newBuilder().setMeta(meta).build());
  }

  @Override
  public Uni<CreateTableResponse> createTable(CreateTableRequest req) {
    var p = principal.get();
    authz.require(p, "table.write");

    var tenant = p.getTenantId();
    var idemKey = req.hasIdempotency() ? req.getIdempotency().getKey() : "";

    if (idemKey.isBlank()) {
      var catName = nameIndex.getCatalogById(tenant, req.getSpec().getCatalogId().getId())
          .map(NameRef::getCatalog)
          .orElseThrow(() -> GrpcErrors.notFound(corrId(), "catalog",
              Map.of("id", req.getSpec().getCatalogId().getId())));

      var nsOpt = nameIndex.getNamespaceById(tenant, req.getSpec().getNamespaceId().getId());
        var nsPaths = nsOpt.map(NameRef::getPathList)
            .map(List::copyOf)
            .orElse(List.of());

      var tableRef = NameRef.newBuilder()
          .setCatalog(catName)
          .addAllPath(nsPaths)
          .setName(req.getSpec().getDisplayName())
          .build();

      if (nameIndex.getTableByName(tenant, tableRef).isPresent()) {
        throw GrpcErrors.conflict(corrId(), "table.already_exists",
            Map.of("name", tableRef.getName(),
                "path", String.join("/", tableRef.getPathList())));
      }
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

        var td = TableDescriptor.newBuilder()
            .setResourceId(tableId)
            .setDisplayName(mustNonEmpty(req.getSpec().getDisplayName(), "display_name"))
            .setDescription(req.getSpec().getDescription())
            .setCatalogId(req.getSpec().getCatalogId())
            .setNamespaceId(req.getSpec().getNamespaceId())
            .setRootUri(mustNonEmpty(req.getSpec().getRootUri(), "root_uri"))
            .setSchemaJson(mustNonEmpty(req.getSpec().getSchemaJson(), "schema_json"))
            .setCreatedAt(nowTs)
            .build();

        tables.put(td);
        return new IdempotencyGuard.CreateResult<>(td, tableId);
      },
      (t) -> tables.metaFor(t.getResourceId()),
      idempotencyStore,
      nowTs,
      86_400L,
      this::corrId,
      TableDescriptor::parseFrom
    );

    return Uni.createFrom().item(
        CreateTableResponse.newBuilder()
            .setTable(out.body)
            .setMeta(out.meta)
            .build()
    );
  }

  @Override
  public Uni<UpdateTableSchemaResponse> updateTableSchema(UpdateTableSchemaRequest req) {
    var p = principal.get();
    authz.require(p, "table.write");

    var tableId = req.getTableId();
    ensureKind(tableId, ResourceKind.RK_TABLE, "UpdateTableSchema");

    var corr = corrId();

    var cur = tables.get(tableId).orElseThrow(() ->
        GrpcErrors.notFound(corr, "table", Map.of("id", tableId.getId())));

    var updated = cur.toBuilder()
        .setSchemaJson(req.getSchemaJson())
        .clearDescription()
        .setDescription(cur.getDescription())
        .build();

    if (updated.equals(cur)) {
      var metaNoop = tables.metaFor(tableId);
      enforcePreconditions(corr, metaNoop, req.getPrecondition());
      return Uni.createFrom().item(
          UpdateTableSchemaResponse.newBuilder()
              .setTable(cur)
              .setMeta(metaNoop)
              .build());
    }

    var meta = tables.metaFor(tableId);
    enforcePreconditions(corr, meta, req.getPrecondition());

    boolean ok = tables.update(updated, meta.getPointerVersion());
    if (!ok) {
      var now = tables.metaFor(tableId);
      throw GrpcErrors.preconditionFailed(
          corr, "version_mismatch",
          Map.of("expected", Long.toString(meta.getPointerVersion()),
                "actual", Long.toString(now.getPointerVersion())));
    }

    var outMeta = tables.metaFor(tableId);
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

    var cur = tables.get(tableId).orElseThrow(() ->
        GrpcErrors.notFound(corr, "table", Map.of("id", tableId.getId())));

    var newName = req.getNewDisplayName();
    var updated = cur.toBuilder().setDisplayName(mustNonEmpty(newName, "display_name")).build();

    if (updated.getDisplayName().equals(cur.getDisplayName())) {
      var metaNoop = tables.metaFor(tableId);
      enforcePreconditions(corr, metaNoop, req.getPrecondition());
      return Uni.createFrom().item(
          RenameTableResponse.newBuilder()
              .setTable(cur)
              .setMeta(metaNoop)
              .build());
    }

    var meta = tables.metaFor(tableId);
    enforcePreconditions(corr, meta, req.getPrecondition());

    boolean ok = tables.update(updated, meta.getPointerVersion());
    if (!ok) {
      var now = tables.metaFor(tableId);
      throw GrpcErrors.preconditionFailed(
          corr, "version_mismatch",
          Map.of("expected", Long.toString(meta.getPointerVersion()),
                "actual", Long.toString(now.getPointerVersion())));
    }

    var outMeta = tables.metaFor(tableId);
    return Uni.createFrom().item(
        RenameTableResponse.newBuilder()
            .setTable(updated)
            .setMeta(outMeta)
            .build());
  }

  @Override
  public Uni<MoveTableResponse> moveTable(MoveTableRequest req) {
    final var p = principal.get();
    authz.require(p, "table.write");

    final var tableId = req.getTableId();
    ensureKind(tableId, ResourceKind.RK_TABLE, "MoveTable");

    final var corr = corrId();
    final var tenant = p.getTenantId();

    final var cur = tables.get(tableId).orElseThrow(() ->
        GrpcErrors.notFound(corr, "table", Map.of("id", tableId.getId())));

    if (!req.hasNewNamespaceId() || req.getNewNamespaceId().getId().isBlank()) {
      throw GrpcErrors.invalidArgument(corr, null, Map.of("field", "new_namespace_id"));
    }

    final var newNsId = req.getNewNamespaceId();
    ensureKind(newNsId, ResourceKind.RK_NAMESPACE, "MoveTable.new_namespace_id");

    final var newNsRef = nameIndex.getNamespaceById(tenant, newNsId.getId()).orElseThrow(
        () -> GrpcErrors.notFound(corr, "namespace", Map.of("id", newNsId.getId()))
    );

    final var newCatId = nameIndex.getNamespaceOwner(tenant, newNsId.getId()).orElseThrow(
        () -> GrpcErrors.notFound(corr, "catalog",
            Map.of("reason", "namespace_owner_missing", "namespace_id", newNsId.getId()))
    );
    ensureKind(newCatId, ResourceKind.RK_CATALOG, "MoveTable.new_catalog_id");

    final var targetName =
        (req.getNewDisplayName() != null && !req.getNewDisplayName().isBlank())
            ? req.getNewDisplayName()
            : cur.getDisplayName();

    final boolean sameNs = cur.getNamespaceId().getId().equals(newNsId.getId());
    final boolean sameName = cur.getDisplayName().equals(targetName);
    if (sameNs && sameName) {
      final var metaNoop = tables.metaFor(tableId);
      enforcePreconditions(corr, metaNoop, req.getPrecondition());
      return Uni.createFrom().item(
          MoveTableResponse.newBuilder()
              .setTable(cur)
              .setMeta(metaNoop)
              .build()
      );
    }

    final var newCatDisplay = nameIndex.getCatalogById(tenant, newCatId.getId())
        .map(NameRef::getCatalog)
        .orElseThrow(() -> GrpcErrors.notFound(
            corr, "catalog", Map.of("id", newCatId.getId())));

    final var targetNameRef = NameRef.newBuilder()
        .setCatalog(newCatDisplay)
        .addAllPath(newNsRef.getPathList())
        .setName(targetName)
        .build();

    final var existingAtTarget = nameIndex.getTableByName(tenant, targetNameRef);
    if (existingAtTarget.isPresent()
        && existingAtTarget.get().hasResourceId()
        && !existingAtTarget.get().getResourceId().getId().equals(tableId.getId())) {
      throw GrpcErrors.conflict(
          corr, "table.already_exists",
          Map.of("name", targetName, "path", String.join("/", newNsRef.getPathList())));
    }

    final var updated = cur.toBuilder()
        .setDisplayName(targetName)
        .setCatalogId(newCatId)
        .setNamespaceId(newNsId)
        .build();

    final var meta = tables.metaFor(tableId);
    enforcePreconditions(corr, meta, req.getPrecondition());

    final boolean ok = tables.moveWithPrecondition(
        updated,
        cur.getCatalogId(),
        cur.getNamespaceId(),
        newCatId,
        newNsId,
        meta.getPointerVersion());

    if (!ok) {
      final var now = tables.metaFor(tableId);
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

    final var outMeta = tables.metaFor(tableId);
    return Uni.createFrom().item(
        MoveTableResponse.newBuilder()
            .setTable(updated)
            .setMeta(outMeta)
            .build()
    );
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

    if (canonPtr.isEmpty()) {
      tables.delete(tableId);
      var safe = tables.metaForSafe(tableId);
      return Uni.createFrom().item(DeleteTableResponse.newBuilder().setMeta(safe).build());
    }

    var meta = tables.metaFor(tableId);
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
