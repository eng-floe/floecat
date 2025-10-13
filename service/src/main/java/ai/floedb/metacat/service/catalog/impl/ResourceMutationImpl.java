package ai.floedb.metacat.service.catalog.impl;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
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
import ai.floedb.metacat.catalog.rpc.MutationMeta;
import ai.floedb.metacat.catalog.rpc.Namespace;
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
    var nowMs = clock.millis();
    var nowTs = Timestamps.fromMillis(nowMs);
    var idemKey = req.hasIdempotency() ? req.getIdempotency().getKey() : "";

    var existing = nameIndex.getCatalogByName(tenant, req.getSpec().getDisplayName());
    if (idemKey.isBlank() && existing.isPresent()) {
      throw GrpcErrors.conflict(corrId(), "catalog.already_exists",
        Map.of("display_name", req.getSpec().getDisplayName()));
    }

    byte[] fp = req.getSpec().toBuilder()
      .clearDescription()
      .build()
      .toByteArray();

    Catalog cat = IdempotencyGuard.runOnce(
      tenant,
      "CreateCatalog",
      idemKey,
      fp,
      (Supplier<IdempotencyGuard.CreateResult<Catalog>>) () -> {
        var catalogId = ResourceId.newBuilder()
          .setTenantId(tenant)
          .setId(UUID.randomUUID().toString())
          .setKind(ResourceKind.RK_CATALOG)
          .build();

        var built = Catalog.newBuilder()
          .setResourceId(catalogId)
          .setDisplayName(req.getSpec().getDisplayName())
          .setDescription(req.getSpec().getDescription())
          .setCreatedAt(nowTs)
          .build();

        catalogs.put(built);

        return new IdempotencyGuard.CreateResult<>(built, catalogId);
      },
      (Function<Catalog, MutationMeta>)
        (c) -> catalogs.metaFor(c.getResourceId()),
      (Function<Catalog, byte[]>) Catalog::toByteArray,
      (Function<byte[], Catalog>) bytes -> {
        try {
          return Catalog.parseFrom(bytes);
        }
        catch (Exception e) {
          throw new IllegalStateException(e);
        }
      },
      idempotencyStore,
      86_400L,
      nowTs,
      this::corrId
    );

    return Uni.createFrom().item(
      CreateCatalogResponse.newBuilder()
        .setCatalog(cat)
        .setMeta(metaForCatalog(cat.getResourceId()))
        .build()
    );
  }

  @Override
  public Uni<UpdateCatalogResponse> updateCatalog(UpdateCatalogRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.write");

    var catalogId = req.getCatalogId();
    ensureKind(catalogId, ResourceKind.RK_CATALOG, "UpdateCatalog");
 
    var prev = catalogs.get(catalogId)
      .orElseThrow(() -> GrpcErrors.notFound(
        corrId(), 
        "catalog", 
        Map.of("id", 
        catalogId.getId()))
    );
    
    var updated = prev.toBuilder()
      .setDisplayName(req.getSpec().getDisplayName())
      .setDescription(req.getSpec().getDescription())
      .build();

    catalogs.put(updated);

    return Uni.createFrom().item(
      UpdateCatalogResponse.newBuilder()
        .setCatalog(updated)
        .setMeta(metaForCatalog(catalogId))
        .build()
    );
  }

  @Override
  public Uni<DeleteCatalogResponse> deleteCatalog(DeleteCatalogRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.write");

    var catalogId = req.getCatalogId();
    ensureKind(catalogId, ResourceKind.RK_CATALOG, "DeleteCatalog");
    if (req.getRequireEmpty() && namespaces.count(catalogId) > 0) {
      var catalogNameRef = nameIndex.getCatalogById(p.getTenantId(), catalogId.getId());
      if (catalogNameRef.isPresent()) {
        throw GrpcErrors.conflict(
          corrId(),
          "catalog.not_empty",
          Map.of("display_name", catalogNameRef.get().getCatalog()));
      } else {
        throw GrpcErrors.conflict(
          corrId(),
          "catalog.not_empty",
          Map.of("display_name", catalogId.getId()));
      }
    }

    var m = metaForCatalog(catalogId);
    catalogs.delete(catalogId);

    return Uni.createFrom().item(DeleteCatalogResponse.newBuilder().setMeta(m).build());
  }

  @Override
  public Uni<CreateNamespaceResponse> createNamespace(CreateNamespaceRequest req) {
    var p = principal.get();
    authz.require(p, "namespace.write");

    var tenant = p.getTenantId();
    var nowMs = clock.millis();
    var nowTs = Timestamps.fromMillis(nowMs);
    var idemKey = req.hasIdempotency() ? req.getIdempotency().getKey() : "";

    var parents = req.getSpec().getPathList();
    var full = new ArrayList<>(parents);
    full.add(req.getSpec().getDisplayName());
    var nsExists = nameIndex.getNamespaceByPath(tenant, req.getSpec().getCatalogId().getId(), full);
    if (idemKey.isBlank() && nsExists.isPresent()) {
      throw GrpcErrors.conflict(corrId(), "namespace.already_exists",
        Map.of("catalog", req.getSpec().getCatalogId().getId(), "path", String.join("/", full)));
    }

    byte[] fp = req.getSpec().toBuilder()
      .clearDescription()
      .build()
      .toByteArray();

    Namespace ns = IdempotencyGuard.runOnce(
      tenant,
      "CreateNamespace",
      idemKey,
      fp,
      (Supplier<IdempotencyGuard.CreateResult<Namespace>>) () -> {
        var namespaceId = ResourceId.newBuilder()
          .setTenantId(tenant)
          .setId(UUID.randomUUID().toString())
          .setKind(ResourceKind.RK_NAMESPACE)
          .build();

        var built = Namespace.newBuilder()
          .setResourceId(namespaceId)
          .setDisplayName(req.getSpec().getDisplayName())
          .setDescription(req.getSpec().getDescription())
          .setCreatedAt(nowTs)
          .build();

        namespaces.put(built, req.getSpec().getCatalogId(), req.getSpec().getPathList());

        return new IdempotencyGuard.CreateResult<>(built, namespaceId);
      },
      (Function<Namespace, MutationMeta>)
        (n) -> namespaces.metaFor(req.getSpec().getCatalogId(), n.getResourceId()),
      (Function<Namespace, byte[]>) Namespace::toByteArray,
      (Function<byte[], Namespace>) bytes -> {
        try {
          return Namespace.parseFrom(bytes);
        }
        catch (Exception e) {
          throw new IllegalStateException(e);
        }
      },
      idempotencyStore,
      86_400L,
      nowTs,
      this::corrId
    );

    return Uni.createFrom().item(
      CreateNamespaceResponse.newBuilder()
        .setNamespace(ns)
        .setMeta(metaForNamespace(req.getSpec().getCatalogId(), ns.getResourceId()))
        .build()
    );
  }

  @Override
  public Uni<RenameNamespaceResponse> renameNamespace(RenameNamespaceRequest req) {
    var p = principal.get();
    authz.require(p, "namespace.write");
    var tenantId = p.getTenantId();

    var namespaceId = req.getNamespaceId();
    ensureKind(namespaceId, ResourceKind.RK_NAMESPACE, "RenameNamespace");

    Namespace namespace = namespaces.get(namespaceId)
      .orElseThrow(() -> GrpcErrors.notFound(
        corrId(),
        "namespace", Map.of("id", namespaceId.getId())
      ));

    var newName = req.getNewDisplayName();
    var updated = namespace.toBuilder().setDisplayName(newName).build();

    NameRef namespaceNameRef  = nameIndex.getNamespaceById(tenantId, namespaceId.getId())
      .orElseThrow(() -> GrpcErrors.notFound(
        corrId(),
        "namespace", Map.of("id", namespaceId.getId())
      ));

    NameRef catalogNameRef = nameIndex.getCatalogByName(tenantId, namespaceNameRef.getCatalog())
      .orElseThrow(() -> GrpcErrors.notFound(
        corrId(),
        "catalog", Map.of("id", namespaceId.getId())
      ));
      
    var path = new ArrayList<>(namespaceNameRef.getPathList());
    if (!path.isEmpty()) path.remove(path.size() - 1);

    namespaces.delete(catalogNameRef.getResourceId(), namespaceId);
    namespaces.put(updated, catalogNameRef.getResourceId(), path);

    return Uni.createFrom().item(
      RenameNamespaceResponse.newBuilder()
        .setNamespace(updated)
        .setMeta(metaForNamespace(catalogNameRef.getResourceId(), namespaceId))
        .build()
    );
  }

  @Override
  public Uni<DeleteNamespaceResponse> deleteNamespace(DeleteNamespaceRequest req) {
    var p = principal.get();
    authz.require(p, "namespace.write");

    var namespaceId = req.getNamespaceId();
    ensureKind(namespaceId, ResourceKind.RK_NAMESPACE, "DeleteNamespace");
    var tenantId = p.getTenantId();

    if (req.getRequireEmpty() && tables.count(namespaceId) > 0) {
      var namespaceNameRef = nameIndex.getNamespaceById(p.getTenantId(), namespaceId.getId());
      if (namespaceNameRef.isPresent()) {
        throw GrpcErrors.conflict(
          corrId(),
          "namespace.not_empty",
          Map.of("display_name", NameIndexRepository.joinPath(namespaceNameRef.get().getPathList())));
      } else {
        throw GrpcErrors.conflict(
          corrId(),
          "namespace.not_empty",
          Map.of("display_name", namespaceId.getId())
        );
      }
    }

    NameRef namespaceNameRef = nameIndex.getNamespaceById(tenantId, namespaceId.getId())
      .orElseThrow(() -> GrpcErrors.notFound(
        corrId(),
        "namespace", Map.of("id", namespaceId.getId())
      ));

    NameRef catalogNameRef = nameIndex.getCatalogByName(tenantId, namespaceNameRef.getCatalog())
      .orElseThrow(() -> GrpcErrors.notFound(
        corrId(),
        "catalog", Map.of("id", namespaceId.getId())
      ));

    var m = metaForNamespace(catalogNameRef.getResourceId(), namespaceId);
    namespaces.delete(catalogNameRef.getResourceId(), namespaceId);

    return Uni.createFrom().item(DeleteNamespaceResponse.newBuilder().setMeta(m).build());
  }

  @Override
  public Uni<CreateTableResponse> createTable(CreateTableRequest req) {
    var p = principal.get();
    authz.require(p, "table.write");

    var tenant = p.getTenantId();
    var nowMs = clock.millis();
    var nowTs = Timestamps.fromMillis(nowMs);
    var idemKey = req.hasIdempotency() ? req.getIdempotency().getKey() : "";

    var catName = nameIndex.getCatalogById(tenant, req.getSpec().getCatalogId().getId())
    .map(NameRef::getCatalog)
    .orElseThrow(() -> GrpcErrors.notFound(
      corrId(), "catalog", Map.of("id", req.getSpec().getCatalogId().getId())));

    var nsOpt = nameIndex.getNamespaceById(tenant, req.getSpec().getNamespaceId().getId());
    var nsPaths = nsOpt.map(NameRef::getPathList)
      .map(List::copyOf)
      .orElse(List.of());

    var tableRef = NameRef.newBuilder()
      .setCatalog(catName)
      .addAllPath(nsPaths)
      .setName(req.getSpec().getDisplayName())
      .build();

    if (idemKey.isBlank() && nameIndex.getTableByName(tenant, tableRef).isPresent()) {
      throw GrpcErrors.conflict(
        corrId(),
        "table.already_exists",
        Map.of("name", tableRef.getName(),
              "path", String.join("/", tableRef.getPathList())));
    }

    byte[] fp = req.getSpec().toBuilder()
      .clearDescription()
      .build()
      .toByteArray();

    TableDescriptor td = IdempotencyGuard.runOnce(
      tenant,
      "CreateTable",
      idemKey,
      fp,
      (Supplier<IdempotencyGuard.CreateResult<TableDescriptor>>) () -> {
        var tableId = ResourceId.newBuilder()
          .setTenantId(tenant)
          .setId(UUID.randomUUID().toString())
          .setKind(ResourceKind.RK_TABLE)
          .build();

        var built = TableDescriptor.newBuilder()
          .setResourceId(tableId)
          .setDisplayName(mustNonEmpty(req.getSpec().getDisplayName(), "display_name"))
          .setDescription(req.getSpec().getDescription())
          .setCatalogId(req.getSpec().getCatalogId())
          .setNamespaceId(req.getSpec().getNamespaceId())
          .setRootUri(mustNonEmpty(req.getSpec().getRootUri(), "root_uri"))
          .setSchemaJson(mustNonEmpty(req.getSpec().getSchemaJson(), "schema_json"))
          .setCreatedAt(nowTs)
          .build();

        tables.put(built);
        return new IdempotencyGuard.CreateResult<>(built, tableId);
      },
      (Function<TableDescriptor, MutationMeta>)
        (t) -> tables.metaFor(t.getResourceId()),
      (Function<TableDescriptor, byte[]>) TableDescriptor::toByteArray,
      (Function<byte[], TableDescriptor>) bytes -> {
        try {
          return TableDescriptor.parseFrom(bytes);
        }
        catch (Exception e) {
          throw new IllegalStateException(e);
        }
      },
      idempotencyStore,
      86_400L,
      nowTs,
      this::corrId
    );

    return Uni.createFrom().item(
      CreateTableResponse.newBuilder()
        .setTable(td)
        .setMeta(tables.metaFor(td.getResourceId()))
        .build()
    );
  }

  @Override
  public Uni<UpdateTableSchemaResponse> updateTableSchema(UpdateTableSchemaRequest req) {
    var p = principal.get();
    authz.require(p, "table.write");

    var tableId = req.getTableId();
    ensureKind(tableId, ResourceKind.RK_TABLE, "UpdateTableSchema");

    var cur = tables.get(tableId)
      .orElseThrow(() -> GrpcErrors.notFound(corrId(), "table",
        Map.of("id", tableId.getId())));

    var updated = cur.toBuilder()
      .setSchemaJson(req.getSchemaJson())
      .clearDescription()
      .setDescription(cur.getDescription())
      .build();

    tables.put(updated);

    return Uni.createFrom().item(
      UpdateTableSchemaResponse.newBuilder()
        .setTable(updated)
        .setMeta(metaForTable(tableId))
        .build());
  }

  @Override
  public Uni<RenameTableResponse> renameTable(RenameTableRequest req) {
    var p = principal.get();
    authz.require(p, "table.write");

    var tableId = req.getTableId();
    ensureKind(tableId, ResourceKind.RK_TABLE, "RenameTable");

    var cur = tables.get(tableId).
      orElseThrow(() -> GrpcErrors.notFound(corrId(), "table",
        Map.of("id", tableId.getId())));

    var updated = cur.toBuilder().setDisplayName(req.getNewDisplayName()).build();

    tables.put(updated);

    return Uni.createFrom().item(
      RenameTableResponse.newBuilder()
        .setTable(updated)
        .setMeta(metaForTable(tableId))
        .build());
  }

  @Override
  public Uni<DeleteTableResponse> deleteTable(DeleteTableRequest req) {
    var p = principal.get();
    authz.require(p, "table.write");

    var tableId  = req.getTableId();
    ensureKind(tableId, ResourceKind.RK_TABLE, "DeleteTable");

    var m = metaForTable(tableId);
    if (!tables.delete(tableId)) {
      throw GrpcErrors.internal(corrId(), "table.delete_failed", Map.of("id", tableId.getId()));
    }

    return Uni.createFrom().item(DeleteTableResponse.newBuilder().setMeta(m).build());
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

  private long nowMs() { 
    return clock.millis(); 
  }

  private String corrId() {
    var pctx = principal != null ? principal.get() : null;
    return pctx != null ? pctx.getCorrelationId() : "";
  }

  private MutationMeta meta(String pointerKey, String blobUri) {
    long version = ptr.get(pointerKey).map(Pointer::getVersion).orElse(0L);
    var etag = blobs.head(blobUri).map(h -> h.getEtag()).orElse("");
    return MutationMeta.newBuilder()
      .setPointerKey(pointerKey)
      .setBlobUri(blobUri)
      .setPointerVersion(version)
      .setEtag(etag)
      .setUpdatedAt(Timestamps.fromMillis(nowMs()))
      .build();
  }

  private MutationMeta metaForCatalog(ResourceId catId) {
    String tenant = catId.getTenantId();
    String ptrKey = Keys.catPtr(tenant, catId.getId());
    String blob = Keys.catBlob(tenant, catId.getId());
    return meta(ptrKey, blob);
  }

  private MutationMeta metaForNamespace(ResourceId catId, ResourceId nsId) {
    String tenant = nsId.getTenantId();
    String ptrKey = Keys.nsPtr(tenant, catId.getId(), nsId.getId());
    String blob = Keys.nsBlob(tenant, catId.getId(), nsId.getId());
    return meta(ptrKey, blob);
  }

  private MutationMeta metaForTable(ResourceId tblId) {
    String tenant = tblId.getTenantId();
    String ptrKey = Keys.tblCanonicalPtr(tenant, tblId.getId());
    String blob = Keys.tblBlob(tenant, tblId.getId());
    return meta(ptrKey, blob);
  }
}
