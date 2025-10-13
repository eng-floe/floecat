package ai.floedb.metacat.service.catalog.impl;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;

@GrpcService
public class ResourceMutationImpl implements ResourceMutation {

  @Inject CatalogRepository catalogs;
  @Inject NamespaceRepository namespaces;
  @Inject TableRepository tables;
  @Inject NameIndexRepository nameIndex;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;

  private final Clock clock;

  public ResourceMutationImpl() {
    this.clock = Clock.systemUTC();
  }

  @Override
  public Uni<CreateCatalogResponse> createCatalog(CreateCatalogRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.write");

    var spec = req.getSpec();
    var tenantId = p.getTenantId();

    if (catalogs.get(spec.getDisplayName()).isPresent()) {
      throw GrpcErrors.conflict(
        corrId(),
        "catalog.already_exists",
        Map.of("display_name", spec.getDisplayName())
      );
    }

    var catalogId = ResourceId.newBuilder()
      .setTenantId(tenantId)
      .setId(UUID.randomUUID().toString())
      .setKind(ResourceKind.RK_CATALOG)
      .build();

    var now = nowMs();
    var c = Catalog.newBuilder()
      .setResourceId(catalogId)
      .setDisplayName(spec.getDisplayName())
      .setDescription(spec.getDescription())
      .setCreatedAt(Timestamps.fromMillis(now))
      .build();

    catalogs.put(c);

    return Uni.createFrom().item(
      CreateCatalogResponse.newBuilder()
      .setCatalog(c)
      .build());
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

    catalogs.delete(catalogId);

    return Uni.createFrom().item(DeleteCatalogResponse.newBuilder().build());
  }

  @Override
  public Uni<CreateNamespaceResponse> createNamespace(CreateNamespaceRequest req) {
    var p = principal.get();
    authz.require(p, "namespace.write");

    var spec = req.getSpec();
    var tenantId = p.getTenantId();

    var parents = spec.getPathList();
    var full = new ArrayList<>(parents);
    full.addAll(List.of(spec.getDisplayName()));

    var existing = nameIndex.getNamespaceByPath(
      p.getTenantId(), spec.getCatalogId().getId(), full);

    if (existing.isPresent()) {
      throw GrpcErrors.conflict(corrId(), "namespace.already_exists",
        Map.of("catalog", spec.getCatalogId().getId(),
          "path", String.join("/", full)));
    }

    var namespaceId = ResourceId.newBuilder()
      .setTenantId(tenantId)
      .setId(UUID.randomUUID().toString())
      .setKind(ResourceKind.RK_NAMESPACE)
      .build();

    var now = nowMs();
    var namespace = Namespace.newBuilder()
      .setResourceId(namespaceId)
      .setDisplayName(spec.getDisplayName())
      .setDescription(spec.getDescription())
      .setCreatedAt(Timestamps.fromMillis(now))
      .build();

    namespaces.put(namespace, spec.getCatalogId(), spec.getPathList());

    return Uni.createFrom().item(
      CreateNamespaceResponse.newBuilder()
        .setNamespace(namespace)
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

    namespaces.delete(catalogNameRef.getResourceId(), namespaceId);

    return Uni.createFrom().item(DeleteNamespaceResponse.newBuilder().build());
  }

  @Override
  public Uni<CreateTableResponse> createTable(CreateTableRequest req) {
    var p = principal.get();
    authz.require(p, "table.write");

    var spec = req.getSpec();
    var tenantId = p.getTenantId();

    var tableId = ResourceId.newBuilder()
      .setTenantId(tenantId)
      .setId(UUID.randomUUID().toString())
      .setKind(ResourceKind.RK_TABLE)
      .build();

    var now = nowMs();
    var td = TableDescriptor.newBuilder()
      .setResourceId(tableId)
      .setDisplayName(mustNonEmpty(spec.getDisplayName(), "display_name"))
      .setDescription(spec.getDescription())
      .setCatalogId(spec.getCatalogId())
      .setNamespaceId(spec.getNamespaceId())
      .setRootUri(mustNonEmpty(spec.getRootUri(), "root_uri"))
      .setCreatedAt(Timestamps.fromMillis(now))
      .setSchemaJson(mustNonEmpty(spec.getSchemaJson(), "schema_json"))
      .build();

    tables.put(td);

    return Uni.createFrom().item(
      CreateTableResponse.newBuilder()
        .setTable(td)
        .build());
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
        .build());
  }

  @Override
  public Uni<DeleteTableResponse> deleteTable(DeleteTableRequest req) {
    var p = principal.get();
    authz.require(p, "table.write");

    var tableId  = req.getTableId();
    ensureKind(tableId, ResourceKind.RK_TABLE, "DeleteTable");

    if (!tables.delete(tableId)) {
      throw GrpcErrors.internal(corrId(), "table.delete_failed", Map.of("id", tableId.getId()));
    }

    return Uni.createFrom().item(DeleteTableResponse.newBuilder().build());
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
}
