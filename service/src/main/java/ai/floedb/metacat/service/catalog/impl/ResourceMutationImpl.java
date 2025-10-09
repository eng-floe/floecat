package ai.floedb.metacat.service.catalog.impl;

import java.time.Clock;
import java.util.UUID;

import com.google.protobuf.Any;

import jakarta.inject.Inject;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.BlobHeader;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.Pointer;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.repo.impl.CatalogRepository;
import ai.floedb.metacat.service.repo.impl.NameIndexRepository;
import ai.floedb.metacat.service.repo.impl.NamespaceRepository;
import ai.floedb.metacat.service.repo.impl.TableRepository;
import ai.floedb.metacat.service.repo.util.Keys;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;

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

  private final Clock clock;

  public ResourceMutationImpl() {
    this.clock = Clock.systemUTC();
  }

  // ======================
  // Catalog
  // ======================

  @Override
  public Uni<CreateCatalogResponse> createCatalog(CreateCatalogRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.write");

    var spec = req.getSpec();
    var tenantId = p.getTenantId();
    var display = mustNonEmpty(spec.getDisplayName(), "display_name");

    if (nameIndex.getCatalogByName(tenantId, display).isPresent()) {
      throw conflict("catalog '%s' already exists", display);
    }

    var catalogId = UUID.randomUUID().toString();
    var rid = ResourceId.newBuilder()
      .setTenantId(tenantId).setId(catalogId).setKind(ResourceKind.RK_CATALOG).build();

    var now = nowMs();
    var c = Catalog.newBuilder()
      .setResourceId(rid)
      .setDisplayName(display)
      .setDescription(spec.getDescription())
      .setCreatedAtMs(now)
      .build();

    catalogs.put(c);

    nameIndex.putCatalogIndex(tenantId, display, rid);

    var ptr = mustGetPointer(Keys.catPtr(tenantId, catalogId));
    var hdr = blobs.head(Keys.catBlob(tenantId, catalogId)).orElse(BlobHeader.newBuilder().build());

    return Uni.createFrom().item(
      CreateCatalogResponse.newBuilder()
      .setCatalog(c)
      .setMeta(MutationMeta.newBuilder()
        .setPointerKey(ptr.getKey())
        .setBlobUri(ptr.getBlobUri())
        .setPointerVersion(ptr.getVersion())
        .setEtag(hdr.getEtag())
        .setUpdatedAtMs(now)
        .build())
      .build());
  }

  @Override
  public Uni<UpdateCatalogResponse> updateCatalog(UpdateCatalogRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.write");

    var rid = req.getResourceId();
    ensureKind(rid, ResourceKind.RK_CATALOG, "UpdateCatalog");
    var tenantId = p.getTenantId();
    var catalogId= rid.getId();
    var ptrKey   = Keys.catPtr(tenantId, catalogId);

    // Precondition
    var current = mustGetPointer(ptrKey);
    checkPrecondition(req.getPrecondition(), current);

    // Load prior to preserve created_at
    var prev = catalogs.get(rid).orElseThrow(() -> notFound("catalog not found: %s", catalogId));
    var display = mustNonEmpty(req.getSpec().getDisplayName(), "display_name");
    var updated = prev.toBuilder()
        .setDisplayName(display)
        .setDescription(req.getSpec().getDescription())
        .build();

    catalogs.put(updated); // repo CAS flips pointer to new blob

    // Update indexes
    nameIndex.putCatalogIndex(tenantId, display, rid);

    var ptr = mustGetPointer(ptrKey);
    var hdr = blobs.head(ptr.getBlobUri()).orElse(BlobHeader.newBuilder().build());

    return Uni.createFrom().item(
      UpdateCatalogResponse.newBuilder()
        .setCatalog(updated)
        .setMeta(meta(ptr, hdr.getEtag()))
        .build()
    );
  }

  @Override
  public Uni<DeleteCatalogResponse> deleteCatalog(DeleteCatalogRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.write");

    var rid = req.getResourceId();
    ensureKind(rid, ResourceKind.RK_CATALOG, "DeleteCatalog");
    var tenantId = p.getTenantId();

    var catPtrKey = Keys.catPtr(tenantId, rid.getId());
    var current   = mustGetPointer(catPtrKey);
    checkPrecondition(req.getPrecondition(), current);

    if (req.getRequireEmpty() && namespaces.count(rid) > 0) {
      throw conflict("catalog not empty");
    }

    // Get display name for by-name index deletion (optional but nice)
    var prev = catalogs.get(rid).orElse(null);

    // physical delete ptr + blob
    catalogs.delete(rid);

    // delete both catalog index entries
    if (prev != null) {
      nameIndex.deleteCatalogByName(tenantId, prev.getDisplayName());
    }
    nameIndex.deleteCatalogById(tenantId, rid.getId());

    return Uni.createFrom().item(DeleteCatalogResponse.newBuilder().build());
  }

  // ======================
  // Namespace
  // ======================

  @Override
  public Uni<CreateNamespaceResponse> createNamespace(CreateNamespaceRequest req) {
    var p = principal.get();
    authz.require(p, "namespace.write");

    var spec = req.getSpec();
    var catId = requireId(spec.getCatalogId(), ResourceKind.RK_CATALOG, "NamespaceSpec.catalog_id");
    var tenantId = p.getTenantId();

    // Parent must exist
    mustGetPointer(Keys.catPtr(tenantId, catId));

    var nsId = UUID.randomUUID().toString();
    var rid = ResourceId.newBuilder()
        .setTenantId(tenantId).setId(nsId).setKind(ResourceKind.RK_NAMESPACE).build();

    var now = nowMs();
    var ns = Namespace.newBuilder()
        .setResourceId(rid)
        .setDisplayName(mustNonEmpty(spec.getDisplayName(), "display_name"))
        .setDescription(spec.getDescription())
        .setCreatedAtMs(now)
        .build();

    namespaces.put(ns, spec.getCatalogId()); // writes pointer & blob

    // Name indexes (by-path and by-id)
    var ref = NamespaceRef.newBuilder()
        .setCatalogId(spec.getCatalogId())
        .addAllNamespacePath(spec.getPathList())
        .build();
    nameIndex.putNamespaceIndex(tenantId, ref, rid);

    var ptr = mustGetPointer(Keys.nsPtr(tenantId, catId, nsId));
    var hdr = blobs.head(ptr.getBlobUri()).orElse(BlobHeader.newBuilder().build());

    return Uni.createFrom().item(
      CreateNamespaceResponse.newBuilder()
        .setNamespace(ns)
        .setMeta(meta(ptr, hdr.getEtag()))
        .build()
    );
  }

  @Override
  public Uni<RenameNamespaceResponse> renameNamespace(RenameNamespaceRequest req) {
    var p = principal.get();
    authz.require(p, "namespace.write");

    var rid = req.getResourceId();
    ensureKind(rid, ResourceKind.RK_NAMESPACE, "RenameNamespace");
    var tenantId = p.getTenantId();
    var nsId = rid.getId();

    var nsIdx = nameIndex.getNamespaceById(tenantId, nsId)
        .orElseThrow(() -> notFound("namespace index missing (by-id): %s", nsId));
    var catalogId = nsIdx.getRef().getCatalogId().getId();

    var ptrKey = Keys.nsPtr(tenantId, catalogId, nsId);
    var current = mustGetPointer(ptrKey);
    checkPrecondition(req.getPrecondition(), current);

    var cur = namespaces.get(rid, nsIdx.getRef().getCatalogId())
      .orElseThrow(() -> notFound("namespace not found: %s", nsId));

    var newName = mustNonEmpty(req.getNewDisplayName(), "new_display_name");
    var updated = cur.toBuilder().setDisplayName(newName).build();

    namespaces.put(updated, nsIdx.getRef().getCatalogId());

    nameIndex.putNamespaceIndex(tenantId, nsIdx.getRef(), rid);
    if (!req.getNewPathList().isEmpty()) {
      nameIndex.deleteNamespaceByPath(tenantId, catalogId, nsIdx.getRef().getNamespacePathList());
      var newRef = nsIdx.getRef().toBuilder().clearNamespacePath().addAllNamespacePath(req.getNewPathList()).build();
      nameIndex.putNamespaceIndex(tenantId, newRef, rid);
    }

    var ptr = mustGetPointer(ptrKey);
    var hdr = blobs.head(ptr.getBlobUri()).orElse(BlobHeader.newBuilder().build());

    return Uni.createFrom().item(
      RenameNamespaceResponse.newBuilder()
        .setNamespace(updated)
        .setMeta(meta(ptr, hdr.getEtag()))
        .build()
    );
  }

  @Override
  public Uni<DeleteNamespaceResponse> deleteNamespace(DeleteNamespaceRequest req) {
    var p = principal.get();
    authz.require(p, "namespace.write");

    var rid = req.getResourceId();
    ensureKind(rid, ResourceKind.RK_NAMESPACE, "DeleteNamespace");
    var tenantId = p.getTenantId();

    var nsIdx = nameIndex.getNamespaceById(tenantId, rid.getId())
        .orElseThrow(() -> notFound("namespace index missing (by-id): %s", rid.getId()));
    var catalogId = nsIdx.getRef().getCatalogId().getId();

    var nsPtrKey = Keys.nsPtr(tenantId, catalogId, rid.getId());
    var current  = mustGetPointer(nsPtrKey);
    checkPrecondition(req.getPrecondition(), current);

    if (req.getRequireEmpty()) {
      if (tables.count(tenantId, catalogId, rid.getId()) > 0) {
        throw conflict("namespace not empty");
      }
    }

    // physical delete ptr + blob
    namespaces.delete(nsIdx.getRef().getCatalogId(), rid);

    // delete both namespace index entries
    nameIndex.deleteNamespaceById(tenantId, rid.getId());
    nameIndex.deleteNamespaceByPath(tenantId, catalogId, nsIdx.getRef().getNamespacePathList());

    return Uni.createFrom().item(DeleteNamespaceResponse.newBuilder().build());
  }

  // ======================
  // Table
  // ======================

  @Override
  public Uni<CreateTableResponse> createTable(CreateTableRequest req) {
    var p = principal.get();
    authz.require(p, "table.write");

    var spec = req.getSpec();
    var tenantId = p.getTenantId();
    var catalogId= requireId(spec.getCatalogId(), ResourceKind.RK_CATALOG, "TableSpec.catalog_id");
    var nsId     = requireId(spec.getNamespaceId(), ResourceKind.RK_NAMESPACE, "TableSpec.namespace_id");

    // parents exist
    mustGetPointer(Keys.catPtr(tenantId, catalogId));
    mustGetPointer(Keys.nsPtr(tenantId, catalogId, nsId));

    var tblId = UUID.randomUUID().toString();
    var rid = ResourceId.newBuilder()
        .setTenantId(tenantId).setId(tblId).setKind(ResourceKind.RK_TABLE).build();

    var now = nowMs();
    var td = TableDescriptor.newBuilder()
        .setResourceId(rid)
        .setDisplayName(mustNonEmpty(spec.getDisplayName(), "display_name"))
        .setDescription(spec.getDescription())
        .setCatalogId(spec.getCatalogId())
        .setNamespaceId(spec.getNamespaceId())
        .setRootUri(mustNonEmpty(spec.getRootUri(), "root_uri"))
        .setCreatedAtMs(now)
        .setSchemaJson(mustNonEmpty(spec.getSchemaJson(), "schema_json"))
        .build();

    tables.put(td); // writes canonical + ns index pointers & blob

    // Name indexes
    NameRef name = NameRef.newBuilder()
        .setCatalog(spec.getCatalogId().getId())
        .addAllNamespacePath(/* if you keep path strings, add here */ java.util.List.of(nsId))
        .setName(td.getDisplayName())
        .build();
    nameIndex.putTableIndex(tenantId, name, rid);

    var ptr = mustGetPointer(Keys.tblCanonicalPtr(tenantId, tblId));
    var hdr = blobs.head(ptr.getBlobUri()).orElse(BlobHeader.newBuilder().build());

    return Uni.createFrom().item(
      CreateTableResponse.newBuilder()
        .setTable(td)
        .setMeta(meta(ptr, hdr.getEtag()))
        .build()
    );
  }

  @Override
  public Uni<UpdateTableSchemaResponse> updateTableSchema(UpdateTableSchemaRequest req) {
    var p = principal.get();
    authz.require(p, "table.write");

    var tableId = req.getTableId();
    ensureKind(tableId, ResourceKind.RK_TABLE, "UpdateTableSchema");
    var tenantId = p.getTenantId();
    var tblId    = tableId.getId();

    var canonPtr = Keys.tblCanonicalPtr(tenantId, tblId);
    var current = mustGetPointer(canonPtr);
    checkPrecondition(req.getPrecondition(), current);

    var cur = tables.getById(tableId).orElseThrow(() -> notFound("table not found: %s", tblId));
    var updated = cur.toBuilder()
        .setSchemaJson(mustNonEmpty(req.getSchemaJson(), "schema_json"))
        .clearDescription().setDescription(cur.getDescription())
        .build();

    tables.put(updated);

    // update indexes
    NameRef name = NameRef.newBuilder()
        .setCatalog(updated.getCatalogId().getId())
        .addAllNamespacePath(java.util.List.of(updated.getNamespaceId().getId()))
        .setName(updated.getDisplayName())
        .build();
    nameIndex.putTableIndex(tenantId, name, updated.getResourceId());

    var ptr = mustGetPointer(canonPtr);
    var hdr = blobs.head(ptr.getBlobUri()).orElse(BlobHeader.newBuilder().build());

    return Uni.createFrom().item(
      UpdateTableSchemaResponse.newBuilder()
        .setTable(updated)
        .setMeta(meta(ptr, hdr.getEtag()))
        .build()
    );
  }

  @Override
  public Uni<RenameTableResponse> renameTable(RenameTableRequest req) {
    var p = principal.get();
    authz.require(p, "table.write");

    var tableId = req.getTableId();
    ensureKind(tableId, ResourceKind.RK_TABLE, "RenameTable");

    var tenantId = p.getTenantId();
    var canonPtr = Keys.tblCanonicalPtr(tenantId, tableId.getId());
    var current = mustGetPointer(canonPtr);
    checkPrecondition(req.getPrecondition(), current);

    var cur = tables.getById(tableId).orElseThrow(() -> notFound("table not found: %s", tableId.getId()));
    var newName = mustNonEmpty(req.getNewDisplayName(), "new_display_name");

    // remove old by-name index
    var oldFq = NameRef.newBuilder()
        .setCatalog(cur.getCatalogId().getId())
        .addNamespacePath(cur.getNamespaceId().getId())
        .setName(cur.getDisplayName())
        .build();
    nameIndex.deleteTableByName(tenantId, oldFq);

    // write new table + new by-name index
    var updated = cur.toBuilder().setDisplayName(newName).build();
    tables.put(updated);
    var newFq = NameRef.newBuilder()
        .setCatalog(updated.getCatalogId().getId())
        .addNamespacePath(updated.getNamespaceId().getId())
        .setName(updated.getDisplayName())
        .build();
    nameIndex.putTableIndex(tenantId, newFq, updated.getResourceId());

    var ptr = mustGetPointer(canonPtr);
    var hdr = blobs.head(ptr.getBlobUri()).orElse(BlobHeader.newBuilder().build());

    return Uni.createFrom().item(
        RenameTableResponse.newBuilder().setTable(updated).setMeta(meta(ptr, hdr.getEtag())).build()
    );
  }

  @Override
  public Uni<DeleteTableResponse> deleteTable(DeleteTableRequest req) {
    var p = principal.get();
    authz.require(p, "table.write");

    var tableId = req.getTableId();
    ensureKind(tableId, ResourceKind.RK_TABLE, "DeleteTable");
    var tenantId = p.getTenantId();

    var canonPtrKey = Keys.tblCanonicalPtr(tenantId, tableId.getId());
    var current = mustGetPointer(canonPtrKey);
    checkPrecondition(req.getPrecondition(), current);

    var cur = tables.getById(tableId).orElseThrow(() -> notFound("table not found: %s", tableId.getId()));

    // delete pointers + blob
    tables.delete(tableId, cur.getCatalogId(), cur.getNamespaceId());

    // delete index by-id and by-name
    nameIndex.deleteTableById(tenantId, tableId.getId());
    var oldFq = NameRef.newBuilder()
        .setCatalog(cur.getCatalogId().getId())
        .addNamespacePath(cur.getNamespaceId().getId())
        .setName(cur.getDisplayName())
        .build();
    nameIndex.deleteTableByName(tenantId, oldFq);

    return Uni.createFrom().item(DeleteTableResponse.newBuilder().build());
  }

  // ======================
  // Helpers
  // ======================

  private void ensureKind(ResourceId rid, ResourceKind want, String op) {
    if (rid == null) throw invalid("%s: missing resource_id", op);
    if (rid.getKind() != want) throw invalid("%s: wrong resource kind: %s", op, rid.getKind());
  }

  private String requireId(ResourceId rid, ResourceKind kind, String field) {
    if (rid == null || rid.getId().isBlank() || rid.getTenantId().isBlank())
      throw invalid("missing %s", field);
    if (rid.getKind() != kind) throw invalid("wrong kind for %s: %s", field, rid.getKind());
    return rid.getId();
  }

  private String mustNonEmpty(String v, String name) {
    if (v == null || v.isBlank()) throw invalid("missing %s", name);
    return v;
  }

  private long nowMs() { return clock.millis(); }

  private Pointer mustGetPointer(String key) {
    return ptr.get(key).orElseThrow(() -> notFound("pointer not found: %s", key));
  }

  private void checkPrecondition(Precondition pre, Pointer current) {
    if (pre == null) return;
    if (pre.getExpectedVersion() != 0) {
      if (current.getVersion() != pre.getExpectedVersion()) {
        throw precond("expected_version=%d, current=%d", pre.getExpectedVersion(), current.getVersion());
      }
    } else if (!pre.getExpectedEtag().isBlank()) {
      var hdr = blobs.head(current.getBlobUri());
      if (hdr.isEmpty() || !pre.getExpectedEtag().equals(hdr.get().getEtag())) {
        throw precond("etag mismatch");
      }
    }
  }

  private MutationMeta meta(Pointer p, String etag) {
    return MutationMeta.newBuilder()
        .setPointerKey(p.getKey())
        .setBlobUri(p.getBlobUri())
        .setPointerVersion(p.getVersion())
        .setEtag(etag == null ? "" : etag)
        .setUpdatedAtMs(clock.millis())
        .build();
  }

  private StatusRuntimeException invalid(String fmt, Object... args) {
    return withDetails(io.grpc.Status.INVALID_ARGUMENT, "INVALID_ARGUMENT", fmt, args);
  }

  private StatusRuntimeException notFound(String fmt, Object... args) {
    return withDetails(io.grpc.Status.NOT_FOUND, "NOT_FOUND", fmt, args);
  }

  private StatusRuntimeException conflict(String fmt, Object... args) {
    // ABORTED is what your test expects
    return withDetails(io.grpc.Status.ABORTED, "ABORTED", fmt, args);
  }

  private StatusRuntimeException precond(String fmt, Object... args) {
    return withDetails(io.grpc.Status.FAILED_PRECONDITION, "FAILED_PRECONDITION", fmt, args);
  }

  private StatusRuntimeException withDetails(io.grpc.Status grpcStatus, String codeStr, String fmt, Object... args) {
    var message = String.format(fmt, args);
    var pctx = principal != null ? principal.get() : null;

    var err = ai.floedb.metacat.common.rpc.Error.newBuilder()
        .setCode(codeStr)
        .setMessage(message)
        .putDetails("service", "ResourceMutation")
        .setCorrelationId(pctx != null ? pctx.getCorrelationId() : "")
        .build();

    var rpc = com.google.rpc.Status.newBuilder()
        .setCode(grpcStatus.getCode().value())   // map io.grpc status code -> google.rpc.Status code
        .setMessage(message)
        .addDetails(Any.pack(err))
        .build();

    return StatusProto.toStatusRuntimeException(rpc);
  }
}