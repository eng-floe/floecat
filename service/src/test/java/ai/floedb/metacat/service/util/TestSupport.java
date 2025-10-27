package ai.floedb.metacat.service.util;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.ErrorCode;
import ai.floedb.metacat.common.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.connector.rpc.Connector;
import ai.floedb.metacat.connector.rpc.ConnectorSpec;
import ai.floedb.metacat.connector.rpc.ConnectorsGrpc;
import ai.floedb.metacat.connector.rpc.CreateConnectorRequest;
import ai.floedb.metacat.service.repo.model.Keys;
import ai.floedb.metacat.storage.BlobStore;
import ai.floedb.metacat.storage.PointerStore;
import ai.floedb.metacat.tenancy.rpc.CreateTenantRequest;
import ai.floedb.metacat.tenancy.rpc.TenancyGrpc;
import ai.floedb.metacat.tenancy.rpc.Tenant;
import ai.floedb.metacat.tenancy.rpc.TenantSpec;
import com.google.protobuf.Any;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public final class TestSupport {
  private TestSupport() {}

  public static final String DEFAULT_SEED_TENANT = "t-0001";

  public static ResourceId rid(String tenantId, String id, ResourceKind kind) {
    return ResourceId.newBuilder().setTenantId(tenantId).setId(id).setKind(kind).build();
  }

  public static NameRef fq(String catalog, List<String> path, String name) {
    var b = NameRef.newBuilder().setCatalog(catalog).addAllPath(path);
    if (name != null) {
      b.setName(name);
    }
    return b.build();
  }

  public static ResourceId createTenantId(String tid) {
    String tidUUID = UUID.nameUUIDFromBytes(("/tenant:" + tid).getBytes()).toString();
    ResourceId tenantId =
        ResourceId.newBuilder()
            .setTenantId(tidUUID)
            .setId(tidUUID)
            .setKind(ResourceKind.RK_TENANT)
            .build();
    return tenantId;
  }

  public static Tenant createTenant(
      TenancyGrpc.TenancyBlockingStub tenants, String displayName, String description, long now) {
    var resp =
        tenants.createTenant(
            CreateTenantRequest.newBuilder()
                .setSpec(
                    TenantSpec.newBuilder().setDisplayName(displayName).setDescription(description))
                .build());
    return resp.getTenant();
  }

  public static Catalog createCatalog(
      CatalogServiceGrpc.CatalogServiceBlockingStub mutation,
      String displayName,
      String description) {
    var resp =
        mutation.createCatalog(
            CreateCatalogRequest.newBuilder()
                .setSpec(
                    CatalogSpec.newBuilder()
                        .setDisplayName(displayName)
                        .setDescription(description))
                .build());
    return resp.getCatalog();
  }

  public static Namespace createNamespace(
      NamespaceServiceGrpc.NamespaceServiceBlockingStub mutation,
      ResourceId catalogId,
      String displayName,
      List<String> path,
      String desc) {
    var specB =
        NamespaceSpec.newBuilder()
            .setCatalogId(catalogId)
            .setDisplayName(displayName)
            .setDescription(desc);
    if (path != null) {
      specB.addAllPath(path);
    }
    var resp = mutation.createNamespace(CreateNamespaceRequest.newBuilder().setSpec(specB).build());
    return resp.getNamespace();
  }

  public static Table createTable(
      TableServiceGrpc.TableServiceBlockingStub mutation,
      ResourceId catalogId,
      ResourceId namespaceId,
      String displayName,
      String rootUri,
      String schemaJson,
      String desc) {
    var resp =
        mutation.createTable(
            CreateTableRequest.newBuilder()
                .setSpec(
                    TableSpec.newBuilder()
                        .setCatalogId(catalogId)
                        .setNamespaceId(namespaceId)
                        .setDisplayName(displayName)
                        .setDescription(desc)
                        .setFormat(TableFormat.TF_ICEBERG)
                        .setRootUri(rootUri)
                        .setSchemaJson(schemaJson))
                .build());
    return resp.getTable();
  }

  public static Snapshot createSnapshot(
      SnapshotServiceGrpc.SnapshotServiceBlockingStub mutation,
      ResourceId tableId,
      long snapshotId,
      long upstreamCreatedAtMs) {
    var resp =
        mutation.createSnapshot(
            CreateSnapshotRequest.newBuilder()
                .setSpec(
                    SnapshotSpec.newBuilder()
                        .setTableId(tableId)
                        .setSnapshotId(snapshotId)
                        .setUpstreamCreatedAt(Timestamps.fromMillis(upstreamCreatedAtMs)))
                .build());
    return resp.getSnapshot();
  }

  public static View createView(
      ViewServiceGrpc.ViewServiceBlockingStub mutation,
      ResourceId catalogId,
      ResourceId namespaceId,
      String displayName,
      String sql,
      String desc) {
    var resp =
        mutation.createView(
            CreateViewRequest.newBuilder()
                .setSpec(
                    ViewSpec.newBuilder()
                        .setCatalogId(catalogId)
                        .setNamespaceId(namespaceId)
                        .setDisplayName(displayName)
                        .setDescription(desc)
                        .setSql(sql))
                .build());
    return resp.getView();
  }

  public static View renameView(
      ViewServiceGrpc.ViewServiceBlockingStub mutation, ResourceId viewId, String newName) {
    var spec = ViewSpec.newBuilder().setDisplayName(newName).build();
    return mutation
        .updateView(UpdateViewRequest.newBuilder().setViewId(viewId).setSpec(spec).build())
        .getView();
  }

  public static View updateViewSql(
      ViewServiceGrpc.ViewServiceBlockingStub mutation, ResourceId viewId, String newSql) {
    var spec = ViewSpec.newBuilder().setSql(newSql).build();
    return mutation
        .updateView(UpdateViewRequest.newBuilder().setViewId(viewId).setSpec(spec).build())
        .getView();
  }

  public static void deleteView(
      ViewServiceGrpc.ViewServiceBlockingStub mutation, ResourceId viewId) {
    mutation.deleteView(DeleteViewRequest.newBuilder().setViewId(viewId).build());
  }

  public static Connector createConnector(
      ConnectorsGrpc.ConnectorsBlockingStub connectors, ConnectorSpec spec) {
    return connectors
        .createConnector(CreateConnectorRequest.newBuilder().setSpec(spec).build())
        .getConnector();
  }

  public static ResourceId resolveCatalogId(
      DirectoryGrpc.DirectoryBlockingStub directory, String catalogName) {
    var r =
        directory.resolveCatalog(
            ResolveCatalogRequest.newBuilder()
                .setRef(NameRef.newBuilder().setCatalog(catalogName))
                .build());
    return r.getResourceId();
  }

  public static ResourceId resolveNamespaceId(
      DirectoryGrpc.DirectoryBlockingStub directory, String catalog, List<String> path) {
    var r =
        directory.resolveNamespace(
            ResolveNamespaceRequest.newBuilder().setRef(fq(catalog, path, null)).build());
    return r.getResourceId();
  }

  public static ResourceId resolveTableId(
      DirectoryGrpc.DirectoryBlockingStub directory,
      String catalog,
      List<String> path,
      String name) {
    var r =
        directory.resolveTable(
            ResolveTableRequest.newBuilder().setRef(fq(catalog, path, name)).build());
    return r.getResourceId();
  }

  public static Table updateSchema(
      TableServiceGrpc.TableServiceBlockingStub mutation,
      ResourceId tableId,
      String newSchemaJson) {
    TableSpec spec = TableSpec.newBuilder().setSchemaJson(newSchemaJson).build();
    return mutation
        .updateTable(UpdateTableRequest.newBuilder().setTableId(tableId).setSpec(spec).build())
        .getTable();
  }

  public static Table renameTable(
      TableServiceGrpc.TableServiceBlockingStub mutation, ResourceId tableId, String newName) {
    TableSpec spec = TableSpec.newBuilder().setDisplayName(newName).build();
    return mutation
        .updateTable(UpdateTableRequest.newBuilder().setTableId(tableId).setSpec(spec).build())
        .getTable();
  }

  public static void deleteTable(
      TableServiceGrpc.TableServiceBlockingStub mutation,
      ResourceId namespaceId,
      ResourceId tableId) {
    mutation.deleteTable(DeleteTableRequest.newBuilder().setTableId(tableId).build());
  }

  public static void deleteSnapshot(
      SnapshotServiceGrpc.SnapshotServiceBlockingStub mutation,
      ResourceId tableId,
      long snapshotId) {
    mutation.deleteSnapshot(
        DeleteSnapshotRequest.newBuilder().setTableId(tableId).setSnapshotId(snapshotId).build());
  }

  public static void deleteNamespace(
      NamespaceServiceGrpc.NamespaceServiceBlockingStub mutation,
      ResourceId nsId,
      boolean requireEmpty) {
    mutation.deleteNamespace(
        DeleteNamespaceRequest.newBuilder()
            .setNamespaceId(nsId)
            .setRequireEmpty(requireEmpty)
            .build());
  }

  public static void deleteCatalog(
      CatalogServiceGrpc.CatalogServiceBlockingStub mutation,
      ResourceId catalogId,
      boolean requireEmpty) {
    mutation.deleteCatalog(
        DeleteCatalogRequest.newBuilder()
            .setCatalogId(catalogId)
            .setRequireEmpty(requireEmpty)
            .build());
  }

  public static ai.floedb.metacat.common.rpc.Error unpackMcError(StatusRuntimeException ex)
      throws Exception {
    var st = StatusProto.fromThrowable(ex);
    if (st == null) return null;
    for (Any any : st.getDetailsList()) {
      if (any.is(ai.floedb.metacat.common.rpc.Error.class)) {
        return any.unpack(ai.floedb.metacat.common.rpc.Error.class);
      }
    }
    return null;
  }

  public static void assertGrpcAndMc(
      StatusRuntimeException ex, Status.Code grpcCode, ErrorCode mcCode, String msgContains)
      throws Exception {
    Objects.requireNonNull(ex, "ex");
    assert ex.getStatus().getCode() == grpcCode
        : "expected gRPC code " + grpcCode + " got " + ex.getStatus().getCode();
    var mc = unpackMcError(ex);
    assert mc != null : "missing mc.Error";
    if (mcCode != null) {
      assert mc.getCode() == mcCode : "expected mc code " + mcCode + " got " + mc.getCode();
    }
    if (msgContains != null && !msgContains.isBlank()) {
      assert mc.getMessage().contains(msgContains)
          : "expected message to contain '" + msgContains + "' but was: " + mc.getMessage();
    }
  }

  public interface PageFetcher<T> {
    List<T> fetch(String token, StringBuilder nextTokenOut);
  }

  public static <T> List<T> collectAll(PageFetcher<T> fetcher, int maxPages) {
    var all = new ArrayList<T>();
    String token = "";
    for (int i = 0; i < maxPages; i++) {
      var next = new StringBuilder();
      var items = fetcher.fetch(token, next);
      all.addAll(items);
      var nt = next.toString();
      if (nt.isEmpty()) break;
      token = nt;
    }
    return all;
  }

  public static MutationMeta metaForNamespace(
      PointerStore ptr,
      BlobStore blobs,
      String tenant,
      String catalogDisplayName,
      List<String> fullPath) {

    var catIdxKey = Keys.catalogPointerByName(tenant, catalogDisplayName);
    var catPtr =
        ptr.get(catIdxKey)
            .orElseThrow(() -> new AssertionError("catalog by-name pointer missing: " + catIdxKey));

    Catalog cat;
    try {
      cat = Catalog.parseFrom(blobs.get(catPtr.getBlobUri()));
    } catch (Exception e) {
      throw new AssertionError("failed to parse Catalog blob: " + catPtr.getBlobUri(), e);
    }
    var catalogId = cat.getResourceId().getId();

    var nsIdxKey = Keys.namespacePointerByPath(tenant, catalogId, fullPath);
    var nsIdxPtr =
        ptr.get(nsIdxKey)
            .orElseThrow(
                () -> new AssertionError("namespace by-path pointer missing: " + nsIdxKey));

    Namespace ns;
    try {
      ns = Namespace.parseFrom(blobs.get(nsIdxPtr.getBlobUri()));
    } catch (Exception e) {
      throw new AssertionError("failed to parse Namespace blob: " + nsIdxPtr.getBlobUri(), e);
    }

    var nsRid = ns.getResourceId();
    var nsTenant = nsRid.getTenantId();

    var nsPtrKey = Keys.namespacePointerById(nsTenant, nsRid.getId());
    var nsBlob = Keys.namespaceBlobUri(nsTenant, nsRid.getId());

    var nsPtr =
        ptr.get(nsPtrKey)
            .orElseThrow(
                () -> new AssertionError("namespace canonical pointer missing: " + nsPtrKey));
    var hdr =
        blobs
            .head(nsBlob)
            .orElseThrow(() -> new AssertionError("namespace blob header missing: " + nsBlob));

    return MutationMeta.newBuilder()
        .setPointerKey(nsPtrKey)
        .setBlobUri(nsBlob)
        .setPointerVersion(nsPtr.getVersion())
        .setEtag(hdr.getEtag())
        .setUpdatedAt(com.google.protobuf.util.Timestamps.fromMillis(System.currentTimeMillis()))
        .build();
  }

  public static MutationMeta metaForTable(PointerStore ptr, BlobStore blobs, ResourceId tableId) {

    var tenant = tableId.getTenantId();
    var tblPtrKey = Keys.tablePointerById(tenant, tableId.getId());
    var tblBlob = Keys.tableBlobUri(tenant, tableId.getId());

    var tblPtr =
        ptr.get(tblPtrKey)
            .orElseThrow(() -> new AssertionError("table pointer missing: " + tblPtrKey));
    var hdr =
        blobs
            .head(tblBlob)
            .orElseThrow(() -> new AssertionError("table blob header missing: " + tblBlob));

    return MutationMeta.newBuilder()
        .setPointerKey(tblPtrKey)
        .setBlobUri(tblBlob)
        .setPointerVersion(tblPtr.getVersion())
        .setEtag(hdr.getEtag())
        .setUpdatedAt(Timestamps.fromMillis(System.currentTimeMillis()))
        .build();
  }

  public static MutationMeta metaForView(PointerStore ptr, BlobStore blobs, ResourceId viewId) {
    var tenant = viewId.getTenantId();
    var viewPtrKey = Keys.viewPointerById(tenant, viewId.getId());
    var viewBlob = Keys.viewBlobUri(tenant, viewId.getId());

    var viewPtr =
        ptr.get(viewPtrKey)
            .orElseThrow(() -> new AssertionError("view pointer missing: " + viewPtrKey));
    var hdr =
        blobs
            .head(viewBlob)
            .orElseThrow(() -> new AssertionError("view blob header missing: " + viewBlob));

    return MutationMeta.newBuilder()
        .setPointerKey(viewPtrKey)
        .setBlobUri(viewBlob)
        .setPointerVersion(viewPtr.getVersion())
        .setEtag(hdr.getEtag())
        .setUpdatedAt(Timestamps.fromMillis(System.currentTimeMillis()))
        .build();
  }
}
