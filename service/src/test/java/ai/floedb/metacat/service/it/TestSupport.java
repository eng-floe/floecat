package ai.floedb.metacat.service.it;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.google.protobuf.Any;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.ErrorCode;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;

public final class TestSupport {
  private TestSupport() {}

  public static ResourceId rid(String tenantId, String id, ResourceKind kind) {
    return ResourceId.newBuilder().setTenantId(tenantId).setId(id).setKind(kind).build();
  }

  public static NameRef fq(String catalog, List<String> path, String name) {
    var b = NameRef.newBuilder().setCatalog(catalog).addAllPath(path);
    if (name != null) b.setName(name);
    return b.build();
  }

  public static String seedTenantId(DirectoryGrpc.DirectoryBlockingStub directory, String seededCatalogName) {
    var seeded = directory.resolveCatalog(
      ResolveCatalogRequest.newBuilder().setRef(NameRef.newBuilder().setCatalog(seededCatalogName)).build());
    return seeded.getResourceId().getTenantId();
  }

  public static Catalog createCatalog(ResourceMutationGrpc.ResourceMutationBlockingStub mutation,
                                      String displayName, String description) {
    var resp = mutation.createCatalog(CreateCatalogRequest.newBuilder()
      .setSpec(CatalogSpec.newBuilder().setDisplayName(displayName).setDescription(description))
      .build());
    return resp.getCatalog();
  }

  public static Namespace createNamespace(ResourceMutationGrpc.ResourceMutationBlockingStub mutation,
                                          ResourceId catalogId, String displayName, List<String> path, String desc) {
    var resp = mutation.createNamespace(CreateNamespaceRequest.newBuilder()
      .setSpec(NamespaceSpec.newBuilder()
        .setCatalogId(catalogId)
        .setDisplayName(displayName)
        .addAllPath(path)
        .setDescription(desc))
      .build());
    return resp.getNamespace();
  }

  public static TableDescriptor createTable(ResourceMutationGrpc.ResourceMutationBlockingStub mutation,
                                            ResourceId catalogId, ResourceId namespaceId,
                                            String displayName, String rootUri, String schemaJson, String desc) {
    var resp = mutation.createTable(CreateTableRequest.newBuilder()
      .setSpec(TableSpec.newBuilder()
        .setCatalogId(catalogId)
        .setNamespaceId(namespaceId)
        .setDisplayName(displayName)
        .setDescription(desc)
        .setRootUri(rootUri)
        .setSchemaJson(schemaJson))
      .build());
    return resp.getTable();
  }

  public static ResourceId resolveCatalogId(DirectoryGrpc.DirectoryBlockingStub directory, String catalogName) {
    var r = directory.resolveCatalog(ResolveCatalogRequest.newBuilder()
      .setRef(NameRef.newBuilder().setCatalog(catalogName)).build());
    return r.getResourceId();
  }

  public static ResourceId resolveNamespaceId(DirectoryGrpc.DirectoryBlockingStub directory,
                                              String catalog, List<String> path) {
    var r = directory.resolveNamespace(ResolveNamespaceRequest.newBuilder()
      .setRef(fq(catalog, path, null)).build());
    return r.getResourceId();
  }

  public static ResourceId resolveTableId(DirectoryGrpc.DirectoryBlockingStub directory,
                                          String catalog, List<String> path, String name) {
    var r = directory.resolveTable(ResolveTableRequest.newBuilder()
      .setRef(fq(catalog, path, name)).build());
    return r.getResourceId();
  }

  public static TableDescriptor updateSchema(ResourceMutationGrpc.ResourceMutationBlockingStub mutation,
                                             ResourceId tableId, String newSchemaJson) {
    return mutation.updateTableSchema(UpdateTableSchemaRequest.newBuilder()
      .setTableId(tableId).setSchemaJson(newSchemaJson).build()).getTable();
  }

  public static TableDescriptor renameTable(ResourceMutationGrpc.ResourceMutationBlockingStub mutation,
                                            ResourceId tableId, String newName) {
    return mutation.renameTable(RenameTableRequest.newBuilder()
      .setTableId(tableId).setNewDisplayName(newName).build()).getTable();
  }

  public static void deleteTable(ResourceMutationGrpc.ResourceMutationBlockingStub mutation, ResourceId tableId) {
    mutation.deleteTable(DeleteTableRequest.newBuilder().setTableId(tableId).build());
  }

  public static void deleteNamespace(ResourceMutationGrpc.ResourceMutationBlockingStub mutation,
                                     ResourceId nsId, boolean requireEmpty) {
    mutation.deleteNamespace(DeleteNamespaceRequest.newBuilder()
      .setNamespaceId(nsId).setRequireEmpty(requireEmpty).build());
  }

  public static void deleteCatalog(ResourceMutationGrpc.ResourceMutationBlockingStub mutation,
                                   ResourceId catalogId, boolean requireEmpty) {
    mutation.deleteCatalog(DeleteCatalogRequest.newBuilder()
      .setCatalogId(catalogId).setRequireEmpty(requireEmpty).build());
  }

  public static ai.floedb.metacat.common.rpc.Error unpackMcError(StatusRuntimeException ex) throws Exception {
    var st = StatusProto.fromThrowable(ex);
    if (st == null) return null;
    for (Any any : st.getDetailsList()) {
      if (any.is(ai.floedb.metacat.common.rpc.Error.class)) {
        return any.unpack(ai.floedb.metacat.common.rpc.Error.class);
      }
    }
    return null;
  }

  public static void assertGrpcAndMc(StatusRuntimeException ex,
                                     Status.Code grpcCode,
                                     ErrorCode mcCode,
                                     String msgContains) throws Exception {
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
}
