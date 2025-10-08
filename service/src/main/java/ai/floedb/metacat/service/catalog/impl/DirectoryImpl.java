package ai.floedb.metacat.service.catalog.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import io.grpc.Status;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

import ai.floedb.metacat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.metacat.catalog.rpc.ResolveCatalogResponse;
import ai.floedb.metacat.catalog.rpc.ResolveFQTablesRequest;
import ai.floedb.metacat.catalog.rpc.ResolveFQTablesResponse;
import ai.floedb.metacat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.ResolveNamespaceResponse;
import ai.floedb.metacat.catalog.rpc.ResolveTableRequest;
import ai.floedb.metacat.catalog.rpc.ResolveTableResponse;
import ai.floedb.metacat.catalog.rpc.LookupCatalogRequest;
import ai.floedb.metacat.catalog.rpc.LookupCatalogResponse;
import ai.floedb.metacat.catalog.rpc.LookupNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.LookupNamespaceResponse;
import ai.floedb.metacat.catalog.rpc.LookupTableRequest;
import ai.floedb.metacat.catalog.rpc.LookupTableResponse;
import ai.floedb.metacat.catalog.rpc.NamespaceRef;
import ai.floedb.metacat.catalog.rpc.Directory;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.PageResponse;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;

@GrpcService
public class DirectoryImpl implements Directory {
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;

  private static final Map<String,String> NAME_TO_ID = new ConcurrentHashMap<>();
  private static final Map<String,String> ID_TO_NAME = new ConcurrentHashMap<>();
  private static final Map<String,String> NS_KEY_TO_ID = new ConcurrentHashMap<>();
  private static final Map<String,NamespaceRef> NS_ID_TO_REF = new ConcurrentHashMap<>();
  private static final Map<String,String> TBL_NAME_TO_ID = new ConcurrentHashMap<>();
  private static final Map<String,String> TBL_ID_TO_NAME = new ConcurrentHashMap<>();

  public static void putCatalogIndex(String displayName, String id) {
    NAME_TO_ID.put(displayName, id);
    ID_TO_NAME.put(id, displayName);
  }

  @Override
  public Uni<ResolveCatalogResponse> resolveCatalog(ResolveCatalogRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.read");
    String id = NAME_TO_ID.get(req.getDisplayName());
    if (id == null) id = UUID.randomUUID().toString();
    var rid = ResourceId.newBuilder().setTenantId(p.getTenantId()).setId(id).setKind(ResourceKind.RK_CATALOG).build();
    return Uni.createFrom().item(() ->
      ResolveCatalogResponse.newBuilder().setResourceId(rid).build()
    );
  }

  @Override
  public Uni<LookupCatalogResponse> lookupCatalog(LookupCatalogRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.read");
    String name = ID_TO_NAME.get(req.getResourceId().getId());
    return Uni.createFrom().item(() ->
      LookupCatalogResponse.newBuilder().setDisplayName(name == null ? "" : name).build()
    );
  }

  private static String joinPath(List<String> path) {
    return String.join("/", path);
  }

  private static String nsKey(String tenantId, String catalogId, List<String> path) {
    return tenantId + "|" + catalogId + "|" + joinPath(path);
  }

  public static void putNamespaceIndex(NamespaceRef ref, String id) {
    var key = nsKey(ref.getCatalogId().getTenantId(), ref.getCatalogId().getId(), ref.getNamespacePathList());
    NS_KEY_TO_ID.put(key, id);
    NS_ID_TO_REF.put(id, ref);
  }

  @Override
  public Uni<ResolveNamespaceResponse> resolveNamespace(ResolveNamespaceRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.read");

    var ref = req.getRef();
    var tenantId = p.getTenantId();
    var catalogId = ref.getCatalogId().getId();
    var path = ref.getNamespacePathList();

    var key = nsKey(tenantId, catalogId, path);
    String nsId = NS_KEY_TO_ID.computeIfAbsent(key, k -> {
      String gen = UUID.randomUUID().toString();
      var storedRef = NamespaceRef.newBuilder()
        .setCatalogId(ResourceId.newBuilder()
          .setTenantId(tenantId)
          .setId(catalogId)
          .setKind(ResourceKind.RK_CATALOG)
          .build())
        .addAllNamespacePath(path)
        .build();
      NS_ID_TO_REF.put(gen, storedRef);
      return gen;
    });

    var rid = ResourceId.newBuilder()
      .setTenantId(tenantId)
      .setId(nsId)
      .setKind(ResourceKind.RK_NAMESPACE)
      .build();

    return Uni.createFrom().item(() ->
      ResolveNamespaceResponse.newBuilder().setResourceId(rid).build());
  }

  @Override
  public Uni<LookupNamespaceResponse> lookupNamespace(LookupNamespaceRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.read");

    var rid = req.getResourceId();
    var ref = NS_ID_TO_REF.get(rid.getId());

    String display = (ref == null || ref.getNamespacePathCount() == 0)
      ? ""
      : ref.getNamespacePath(ref.getNamespacePathCount() - 1);

    LookupNamespaceResponse.Builder out = LookupNamespaceResponse.newBuilder().setDisplayName(display);
    if (ref != null) {
      NamespaceRef adjusted = ref;
      if (!ref.getCatalogId().getTenantId().equals(p.getTenantId())) {
        adjusted = ref.toBuilder()
          .setCatalogId(ref.getCatalogId().toBuilder().setTenantId(p.getTenantId()))
          .build();
      }
      out.setRef(adjusted);
    }
    return Uni.createFrom().item(out.build());
  }

  public static void putTableIndex(NameRef ref, String tableId) {
    String key = toKey(ref);
    TBL_NAME_TO_ID.put(key, tableId);
    TBL_ID_TO_NAME.put(tableId, key);
  }

  private static String toKey(NameRef n) {
    StringBuilder sb = new StringBuilder();
    sb.append(n.getCatalog());
    for (String part : n.getNamespacePathList()) sb.append('/').append(part);
    if (!n.getName().isEmpty()) sb.append('/').append(n.getName());
    return sb.toString();
  }

  @Override
  public Uni<ResolveTableResponse> resolveTable(ResolveTableRequest req) {
    var p = principal.get(); 
    authz.require(p, List.of("table.read", "catalog.read"));

    String key = toKey(req.getName());
    String id = TBL_NAME_TO_ID.get(key);
    if (id == null) {
      throw GrpcErrors.notFound("table not found", null);
    }
    var rid = ResourceId.newBuilder()
      .setTenantId(p.getTenantId()).setId(id).setKind(ResourceKind.RK_TABLE).build();
    return Uni.createFrom().item(() -> ResolveTableResponse.newBuilder().setResourceId(rid).build());
  }

  @Override
  public Uni<ResolveFQTablesResponse> resolveFQTables(ResolveFQTablesRequest req) {
    var p = principal.get();
    authz.require(p, List.of("catalog.read", "table.read"));

    return Uni.createFrom().item(() -> {
      final int limit = (req.hasPage() && req.getPage().getPageSize() > 0) ? req.getPage().getPageSize() : 50;
      final String token = req.hasPage() ? req.getPage().getPageToken() : "";
      final ResolveFQTablesResponse.Builder out = ResolveFQTablesResponse.newBuilder();

      if (req.hasList()) {
        final var names = req.getList().getNamesList();
        int start = parseIndexToken(token);
        int end   = Math.min(names.size(), start + limit);

        for (int i = start; i < end; i++) {
          var name = names.get(i);
          var key  = toKey(name);
          var id   = TBL_NAME_TO_ID.get(key);
          out.addTables(
            ResolveFQTablesResponse.Entry.newBuilder()
              .setName(name)
              .setResourceId(id == null
                ? ResourceId.getDefaultInstance()
                : ResourceId.newBuilder()
                  .setTenantId(p.getTenantId())
                  .setId(id)
                  .setKind(ResourceKind.RK_TABLE)
                  .build()));
      }

        var page = PageResponse.newBuilder()
          .setTotalSize(names.size())
          .setNextPageToken(end < names.size() ? Integer.toString(end) : "");
        out.setPage(page);

      } else if (req.hasPrefix()) {
        String prefixKey = toPrefixKey(req.getPrefix());
        List<String> matching = new ArrayList<>();
        for (String k : TBL_NAME_TO_ID.keySet()) {
          if (k.startsWith(prefixKey)) matching.add(k);
        }
        Collections.sort(matching);

        int start = token.isEmpty() ? 0 : nextIndexAfter(matching, token);
        int end   = Math.min(matching.size(), start + limit);

        for (int i = start; i < end; i++) {
          String k   = matching.get(i);
          String id  = TBL_NAME_TO_ID.get(k);
          var name   = fromKey(k);
          out.addTables(
            ResolveFQTablesResponse.Entry.newBuilder()
              .setName(name)
              .setResourceId(ResourceId.newBuilder()
                .setTenantId(p.getTenantId())
                .setId(id)
                .setKind(ResourceKind.RK_TABLE)
                .build()));
        }

        var page = PageResponse.newBuilder()
            .setTotalSize(matching.size())
            .setNextPageToken(end < matching.size() ? matching.get(end - 1) : "");
        out.setPage(page);
      } else {
        throw Status.INVALID_ARGUMENT.withDescription("selector is required").asRuntimeException();
      }

      return out.build();
    });
  }

  private static String toPrefixKey(NameRef prefix) {
    String k = toKey(prefix);
    return k.endsWith("/") ? k : k + "/";
  }

  private static NameRef fromKey(String key) {
    var parts = key.split("/");
    var b = NameRef.newBuilder();
    if (parts.length > 0) b.setCatalog(parts[0]);
    if (parts.length > 1) {
      for (int i = 1; i < parts.length - 1; i++) b.addNamespacePath(parts[i]);
      if (parts.length >= 2) b.setName(parts[parts.length - 1]);
    }
    return b.build();
  }

  private static int parseIndexToken(String t) { 
    return (t == null || t.isEmpty()) ? 0 : Integer.parseInt(t); 
  }

  private static int nextIndexAfter(List<String> sorted, String lastKey) {
    int idx = Collections.binarySearch(sorted, lastKey);
    return (idx >= 0) ? idx + 1 : Math.max(0, -idx - 1);
  }

  @Override
  public Uni<LookupTableResponse> lookupTable(LookupTableRequest req) {
    var p = principal.get();
    authz.require(p, List.of("table.read", "catalog.read"));

    String key = TBL_ID_TO_NAME.get(req.getResourceId().getId());
    if (key == null) {
      return Uni.createFrom().item(LookupTableResponse.newBuilder().build());
    }
    NameRef nameRef = fromKey(key);

    return Uni.createFrom().item(
      LookupTableResponse.newBuilder().setName(nameRef).build()
    );
  }
}