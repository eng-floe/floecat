package ai.floedb.metacat.service.directory.impl;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

import ai.floedb.metacat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.metacat.catalog.rpc.ResolveCatalogResponse;
import ai.floedb.metacat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.ResolveNamespaceResponse;
import ai.floedb.metacat.catalog.rpc.LookupCatalogRequest;
import ai.floedb.metacat.catalog.rpc.LookupCatalogResponse;
import ai.floedb.metacat.catalog.rpc.LookupNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.LookupNamespaceResponse;
import ai.floedb.metacat.catalog.rpc.NamespaceRef;
import ai.floedb.metacat.catalog.rpc.DirectoryService;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;

@GrpcService
public class DirectoryServiceImpl implements DirectoryService {
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;

  private static final Map<String,String> NAME_TO_ID = new ConcurrentHashMap<>();
  private static final Map<String,String> ID_TO_NAME = new ConcurrentHashMap<>();
  private static final Map<String,String> NS_KEY_TO_ID = new ConcurrentHashMap<>();
  private static final Map<String,NamespaceRef> NS_ID_TO_REF = new ConcurrentHashMap<>();

  public static void putIndex(String displayName, String id) {
    NAME_TO_ID.put(displayName, id);
    ID_TO_NAME.put(id, displayName);
  }

  @Override
  public Uni<ResolveCatalogResponse> resolveCatalog(ResolveCatalogRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.read");
    String id = NAME_TO_ID.get(req.getDisplayName());
    if (id == null) id = UUID.randomUUID().toString(); // optional fallback
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
}