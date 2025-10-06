package ai.floedb.metacat.service.directory.impl;

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
  private static final Map<String,String> NS_NAME_TO_ID = new ConcurrentHashMap<>();
  private static final Map<String,String> NS_ID_TO_NAME = new ConcurrentHashMap<>();

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

  public static void putNamespaceIndex(String displayName, String id) {
    NS_NAME_TO_ID.put(displayName, id);
    NS_ID_TO_NAME.put(id, displayName);
  }

  @Override
  public Uni<ResolveNamespaceResponse> resolveNamespace(ResolveNamespaceRequest req) {
    var p = principal.get(); authz.require(p, "catalog.read");
    var id = NS_NAME_TO_ID.getOrDefault(req.getDisplayName(), UUID.randomUUID().toString());
    var rid = ResourceId.newBuilder()
        .setTenantId(p.getTenantId()).setId(id).setKind(ResourceKind.RK_NAMESPACE).build();
    return Uni.createFrom().item(() -> ResolveNamespaceResponse.newBuilder().setResourceId(rid).build());
  }

  @Override
  public Uni<LookupNamespaceResponse> lookupNamespace(LookupNamespaceRequest req) {
    var p = principal.get(); authz.require(p, "catalog.read");
    var name = NS_ID_TO_NAME.getOrDefault(req.getResourceId().getId(), "");
    return Uni.createFrom().item(() -> LookupNamespaceResponse.newBuilder().setDisplayName(name).build());
  }
}