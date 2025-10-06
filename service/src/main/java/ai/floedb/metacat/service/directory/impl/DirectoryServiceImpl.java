package ai.floedb.metacat.service.directory.impl;

import ai.floedb.metacat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.metacat.catalog.rpc.ResolveCatalogResponse;
import ai.floedb.metacat.catalog.rpc.LookupCatalogRequest;
import ai.floedb.metacat.catalog.rpc.LookupCatalogResponse;
import ai.floedb.metacat.catalog.rpc.DirectoryService;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@GrpcService
public class DirectoryServiceImpl implements DirectoryService {
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;

  private static final Map<String,String> NAME_TO_ID = new ConcurrentHashMap<>();
  private static final Map<String,String> ID_TO_NAME = new ConcurrentHashMap<>();

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
}