package ai.floedb.metacat.service.catalog.impl;

import ai.floedb.metacat.catalog.rpc.GetCatalogRequest;
import ai.floedb.metacat.catalog.rpc.GetCatalogResponse;
import ai.floedb.metacat.catalog.rpc.ListCatalogsRequest;
import ai.floedb.metacat.catalog.rpc.ListCatalogsResponse;
import ai.floedb.metacat.catalog.rpc.CatalogService;
import ai.floedb.metacat.common.rpc.PageResponse;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.impl.CatalogRepository;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

@GrpcService
public class CatalogServiceImpl implements CatalogService {
  @Inject CatalogRepository repo;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;

  @Override
  public Uni<GetCatalogResponse> getCatalog(GetCatalogRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.read");
    return Uni.createFrom().item(() ->
        repo.getCatalog(req.getResourceId())
            .map(c -> GetCatalogResponse.newBuilder().setCatalog(c).build())
            .orElseThrow(() -> GrpcErrors.notFound("catalog not found", null))
    );
  }

  @Override
  public Uni<ListCatalogsResponse> listCatalogs(ListCatalogsRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.read");

    int limit = (req.hasPage() && req.getPage().getPageSize() > 0) ? req.getPage().getPageSize() : 50;
    String token = req.hasPage() ? req.getPage().getPageToken() : "";
    StringBuilder next = new StringBuilder();

    return Uni.createFrom().item(() -> {
      var items = repo.listCatalogs(p.getTenantId(), limit, token, next);
      var page = PageResponse.newBuilder()
          .setNextPageToken(next.toString())
          .setTotalSize(items.size())
          .build();
      return ListCatalogsResponse.newBuilder()
          .addAllCatalogs(items)
          .setPage(page)
          .build();
    });
  }
}