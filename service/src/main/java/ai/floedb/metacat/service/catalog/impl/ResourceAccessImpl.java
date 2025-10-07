package ai.floedb.metacat.service.catalog.impl;

import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

import java.util.Map;

import ai.floedb.metacat.catalog.rpc.GetCatalogRequest;
import ai.floedb.metacat.catalog.rpc.GetCatalogResponse;
import ai.floedb.metacat.catalog.rpc.GetNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.GetNamespaceResponse;
import ai.floedb.metacat.catalog.rpc.ListCatalogsRequest;
import ai.floedb.metacat.catalog.rpc.ListCatalogsResponse;
import ai.floedb.metacat.catalog.rpc.ListNamespacesRequest;
import ai.floedb.metacat.catalog.rpc.ListNamespacesResponse;
import ai.floedb.metacat.catalog.rpc.ResourceAccess;
import ai.floedb.metacat.common.rpc.PageResponse;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.impl.CatalogRepository;
import ai.floedb.metacat.service.repo.impl.NamespaceRepository;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;

@GrpcService
public class ResourceAccessImpl implements ResourceAccess {
  @Inject CatalogRepository repo;
  @Inject NamespaceRepository nsRepo;
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
      int total = repo.countCatalogs(p.getTenantId());
      var page = PageResponse.newBuilder()
        .setNextPageToken(next.toString())
        .setTotalSize(total)
        .build();
      return ListCatalogsResponse.newBuilder()
        .addAllCatalogs(items)
        .setPage(page)
        .build();
    });
  }

  @Override
  public Uni<GetNamespaceResponse> getNamespace(GetNamespaceRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.read");

    return Uni.createFrom().item(() ->
      nsRepo.get(req.getResourceId(), req.getCatalogId())
        .map(n -> GetNamespaceResponse.newBuilder().setNamespace(n).build())
        .orElseThrow(() -> GrpcErrors.notFound("namespace not found", null))
  );
  }

  @Override
  public Uni<ListNamespacesResponse> listNamespaces(ListNamespacesRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.read");

    var catRid = req.getCatalogId();
    int limit = (req.hasPage() && req.getPage().getPageSize() > 0) ? req.getPage().getPageSize() : 50;
    String token = req.hasPage() ? req.getPage().getPageToken() : "";
    StringBuilder next = new StringBuilder();

    return Uni.createFrom().item(() -> {
      var items = nsRepo.list(catRid, limit, token, next);
      int total = nsRepo.count(catRid);
      var page = PageResponse.newBuilder()
        .setNextPageToken(next.toString())
        .setTotalSize(total)
        .build();
      return ListNamespacesResponse.newBuilder()
        .addAllNamespaces(items)
        .setPage(page)
        .build();
    });
  }
}