package ai.floedb.metacat.service.catalog.impl;

import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

import ai.floedb.metacat.catalog.rpc.GetCatalogRequest;
import ai.floedb.metacat.catalog.rpc.GetCatalogResponse;
import ai.floedb.metacat.catalog.rpc.GetCurrentSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.GetCurrentSnapshotResponse;
import ai.floedb.metacat.catalog.rpc.GetNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.GetNamespaceResponse;
import ai.floedb.metacat.catalog.rpc.GetTableDescriptorRequest;
import ai.floedb.metacat.catalog.rpc.GetTableDescriptorResponse;
import ai.floedb.metacat.catalog.rpc.ListCatalogsRequest;
import ai.floedb.metacat.catalog.rpc.ListCatalogsResponse;
import ai.floedb.metacat.catalog.rpc.ListNamespacesRequest;
import ai.floedb.metacat.catalog.rpc.ListNamespacesResponse;
import ai.floedb.metacat.catalog.rpc.ListSnapshotsRequest;
import ai.floedb.metacat.catalog.rpc.ListSnapshotsResponse;
import ai.floedb.metacat.catalog.rpc.ListTablesRequest;
import ai.floedb.metacat.catalog.rpc.ListTablesResponse;
import ai.floedb.metacat.catalog.rpc.ResourceAccess;
import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.PageResponse;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.impl.CatalogRepository;
import ai.floedb.metacat.service.repo.impl.NameIndexRepository;
import ai.floedb.metacat.service.repo.impl.NamespaceRepository;
import ai.floedb.metacat.service.repo.impl.SnapshotRepository;
import ai.floedb.metacat.service.repo.impl.TableRepository;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;

@GrpcService
public class ResourceAccessImpl implements ResourceAccess {
  @Inject CatalogRepository repo;
  @Inject NamespaceRepository nsRepo;
  @Inject NameIndexRepository nameIndexRepo;
  @Inject TableRepository tableRepo;
  @Inject SnapshotRepository snapshotRepo;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;

  @Override
  public Uni<GetCatalogResponse> getCatalog(GetCatalogRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.read");
    return Uni.createFrom().item(
      repo.get(req.getResourceId())
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

    return Uni.createFrom().deferred(() ->
      Uni.createFrom().item(() -> {
        var items = repo.list(p.getTenantId(), limit, token, next);
        int total = repo.count(p.getTenantId());
        var page = PageResponse.newBuilder()
          .setNextPageToken(next.toString())
          .setTotalSize(total)
          .build();
        return ListCatalogsResponse.newBuilder()
          .addAllCatalogs(items)
          .setPage(page)
          .build();
      })
    );
  }

  private ResourceId requireCatalogIdByName(String tenantId, String catalogName) {
    return nameIndexRepo.getCatalogByName(tenantId, catalogName)
      .map(NameRef::getResourceId)
      .orElseThrow(() -> GrpcErrors.notFound("catalog not found: " + catalogName, null));
  }

@Override
public Uni<GetNamespaceResponse> getNamespace(GetNamespaceRequest req) {
  var p = principal.get();
  authz.require(p, "catalog.read");

  return Uni.createFrom().item(req)
    .map(r -> {
      var tenantId = p.getTenantId();
      var nsRid = r.getResourceId();
      var nsRef = nameIndexRepo.getNamespaceById(tenantId, nsRid.getId())
        .orElseThrow(() -> GrpcErrors.notFound(
            "namespace index missing (by-id): " + nsRid.getId(), null));

      var catalogRid = requireCatalogIdByName(tenantId, nsRef.getCatalog());

      var ns = nsRepo.get(nsRid, catalogRid)
        .orElseThrow(() -> GrpcErrors.notFound(
            "namespace not found: " + nsRid.getId(), null));

      return GetNamespaceResponse.newBuilder().setNamespace(ns).build();
    });
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

  @Override
  public Uni<GetTableDescriptorResponse> getTableDescriptor(GetTableDescriptorRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.read");

    return Uni.createFrom().item(req)
      .map(r -> {
        var table = tableRepo
          .getById(r.getResourceId())
          .orElseThrow(() -> GrpcErrors.notFound("table not found", null));

        return GetTableDescriptorResponse.newBuilder()
          .setTable(table)
          .build();
      });
  }

  @Override
  public Uni<ListTablesResponse> listTables(ListTablesRequest req) {
    var p = principal.get(); 
    authz.require(p, "table.read");

    int limit = req.hasPage() && req.getPage().getPageSize() > 0 ? req.getPage().getPageSize() : 50;
    String token = req.hasPage() ? req.getPage().getPageToken() : "";
    StringBuilder next = new StringBuilder();

    return Uni.createFrom().item(() -> {
      var items = tableRepo.list(p.getTenantId(), req.getCatalogId().getId(), req.getNamespaceId().getId(), limit, token, next);
      int total = tableRepo.count(p.getTenantId(), req.getCatalogId().getId(), req.getNamespaceId().getId());
      var page = PageResponse.newBuilder().setNextPageToken(next.toString()).setTotalSize(total).build();
      return ListTablesResponse.newBuilder().addAllTables(items).setPage(page).build();
    });
  }

  @Override
  public Uni<ListSnapshotsResponse> listSnapshots(ListSnapshotsRequest req) {
    var p = principal.get(); authz.require(p, "catalog.read");

    int limit = req.hasPage() && req.getPage().getPageSize() > 0 ? req.getPage().getPageSize() : 50;
    String token = req.hasPage() ? req.getPage().getPageToken() : "";
    StringBuilder next = new StringBuilder();

    return Uni.createFrom().item(() -> {
      var snaps = snapshotRepo.list(req.getTableId(), limit, token, next);
      int total = snapshotRepo.count(req.getTableId());
      var page = PageResponse.newBuilder().setNextPageToken(next.toString()).setTotalSize(total).build();
      return ListSnapshotsResponse.newBuilder().addAllSnapshots(snaps).setPage(page).build();
    });
  }

  @Override
  public Uni<GetCurrentSnapshotResponse> getCurrentSnapshot(GetCurrentSnapshotRequest req) {
    var p = principal.get(); authz.require(p, "catalog.read");
    return Uni.createFrom().item(() ->
      tableRepo.getById(req.getTableId())
        .filter(t -> t.getCurrentSnapshotId() != 0)
        .map(t -> GetCurrentSnapshotResponse.newBuilder()
          .setSnapshot(Snapshot.newBuilder()
            .setSnapshotId(t.getCurrentSnapshotId())
            .setCreatedAtMs(t.getCreatedAtMs())
            .build())
          .build())
        .orElseThrow(() -> GrpcErrors.notFound("snapshot not found", null)));
  }
}