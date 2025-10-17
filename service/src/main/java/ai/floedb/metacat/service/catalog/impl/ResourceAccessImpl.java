package ai.floedb.metacat.service.catalog.impl;

import java.util.Map;

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
import ai.floedb.metacat.common.rpc.PageResponse;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.impl.CatalogRepository;
import ai.floedb.metacat.service.repo.impl.NamespaceRepository;
import ai.floedb.metacat.service.repo.impl.SnapshotRepository;
import ai.floedb.metacat.service.repo.impl.TableRepository;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;

@GrpcService
public class ResourceAccessImpl implements ResourceAccess {
  @Inject CatalogRepository catalogRepo;
  @Inject NamespaceRepository nsRepo;
  @Inject TableRepository tableRepo;
  @Inject SnapshotRepository snapshotRepo;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;

  @Override
  public Uni<GetCatalogResponse> getCatalog(GetCatalogRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.read");

    return Uni.createFrom().item(
        catalogRepo.getById(req.getCatalogId())
            .map(c -> GetCatalogResponse.newBuilder().setCatalog(c).build())
            .orElseThrow(() -> GrpcErrors.notFound(
                corrId(), "catalog", Map.of("id", req.getCatalogId().getId()))));
  }

  @Override
  public Uni<ListCatalogsResponse> listCatalogs(ListCatalogsRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.read");

    final int limit = (req.hasPage() && req.getPage().getPageSize() > 0)
        ? req.getPage().getPageSize() : 50;
    final String token = req.hasPage() ? req.getPage().getPageToken() : "";
    final StringBuilder next = new StringBuilder();

    return Uni.createFrom().item(() -> {
      var catalogs = catalogRepo.listByName(p.getTenantId(), Math.max(1, limit), token, next);
      int total = catalogRepo.countAll(p.getTenantId());

      var page = PageResponse.newBuilder()
          .setNextPageToken(next.toString())
          .setTotalSize(total)
          .build();

      return ListCatalogsResponse.newBuilder()
          .addAllCatalogs(catalogs)
          .setPage(page)
          .build();
    });
  }

  @Override
  public Uni<GetNamespaceResponse> getNamespace(GetNamespaceRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.read");

    return Uni.createFrom().item(() -> {
      var nsRid = req.getNamespaceId();

      var catId = nsRepo.findOwnerCatalog(p.getTenantId(), nsRid.getId())
          .orElseThrow(() -> GrpcErrors.notFound(
              corrId(), "namespace", Map.of("id", nsRid.getId())));

      var ns = nsRepo.get(catId, nsRid)
          .orElseThrow(() -> GrpcErrors.notFound(
              corrId(), "namespace", Map.of("id", nsRid.getId())));

      return GetNamespaceResponse.newBuilder().setNamespace(ns).build();
    });
  }

  @Override
  public Uni<ListNamespacesResponse> listNamespaces(ListNamespacesRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.read");

    var catRid = req.getCatalogId();
    catalogRepo.getById(catRid).orElseThrow(() -> GrpcErrors.notFound(
        corrId(), "catalog", Map.of("id", catRid.getId())));

    final int limit = (req.hasPage() && req.getPage().getPageSize() > 0)
        ? req.getPage().getPageSize() : 50;
    final String token = req.hasPage() ? req.getPage().getPageToken() : "";
    final StringBuilder next = new StringBuilder();

    return Uni.createFrom().item(() -> {
      var namespaces = nsRepo.list(catRid, null, Math.max(1, limit), token, next);
      int total = nsRepo.countUnderCatalog(catRid);

      var page = PageResponse.newBuilder()
          .setNextPageToken(next.toString())
          .setTotalSize(total)
          .build();

      return ListNamespacesResponse.newBuilder()
          .addAllNamespaces(namespaces)
          .setPage(page)
          .build();
    });
  }

  @Override
  public Uni<GetTableDescriptorResponse> getTableDescriptor(GetTableDescriptorRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.read");

    return Uni.createFrom().item(() -> {
      var table = tableRepo.get(req.getTableId())
          .orElseThrow(() -> GrpcErrors.notFound(
              corrId(), "table", Map.of("id", req.getTableId().getId())));
      return GetTableDescriptorResponse.newBuilder().setTable(table).build();
    });
  }

  @Override
  public Uni<ListTablesResponse> listTables(ListTablesRequest req) {
    var p = principal.get();
    authz.require(p, "table.read");

    var nsId = req.getNamespaceId();
    var catId = nsRepo.findOwnerCatalog(p.getTenantId(), nsId.getId())
        .orElseThrow(() -> GrpcErrors.notFound(
            corrId(), "namespace", Map.of("id", nsId.getId())));

    nsRepo.get(catId, nsId).orElseThrow(() -> GrpcErrors.notFound(
        corrId(), "namespace", Map.of("id", nsId.getId())));

    final int limit = (req.hasPage() && req.getPage().getPageSize() > 0)
        ? req.getPage().getPageSize() : 50;
    final String token = req.hasPage() ? req.getPage().getPageToken() : "";
    final StringBuilder next = new StringBuilder();

    return Uni.createFrom().item(() -> {
      var items = tableRepo.listByNamespace(catId, nsId, Math.max(1, limit), token, next);
      int total = tableRepo.countUnderNamespace(catId, nsId);

      var page = PageResponse.newBuilder()
          .setNextPageToken(next.toString())
          .setTotalSize(total)
          .build();

      return ListTablesResponse.newBuilder()
          .addAllTables(items)
          .setPage(page)
          .build();
    });
  }

  @Override
  public Uni<ListSnapshotsResponse> listSnapshots(ListSnapshotsRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.read");

    tableRepo.get(req.getTableId()).orElseThrow(() -> GrpcErrors.notFound(
        corrId(), "table", Map.of("id", req.getTableId().getId())));

    final int limit = (req.hasPage() && req.getPage().getPageSize() > 0)
        ? req.getPage().getPageSize() : 50;
    final String token = req.hasPage() ? req.getPage().getPageToken() : "";
    final StringBuilder next = new StringBuilder();

    return Uni.createFrom().item(() -> {
      var snaps = snapshotRepo.list(req.getTableId(), Math.max(1, limit), token, next);
      int total = snapshotRepo.count(req.getTableId());

      var page = PageResponse.newBuilder()
          .setNextPageToken(next.toString())
          .setTotalSize(total)
          .build();

      return ListSnapshotsResponse.newBuilder()
          .addAllSnapshots(snaps)
          .setPage(page)
          .build();
    });
  }

  @Override
  public Uni<GetCurrentSnapshotResponse> getCurrentSnapshot(GetCurrentSnapshotRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.read");

    return Uni.createFrom().item(() -> {
      tableRepo.get(req.getTableId()).orElseThrow(() -> GrpcErrors.notFound(
          corrId(), "table", Map.of("id", req.getTableId().getId())));

      var snap = snapshotRepo.getCurrentSnapshot(req.getTableId())
          .orElseThrow(() -> GrpcErrors.notFound(
              corrId(), "snapshot", Map.of("reason", "no_snapshots", "table_id", req.getTableId().getId())));

      return GetCurrentSnapshotResponse.newBuilder().setSnapshot(snap).build();
    });
  }

  private String corrId() {
    var pctx = principal != null ? principal.get() : null;
    return pctx != null ? pctx.getCorrelationId() : "";
  }
}
