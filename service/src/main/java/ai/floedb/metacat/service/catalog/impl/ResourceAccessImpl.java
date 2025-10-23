package ai.floedb.metacat.service.catalog.impl;

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
import ai.floedb.metacat.service.common.BaseServiceImpl;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.impl.CatalogRepository;
import ai.floedb.metacat.service.repo.impl.NamespaceRepository;
import ai.floedb.metacat.service.repo.impl.SnapshotRepository;
import ai.floedb.metacat.service.repo.impl.StatsRepository;
import ai.floedb.metacat.service.repo.impl.TableRepository;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.Map;

@GrpcService
public class ResourceAccessImpl extends BaseServiceImpl implements ResourceAccess {
  @Inject CatalogRepository catalogRepo;
  @Inject NamespaceRepository nsRepo;
  @Inject TableRepository tableRepo;
  @Inject SnapshotRepository snapshotRepo;
  @Inject StatsRepository stats;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;

  @Override
  public Uni<GetCatalogResponse> getCatalog(GetCatalogRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principal.get();

              authz.require(principalContext, "catalog.read");

              return catalogRepo
                  .getById(request.getCatalogId())
                  .map(c -> GetCatalogResponse.newBuilder().setCatalog(c).build())
                  .orElseThrow(
                      () ->
                          GrpcErrors.notFound(
                              correlationId(),
                              "catalog",
                              Map.of("id", request.getCatalogId().getId())));
            }),
        correlationId());
  }

  @Override
  public Uni<ListCatalogsResponse> listCatalogs(ListCatalogsRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principal.get();

              authz.require(principalContext, "catalog.read");

              final int limit =
                  (request.hasPage() && request.getPage().getPageSize() > 0)
                      ? request.getPage().getPageSize()
                      : 50;
              final String token = request.hasPage() ? request.getPage().getPageToken() : "";
              final StringBuilder next = new StringBuilder();

              var catalogs =
                  catalogRepo.listByName(
                      principalContext.getTenantId().getId(), Math.max(1, limit), token, next);

              int total = catalogRepo.count(principalContext.getTenantId().getId());

              var page =
                  PageResponse.newBuilder()
                      .setNextPageToken(next.toString())
                      .setTotalSize(total)
                      .build();

              return ListCatalogsResponse.newBuilder()
                  .addAllCatalogs(catalogs)
                  .setPage(page)
                  .build();
            }),
        correlationId());
  }

  @Override
  public Uni<GetNamespaceResponse> getNamespace(GetNamespaceRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principal.get();

              authz.require(principalContext, "namespace.read");

              var namespaceId = request.getNamespaceId();

              var namespace =
                  nsRepo
                      .getById(namespaceId)
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId(), "namespace", Map.of("id", namespaceId.getId())));

              return GetNamespaceResponse.newBuilder().setNamespace(namespace).build();
            }),
        correlationId());
  }

  @Override
  public Uni<ListNamespacesResponse> listNamespaces(ListNamespacesRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principal.get();

              authz.require(principalContext, "namespace.read");

              var catalogId = request.getCatalogId();
              catalogRepo
                  .getById(catalogId)
                  .orElseThrow(
                      () ->
                          GrpcErrors.notFound(
                              correlationId(), "catalog", Map.of("id", catalogId.getId())));

              final int limit =
                  (request.hasPage() && request.getPage().getPageSize() > 0)
                      ? request.getPage().getPageSize()
                      : 50;
              final String token = request.hasPage() ? request.getPage().getPageToken() : "";
              final StringBuilder next = new StringBuilder();

              var namespaces = nsRepo.listByName(catalogId, null, Math.max(1, limit), token, next);
              int total = nsRepo.count(catalogId);

              var page =
                  PageResponse.newBuilder()
                      .setNextPageToken(next.toString())
                      .setTotalSize(total)
                      .build();

              return ListNamespacesResponse.newBuilder()
                  .addAllNamespaces(namespaces)
                  .setPage(page)
                  .build();
            }),
        correlationId());
  }

  @Override
  public Uni<GetTableDescriptorResponse> getTableDescriptor(GetTableDescriptorRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principal.get();

              authz.require(principalContext, "table.read");

              var table =
                  tableRepo
                      .getById(request.getTableId())
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId(),
                                  "table",
                                  Map.of("id", request.getTableId().getId())));
              return GetTableDescriptorResponse.newBuilder().setTable(table).build();
            }),
        correlationId());
  }

  @Override
  public Uni<ListTablesResponse> listTables(ListTablesRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principal.get();

              authz.require(principalContext, "table.read");

              var namespaceId = request.getNamespaceId();
              var namespace =
                  nsRepo
                      .getById(namespaceId)
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId(), "namespace", Map.of("id", namespaceId.getId())));

              final int limit =
                  (request.hasPage() && request.getPage().getPageSize() > 0)
                      ? request.getPage().getPageSize()
                      : 50;
              final String token = request.hasPage() ? request.getPage().getPageToken() : "";
              final StringBuilder next = new StringBuilder();

              var catalogId = namespace.getCatalogId();
              var items =
                  tableRepo.listByNamespace(
                      catalogId, namespaceId, Math.max(1, limit), token, next);

              int total = tableRepo.count(catalogId, namespaceId);

              var page =
                  PageResponse.newBuilder()
                      .setNextPageToken(next.toString())
                      .setTotalSize(total)
                      .build();

              return ListTablesResponse.newBuilder().addAllTables(items).setPage(page).build();
            }),
        correlationId());
  }

  @Override
  public Uni<ListSnapshotsResponse> listSnapshots(ListSnapshotsRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principal.get();

              authz.require(principalContext, "table.read");

              tableRepo
                  .getById(request.getTableId())
                  .orElseThrow(
                      () ->
                          GrpcErrors.notFound(
                              correlationId(),
                              "table",
                              Map.of("id", request.getTableId().getId())));

              final int limit =
                  (request.hasPage() && request.getPage().getPageSize() > 0)
                      ? request.getPage().getPageSize()
                      : 50;
              final String token = request.hasPage() ? request.getPage().getPageToken() : "";
              final StringBuilder next = new StringBuilder();

              var snaps = snapshotRepo.list(request.getTableId(), Math.max(1, limit), token, next);
              int total = snapshotRepo.count(request.getTableId());

              var page =
                  PageResponse.newBuilder()
                      .setNextPageToken(next.toString())
                      .setTotalSize(total)
                      .build();

              return ListSnapshotsResponse.newBuilder()
                  .addAllSnapshots(snaps)
                  .setPage(page)
                  .build();
            }),
        correlationId());
  }

  @Override
  public Uni<GetCurrentSnapshotResponse> getCurrentSnapshot(GetCurrentSnapshotRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principal.get();

              authz.require(principalContext, "table.read");

              tableRepo
                  .getById(request.getTableId())
                  .orElseThrow(
                      () ->
                          GrpcErrors.notFound(
                              correlationId(),
                              "table",
                              Map.of("id", request.getTableId().getId())));

              var snap =
                  snapshotRepo
                      .getCurrentSnapshot(request.getTableId())
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId(),
                                  "snapshot",
                                  Map.of(
                                      "reason",
                                      "no_snapshots",
                                      "table_id",
                                      request.getTableId().getId())));

              return GetCurrentSnapshotResponse.newBuilder().setSnapshot(snap).build();
            }),
        correlationId());
  }
}
