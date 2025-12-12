package ai.floedb.floecat.service.directory;

import ai.floedb.floecat.catalog.rpc.DirectoryService;
import ai.floedb.floecat.catalog.rpc.LookupCatalogRequest;
import ai.floedb.floecat.catalog.rpc.LookupCatalogResponse;
import ai.floedb.floecat.catalog.rpc.LookupNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.LookupNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.LookupTableRequest;
import ai.floedb.floecat.catalog.rpc.LookupTableResponse;
import ai.floedb.floecat.catalog.rpc.LookupViewRequest;
import ai.floedb.floecat.catalog.rpc.LookupViewResponse;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogResponse;
import ai.floedb.floecat.catalog.rpc.ResolveFQTablesRequest;
import ai.floedb.floecat.catalog.rpc.ResolveFQTablesResponse;
import ai.floedb.floecat.catalog.rpc.ResolveFQViewsRequest;
import ai.floedb.floecat.catalog.rpc.ResolveFQViewsResponse;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.ResolveTableRequest;
import ai.floedb.floecat.catalog.rpc.ResolveTableResponse;
import ai.floedb.floecat.catalog.rpc.ResolveViewRequest;
import ai.floedb.floecat.catalog.rpc.ResolveViewResponse;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PageResponse;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.metagraph.overlay.CatalogOverlay;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import org.jboss.logging.Logger;

@GrpcService
public class DirectoryServiceImpl extends BaseServiceImpl implements DirectoryService {
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject CatalogOverlay catalogOverlay;

  private static final Logger LOG = Logger.getLogger(DirectoryService.class);

  @Override
  public Uni<ResolveCatalogResponse> resolveCatalog(ResolveCatalogRequest request) {
    var L = LogHelper.start(LOG, "ResolveCatalog");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();

                  authz.require(principalContext, "catalog.read");

                  var resourceId =
                      catalogOverlay.resolveCatalog(correlationId(), request.getRef().getCatalog());

                  return ResolveCatalogResponse.newBuilder().setResourceId(resourceId).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<LookupCatalogResponse> lookupCatalog(LookupCatalogRequest request) {
    var L = LogHelper.start(LOG, "LookupCatalog");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();

                  authz.require(principalContext, "catalog.read");

                  var catalogNode = catalogOverlay.catalog(request.getResourceId());
                  if (catalogNode.isEmpty()) {
                    return LookupCatalogResponse.newBuilder().build();
                  }

                  return LookupCatalogResponse.newBuilder()
                      .setDisplayName(catalogNode.get().displayName())
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<ResolveNamespaceResponse> resolveNamespace(ResolveNamespaceRequest request) {
    var L = LogHelper.start(LOG, "ResolveNamespace");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();

                  authz.require(principalContext, "catalog.read");

                  var ref = request.getRef();
                  validateNameRefOrThrow(ref);

                  var namespaceId = catalogOverlay.resolveNamespace(correlationId(), ref);

                  return ResolveNamespaceResponse.newBuilder().setResourceId(namespaceId).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<LookupNamespaceResponse> lookupNamespace(LookupNamespaceRequest request) {
    var L = LogHelper.start(LOG, "LookupNamespace");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();

                  authz.require(principalContext, "catalog.read");

                  var namespaceName = catalogOverlay.namespaceName(request.getResourceId());
                  if (namespaceName == null || namespaceName.isEmpty()) {
                    return LookupNamespaceResponse.newBuilder().build();
                  }

                  return LookupNamespaceResponse.newBuilder().setRef(namespaceName.get()).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<ResolveTableResponse> resolveTable(ResolveTableRequest request) {
    var L = LogHelper.start(LOG, "ResolveTable");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();

                  authz.require(principalContext, List.of("catalog.read", "table.read"));

                  var nameRef = request.getRef();
                  validateNameRefOrThrow(nameRef);
                  validateTableNameOrThrow(nameRef);

                  var tableId = catalogOverlay.resolveTable(correlationId(), nameRef);

                  return ResolveTableResponse.newBuilder().setResourceId(tableId).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<LookupTableResponse> lookupTable(LookupTableRequest request) {
    var L = LogHelper.start(LOG, "LookupTable");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();

                  authz.require(principalContext, List.of("catalog.read", "table.read"));

                  var tableName = catalogOverlay.tableName(request.getResourceId());
                  if (tableName == null || tableName.isEmpty()) {
                    return LookupTableResponse.newBuilder().build();
                  }

                  return LookupTableResponse.newBuilder().setName(tableName.get()).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<ResolveViewResponse> resolveView(ResolveViewRequest request) {
    var L = LogHelper.start(LOG, "ResolveView");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();

                  authz.require(principalContext, List.of("catalog.read", "view.read"));

                  var nameRef = request.getRef();
                  validateNameRefOrThrow(nameRef);
                  validateViewNameOrThrow(nameRef);

                  var viewId = catalogOverlay.resolveView(correlationId(), nameRef);

                  return ResolveViewResponse.newBuilder().setResourceId(viewId).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<LookupViewResponse> lookupView(LookupViewRequest request) {
    var L = LogHelper.start(LOG, "LookupView");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();

                  authz.require(principalContext, List.of("catalog.read", "view.read"));

                  var viewName = catalogOverlay.viewName(request.getResourceId());
                  if (viewName == null || viewName.isEmpty()) {
                    return LookupViewResponse.newBuilder().build();
                  }

                  return LookupViewResponse.newBuilder().setName(viewName.get()).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<ResolveFQViewsResponse> resolveFQViews(ResolveFQViewsRequest request) {
    var L = LogHelper.start(LOG, "ResolveFQViews");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();
                  authz.require(principalContext, List.of("catalog.read", "view.read"));

                  final int limit =
                      (request.hasPage() && request.getPage().getPageSize() > 0)
                          ? request.getPage().getPageSize()
                          : 50;
                  final String token = request.hasPage() ? request.getPage().getPageToken() : "";

                  var builder = ResolveFQViewsResponse.newBuilder();

                  if (request.hasList()) {
                    var result =
                        catalogOverlay.resolveViews(
                            correlationId(), request.getList().getNamesList(), limit, token);

                    result
                        .relations()
                        .forEach(
                            qr ->
                                builder.addViews(
                                    ResolveFQViewsResponse.Entry.newBuilder()
                                        .setName(qr.name())
                                        .setResourceId(qr.resourceId())));

                    builder.setPage(
                        PageResponse.newBuilder()
                            .setTotalSize(result.totalSize())
                            .setNextPageToken(result.nextToken()));

                    return builder.build();
                  }

                  if (request.hasPrefix()) {
                    var result =
                        catalogOverlay.resolveViews(
                            correlationId(), request.getPrefix(), limit, token);

                    result
                        .relations()
                        .forEach(
                            qr ->
                                builder.addViews(
                                    ResolveFQViewsResponse.Entry.newBuilder()
                                        .setName(qr.name())
                                        .setResourceId(qr.resourceId())));

                    builder.setPage(
                        PageResponse.newBuilder()
                            .setTotalSize(result.totalSize())
                            .setNextPageToken(result.nextToken()));
                    return builder.build();
                  }

                  throw GrpcErrors.invalidArgument(correlationId(), "selector.required", Map.of());
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<ResolveFQTablesResponse> resolveFQTables(ResolveFQTablesRequest request) {
    var L = LogHelper.start(LOG, "ResolveFQTables");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();
                  authz.require(principalContext, List.of("catalog.read", "table.read"));

                  final int limit =
                      (request.hasPage() && request.getPage().getPageSize() > 0)
                          ? request.getPage().getPageSize()
                          : 50;
                  final String token = request.hasPage() ? request.getPage().getPageToken() : "";

                  var builder = ResolveFQTablesResponse.newBuilder();

                  if (request.hasList()) {
                    var result =
                        catalogOverlay.resolveTables(
                            correlationId(), request.getList().getNamesList(), limit, token);

                    result
                        .relations()
                        .forEach(
                            qr ->
                                builder.addTables(
                                    ResolveFQTablesResponse.Entry.newBuilder()
                                        .setName(qr.name())
                                        .setResourceId(qr.resourceId())));

                    builder.setPage(
                        PageResponse.newBuilder()
                            .setTotalSize(result.totalSize())
                            .setNextPageToken(result.nextToken()));

                    return builder.build();
                  }

                  if (request.hasPrefix()) {
                    var result =
                        catalogOverlay.resolveTables(
                            correlationId(), request.getPrefix(), limit, token);

                    result
                        .relations()
                        .forEach(
                            qr ->
                                builder.addTables(
                                    ResolveFQTablesResponse.Entry.newBuilder()
                                        .setName(qr.name())
                                        .setResourceId(qr.resourceId())));

                    builder.setPage(
                        PageResponse.newBuilder()
                            .setTotalSize(result.totalSize())
                            .setNextPageToken(result.nextToken()));
                    return builder.build();
                  }

                  throw GrpcErrors.invalidArgument(correlationId(), "selector.required", Map.of());
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private void validateNameRefOrThrow(NameRef ref) {
    if (ref.getCatalog() == null || ref.getCatalog().isBlank()) {
      throw GrpcErrors.invalidArgument(correlationId(), "catalog.missing", Map.of());
    }
    for (String pathSegment : ref.getPathList()) {
      if (pathSegment == null || pathSegment.isBlank()) {
        throw GrpcErrors.invalidArgument(correlationId(), "path.segment.blank", Map.of());
      }
    }
  }

  private void validateTableNameOrThrow(NameRef ref) {
    if (ref.getName() == null || ref.getName().isBlank()) {
      throw GrpcErrors.invalidArgument(correlationId(), "table.name.missing", Map.of());
    }
  }

  private void validateViewNameOrThrow(NameRef ref) {
    if (ref.getName() == null || ref.getName().isBlank()) {
      throw GrpcErrors.invalidArgument(correlationId(), "view.name.missing", Map.of());
    }
  }
}
