package ai.floedb.metacat.service.directory;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.catalog.rpc.Directory;
import ai.floedb.metacat.catalog.rpc.LookupCatalogRequest;
import ai.floedb.metacat.catalog.rpc.LookupCatalogResponse;
import ai.floedb.metacat.catalog.rpc.LookupNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.LookupNamespaceResponse;
import ai.floedb.metacat.catalog.rpc.LookupTableRequest;
import ai.floedb.metacat.catalog.rpc.LookupTableResponse;
import ai.floedb.metacat.catalog.rpc.LookupViewRequest;
import ai.floedb.metacat.catalog.rpc.LookupViewResponse;
import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.metacat.catalog.rpc.ResolveCatalogResponse;
import ai.floedb.metacat.catalog.rpc.ResolveFQTablesRequest;
import ai.floedb.metacat.catalog.rpc.ResolveFQTablesResponse;
import ai.floedb.metacat.catalog.rpc.ResolveFQViewsRequest;
import ai.floedb.metacat.catalog.rpc.ResolveFQViewsResponse;
import ai.floedb.metacat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.ResolveNamespaceResponse;
import ai.floedb.metacat.catalog.rpc.ResolveTableRequest;
import ai.floedb.metacat.catalog.rpc.ResolveTableResponse;
import ai.floedb.metacat.catalog.rpc.ResolveViewRequest;
import ai.floedb.metacat.catalog.rpc.ResolveViewResponse;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.View;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.PageResponse;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.common.BaseServiceImpl;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.impl.CatalogRepository;
import ai.floedb.metacat.service.repo.impl.NamespaceRepository;
import ai.floedb.metacat.service.repo.impl.TableRepository;
import ai.floedb.metacat.service.repo.impl.ViewRepository;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@GrpcService
public class DirectoryImpl extends BaseServiceImpl implements Directory {
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject CatalogRepository catalogs;
  @Inject NamespaceRepository namespaces;
  @Inject TableRepository tables;
  @Inject ViewRepository views;

  @Override
  public Uni<ResolveCatalogResponse> resolveCatalog(ResolveCatalogRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principal.get();

              authz.require(principalContext, "catalog.read");

              var tenantId = principalContext.getTenantId();
              Catalog cat =
                  catalogs
                      .getByName(tenantId, request.getRef().getCatalog())
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId(),
                                  "catalog",
                                  Map.of("id", request.getRef().getCatalog())));

              return ResolveCatalogResponse.newBuilder().setResourceId(cat.getResourceId()).build();
            }),
        correlationId());
  }

  @Override
  public Uni<LookupCatalogResponse> lookupCatalog(LookupCatalogRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principal.get();

              authz.require(principalContext, "catalog.read");

              var catalogOpt = catalogs.getById(request.getResourceId());
              if (catalogOpt.isEmpty()) {
                return LookupCatalogResponse.newBuilder().build();
              }
              var catalog = catalogOpt.get();

              return LookupCatalogResponse.newBuilder()
                  .setDisplayName(catalog.getDisplayName())
                  .build();
            }),
        correlationId());
  }

  @Override
  public Uni<ResolveNamespaceResponse> resolveNamespace(ResolveNamespaceRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principal.get();

              authz.require(principalContext, "catalog.read");

              var ref = request.getRef();
              validateNameRefOrThrow(ref);
              var tenantId = principalContext.getTenantId();

              Catalog cat =
                  catalogs
                      .getByName(tenantId, ref.getCatalog())
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId(), "catalog", Map.of("id", ref.getCatalog())));

              var fullPath = new ArrayList<>(ref.getPathList());
              if (!ref.getName().isBlank()) {
                fullPath.add(ref.getName());
              }

              Namespace namespace =
                  namespaces
                      .getByPath(tenantId, cat.getResourceId().getId(), fullPath)
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId(),
                                  "namespace.by_path_missing",
                                  Map.of(
                                      "catalog_id",
                                      cat.getResourceId().getId(),
                                      "path",
                                      String.join("/", fullPath))));

              return ResolveNamespaceResponse.newBuilder()
                  .setResourceId(namespace.getResourceId())
                  .build();
            }),
        correlationId());
  }

  @Override
  public Uni<LookupNamespaceResponse> lookupNamespace(LookupNamespaceRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principal.get();

              authz.require(principalContext, "catalog.read");

              var namespaceId = request.getResourceId();
              var namespaceOpt = namespaces.getById(namespaceId);

              if (namespaceOpt.isEmpty()) {
                return LookupNamespaceResponse.newBuilder().build();
              }
              var namespace = namespaceOpt.get();

              var catalogId = namespace.getCatalogId();
              var catalog =
                  catalogs
                      .getById(catalogId)
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId(), "catalog", Map.of("id", catalogId.getId())));

              var nr =
                  NameRef.newBuilder()
                      .setCatalog(catalog.getDisplayName())
                      .addAllPath(namespace.getParentsList())
                      .setName(namespace.getDisplayName())
                      .build();

              return LookupNamespaceResponse.newBuilder().setRef(nr).build();
            }),
        correlationId());
  }

  @Override
  public Uni<ResolveTableResponse> resolveTable(ResolveTableRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principal.get();

              authz.require(principalContext, List.of("catalog.read", "table.read"));

              var nameRef = request.getRef();
              validateNameRefOrThrow(nameRef);
              validateTableNameOrThrow(nameRef);

              var tenantId = principalContext.getTenantId();

              Catalog catalog =
                  catalogs
                      .getByName(tenantId, nameRef.getCatalog())
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId(), "catalog", Map.of("id", nameRef.getCatalog())));

              Namespace namespace =
                  namespaces
                      .getByPath(tenantId, catalog.getResourceId().getId(), nameRef.getPathList())
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId(),
                                  "namespace.by_path_missing",
                                  Map.of(
                                      "catalog_id",
                                      catalog.getResourceId().getId(),
                                      "path",
                                      String.join("/", nameRef.getPathList()))));

              Table table =
                  tables
                      .getByName(
                          tenantId,
                          catalog.getResourceId().getId(),
                          namespace.getResourceId().getId(),
                          nameRef.getName())
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId(),
                                  "table.by_name_missing",
                                  Map.of(
                                      "catalog",
                                      nameRef.getCatalog(),
                                      "path",
                                      String.join("/", nameRef.getPathList()),
                                      "name",
                                      nameRef.getName())));

              return ResolveTableResponse.newBuilder().setResourceId(table.getResourceId()).build();
            }),
        correlationId());
  }

  @Override
  public Uni<LookupTableResponse> lookupTable(LookupTableRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principal.get();

              authz.require(principalContext, List.of("catalog.read", "table.read"));

              var tableId = request.getResourceId();

              var tableOpt = tables.getById(tableId);
              if (tableOpt.isEmpty()) {
                return LookupTableResponse.newBuilder().build();
              }
              var table = tableOpt.get();

              var catalogOpt = catalogs.getById(table.getCatalogId());
              var namespaceOpt = namespaces.getById(table.getNamespaceId());

              if (catalogOpt.isEmpty() || namespaceOpt.isEmpty()) {
                return LookupTableResponse.newBuilder().build();
              }

              var catalog = catalogOpt.get();
              var namespace = namespaceOpt.get();

              var path = new ArrayList<>(namespace.getParentsList());
              if (!namespace.getDisplayName().isBlank()) {
                path.add(namespace.getDisplayName());
              }

              var nameRef =
                  NameRef.newBuilder()
                      .setCatalog(catalog.getDisplayName())
                      .addAllPath(path)
                      .setName(table.getDisplayName())
                      .build();

              return LookupTableResponse.newBuilder().setName(nameRef).build();
            }),
        correlationId());
  }

  @Override
  public Uni<ResolveViewResponse> resolveView(ResolveViewRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principal.get();

              authz.require(principalContext, List.of("catalog.read", "view.read"));

              var nameRef = request.getRef();
              validateNameRefOrThrow(nameRef);
              validateViewNameOrThrow(nameRef);

              var tenantId = principalContext.getTenantId();

              Catalog catalog =
                  catalogs
                      .getByName(tenantId, nameRef.getCatalog())
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId(), "catalog", Map.of("id", nameRef.getCatalog())));  

              Namespace namespace =
                  namespaces
                      .getByPath(tenantId, catalog.getResourceId().getId(), nameRef.getPathList())
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId(),
                                  "namespace.by_path_missing",
                                  Map.of(
                                      "catalog_id",
                                      catalog.getResourceId().getId(),
                                      "path",
                                      String.join("/", nameRef.getPathList()))));

              View view =
                  views
                      .getByName(
                          tenantId,
                          catalog.getResourceId().getId(),
                          namespace.getResourceId().getId(),
                          nameRef.getName())
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId(),
                                  "view.by_name_missing",
                                  Map.of(
                                      "catalog",
                                      nameRef.getCatalog(),
                                      "path",
                                      String.join("/", nameRef.getPathList()),
                                      "name",
                                      nameRef.getName())));

              return ResolveViewResponse.newBuilder().setResourceId(view.getResourceId()).build();
            }),
        correlationId());
  }

  @Override
  public Uni<LookupViewResponse> lookupView(LookupViewRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principal.get();

              authz.require(principalContext, List.of("catalog.read", "view.read"));

              var viewId = request.getResourceId();

              var viewOpt = views.getById(viewId);
              if (viewOpt.isEmpty()) {
                return LookupViewResponse.newBuilder().build();
              }
              var view = viewOpt.get();

              var catalogOpt = catalogs.getById(view.getCatalogId());
              var namespaceOpt = namespaces.getById(view.getNamespaceId());

              if (catalogOpt.isEmpty() || namespaceOpt.isEmpty()) {
                return LookupViewResponse.newBuilder().build();
              }

              var catalog = catalogOpt.get();
              var namespace = namespaceOpt.get();

              var path = new ArrayList<>(namespace.getParentsList());
              if (!namespace.getDisplayName().isBlank()) {
                path.add(namespace.getDisplayName());
              }

              var nameRef =
                  NameRef.newBuilder()
                      .setCatalog(catalog.getDisplayName())
                      .addAllPath(path)
                      .setName(view.getDisplayName())
                      .build();

              return LookupViewResponse.newBuilder().setName(nameRef).build();
            }),
        correlationId());
  }

  @Override
  public Uni<ResolveFQViewsResponse> resolveFQViews(ResolveFQViewsRequest request) {
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
              var tenantId = principalContext.getTenantId();

              if (request.hasList()) {
                var names = request.getList().getNamesList();

                final int start;
                if (token == null || token.isEmpty()) {
                  start = 0;
                } else {
                  try {
                    start = parseIntToken(token, correlationId());
                  } catch (NumberFormatException nfe) {
                    throw GrpcErrors.invalidArgument(
                        correlationId(), "page_token.invalid", Map.of("page_token", token));
                  }
                }
                final int end = Math.min(names.size(), start + Math.max(1, limit));

                for (int i = start; i < end; i++) {
                  NameRef nameRef = names.get(i);
                  try {
                    validateNameRefOrThrow(nameRef);
                    validateViewNameOrThrow(nameRef);

                    Catalog catalog =
                        catalogs
                            .getByName(tenantId, nameRef.getCatalog())
                            .orElseThrow(
                                () ->
                                    GrpcErrors.notFound(
                                        correlationId(),
                                        "catalog",
                                        Map.of("id", nameRef.getCatalog())));

                    Namespace namespace =
                        namespaces
                            .getByPath(tenantId, catalog.getResourceId().getId(), nameRef.getPathList())
                            .orElseThrow(
                                () ->
                                    GrpcErrors.notFound(
                                        correlationId(),
                                        "namespace.by_path_missing",
                                        Map.of(
                                            "catalog_id",
                                            catalog.getResourceId().getId(),
                                            "path",
                                            String.join("/", nameRef.getPathList()))));

                    Optional<View> viewOpt =
                        views.getByName(
                            tenantId,
                            catalog.getResourceId().getId(),
                            namespace.getResourceId().getId(),
                            nameRef.getName());

                    var viewId =
                        viewOpt.map(View::getResourceId).orElse(ResourceId.getDefaultInstance());
                    var outName =
                        viewOpt.isPresent()
                            ? NameRef.newBuilder()
                                .setCatalog(catalog.getDisplayName())
                                .addAllPath(nameRef.getPathList())
                                .setName(nameRef.getName())
                                .setResourceId(viewId)
                                .build()
                            : nameRef;

                    builder.addViews(
                        ResolveFQViewsResponse.Entry.newBuilder()
                            .setName(outName)
                            .setResourceId(viewId));
                  } catch (Throwable t) {
                    builder.addViews(
                        ResolveFQViewsResponse.Entry.newBuilder()
                            .setName(nameRef)
                            .setResourceId(ResourceId.getDefaultInstance()));
                  }
                }

                builder.setPage(
                    PageResponse.newBuilder()
                        .setTotalSize(names.size())
                        .setNextPageToken(end < names.size() ? Integer.toString(end) : ""));
                return builder.build();
              }

              if (request.hasPrefix()) {
                var prefix = request.getPrefix();
                validateNameRefOrThrow(prefix);

                Catalog catalog =
                    catalogs
                        .getByName(tenantId, prefix.getCatalog())
                        .orElseThrow(
                            () ->
                                GrpcErrors.notFound(
                                    correlationId(), "catalog", Map.of("id", prefix.getCatalog())));

                Namespace namespace =
                    namespaces
                        .getByPath(tenantId, catalog.getResourceId().getId(), prefix.getPathList())
                        .orElseThrow(
                            () ->
                                GrpcErrors.notFound(
                                    correlationId(),
                                    "namespace.by_path_missing",
                                    Map.of(
                                        "catalog_id",
                                        catalog.getResourceId().getId(),
                                        "path",
                                        String.join("/", prefix.getPathList()))));

                StringBuilder next = new StringBuilder();
                var entries =
                    views.list(
                        tenantId,
                        catalog.getResourceId().getId(),
                        namespace.getResourceId().getId(),
                        Math.max(1, limit),
                        token,
                        next);
                int total =
                    views.count(
                        tenantId,
                        catalog.getResourceId().getId(),
                        namespace.getResourceId().getId());

                for (View view : entries) {
                  var nr =
                      NameRef.newBuilder()
                          .setCatalog(catalog.getDisplayName())
                          .addAllPath(prefix.getPathList())
                          .setName(view.getDisplayName())
                          .setResourceId(view.getResourceId())
                          .build();

                  builder.addViews(
                      ResolveFQViewsResponse.Entry.newBuilder()
                          .setName(nr)
                          .setResourceId(view.getResourceId()));
                }

                builder.setPage(
                    PageResponse.newBuilder()
                        .setTotalSize(total)
                        .setNextPageToken(next.toString()));
                return builder.build();
              }

              throw GrpcErrors.invalidArgument(correlationId(), "selector.required", Map.of());
            }),
        correlationId());
  }

  @Override
  public Uni<ResolveFQTablesResponse> resolveFQTables(ResolveFQTablesRequest request) {
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
              var tenantId = principalContext.getTenantId();

              if (request.hasList()) {
                var names = request.getList().getNamesList();

                final int start;
                if (token == null || token.isEmpty()) {
                  start = 0;
                } else {
                  try {
                    start = parseIntToken(token, correlationId());
                  } catch (NumberFormatException nfe) {
                    throw GrpcErrors.invalidArgument(
                        correlationId(), "page_token.invalid", Map.of("page_token", token));
                  }
                }
                final int end = Math.min(names.size(), start + Math.max(1, limit));

                for (int i = start; i < end; i++) {
                  NameRef nameRef = names.get(i);
                  try {
                    validateNameRefOrThrow(nameRef);
                    validateTableNameOrThrow(nameRef);

                    Catalog catalog =
                        catalogs.getByName(tenantId, nameRef.getCatalog()).orElseThrow();
                    Namespace namespace =
                        namespaces
                            .getByPath(
                                tenantId, catalog.getResourceId().getId(), nameRef.getPathList())
                            .orElseThrow();

                    Optional<Table> tableOpt =
                        tables
                            .list(
                                tenantId,
                                catalog.getResourceId().getId(),
                                namespace.getResourceId().getId(),
                                Integer.MAX_VALUE,
                                "",
                                new StringBuilder())
                            .stream()
                            .filter(t -> t.getDisplayName().equals(nameRef.getName()))
                            .findFirst();

                    var tableId =
                        tableOpt.map(Table::getResourceId).orElse(ResourceId.getDefaultInstance());
                    var outName =
                        tableOpt.isPresent()
                            ? NameRef.newBuilder()
                                .setCatalog(catalog.getDisplayName())
                                .addAllPath(nameRef.getPathList())
                                .setName(nameRef.getName())
                                .setResourceId(tableId)
                                .build()
                            : nameRef;

                    builder.addTables(
                        ResolveFQTablesResponse.Entry.newBuilder()
                            .setName(outName)
                            .setResourceId(tableId));

                  } catch (Throwable t) {
                    builder.addTables(
                        ResolveFQTablesResponse.Entry.newBuilder()
                            .setName(nameRef)
                            .setResourceId(ResourceId.getDefaultInstance()));
                  }
                }

                builder.setPage(
                    PageResponse.newBuilder()
                        .setTotalSize(names.size())
                        .setNextPageToken(end < names.size() ? Integer.toString(end) : ""));
                return builder.build();
              }

              if (request.hasPrefix()) {
                var prefix = request.getPrefix();
                validateNameRefOrThrow(prefix);

                Catalog catalog =
                    catalogs
                        .getByName(tenantId, prefix.getCatalog())
                        .orElseThrow(
                            () ->
                                GrpcErrors.notFound(
                                    correlationId(), "catalog", Map.of("id", prefix.getCatalog())));

                Namespace namespace =
                    namespaces
                        .getByPath(tenantId, catalog.getResourceId().getId(), prefix.getPathList())
                        .orElseThrow(
                            () ->
                                GrpcErrors.notFound(
                                    correlationId(),
                                    "namespace.by_path_missing",
                                    Map.of(
                                        "catalog_id",
                                        catalog.getResourceId().getId(),
                                        "path",
                                        String.join("/", prefix.getPathList()))));

                StringBuilder next = new StringBuilder();
                var entries =
                    tables.list(
                        tenantId,
                        catalog.getResourceId().getId(),
                        namespace.getResourceId().getId(),
                        Math.max(1, limit),
                        token,
                        next);
                int total =
                    tables.count(
                        tenantId,
                        catalog.getResourceId().getId(),
                        namespace.getResourceId().getId());

                for (Table table : entries) {
                  var nr =
                      NameRef.newBuilder()
                          .setCatalog(catalog.getDisplayName())
                          .addAllPath(prefix.getPathList())
                          .setName(table.getDisplayName())
                          .setResourceId(table.getResourceId())
                          .build();

                  builder.addTables(
                      ResolveFQTablesResponse.Entry.newBuilder()
                          .setName(nr)
                          .setResourceId(table.getResourceId()));
                }

                builder.setPage(
                    PageResponse.newBuilder()
                        .setTotalSize(total)
                        .setNextPageToken(next.toString()));
                return builder.build();
              }

              throw GrpcErrors.invalidArgument(correlationId(), "selector.required", Map.of());
            }),
        correlationId());
  }

  private void validateNameRefOrThrow(NameRef ref) {
    if (ref.getCatalog() == null || ref.getCatalog().isBlank()) {
      throw GrpcErrors.invalidArgument(correlationId(), "catalog.missing", Map.of());
    }
    for (String pathSegment : ref.getPathList()) {
      if (pathSegment == null || pathSegment.isBlank()) {
        throw GrpcErrors.invalidArgument(correlationId(), "path.segment.blank", Map.of());
      }
      if (pathSegment.contains("/")) {
        throw GrpcErrors.invalidArgument(
            correlationId(), "path.segment.contains_slash", Map.of("segment", pathSegment));
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
