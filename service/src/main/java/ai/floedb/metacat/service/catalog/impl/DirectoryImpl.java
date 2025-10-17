package ai.floedb.metacat.service.catalog.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

import ai.floedb.metacat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.metacat.catalog.rpc.ResolveCatalogResponse;
import ai.floedb.metacat.catalog.rpc.ResolveFQTablesRequest;
import ai.floedb.metacat.catalog.rpc.ResolveFQTablesResponse;
import ai.floedb.metacat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.ResolveNamespaceResponse;
import ai.floedb.metacat.catalog.rpc.ResolveTableRequest;
import ai.floedb.metacat.catalog.rpc.ResolveTableResponse;
import ai.floedb.metacat.catalog.rpc.LookupCatalogRequest;
import ai.floedb.metacat.catalog.rpc.LookupCatalogResponse;
import ai.floedb.metacat.catalog.rpc.LookupNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.LookupNamespaceResponse;
import ai.floedb.metacat.catalog.rpc.LookupTableRequest;
import ai.floedb.metacat.catalog.rpc.LookupTableResponse;
import ai.floedb.metacat.catalog.rpc.Directory;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.PageResponse;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.impl.CatalogRepository;
import ai.floedb.metacat.service.repo.impl.NamespaceRepository;
import ai.floedb.metacat.service.repo.impl.TableRepository;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;

@GrpcService
public class DirectoryImpl implements Directory {
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;

  @Inject CatalogRepository catalogs;
  @Inject NamespaceRepository namespaces;
  @Inject TableRepository tables;

  @Override
  public Uni<ResolveCatalogResponse> resolveCatalog(ResolveCatalogRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.read");

    return Uni.createFrom().item(() -> {
      var tenant = p.getTenantId();
      Catalog cat = catalogs.getByName(tenant, req.getRef().getCatalog())
          .orElseThrow(() -> GrpcErrors.notFound(
              corrId(), "catalog", Map.of("id", req.getRef().getCatalog())));
      return ResolveCatalogResponse.newBuilder()
          .setResourceId(cat.getResourceId())
          .build();
    });
  }

  @Override
  public Uni<LookupCatalogResponse> lookupCatalog(LookupCatalogRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.read");

    return Uni.createFrom().item(() -> {
      var opt = catalogs.getById(req.getResourceId());
      var display = opt.map(Catalog::getDisplayName).orElse("");
      return LookupCatalogResponse.newBuilder().setDisplayName(display).build();
    });
  }

  @Override
  public Uni<ResolveNamespaceResponse> resolveNamespace(ResolveNamespaceRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.read");

    var ref = req.getRef();
    validateNameRefOrThrow(ref);
    var tenant = p.getTenantId();

    return Uni.createFrom().item(() -> {
      Catalog cat = catalogs.getByName(tenant, ref.getCatalog())
          .orElseThrow(() -> GrpcErrors.notFound(
              corrId(), "catalog", Map.of("id", ref.getCatalog())));

      var fullPath = new ArrayList<>(ref.getPathList());
      if (!ref.getName().isBlank()) {
        fullPath.add(ref.getName());
      }

      ResourceId nsId = namespaces.getByPath(tenant, cat.getResourceId(), fullPath)
          .orElseThrow(() -> GrpcErrors.notFound(
              corrId(), "namespace.by_path_missing",
              Map.of("catalog_id", cat.getResourceId().getId(),
                     "path", String.join("/", fullPath))));

      return ResolveNamespaceResponse.newBuilder().setResourceId(nsId).build();
    });
  }

  @Override
  public Uni<LookupNamespaceResponse> lookupNamespace(LookupNamespaceRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.read");

    return Uni.createFrom().item(() -> {
      var tenant = p.getTenantId();
      var nsRid = req.getResourceId();

      var catIdOpt = namespaces.findOwnerCatalog(tenant, nsRid.getId());
      if (catIdOpt.isEmpty()) {
        return LookupNamespaceResponse.newBuilder().build();
      }
      var catId = catIdOpt.get();

      var nsOpt = namespaces.get(catId, nsRid);
      var catOpt = catalogs.getById(catId);

      if (nsOpt.isEmpty() || catOpt.isEmpty()) {
        return LookupNamespaceResponse.newBuilder().build();
      }

      Namespace ns = nsOpt.get();
      Catalog cat = catOpt.get();
      var nr = NameRef.newBuilder()
          .setCatalog(cat.getDisplayName())
          .addAllPath(ns.getParentsList())
          .setName(ns.getDisplayName())
          .build();

      return LookupNamespaceResponse.newBuilder()
          .setRef(nr)
          .build();
    });
  }

  @Override
  public Uni<ResolveTableResponse> resolveTable(ResolveTableRequest req) {
    var p = principal.get();
    authz.require(p, List.of("catalog.read", "table.read"));

    var ref = req.getRef();
    validateNameRefOrThrow(ref);
    validateTableNameOrThrow(ref);

    return Uni.createFrom().item(() -> {
      var tenant = p.getTenantId();

      Catalog cat = catalogs.getByName(tenant, ref.getCatalog())
          .orElseThrow(() -> GrpcErrors.notFound(
              corrId(), "catalog", Map.of("id", ref.getCatalog())));

      ResourceId nsId = namespaces.getByPath(tenant, cat.getResourceId(), ref.getPathList())
          .orElseThrow(() -> GrpcErrors.notFound(
              corrId(), "namespace.by_path_missing",
              Map.of("catalog_id", cat.getResourceId().getId(),
                     "path", String.join("/", ref.getPathList()))));

      Table td = tables.listByNamespace(
          cat.getResourceId(),
          nsId,
          Integer.MAX_VALUE,
          "",
          new StringBuilder())
              .stream()
              .filter(t -> t.getDisplayName().equals(ref.getName()))
              .findFirst()
              .orElseThrow(() -> GrpcErrors.notFound(
                  corrId(), "table.by_name_missing",
                  Map.of("catalog", ref.getCatalog(),
                      "path", String.join("/", ref.getPathList()),
                      "name", ref.getName())));

      return ResolveTableResponse.newBuilder()
          .setResourceId(td.getResourceId())
          .build();
    });
  }

  @Override
  public Uni<LookupTableResponse> lookupTable(LookupTableRequest req) {
    var p = principal.get();
    authz.require(p, List.of("catalog.read", "table.read"));

    return Uni.createFrom().item(() -> {
      var tableId = req.getResourceId();

      var tdOpt = tables.get(tableId);
      if (tdOpt.isEmpty()) {
        return LookupTableResponse.newBuilder().build();
      }
      var td = tdOpt.get();

      var catOpt = catalogs.getById(td.getCatalogId());
      var nsOpt = namespaces.get(td.getCatalogId(), td.getNamespaceId());

      if (catOpt.isEmpty() || nsOpt.isEmpty()) {
        return LookupTableResponse.newBuilder().build();
      }

      var cat = catOpt.get();
      var ns = nsOpt.get();

      var path = new ArrayList<>(ns.getParentsList());
      if (!ns.getDisplayName().isBlank()) {
        path.add(ns.getDisplayName());
      }

      var nameRef = NameRef.newBuilder()
          .setCatalog(cat.getDisplayName())
          .addAllPath(path)
          .setName(td.getDisplayName())
          .build();

      return LookupTableResponse.newBuilder()
          .setName(nameRef)
          .build();
    });
  }

  @Override
  public Uni<ResolveFQTablesResponse> resolveFQTables(ResolveFQTablesRequest req) {
    var p = principal.get();
    authz.require(p, List.of("catalog.read", "table.read"));

    final int limit = (req.hasPage() && req.getPage().getPageSize() > 0)
        ? req.getPage().getPageSize() : 50;
    final String token = req.hasPage() ? req.getPage().getPageToken() : "";

    return Uni.createFrom().item(() -> {
      var out = ResolveFQTablesResponse.newBuilder();
      var tenant = p.getTenantId();

      if (req.hasList()) {
        var names = req.getList().getNamesList();

        final int start;
        if (token == null || token.isEmpty()) {
          start = 0;
        } else {
          try {
            start = Integer.parseInt(token);
          } catch (NumberFormatException nfe) {
            throw GrpcErrors.invalidArgument(
                corrId(), "page_token.invalid", Map.of("page_token", token));
          }
        }
        final int end = Math.min(names.size(), start + Math.max(1, limit));

        for (int i = start; i < end; i++) {
          NameRef n = names.get(i);
          try {
            validateNameRefOrThrow(n);
            validateTableNameOrThrow(n);

            Catalog cat = catalogs.getByName(tenant, n.getCatalog())
                .orElseThrow();
            ResourceId nsId = namespaces.getByPath(tenant, cat.getResourceId(), n.getPathList())
                .orElseThrow();

            // Resolve by scanning tables under ns (see note above)
            Optional<Table> tdOpt = tables
                .listByNamespace(
                      cat.getResourceId(),
                      nsId,
                      Integer.MAX_VALUE,
                      "",
                      new StringBuilder())
                          .stream()
                          .filter(t -> t.getDisplayName().equals(n.getName()))
                          .findFirst();

            var id = tdOpt.map(Table::getResourceId).orElse(ResourceId.getDefaultInstance());
            var outName = tdOpt.isPresent()
                ? NameRef.newBuilder()
                    .setCatalog(cat.getDisplayName())
                    .addAllPath(n.getPathList())
                    .setName(n.getName())
                    .setResourceId(id)
                    .build()
                : n;

            out.addTables(ResolveFQTablesResponse.Entry.newBuilder()
                .setName(outName)
                .setResourceId(id));

          } catch (Throwable t) {
            out.addTables(ResolveFQTablesResponse.Entry.newBuilder()
                .setName(n)
                .setResourceId(ResourceId.getDefaultInstance()));
          }
        }

        out.setPage(PageResponse.newBuilder()
            .setTotalSize(names.size())
            .setNextPageToken(end < names.size() ? Integer.toString(end) : ""));
        return out.build();
      }

      if (req.hasPrefix()) {
        var pref = req.getPrefix();
        validateNameRefOrThrow(pref);

        Catalog cat = catalogs.getByName(tenant, pref.getCatalog())
            .orElseThrow(() -> GrpcErrors.notFound(
                corrId(), "catalog", Map.of("id", pref.getCatalog())));

        ResourceId nsId = namespaces.getByPath(tenant, cat.getResourceId(), pref.getPathList())
            .orElseThrow(() -> GrpcErrors.notFound(
                corrId(), "namespace.by_path_missing",
                Map.of("catalog_id", cat.getResourceId().getId(),
                    "path", String.join("/", pref.getPathList()))));

        StringBuilder next = new StringBuilder();
        var entries = tables.listByNamespace(cat.getResourceId(), nsId, Math.max(1, limit), token, next);
        int total = tables.countUnderNamespace(cat.getResourceId(), nsId);

        for (Table td : entries) {
          var nr = NameRef.newBuilder()
              .setCatalog(cat.getDisplayName())
              .addAllPath(pref.getPathList())
              .setName(td.getDisplayName())
              .setResourceId(td.getResourceId())
              .build();
          out.addTables(ResolveFQTablesResponse.Entry.newBuilder()
              .setName(nr)
              .setResourceId(td.getResourceId()));
        }

        out.setPage(PageResponse.newBuilder()
            .setTotalSize(total)
            .setNextPageToken(next.toString()));
        return out.build();
      }

      throw GrpcErrors.invalidArgument(corrId(), "selector.required", Map.of());
    });
  }

  private String corrId() {
    var pctx = principal != null ? principal.get() : null;
    return pctx != null ? pctx.getCorrelationId() : "";
  }

  private void validateNameRefOrThrow(NameRef ref) {
    if (ref.getCatalog() == null || ref.getCatalog().isBlank()) {
      throw GrpcErrors.invalidArgument(corrId(), "catalog.missing", Map.of());
    }
    for (String s : ref.getPathList()) {
      if (s == null || s.isBlank()) {
        throw GrpcErrors.invalidArgument(corrId(), "path.segment.blank", Map.of());
      }
      if (s.contains("/")) {
        throw GrpcErrors.invalidArgument(corrId(), "path.segment.contains_slash", Map.of("segment", s));
      }
    }
  }

  private void validateTableNameOrThrow(NameRef ref) {
    if (ref.getName() == null || ref.getName().isBlank()) {
      throw GrpcErrors.invalidArgument(corrId(), "table.name.missing", Map.of());
    }
  }
}
