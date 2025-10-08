package ai.floedb.metacat.service.catalog.impl;

import java.util.List;

import io.grpc.Status;
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
import ai.floedb.metacat.catalog.rpc.TableIndexEntry;
import ai.floedb.metacat.catalog.rpc.LookupCatalogRequest;
import ai.floedb.metacat.catalog.rpc.LookupCatalogResponse;
import ai.floedb.metacat.catalog.rpc.LookupNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.LookupNamespaceResponse;
import ai.floedb.metacat.catalog.rpc.LookupTableRequest;
import ai.floedb.metacat.catalog.rpc.LookupTableResponse;
import ai.floedb.metacat.catalog.rpc.NamespaceIndexEntry;
import ai.floedb.metacat.catalog.rpc.CatalogIndexEntry;
import ai.floedb.metacat.catalog.rpc.Directory;
import ai.floedb.metacat.common.rpc.PageResponse;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.impl.NameIndexRepository;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;

@GrpcService
public class DirectoryImpl implements Directory {
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject NameIndexRepository nameIndex;

  @Override
  public Uni<ResolveCatalogResponse> resolveCatalog(ResolveCatalogRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.read");

    return Uni.createFrom().item(() ->
      nameIndex.getCatalogByName(p.getTenantId(), req.getDisplayName())
        .map(CatalogIndexEntry::getResourceId)
        .map(id -> ResolveCatalogResponse.newBuilder().setResourceId(id).build())
        .orElseThrow(() -> GrpcErrors.notFound("catalog not found: " + req.getDisplayName(), null)));
  }

  @Override
  public Uni<LookupCatalogResponse> lookupCatalog(LookupCatalogRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.read");

    return Uni.createFrom().item(() -> {
      var entry = nameIndex.getCatalogById(p.getTenantId(), req.getResourceId().getId());
      var display = entry.map(CatalogIndexEntry::getDisplayName).orElse("");
      return LookupCatalogResponse.newBuilder().setDisplayName(display).build();
    });
  }

  @Override
  public Uni<ResolveNamespaceResponse> resolveNamespace(ResolveNamespaceRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.read");

    var ref = req.getRef();
    return Uni.createFrom().item(() ->
      nameIndex.getNamespaceByPath(p.getTenantId(),
                                  ref.getCatalogId().getId(),
                                  ref.getNamespacePathList())
        .map(NamespaceIndexEntry::getResourceId)
        .map(id -> ResolveNamespaceResponse.newBuilder().setResourceId(id).build())
        .orElseThrow(() -> GrpcErrors.notFound("namespace not found", null)));
  }

  @Override
  public Uni<LookupNamespaceResponse> lookupNamespace(LookupNamespaceRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.read");

    return Uni.createFrom().item(() -> {
      var entry = nameIndex.getNamespaceById(p.getTenantId(), req.getResourceId().getId());
      if (entry.isEmpty()) {
        return LookupNamespaceResponse.newBuilder().setDisplayName("").build();
      }

      var storedRef = entry.get().getRef();
      var display = storedRef.getNamespacePathCount() == 0
        ? "" : storedRef.getNamespacePath(storedRef.getNamespacePathCount() - 1);

      return LookupNamespaceResponse.newBuilder()
        .setRef(storedRef)
        .setDisplayName(display)
        .build();
    });
  }

  @Override
  public Uni<ResolveTableResponse> resolveTable(ResolveTableRequest req) {
    var p = principal.get();
    authz.require(p, List.of("catalog.read", "table.read"));

    return Uni.createFrom().item(() ->
      nameIndex.getTableByName(p.getTenantId(), req.getName())
        .map(TableIndexEntry::getResourceId)
        .map(id -> ResolveTableResponse.newBuilder().setResourceId(id).build())
        .orElseThrow(() -> GrpcErrors.notFound("table not found", null)));
  }

  @Override
  public Uni<LookupTableResponse> lookupTable(LookupTableRequest req) {
    var p = principal.get();
    authz.require(p, List.of("catalog.read", "table.read"));

    return Uni.createFrom().item(() ->
      nameIndex.getTableById(p.getTenantId(), req.getResourceId().getId())
        .map(TableIndexEntry::getFqName)
        .map(n -> LookupTableResponse.newBuilder().setName(n).build())
        .orElse(LookupTableResponse.newBuilder().build()));
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

      if (req.hasList()) {
        var names = req.getList().getNamesList();
        int start = token.isEmpty() ? 0 : Integer.parseInt(token);
        int end = Math.min(names.size(), start + limit);

        for (int i = start; i < end; i++) {
          var n = names.get(i);
          var id = nameIndex.getTableByName(p.getTenantId(), n)
            .map(TableIndexEntry::getResourceId)
            .orElse(ResourceId.getDefaultInstance());

          out.addTables(ResolveFQTablesResponse.Entry.newBuilder()
            .setName(n)
            .setResourceId(id));
        }

        out.setPage(PageResponse.newBuilder()
          .setTotalSize(names.size())
          .setNextPageToken(end < names.size() ? Integer.toString(end) : ""));
        return out.build();
      }

      if (req.hasPrefix()) {
        StringBuilder next = new StringBuilder();
        var entries = nameIndex.listTablesByPrefix(p.getTenantId(), req.getPrefix(), limit, token, next);

        for (var e : entries) {
          var id = nameIndex.getTableByName(p.getTenantId(), e.getFqName())
            .map(TableIndexEntry::getResourceId)
            .orElse(ResourceId.getDefaultInstance());
          out.addTables(ResolveFQTablesResponse.Entry.newBuilder()
            .setName(e.getFqName())
            .setResourceId(id));
        }

        out.setPage(PageResponse.newBuilder()
          .setTotalSize(0)
          .setNextPageToken(next.toString()));
        return out.build();
      }

      throw Status.INVALID_ARGUMENT.withDescription("selector is required").asRuntimeException();
    });
  }
}