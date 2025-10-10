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
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.impl.NameIndexRepository;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;

@GrpcService
public class DirectoryImpl implements Directory {
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject NameIndexRepository nameIndex;

  private ResourceId requireCatalogIdByName(String tenantId, String catalogName) {
    return nameIndex.getCatalogByName(tenantId, catalogName)
      .map(NameRef::getResourceId)
      .orElseThrow(() -> GrpcErrors.notFound("catalog not found: " + catalogName, null));
  }

  @Override
  public Uni<ResolveCatalogResponse> resolveCatalog(ResolveCatalogRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.read");

    return Uni.createFrom().item(() ->
      nameIndex.getCatalogByName(p.getTenantId(), req.getRef().getCatalog())
        .map(NameRef::getResourceId)
        .map(id -> ResolveCatalogResponse.newBuilder().setResourceId(id).build())
        .orElseThrow(() -> 
          GrpcErrors.notFound("catalog not found: " + req.getRef().getCatalog(), null))
    );
  }

  @Override
  public Uni<LookupCatalogResponse> lookupCatalog(LookupCatalogRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.read");

    return Uni.createFrom().item(() -> {
      var nr = nameIndex.getCatalogById(p.getTenantId(), req.getResourceId().getId());
      var display = nr.map(NameRef::getCatalog).orElse("");
      return LookupCatalogResponse.newBuilder().setDisplayName(display).build();
    });
  }

  @Override
  public Uni<ResolveNamespaceResponse> resolveNamespace(ResolveNamespaceRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.read");

    var ref = req.getRef();
    return Uni.createFrom().item(() -> {
      var catalogRid = requireCatalogIdByName(p.getTenantId(), ref.getCatalog());
      return nameIndex.getNamespaceByPath(p.getTenantId(), catalogRid.getId(), ref.getPathList())
        .map(NameRef::getResourceId)
        .map(id -> ResolveNamespaceResponse.newBuilder().setResourceId(id).build())
        .orElseThrow(() -> GrpcErrors.notFound("namespace not found", null));
    });
  }

  @Override
  public Uni<LookupNamespaceResponse> lookupNamespace(LookupNamespaceRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.read");

    return Uni.createFrom().item(() -> {
      var nrOpt = nameIndex.getNamespaceById(p.getTenantId(), req.getResourceId().getId());
      if (nrOpt.isEmpty()) {
        return LookupNamespaceResponse.newBuilder().build();
      }
      
      var storedRef = nrOpt.get();
      return LookupNamespaceResponse.newBuilder()
        .setRef(storedRef)
        .build();
    });
  }

  @Override
  public Uni<ResolveTableResponse> resolveTable(ResolveTableRequest req) {
    var p = principal.get();
    authz.require(p, List.of("catalog.read", "table.read"));

    return Uni.createFrom().item(() ->
      nameIndex.getTableByName(p.getTenantId(), req.getRef())
        .map(NameRef::getResourceId)
        .map(id -> ResolveTableResponse.newBuilder().setResourceId(id).build())
        .orElseThrow(() -> GrpcErrors.notFound("table not found", null))
    );
  }

  @Override
  public Uni<LookupTableResponse> lookupTable(LookupTableRequest req) {
    var p = principal.get();
    authz.require(p, List.of("catalog.read", "table.read"));

    return Uni.createFrom().item(() ->
      nameIndex.getTableById(p.getTenantId(), req.getResourceId().getId())
        .map(nr -> LookupTableResponse.newBuilder().setName(nr).build())
        .orElse(LookupTableResponse.newBuilder().build())
    );
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
        final int start;
        if (token == null || token.isEmpty()) {
          start = 0;
        } else {
          try {
            start = Integer.parseInt(token);
          } catch (NumberFormatException nfe) {
            throw Status.INVALID_ARGUMENT.withDescription("invalid page_token").asRuntimeException();
          }
        }
        final int end = Math.min(names.size(), start + Math.max(1, limit));

        for (int i = start; i < end; i++) {
          NameRef n = names.get(i);
          var resolved = nameIndex.getTableByName(p.getTenantId(), n);

          var id = resolved.map(NameRef::getResourceId).orElse(ResourceId.getDefaultInstance());
          out.addTables(ResolveFQTablesResponse.Entry.newBuilder()
            .setName(resolved.orElse(n))
            .setResourceId(id));
        }

        out.setPage(PageResponse.newBuilder()
          .setTotalSize(names.size())
          .setNextPageToken(end < names.size() ? Integer.toString(end) : ""));
        return out.build();
      }

      if (req.hasPrefix()) {
        StringBuilder next = new StringBuilder();

        var entries = nameIndex.listTablesByPrefix(
          p.getTenantId(), req.getPrefix(), Math.max(1, limit), token, next);

        for (NameRef nr : entries) {
          var id = nr.hasResourceId() ? nr.getResourceId() : ResourceId.getDefaultInstance();
          out.addTables(ResolveFQTablesResponse.Entry.newBuilder()
            .setName(nr)
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