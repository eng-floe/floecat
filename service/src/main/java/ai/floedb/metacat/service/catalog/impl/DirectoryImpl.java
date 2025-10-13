package ai.floedb.metacat.service.catalog.impl;

import java.util.List;
import java.util.Map;

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

  @Override
  public Uni<ResolveCatalogResponse> resolveCatalog(ResolveCatalogRequest req) {
    var p = principal.get();
    authz.require(p, "catalog.read");

    return Uni.createFrom().item(() ->
      nameIndex.getCatalogByName(p.getTenantId(), req.getRef().getCatalog())
        .map(NameRef::getResourceId)
        .map(id -> ResolveCatalogResponse.newBuilder().setResourceId(id).build())
        .orElseThrow(() -> GrpcErrors.notFound(corrId(), "catalog",
          Map.of("id", req.getRef().getCatalog()))));
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
    var tenantId = p.getTenantId();
    var catalogRid = nameIndex.getCatalogByName(tenantId, ref.getCatalog())
      .map(NameRef::getResourceId)
      .orElseThrow(() -> GrpcErrors.notFound(corrId(), "catalog",
        Map.of("id", ref.getCatalog())));
    var normPath = ref.getPathList();

    var id = nameIndex.getNamespaceByPath(tenantId, catalogRid.getId(), normPath)
        .map(NameRef::getResourceId)
        .orElseThrow(() -> GrpcErrors.notFound(
            corrId(),
            "namespace.by_path_missing",
            Map.of("catalog_id", catalogRid.getId(), "path", String.join("/", normPath))
        ));

    return Uni.createFrom().item(
        ResolveNamespaceResponse.newBuilder().setResourceId(id).build());
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

    var tableRef = NameRef.newBuilder()
      .setCatalog(req.getRef().getCatalog())
      .addAllPath(req.getRef().getPathList())
      .setName(req.getRef().getName())
      .build();

    var id = nameIndex.getTableByName(p.getTenantId(), tableRef)
        .map(NameRef::getResourceId)
        .orElseThrow(() -> GrpcErrors.notFound(
            corrId(),
            "table.by_name_missing",
            Map.of("catalog", req.getRef().getCatalog(), 
              "path", String.join("/", req.getRef().getPathList()), 
              "name", req.getRef().getName())
        ));

    return Uni.createFrom().item(
        ResolveTableResponse.newBuilder().setResourceId(id).build());
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
        var norm = NameRef.newBuilder()
          .setCatalog(req.getPrefix().getCatalog())
          .addAllPath(req.getPrefix().getPathList())
          .build();

        StringBuilder next = new StringBuilder();
        var entries = nameIndex.listTablesByPrefix(p.getTenantId(), norm, Math.max(1, limit), token, next);

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

  private String corrId() {
    var pctx = principal != null ? principal.get() : null;
    return pctx != null ? pctx.getCorrelationId() : "";
  }
}
