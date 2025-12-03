package ai.floedb.metacat.service.catalog.impl;

import ai.floedb.metacat.catalog.builtin.BuiltinCatalogNotFoundException;
import ai.floedb.metacat.catalog.builtin.BuiltinCatalogProtoMapper;
import ai.floedb.metacat.catalog.rpc.BuiltinCatalog;
import ai.floedb.metacat.catalog.rpc.BuiltinCatalogService;
import ai.floedb.metacat.catalog.rpc.GetBuiltinCatalogRequest;
import ai.floedb.metacat.catalog.rpc.GetBuiltinCatalogResponse;
import ai.floedb.metacat.service.common.BaseServiceImpl;
import ai.floedb.metacat.service.context.impl.InboundContextInterceptor;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.query.graph.MetadataGraph;
import ai.floedb.metacat.service.query.graph.builtin.BuiltinNodeRegistry.BuiltinNodes;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Optional;

/**
 * gRPC endpoint exposed to planners so they can fetch builtin metadata once per engine version.
 * Reads data from {@link BuiltinCatalogLoader} and returns empty responses when the caller already
 * holds the latest version.
 */
@GrpcService
public class BuiltinCatalogServiceImpl extends BaseServiceImpl implements BuiltinCatalogService {

  @Inject MetadataGraph metadataGraph;

  @Override
  public Uni<GetBuiltinCatalogResponse> getBuiltinCatalog(GetBuiltinCatalogRequest request) {
    return mapFailures(
        run(
            () -> {
              String engineVersion = InboundContextInterceptor.ENGINE_VERSION_KEY.get();
              if (engineVersion == null || engineVersion.isBlank()) {
                throw GrpcErrors.invalidArgument(
                    correlationId(),
                    "builtin.engine_version.required",
                    Map.of("header", "x-engine-version"));
              }
              String engineKind =
                  Optional.ofNullable(InboundContextInterceptor.ENGINE_KIND_KEY.get()).orElse("");
              if (engineKind.isBlank()) {
                throw GrpcErrors.invalidArgument(
                    correlationId(),
                    "builtin.engine_kind.required",
                    Map.of("header", "x-engine-kind"));
              }

              try {
                BuiltinCatalog catalogProto = fetchBuiltinCatalog(engineKind, engineVersion);

                if (!request.getCurrentVersion().isBlank()
                    && request.getCurrentVersion().equals(catalogProto.getVersion())) {
                  return GetBuiltinCatalogResponse.newBuilder().build();
                }

                return GetBuiltinCatalogResponse.newBuilder().setCatalog(catalogProto).build();
              } catch (BuiltinCatalogNotFoundException e) {
                throw GrpcErrors.notFound(
                    correlationId(),
                    "builtin.catalog.not_found",
                    Map.of("engine_version", engineVersion));
              }
            }),
        correlationId());
  }

  private BuiltinCatalog fetchBuiltinCatalog(String engineKind, String engineVersion) {
    BuiltinNodes nodes = metadataGraph.builtinNodes(engineKind, engineVersion);
    return toProto(nodes);
  }

  private BuiltinCatalog toProto(BuiltinNodes nodes) {
    return BuiltinCatalogProtoMapper.toProto(nodes.toCatalogData());
  }
}
