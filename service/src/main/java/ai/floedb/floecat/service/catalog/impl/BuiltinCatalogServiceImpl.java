package ai.floedb.floecat.service.catalog.impl;

import ai.floedb.floecat.catalog.builtin.graph.BuiltinNodeRegistry.BuiltinNodes;
import ai.floedb.floecat.catalog.builtin.registry.BuiltinCatalogProtoMapper;
import ai.floedb.floecat.query.rpc.BuiltinCatalogService;
import ai.floedb.floecat.query.rpc.BuiltinRegistry;
import ai.floedb.floecat.query.rpc.GetBuiltinCatalogRequest;
import ai.floedb.floecat.query.rpc.GetBuiltinCatalogResponse;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.context.impl.InboundContextInterceptor;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.metagraph.MetadataGraph;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Optional;

/**
 * gRPC endpoint exposed to planners so they can fetch builtin metadata once per engine version.
 * Reads engine builtin catalogs from {@link BuiltinDefinitionRegistry} (plugin-based or empty
 * fallback)
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

              BuiltinRegistry registry = fetchBuiltinCatalog(engineKind, engineVersion);
              return GetBuiltinCatalogResponse.newBuilder().setRegistry(registry).build();
            }),
        correlationId());
  }

  private BuiltinRegistry fetchBuiltinCatalog(String engineKind, String engineVersion) {
    BuiltinNodes nodes = metadataGraph.builtinNodes(engineKind, engineVersion);
    return BuiltinCatalogProtoMapper.toProto(nodes.toCatalogData());
  }
}
