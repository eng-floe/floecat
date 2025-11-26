package ai.floedb.metacat.service.catalog.impl;

import ai.floedb.metacat.catalog.rpc.BuiltinCatalogService;
import ai.floedb.metacat.catalog.rpc.GetBuiltinCatalogRequest;
import ai.floedb.metacat.catalog.rpc.GetBuiltinCatalogResponse;
import ai.floedb.metacat.service.catalog.builtin.BuiltinCatalogLoader;
import ai.floedb.metacat.service.catalog.builtin.BuiltinCatalogNotFoundException;
import ai.floedb.metacat.service.catalog.builtin.BuiltinCatalogProtoMapper;
import ai.floedb.metacat.service.common.BaseServiceImpl;
import ai.floedb.metacat.service.context.impl.InboundContextInterceptor;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.Map;

/**
 * gRPC endpoint exposed to planners so they can fetch builtin metadata once per engine version.
 * Reads data from {@link BuiltinCatalogLoader} and returns empty responses when the caller already
 * holds the latest version.
 */
@GrpcService
public class BuiltinCatalogServiceImpl extends BaseServiceImpl implements BuiltinCatalogService {

  @Inject BuiltinCatalogLoader loader;

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

              try {
                var catalog = loader.getCatalog(engineVersion);

                if (!request.getCurrentVersion().isBlank()
                    && request.getCurrentVersion().equals(catalog.version())) {
                  return GetBuiltinCatalogResponse.newBuilder().build();
                }

                return GetBuiltinCatalogResponse.newBuilder()
                    .setCatalog(BuiltinCatalogProtoMapper.toProto(catalog))
                    .build();
              } catch (BuiltinCatalogNotFoundException e) {
                throw GrpcErrors.notFound(
                    correlationId(),
                    "builtin.catalog.not_found",
                    Map.of("engine_version", engineVersion));
              }
            }),
        correlationId());
  }
}
