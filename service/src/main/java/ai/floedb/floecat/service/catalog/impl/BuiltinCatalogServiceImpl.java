/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.service.catalog.impl;

import ai.floedb.floecat.query.rpc.BuiltinCatalogService;
import ai.floedb.floecat.query.rpc.BuiltinRegistry;
import ai.floedb.floecat.query.rpc.GetBuiltinCatalogRequest;
import ai.floedb.floecat.query.rpc.GetBuiltinCatalogResponse;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.context.impl.InboundContextInterceptor;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogProtoMapper;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Optional;

/**
 * gRPC endpoint exposed to planners so they can fetch builtin metadata once per engine version.
 * Reads engine builtin catalogs from {@link SystemDefinitionRegistry} (plugin-based or empty
 * fallback)
 */
@GrpcService
public class BuiltinCatalogServiceImpl extends BaseServiceImpl implements BuiltinCatalogService {

  @Inject SystemNodeRegistry nodeRegistry;

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
    var nodes = nodeRegistry.nodesFor(engineKind, engineVersion);
    return SystemCatalogProtoMapper.toProto(nodes.toCatalogData());
  }
}
