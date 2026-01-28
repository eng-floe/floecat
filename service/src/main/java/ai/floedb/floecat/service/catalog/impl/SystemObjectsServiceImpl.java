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

import ai.floedb.floecat.query.rpc.GetSystemObjectsRequest;
import ai.floedb.floecat.query.rpc.GetSystemObjectsResponse;
import ai.floedb.floecat.query.rpc.SystemObjectsRegistry;
import ai.floedb.floecat.query.rpc.SystemObjectsService;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.context.EngineContextProvider;
import ai.floedb.floecat.service.error.impl.RequestValidation;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry.BuiltinNodes;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogProtoMapper;
import ai.floedb.floecat.systemcatalog.registry.SystemDefinitionRegistry;
import ai.floedb.floecat.systemcatalog.util.EngineContext;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.List;

/**
 * gRPC endpoint exposed to planners so they can fetch builtin metadata once per engine version.
 * Reads engine builtin catalogs from {@link SystemDefinitionRegistry} (plugin-based or empty
 * fallback)
 */
@GrpcService
public class SystemObjectsServiceImpl extends BaseServiceImpl implements SystemObjectsService {

  @Inject SystemNodeRegistry nodeRegistry;
  @Inject EngineContextProvider engineContextProvider;

  @Override
  public Uni<GetSystemObjectsResponse> getSystemObjects(GetSystemObjectsRequest request) {
    return mapFailures(
        run(
            () -> {
              EngineContext ctx = engineContextProvider.engineContext();
              RequestValidation.requireNonBlank(
                  ctx.engineVersion(),
                  "x-engine-version",
                  correlationId(),
                  "builtin.engine_version.required");
              RequestValidation.requireNonBlank(
                  ctx.engineKind(),
                  "x-engine-kind",
                  correlationId(),
                  "builtin.engine_kind.required");
              SystemObjectsRegistry registry = fetchSystemObjects(ctx);
              return GetSystemObjectsResponse.newBuilder().setRegistry(registry).build();
            }),
        correlationId());
  }

  private SystemObjectsRegistry fetchSystemObjects(EngineContext ctx) {
    BuiltinNodes nodes = nodeRegistry.nodesFor(ctx);
    return SystemCatalogProtoMapper.toProto(sanitize(nodes.toCatalogData()));
  }

  private static SystemCatalogData sanitize(SystemCatalogData data) {
    return new SystemCatalogData(
        data.functions(),
        data.operators(),
        data.types(),
        data.casts(),
        data.collations(),
        data.aggregates(),
        List.of(),
        List.of(),
        List.of(),
        data.registryEngineSpecific());
  }
}
