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

package ai.floedb.floecat.service.query.resolver;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.*;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.scanner.spi.SystemObjectScanner;
import ai.floedb.floecat.scanner.utils.EngineCatalogNames;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.service.context.EngineContextProvider;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.graph.SystemResourceIdGenerator;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import ai.floedb.floecat.systemcatalog.provider.SystemObjectScannerProvider;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@ApplicationScoped
public final class SystemScannerResolver {

  @Inject CatalogOverlay graph;
  @Inject EngineContextProvider engine;
  @Inject List<SystemObjectScannerProvider> providers;

  /**
   * Resolves the scanner for the given table ID, reading the engine context from the current gRPC
   * call's thread-local context via {@link EngineContextProvider}.
   *
   * <p>Use this overload from gRPC service implementations where the engine context is already
   * propagated by {@code InboundContextInterceptor}.
   */
  public SystemObjectScanner resolve(String correlationId, ResourceId tableId) {
    return resolve(correlationId, tableId, engine.engineContext());
  }

  /**
   * Resolves the scanner for the given table ID using an explicitly supplied engine context.
   *
   * <p>Use this overload from transports that carry their own context (e.g. Arrow Flight), where
   * the gRPC thread-local context is not available.
   */
  public SystemObjectScanner resolve(String correlationId, ResourceId tableId, EngineContext ctx) {
    String engineKind = ctx.effectiveEngineKind();
    String engineVersion = ctx.normalizedVersion();

    Optional<SystemTableNode.FloeCatSystemTableNode> nodeOptional =
        resolveSystemTable(graph, tableId, engineKind);
    var node =
        nodeOptional.orElseThrow(
            () ->
                GrpcErrors.invalidArgument(
                    correlationId,
                    SYSTEM_SCAN_NOT_SYSTEM_TABLE,
                    Map.of("table_id", tableId.getId())));

    String scannerId = node.scannerId();
    if (scannerId == null || scannerId.isBlank()) {
      throw GrpcErrors.internal(
          correlationId, SYSTEM_SCAN_MISSING_SCANNER, Map.of("table_id", tableId.getId()));
    }

    for (var provider : providers) {
      if (!provider.supportsEngine(engineKind)) {
        continue;
      }

      var scanner = provider.provide(scannerId, engineKind, engineVersion);

      if (scanner.isPresent()) {
        return scanner.get();
      }
    }

    throw GrpcErrors.notFound(
        correlationId,
        SYSTEM_SCAN_SCANNER_NOT_FOUND,
        Map.of(
            "scanner_id", scannerId, "engine_kind", engineKind, "engine_version", engineVersion));
  }

  private Optional<SystemTableNode.FloeCatSystemTableNode> resolveSystemTable(
      CatalogOverlay graph, ResourceId tableId, String effectiveEngineKind) {
    if (tableId == null || tableId.getId() == null) {
      return Optional.empty();
    }
    Optional<?> resolved = graph.resolve(tableId);
    if (resolved.isPresent()) {
      return resolved
          .filter(SystemTableNode.FloeCatSystemTableNode.class::isInstance)
          .map(SystemTableNode.FloeCatSystemTableNode.class::cast);
    }

    UUID incomingUuid;
    try {
      incomingUuid = UUID.fromString(tableId.getId());
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }
    if (!SystemResourceIdGenerator.isSystemId(incomingUuid)) {
      return Optional.empty();
    }
    byte[] incoming = SystemResourceIdGenerator.bytesFromUuid(incomingUuid);
    if (EngineCatalogNames.FLOECAT_DEFAULT_CATALOG.equals(effectiveEngineKind)) {
      return Optional.empty();
    }

    return translateToDefault(graph, tableId, incoming, effectiveEngineKind);
  }

  private Optional<SystemTableNode.FloeCatSystemTableNode> translateToDefault(
      CatalogOverlay graph, ResourceId tableId, byte[] incoming, String sourceEngineKind) {
    if (EngineCatalogNames.FLOECAT_DEFAULT_CATALOG.equals(sourceEngineKind)) {
      return Optional.empty();
    }

    byte[] base =
        SystemResourceIdGenerator.xor(incoming, SystemResourceIdGenerator.mask(sourceEngineKind));
    byte[] fallbackBytes =
        SystemResourceIdGenerator.xor(
            base, SystemResourceIdGenerator.mask(EngineCatalogNames.FLOECAT_DEFAULT_CATALOG));
    UUID defaultId = SystemResourceIdGenerator.uuidFromBytes(fallbackBytes);
    ResourceId fallback =
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(tableId.getKind())
            .setId(defaultId.toString())
            .build();
    return graph
        .resolve(fallback)
        .filter(SystemTableNode.FloeCatSystemTableNode.class::isInstance)
        .map(SystemTableNode.FloeCatSystemTableNode.class::cast);
  }
}
