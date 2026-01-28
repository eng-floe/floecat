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
import ai.floedb.floecat.service.context.EngineContextProvider;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import ai.floedb.floecat.systemcatalog.provider.SystemObjectScannerProvider;
import ai.floedb.floecat.systemcatalog.spi.scanner.CatalogOverlay;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanner;
import ai.floedb.floecat.systemcatalog.util.EngineCatalogNames;
import ai.floedb.floecat.systemcatalog.util.EngineContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@ApplicationScoped
public final class SystemScannerResolver {

  @Inject CatalogOverlay graph;
  @Inject EngineContextProvider engine;
  @Inject List<SystemObjectScannerProvider> providers;

  public SystemObjectScanner resolve(String correlationId, ResourceId tableId) {

    Optional<SystemTableNode.FloeCatSystemTableNode> nodeOptional =
        resolveSystemTable(graph, tableId);
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

    EngineContext ctx = engine.engineContext();
    String engineKind = ctx.normalizedKind();
    String engineVersion = ctx.normalizedVersion();

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
      CatalogOverlay graph, ResourceId tableId) {
    Optional<SystemTableNode.FloeCatSystemTableNode> node =
        graph
            .resolve(tableId)
            .filter(SystemTableNode.FloeCatSystemTableNode.class::isInstance)
            .map(SystemTableNode.FloeCatSystemTableNode.class::cast);
    if (node.isPresent() || tableId == null || tableId.getId() == null) {
      return node;
    }
    String idPart = tableId.getId();
    int colon = idPart.indexOf(':');
    String suffix = colon < 0 ? idPart : idPart.substring(colon + 1);
    if (suffix.isBlank()) {
      return node;
    }
    ResourceId fallback =
        ResourceId.newBuilder()
            .setAccountId(tableId.getAccountId())
            .setKind(tableId.getKind())
            .setId(EngineCatalogNames.FLOECAT_DEFAULT_CATALOG + ":" + suffix)
            .build();
    return graph
        .resolve(fallback)
        .filter(SystemTableNode.FloeCatSystemTableNode.class::isInstance)
        .map(SystemTableNode.FloeCatSystemTableNode.class::cast);
  }
}
