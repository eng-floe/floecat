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
import ai.floedb.floecat.scanner.spi.CatalogGraphView;
import ai.floedb.floecat.scanner.spi.SystemObjectScanner;
import ai.floedb.floecat.scanner.utils.CatalogContext;
import ai.floedb.floecat.service.context.EngineContextProvider;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import ai.floedb.floecat.systemcatalog.informationschema.InformationSchemaProvider;
import ai.floedb.floecat.systemcatalog.provider.CatalogEnvironmentProvider;
import ai.floedb.floecat.systemcatalog.provider.SystemObjectScannerProvider;
import ai.floedb.floecat.telemetry.PhaseDiagnostics;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@ApplicationScoped
public final class SystemScannerResolver {

  @Inject CatalogGraphView graph;
  @Inject EngineContextProvider engine;
  @Inject List<SystemObjectScannerProvider> providers;
  @Inject List<CatalogEnvironmentProvider> environmentProviders;

  private final InformationSchemaProvider sharedInformationSchema = new InformationSchemaProvider();

  /**
   * Resolves the scanner for the given table ID, reading the complete catalog context from the
   * current gRPC call's thread-local context via {@link EngineContextProvider}.
   *
   * <p>Use this overload from gRPC service implementations where the engine context is already
   * propagated by {@code InboundContextInterceptor}.
   */
  public SystemObjectScanner resolve(String correlationId, ResourceId tableId) {
    return resolve(correlationId, tableId, engine.catalogContext(), PhaseDiagnostics.NOOP);
  }

  public SystemObjectScanner resolve(
      String correlationId, ResourceId tableId, CatalogContext ctx, PhaseDiagnostics diagnostics) {
    PhaseDiagnostics safeDiagnostics = diagnostics == null ? PhaseDiagnostics.NOOP : diagnostics;
    CatalogContext context = Objects.requireNonNull(ctx, "catalogContext");
    String engineKind = context.engine().hasEngineKind() ? context.engine().normalizedKind() : "";
    String engineVersion = context.engine().normalizedVersion();
    safeDiagnostics.put("system_scanner_engine_kind", engineKind);
    safeDiagnostics.put("system_scanner_engine_version", engineVersion);
    safeDiagnostics.put("system_scanner_table_id", tableId == null ? "" : tableId.getId());

    Optional<SystemTableNode.FloeCatSystemTableNode> nodeOptional =
        safeDiagnostics.time(
            "system_scanner_graph_resolve", () -> resolveSystemTable(graph, tableId, context));
    var node =
        nodeOptional.orElseThrow(
            () ->
                GrpcErrors.invalidArgument(
                    correlationId,
                    SYSTEM_SCAN_NOT_SYSTEM_TABLE,
                    Map.of("table_id", tableId.getId())));

    String scannerId = node.scannerId();
    safeDiagnostics.put("system_scanner_id", scannerId);
    if (scannerId == null || scannerId.isBlank()) {
      throw GrpcErrors.internal(
          correlationId, SYSTEM_SCAN_MISSING_SCANNER, Map.of("table_id", tableId.getId()));
    }

    Optional<SystemObjectScanner> scanner =
        context.environment().hasEnvironmentKind()
            ? findEnvironmentScanner(scannerId, context, safeDiagnostics)
            : findEngineScanner(scannerId, context, safeDiagnostics);
    if (scanner.isPresent()) {
      return scanner.get();
    }

    if (context.environment().hasEnvironmentKind()) {
      scanner =
          safeDiagnostics.time(
              "shared_information_schema_scanner",
              () -> sharedInformationSchema.provide(scannerId, context));
      if (scanner.isPresent()) {
        safeDiagnostics.count("system_scanner_provider_matches");
        return scanner.get();
      }
    }

    throw GrpcErrors.notFound(
        correlationId,
        SYSTEM_SCAN_SCANNER_NOT_FOUND,
        Map.of(
            "scanner_id", scannerId, "engine_kind", engineKind, "engine_version", engineVersion));
  }

  private Optional<SystemObjectScanner> findEngineScanner(
      String scannerId, CatalogContext context, PhaseDiagnostics diagnostics) {
    for (var provider : providers) {
      diagnostics.count("system_scanner_provider_checks");
      var scanner =
          diagnostics.time(
              "system_scanner_provider_provide", () -> provide(provider, scannerId, context));
      if (scanner.isPresent()) {
        diagnostics.count("system_scanner_provider_matches");
        return scanner;
      }
    }
    return Optional.empty();
  }

  private Optional<SystemObjectScanner> findEnvironmentScanner(
      String scannerId, CatalogContext context, PhaseDiagnostics diagnostics) {
    for (var provider : environmentProviders) {
      diagnostics.count("system_scanner_provider_checks");
      var scanner =
          diagnostics.time(
              "system_scanner_provider_provide", () -> provide(provider, scannerId, context));
      if (scanner.isPresent()) {
        diagnostics.count("system_scanner_provider_matches");
        return scanner;
      }
    }
    return Optional.empty();
  }

  private Optional<SystemTableNode.FloeCatSystemTableNode> resolveSystemTable(
      CatalogGraphView graph, ResourceId tableId, CatalogContext context) {
    if (tableId == null || tableId.getId() == null) {
      return Optional.empty();
    }
    Optional<?> resolved = graph.resolve(tableId, context);
    if (resolved.isPresent()) {
      return resolved
          .filter(SystemTableNode.FloeCatSystemTableNode.class::isInstance)
          .map(SystemTableNode.FloeCatSystemTableNode.class::cast);
    }

    return Optional.empty();
  }

  private static Optional<SystemObjectScanner> provide(
      SystemObjectScannerProvider provider, String scannerId, CatalogContext context) {
    if (!provider.supports(context)) {
      return Optional.empty();
    }
    return provider.provide(scannerId, context);
  }

  private static Optional<SystemObjectScanner> provide(
      CatalogEnvironmentProvider provider, String scannerId, CatalogContext context) {
    if (!provider.supportsEnvironment(context.environment())) {
      return Optional.empty();
    }
    return provider.provide(scannerId, context);
  }
}
