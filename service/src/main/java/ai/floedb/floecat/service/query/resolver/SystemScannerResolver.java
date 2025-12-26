package ai.floedb.floecat.service.query.resolver;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.context.EngineContextProvider;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import ai.floedb.floecat.systemcatalog.provider.SystemObjectScannerProvider;
import ai.floedb.floecat.systemcatalog.spi.scanner.CatalogOverlay;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanner;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public final class SystemScannerResolver {

  @Inject CatalogOverlay graph;
  @Inject EngineContextProvider engine;
  @Inject List<SystemObjectScannerProvider> providers;

  public SystemObjectScanner resolve(String correlationId, ResourceId tableId) {

    var node =
        graph
            .resolve(tableId)
            .filter(SystemTableNode.class::isInstance)
            .map(SystemTableNode.class::cast)
            .orElseThrow(
                () ->
                    GrpcErrors.invalidArgument(
                        correlationId,
                        "system.scan.not_system_table",
                        Map.of("table_id", tableId.getId())));

    String scannerId = node.scannerId();
    if (scannerId == null || scannerId.isBlank()) {
      throw GrpcErrors.internal(
          correlationId, "system.scan.missing_scanner", Map.of("table_id", tableId.getId()));
    }

    String engineKind = engine.engineKind();
    String engineVersion = engine.engineVersion();

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
        "system.scan.scanner_not_found",
        Map.of(
            "scanner_id", scannerId,
            "engine_kind", engineKind,
            "engine_version", engineVersion));
  }
}
