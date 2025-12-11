package ai.floedb.floecat.extensions.floedb.system_objects;

import ai.floedb.floecat.catalog.system_objects.registry.SystemObjectDefinition;
import ai.floedb.floecat.catalog.system_objects.spi.EngineSystemTablesExtension;
import ai.floedb.floecat.catalog.system_objects.spi.SystemObjectScanner;
import ai.floedb.floecat.common.rpc.NameRef;
import java.util.List;
import java.util.Optional;

/** FloeDB extension stub for future engine-specific system tables. */
public final class FloedbSystemTablesExtension implements EngineSystemTablesExtension {

  @Override
  public List<SystemObjectDefinition> definitions() {
    return List.of();
  }

  @Override
  public boolean supports(NameRef name, String engineKind, String engineVersion) {
    // No custom tables yet.
    return false;
  }

  @Override
  public Optional<SystemObjectScanner> provide(
      String scannerId, String engineKind, String engineVersion) {
    return Optional.empty();
  }
}
