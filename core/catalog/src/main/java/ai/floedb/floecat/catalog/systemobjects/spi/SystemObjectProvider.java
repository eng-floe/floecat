package ai.floedb.floecat.catalog.systemobjects.spi;

import ai.floedb.floecat.catalog.systemobjects.registry.SystemObjectDefinition;
import ai.floedb.floecat.common.rpc.NameRef;
import java.util.List;
import java.util.Optional;

/** SPI for built-in and plugin providers of system objects. */
public interface SystemObjectProvider {

  /** All definitions provided by this provider (no filtering). */
  List<SystemObjectDefinition> definitions();

  /** Checks if this provider supports the engine version. */
  default boolean supportsEngine(String engineKind, String version) {
    return true;
  }

  /** Checks if this provider supports NameRef-based lookup. */
  boolean supports(NameRef name, String engineKind, String engineVersion);

  /** Resolves scanner by scannerId (for SystemObjectNode lookups). */
  default Optional<SystemObjectScanner> provide(
      String scannerId, String engineKind, String engineVersion) {
    return Optional.empty();
  }
}
