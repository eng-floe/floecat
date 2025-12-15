package ai.floedb.floecat.systemcatalog.provider;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.systemcatalog.def.SystemObjectDef;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanner;
import java.util.List;
import java.util.Optional;

/** SPI for built-in and plugin providers of system objects scanner. */
public interface SystemObjectScannerProvider {

  /** All definitions provided by this provider (no filtering). */
  List<SystemObjectDef> definitions();

  /** Checks if this provider supports the engine kind. */
  boolean supportsEngine(String engineKind);

  /** Checks if this provider supports a given object based on a NameRef lookup. */
  boolean supports(NameRef name, String engineKind);

  /** Resolves scanner by scannerId (for SystemObjectNode lookups). */
  Optional<SystemObjectScanner> provide(String scannerId, String engineKind, String engineVersion);
}
