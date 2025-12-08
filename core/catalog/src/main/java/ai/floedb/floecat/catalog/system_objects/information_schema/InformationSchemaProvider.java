package ai.floedb.floecat.catalog.system_objects.information_schema;

import ai.floedb.floecat.catalog.common.util.NameRefUtil;
import ai.floedb.floecat.catalog.system_objects.registry.SystemObjectDefinition;
import ai.floedb.floecat.catalog.system_objects.spi.*;
import ai.floedb.floecat.common.rpc.NameRef;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Registers all builtin information_schema.* system objects.
 *
 * <p>Plugins may override ANY table by returning a matching definition first.
 */
public final class InformationSchemaProvider implements SystemObjectProvider {

  private final Map<String, SystemObjectScanner> scanners =
      Map.of(
          "schemata", new SchemataScanner(),
          "tables", new TablesScanner(),
          "columns", new ColumnsScanner());

  @Override
  public List<SystemObjectDefinition> definitions() {
    return List.of(
        new SystemObjectDefinition(
            NameRefUtil.name("information_schema", "tables"),
            new SystemObjectColumnSet(TablesScanner.SCHEMA),
            "tables",
            List.of()),
        new SystemObjectDefinition(
            NameRefUtil.name("information_schema", "columns"),
            new SystemObjectColumnSet(ColumnsScanner.SCHEMA),
            "columns",
            List.of()),
        new SystemObjectDefinition(
            NameRefUtil.name("information_schema", "schemata"),
            new SystemObjectColumnSet(SchemataScanner.SCHEMA),
            "schemata",
            List.of()));
  }

  @Override
  public boolean supports(NameRef name, String engineKind, String engineVersion) {
    if (name == null) {
      return false;
    }

    // Schema is "information_schema" and object name matches one of our keys
    if (name.getPathCount() != 1 || !"information_schema".equalsIgnoreCase(name.getPath(0))) {
      return false;
    }
    return scanners.containsKey(name.getName().toLowerCase());
  }

  @Override
  public Optional<SystemObjectScanner> provide(
      String scannerId, String engineKind, String engineVersion) {
    if (scannerId == null) return Optional.empty();
    return Optional.ofNullable(scanners.get(scannerId.toLowerCase()));
  }
}
