package ai.floedb.floecat.systemcatalog.informationschema;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.metagraph.model.TableBackendKind;
import ai.floedb.floecat.systemcatalog.def.SystemNamespaceDef;
import ai.floedb.floecat.systemcatalog.def.SystemObjectDef;
import ai.floedb.floecat.systemcatalog.def.SystemTableDef;
import ai.floedb.floecat.systemcatalog.provider.SystemObjectScannerProvider;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanner;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Baseline system catalog provider for the SQL information_schema.
 *
 * <p>This provider is engine-agnostic and is always loaded by the system catalog loader unless
 * explicitly overridden by another provider defining the same canonical object names.
 */
public final class InformationSchemaProvider implements SystemObjectScannerProvider {

  private final Map<String, SystemObjectScanner> scanners =
      Map.of(
          "schemata_scanner", new SchemataScanner(),
          "tables_scanner", new TablesScanner(),
          "columns_scanner", new ColumnsScanner());

  @Override
  public List<SystemObjectDef> definitions() {
    return List.of(
        new SystemNamespaceDef(
            NameRefUtil.name("information_schema"), "information_schema", List.of()),
        new SystemTableDef(
            NameRefUtil.name("information_schema", "tables"),
            "tables",
            TablesScanner.SCHEMA,
            TableBackendKind.FLOECAT,
            "tables_scanner",
            List.of()),
        new SystemTableDef(
            NameRefUtil.name("information_schema", "columns"),
            "columns",
            ColumnsScanner.SCHEMA,
            TableBackendKind.FLOECAT,
            "columns_scanner",
            List.of()),
        new SystemTableDef(
            NameRefUtil.name("information_schema", "schemata"),
            "schemata",
            SchemataScanner.SCHEMA,
            TableBackendKind.FLOECAT,
            "schemata_scanner",
            List.of()));
  }

  @Override
  public boolean supportsEngine(String engineKind) {
    return true;
  }

  @Override
  public boolean supports(NameRef name, String engineKind) {
    if (name == null) {
      return false;
    }

    // Schema is "information_schema" and object name matches one of our keys
    if (name.getPathCount() != 1 || !"information_schema".equalsIgnoreCase(name.getPath(0))) {
      return false;
    }
    return scanners.containsKey(name.getName().toLowerCase() + "_scanner");
  }

  @Override
  public Optional<SystemObjectScanner> provide(
      String scannerId, String engineKind, String engineVersion) {
    if (scannerId == null) return Optional.empty();
    return Optional.ofNullable(scanners.get(scannerId.toLowerCase()));
  }
}
