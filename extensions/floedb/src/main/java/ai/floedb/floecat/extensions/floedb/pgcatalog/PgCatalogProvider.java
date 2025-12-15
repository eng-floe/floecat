package ai.floedb.floecat.extensions.floedb.pgcatalog;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.metagraph.model.TableBackendKind;
import ai.floedb.floecat.systemcatalog.def.SystemObjectDef;
import ai.floedb.floecat.systemcatalog.def.SystemTableDef;
import ai.floedb.floecat.systemcatalog.provider.SystemObjectScannerProvider;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanner;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * pg_catalog system catalog provider for the FloeDB engine.
 *
 * <p>This provider exposes a minimal PostgreSQL-compatible pg_catalog schema by projecting Metacat
 * metadata into well-known pg_* tables. The intent is driver compatibility (JDBC, SQLAlchemy, BI
 * tools), not full PostgreSQL emulation.
 *
 * <p>All objects are derived at runtime via scanners; no pg_catalog objects are loaded from static
 * builtin definitions.
 */
public final class PgCatalogProvider implements SystemObjectScannerProvider {

  private static final String ENGINE_KIND = "floedb";

  private final Map<String, SystemObjectScanner> scanners =
      Map.of(
          // "pg_namespace", new PgNamespaceScanner(),
          // "pg_class", new PgClassScanner(),
          // "pg_attribute", new PgAttributeScanner(),
          // "pg_type", new PgTypeScanner(),
          "pg_proc", new PgProcScanner());

  @Override
  public List<SystemObjectDef> definitions() {
    return List.of(
        // // pg_catalog namespace
        // new SystemNamespaceDef(
        //     NameRefUtil.name("pg_catalog"),
        //     "pg_catalog",
        //     List.of()),

        // // pg_namespace
        // new SystemTableDef(
        //     NameRefUtil.name("pg_catalog", "pg_namespace"),
        //     "pg_namespace",
        //     PgNamespaceScanner.SCHEMA,
        //     TableBackendKind.FLOECAT,
        //     "pg_namespace",
        //     List.of()),

        // // pg_class
        // new SystemTableDef(
        //     NameRefUtil.name("pg_catalog", "pg_class"),
        //     "pg_class",
        //     PgClassScanner.SCHEMA,
        //     TableBackendKind.FLOECAT,
        //     "pg_class",
        //     List.of()),

        // // pg_attribute
        // new SystemTableDef(
        //     NameRefUtil.name("pg_catalog", "pg_attribute"),
        //     "pg_attribute",
        //     PgAttributeScanner.SCHEMA,
        //     TableBackendKind.FLOECAT,
        //     "pg_attribute",
        //     List.of()),

        // // pg_type
        // new SystemTableDef(
        //     NameRefUtil.name("pg_catalog", "pg_type"),
        //     "pg_type",
        //     PgTypeScanner.SCHEMA,
        //     TableBackendKind.FLOECAT,
        //     "pg_type",
        //     List.of()),

        // pg_proc
        new SystemTableDef(
            NameRefUtil.name("pg_catalog", "pg_proc"),
            "pg_proc",
            PgProcScanner.SCHEMA,
            TableBackendKind.FLOECAT,
            "pg_proc",
            List.of()));
  }

  @Override
  public boolean supportsEngine(String engineKind) {
    return ENGINE_KIND.equalsIgnoreCase(engineKind);
  }

  @Override
  public boolean supports(NameRef name, String engineKind) {
    if (!supportsEngine(engineKind) || name == null) {
      return false;
    }

    // Objects must live directly under pg_catalog
    if (name.getPathCount() != 1) {
      return false;
    }

    return "pg_catalog".equalsIgnoreCase(name.getPath(0));
  }

  @Override
  public Optional<SystemObjectScanner> provide(
      String scannerId, String engineKind, String engineVersion) {

    if (!supportsEngine(engineKind) || scannerId == null) {
      return Optional.empty();
    }

    return Optional.ofNullable(scanners.get(scannerId.toLowerCase()));
  }
}
