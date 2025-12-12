package ai.floedb.floecat.systemcatalog.informationschema;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.def.SystemObjectDef;
import ai.floedb.floecat.systemcatalog.def.SystemTableDef;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanner;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class InformationSchemaProviderTest {

  private final InformationSchemaProvider provider = new InformationSchemaProvider();

  // ------------------------------------------------------------------------
  // definitions() correctness
  // ------------------------------------------------------------------------
  @Test
  void definitions_containsAllExpectedObjects() {
    List<SystemObjectDef> defs = provider.definitions();

    assertThat(defs)
        .hasSize(4)
        .extracting(SystemObjectDef::name)
        .map(NameRefUtil::canonical)
        .containsExactlyInAnyOrder(
            "information_schema",
            "information_schema.tables",
            "information_schema.columns",
            "information_schema.schemata");
  }

  @Test
  void definitions_haveCorrectSchemas() {
    var defs = provider.definitions();

    SystemTableDef tables =
        defs.stream()
            .filter(SystemTableDef.class::isInstance)
            .map(SystemTableDef.class::cast)
            .filter(d -> NameRefUtil.canonical(d.name()).equals("information_schema.tables"))
            .findFirst()
            .orElseThrow();

    List<SchemaColumn> cols = tables.columns();
    assertThat(cols).hasSize(TablesScanner.SCHEMA.size());
    assertThat(cols.get(0).getName()).isEqualTo("table_schema");
  }

  @Test
  void definitions_haveCorrectRowGeneratorIds() {
    var defs = provider.definitions();

    List<String> scannerIds =
        defs.stream()
            .filter(SystemTableDef.class::isInstance)
            .map(SystemTableDef.class::cast)
            .map(SystemTableDef::scannerId)
            .toList();
    assertThat(scannerIds)
        .containsExactlyInAnyOrder("tables_scanner", "columns_scanner", "schemata_scanner");
  }

  // ------------------------------------------------------------------------
  // supports(NameRef) logic
  // ------------------------------------------------------------------------
  @Test
  void supports_recognizesInformationSchemaTables() {
    NameRef ref = NameRefUtil.name("information_schema", "tables");

    assertThat(provider.supports(ref, "spark")).isTrue();
  }

  @Test
  void supports_rejectsWrongSchema() {
    NameRef ref = NameRefUtil.name("not_schema", "tables");
    assertThat(provider.supports(ref, "spark")).isFalse();
  }

  @Test
  void supports_rejectsUnknownObject() {
    NameRef ref = NameRefUtil.name("information_schema", "unknown");
    assertThat(provider.supports(ref, "spark")).isFalse();
  }

  @Test
  void supports_isCaseInsensitive() {
    NameRef ref = NameRefUtil.name("InFoRmAtIoN_sChEmA", "TaBlEs");
    assertThat(provider.supports(ref, "spark")).isTrue();
  }

  // ------------------------------------------------------------------------
  // provide(ScannerId) logic
  // ------------------------------------------------------------------------
  @Test
  void provide_returnsCorrectScannerForTables() {
    Optional<SystemObjectScanner> scanner = provider.provide("tables", "spark", "3.5.0");

    assertThat(scanner).isPresent();
    assertThat(scanner.get()).isInstanceOf(TablesScanner.class);
  }

  @Test
  void provide_returnsCorrectScannerForColumns() {
    Optional<SystemObjectScanner> scanner = provider.provide("columns", "spark", "3.5.0");

    assertThat(scanner).isPresent();
    assertThat(scanner.get()).isInstanceOf(ColumnsScanner.class);
  }

  @Test
  void provide_returnsCorrectScannerForSchemata() {
    Optional<SystemObjectScanner> scanner = provider.provide("schemata", "spark", "3.5.0");

    assertThat(scanner).isPresent();
    assertThat(scanner.get()).isInstanceOf(SchemataScanner.class);
  }

  @Test
  void provide_returnsEmptyForUnknownObject() {
    Optional<SystemObjectScanner> scanner = provider.provide("nope", "spark", "3.5.0");

    assertThat(scanner).isEmpty();
  }

  // ------------------------------------------------------------------------
  // Provider should not care about engine kind/version
  // ------------------------------------------------------------------------
  @Test
  void providerIgnoresEngineKindAndVersion() {
    NameRef ref = NameRefUtil.name("information_schema", "tables");

    assertThat(provider.supports(ref, "duckdb")).isTrue();
    assertThat(provider.provide("tables", "trino", "450")).isPresent();
  }
}
