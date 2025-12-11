package ai.floedb.floecat.catalog.systemobjects.informationschema;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.common.util.NameRefUtil;
import ai.floedb.floecat.catalog.systemobjects.registry.SystemObjectDefinition;
import ai.floedb.floecat.catalog.systemobjects.spi.SystemObjectColumnSet;
import ai.floedb.floecat.catalog.systemobjects.spi.SystemObjectScanner;
import ai.floedb.floecat.common.rpc.NameRef;
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
    List<SystemObjectDefinition> defs = provider.definitions();

    assertThat(defs)
        .hasSize(3)
        .extracting(SystemObjectDefinition::canonicalName)
        .containsExactlyInAnyOrder(
            "information_schema.tables",
            "information_schema.columns",
            "information_schema.schemata");
  }

  @Test
  void definitions_haveCorrectSchemas() {
    var defs = provider.definitions();

    SystemObjectDefinition tables =
        defs.stream()
            .filter(d -> d.canonicalName().equals("information_schema.tables"))
            .findFirst()
            .orElseThrow();

    SystemObjectColumnSet cols = tables.columns();
    assertThat(cols.size()).isEqualTo(TablesScanner.SCHEMA.length);
    assertThat(cols.column(0).getName()).isEqualTo("table_schema");
  }

  @Test
  void definitions_haveCorrectRowGeneratorIds() {
    var defs = provider.definitions();

    assertThat(defs)
        .extracting(SystemObjectDefinition::scannerId)
        .containsExactlyInAnyOrder("tables", "columns", "schemata");
  }

  // ------------------------------------------------------------------------
  // supports(NameRef) logic
  // ------------------------------------------------------------------------
  @Test
  void supports_recognizesInformationSchemaTables() {
    NameRef ref = NameRefUtil.name("information_schema", "tables");

    assertThat(provider.supports(ref, "spark", "3.5.0")).isTrue();
  }

  @Test
  void supports_rejectsWrongSchema() {
    NameRef ref = NameRefUtil.name("not_schema", "tables");
    assertThat(provider.supports(ref, "spark", "3.5.0")).isFalse();
  }

  @Test
  void supports_rejectsUnknownObject() {
    NameRef ref = NameRefUtil.name("information_schema", "unknown");
    assertThat(provider.supports(ref, "spark", "3.5.0")).isFalse();
  }

  @Test
  void supports_isCaseInsensitive() {
    NameRef ref = NameRefUtil.name("InFoRmAtIoN_sChEmA", "TaBlEs");
    assertThat(provider.supports(ref, "spark", "3.5.0")).isTrue();
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

    assertThat(provider.supports(ref, "duckdb", "0.9.0")).isTrue();
    assertThat(provider.provide("tables", "trino", "450")).isPresent();
  }
}
