package ai.floedb.metacat.catalog.builtin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BuiltinDefinitionRegistryTest {

  private BuiltinDefinitionRegistry registry;

  @BeforeEach
  void setUp() {
    BuiltinCatalogLoader loader = new BuiltinCatalogLoader();
    loader.configuredLocation = BuiltinCatalogLoader.DEFAULT_LOCATION;
    loader.init();
    registry = new BuiltinDefinitionRegistry(loader);
  }

  @Test
  void loadsCatalogAndCachesLookups() {
    var catalog = registry.catalog("floe-demo");

    assertThat(catalog.engineKind()).isEqualTo("floe-demo");
    assertThat(catalog.fingerprint()).isNotBlank();
    assertThat(catalog.functions())
        .extracting(BuiltinFunctionDef::name)
        .contains("pg_catalog.int4_add");
    assertThat(catalog.functions("pg_catalog.int4_add")).isNotEmpty();
    assertThat(catalog.operator("+")).isPresent();
    assertThat(catalog.type("pg_catalog.int4")).isPresent();
    assertThat(catalog.cast("pg_catalog.text", "pg_catalog.int4")).isPresent();
    assertThat(catalog.collation("pg_catalog.default")).isPresent();
    assertThat(catalog.aggregate("pg_catalog.sum")).isPresent();

    // Second call returns cached instance
    assertThat(registry.catalog("floe-demo")).isSameAs(catalog);
  }

  @Test
  void rejectsBlankVersion() {
    assertThatThrownBy(() -> registry.catalog(" ")).isInstanceOf(IllegalArgumentException.class);
  }
}
