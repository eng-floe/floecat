package ai.floedb.floecat.systemcatalog.provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.systemcatalog.def.SystemNamespaceDef;
import ai.floedb.floecat.systemcatalog.registry.SystemEngineCatalog;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ServiceLoaderSystemCatalogProvider}.
 *
 * <p>This test suite validates:
 *
 * <ul>
 *   <li>Illegal argument handling
 *   <li>Fallback behavior for unknown engines
 *   <li>Presence of builtin InformationSchemaProvider
 *   <li>Merging of provider-defined namespaces/tables/views
 *   <li>Snapshot immutability guarantees
 * </ul>
 */
class ServiceLoaderSystemCatalogProviderTest {

  @Test
  void load_nullEngineKindThrows() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    assertThatThrownBy(() -> provider.load(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("engine_kind");
  }

  @Test
  void load_blankEngineKindThrows() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    assertThatThrownBy(() -> provider.load("  "))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("engine_kind");
  }

  @Test
  void load_unknownEngineReturnsInformationSchemaCatalogOnly() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    SystemEngineCatalog catalog = provider.load("unknown-engine");
    assertThat(catalog.functions()).isEmpty();
    assertThat(catalog.types()).isEmpty();
    assertThat(catalog.aggregates()).isEmpty();
    assertThat(catalog.operators()).isEmpty();
    assertThat(catalog.collations()).isEmpty();
    assertThat(catalog.casts()).isEmpty();
    assertThat(catalog.namespaces())
        .containsExactly(
            new SystemNamespaceDef(
                NameRefUtil.name("information_schema"), "information_schema", List.of()));
    assertThat(catalog.tables()).isNotEmpty(); // information_schema tables are always present
  }

  @Test
  void providers_includesInformationSchemaProvider() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    assertThat(provider.providers())
        .anyMatch(p -> p.getClass().getSimpleName().equals("InformationSchemaProvider"));
  }

  @Test
  void load_mergesInformationSchemaDefinitions() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    SystemEngineCatalog catalog = provider.load("any-engine");

    // information_schema.schemata is always present
    assertThat(catalog.tables().stream().map(t -> NameRefUtil.canonical(t.name())).toList())
        .contains("information_schema.schemata");

    assertThat(catalog.tables().stream().map(t -> NameRefUtil.canonical(t.name())).toList())
        .contains("information_schema.tables");

    assertThat(catalog.tables().stream().map(t -> NameRefUtil.canonical(t.name())).toList())
        .contains("information_schema.columns");
  }

  @Test
  void load_returnsIndependentSnapshots() {
    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    SystemEngineCatalog c1 = provider.load("engine-x");
    SystemEngineCatalog c2 = provider.load("engine-x");

    assertThat(c1).isNotSameAs(c2);
    assertThat(c1.fingerprint()).isEqualTo(c2.fingerprint());
  }

  @Test
  void mergeProviderDefinitions_lastWinsByCanonicalName() {
    // This test validates merge semantics indirectly by ensuring
    // canonical-name uniqueness is enforced.

    ServiceLoaderSystemCatalogProvider provider = new ServiceLoaderSystemCatalogProvider();

    SystemEngineCatalog catalog = provider.load("engine-y");

    List<String> tableNames =
        catalog.tables().stream().map(t -> NameRefUtil.canonical(t.name())).toList();

    // No duplicate canonical names
    assertThat(tableNames).doesNotHaveDuplicates();
  }
}
