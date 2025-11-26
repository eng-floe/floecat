package ai.floedb.metacat.service.catalog.builtin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class BuiltinCatalogLoaderTest {

  @Test
  void loadsCatalogFromClasspath() {
    BuiltinCatalogLoader loader = new BuiltinCatalogLoader();
    loader.configuredLocation = BuiltinCatalogLoader.DEFAULT_LOCATION;
    loader.init();

    var catalog = loader.getCatalog("demo-pg-builtins");
    assertThat(catalog.version()).isEqualTo("demo-pg-builtins");
    assertThat(catalog.functions()).isNotEmpty();
    assertThat(loader.getCatalog("demo-pg-builtins")).isSameAs(catalog);
  }

  @Test
  void throwsWhenMissing() {
    BuiltinCatalogLoader loader = new BuiltinCatalogLoader();
    loader.configuredLocation = BuiltinCatalogLoader.DEFAULT_LOCATION;
    loader.init();

    assertThatThrownBy(() -> loader.getCatalog("missing"))
        .isInstanceOf(BuiltinCatalogNotFoundException.class);
  }
}
