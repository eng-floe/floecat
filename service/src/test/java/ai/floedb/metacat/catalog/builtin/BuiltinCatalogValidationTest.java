package ai.floedb.metacat.catalog.builtin;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Validates every builtin catalog shipped under src/main/resources/builtins. */
class BuiltinCatalogValidationTest {

  @Test
  void allBundledCatalogsValidate() throws IOException {
    Path builtinsDir = Path.of("src/main/resources/builtins");
    Set<String> engineVersions = new HashSet<>();

    try (var files = Files.list(builtinsDir)) {
      files
          .filter(Files::isRegularFile)
          .map(path -> path.getFileName().toString())
          .filter(name -> name.startsWith("builtin_catalog_"))
          .forEach(
              name -> {
                String version =
                    name.replaceFirst("builtin_catalog_", "").replaceFirst("\\.(pb|pbtxt)$", "");
                engineVersions.add(version);
              });
    }

    assertThat(engineVersions).isNotEmpty();

    for (String version : engineVersions) {
      BuiltinCatalogLoader loader = new BuiltinCatalogLoader();
      loader.configuredLocation = "file:" + builtinsDir.toAbsolutePath();
      loader.init();
      var catalog = loader.getCatalog(version);
      var errors = BuiltinCatalogValidator.validate(catalog);
      assertThat(errors).as("catalog " + version + " should be valid").isEmpty();
    }
  }
}
