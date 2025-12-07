package ai.floedb.floecat.catalog.builtin;

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
    Set<String> engineKinds = new HashSet<>();

    try (var files = Files.list(builtinsDir)) {
      files
          .filter(Files::isRegularFile)
          .map(path -> path.getFileName().toString())
          .filter(name -> name.endsWith(".pb") || name.endsWith(".pbtxt"))
          .forEach(
              name -> {
                String base = name.replaceFirst("\\.(pb|pbtxt)$", "");
                engineKinds.add(base);
              });
    }

    assertThat(engineKinds).isNotEmpty();

    for (String kind : engineKinds) {
      BuiltinCatalogLoader loader = new BuiltinCatalogLoader();
      loader.configuredLocation = "file:" + builtinsDir.toAbsolutePath();
      loader.init();
      var catalog = loader.getCatalog(kind);
      var errors = BuiltinCatalogValidator.validate(catalog);
      assertThat(errors).as("catalog " + kind + " should be valid").isEmpty();
    }
  }
}
