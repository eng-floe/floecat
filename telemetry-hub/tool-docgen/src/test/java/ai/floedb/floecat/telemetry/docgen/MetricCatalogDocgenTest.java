package ai.floedb.floecat.telemetry.docgen;

import ai.floedb.floecat.telemetry.MetricDef;
import ai.floedb.floecat.telemetry.Telemetry;
import ai.floedb.floecat.telemetry.TelemetryRegistry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class MetricCatalogDocgenTest {
  private static final Path DOC_DIR = MetricCatalogDocgen.docDir();

  @Test
  void generatedDocsMatchOnDisk() throws IOException {
    Path markdown = DOC_DIR.resolve("contract.md");
    Path json = DOC_DIR.resolve("contract.json");

    TelemetryRegistry registry = Telemetry.newRegistryWithCore();
    Map<String, MetricDef> catalog = Telemetry.metricCatalog(registry);

    String actualMd = Files.readString(markdown);
    String expectedMd =
        MetricCatalogDocgen.injectGeneratedRows(
            actualMd, MetricCatalogDocgen.buildTableSection(catalog));

    Assertions.assertEquals(
        expectedMd,
        actualMd,
        "Markdown contract out of date; run `mvn -pl telemetry-hub/tool-docgen -am process-classes`");

    String expectedJson = MetricCatalogDocgen.buildJson(catalog);
    String actualJson = Files.readString(json);

    Assertions.assertEquals(
        expectedJson,
        actualJson,
        "JSON contract out of date; run `mvn -pl telemetry-hub/tool-docgen -am process-classes`");
  }
}
