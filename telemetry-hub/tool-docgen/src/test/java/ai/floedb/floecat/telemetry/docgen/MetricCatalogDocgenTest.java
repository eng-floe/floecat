/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import org.junit.jupiter.api.io.TempDir;

/**
 * Covers the generator, not the checked-in contract. Generation runs at process-classes, so by the
 * time a test here reads docs/telemetry the build has already refreshed it. CI compares the tree
 * against git after the build to catch a contract that was regenerated but not committed.
 */
class MetricCatalogDocgenTest {
  @Test
  void writtenDocsRoundTripThroughTheGenerator(@TempDir Path tempRepo) throws IOException {
    System.setProperty(
        "telemetry.doc.dir", tempRepo.resolve("docs").resolve("telemetry").toString());

    Path markdown = MetricCatalogDocgen.docDir().resolve("contract.md");
    Path json = MetricCatalogDocgen.docDir().resolve("contract.json");

    MetricCatalogDocgen.main(new String[0]);

    TelemetryRegistry registry = Telemetry.newRegistryWithCore();
    Map<String, MetricDef> catalog = Telemetry.metricCatalog(registry);

    String actualMd = Files.readString(markdown);
    String expectedMd =
        MetricCatalogDocgen.injectGeneratedRows(
            actualMd, MetricCatalogDocgen.buildTableSection(catalog));

    Assertions.assertEquals(
        expectedMd,
        actualMd,
        "Markdown the generator wrote does not round-trip through its own row injection.");

    String expectedJson = MetricCatalogDocgen.buildJson(catalog);
    String actualJson = Files.readString(json);

    Assertions.assertEquals(
        expectedJson,
        actualJson,
        "JSON the generator wrote does not match the catalogue it was built from.");
  }
}
