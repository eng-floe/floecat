package ai.floedb.floecat.gateway.iceberg.rest.common;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestS3FixturesTest {

  @Test
  void seedFixturesOnceRestoresMissingMetadata() throws IOException {
    TestS3Fixtures.seedFixtures();
    Path missingFile =
        TestS3Fixtures.prefixPath()
            .resolve(
                Path.of(
                    "metadata",
                    "00002-503f4508-3824-4cb6-bdf1-4bd6bf5a0ade.metadata.json"));
    Assertions.assertTrue(
        Files.exists(missingFile), "Expected fixture metadata to exist after seedFixtures()");

    Files.deleteIfExists(missingFile);
    Assertions.assertFalse(
        Files.exists(missingFile), "Simulated drift by removing fixture metadata");

    TestS3Fixtures.seedFixturesOnce();

    Assertions.assertTrue(
        Files.exists(missingFile),
        "seedFixturesOnce should detect drift and restore missing metadata");
  }
}
