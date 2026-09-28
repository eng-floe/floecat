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

package ai.floedb.floecat.service.gc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * GC collects only accounts this replica owns, so it reads the account directory through {@link
 * OwnedAccounts} alone. Checked on the sources because ArchUnit imports nothing on this JDK (see
 * {@code DeploymentIndependenceArchTest}).
 */
class GcOwnershipArchTest {

  private static final Path GC_SOURCES = Path.of("src/main/java/ai/floedb/floecat/service/gc");

  @Test
  void gcReadsAccountsOnlyThroughOwnedAccounts() throws IOException {
    List<Path> sources;
    try (Stream<Path> files = Files.list(GC_SOURCES)) {
      sources = files.filter(path -> path.toString().endsWith(".java")).toList();
    }
    assertTrue(
        sources.stream().anyMatch(path -> path.endsWith("CasBlobGcScheduler.java")),
        "gc sources not scanned");
    List<String> offenders =
        sources.stream()
            .filter(path -> !path.endsWith("OwnedAccounts.java"))
            .filter(GcOwnershipArchTest::mentionsAccountRepository)
            .map(path -> path.getFileName().toString())
            .toList();
    assertEquals(List.of(), offenders, "GC must list accounts through OwnedAccounts");
  }

  private static boolean mentionsAccountRepository(Path path) {
    try {
      return Files.readString(path).contains("AccountRepository");
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
