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

package ai.floedb.floecat.service.repo.cache;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/** Holds read consistency at the injected store view instead of at individual call sites. */
class ConsistentReadRulesTest {
  private static final List<String> CONSISTENCY_METHODS =
      List.of(
          ".getConsistent(",
          ".getBatchConsistent(",
          ".listPointersByPrefixConsistent(",
          ".countByPrefixConsistent(");

  @Test
  void serviceCodeSelectsConsistencyByStoreViewNotByMethodCall() throws IOException {
    Path root = Path.of("src/main/java/ai/floedb/floecat/service").toAbsolutePath();
    List<Path> scanned = new ArrayList<>();
    List<String> offenders = new ArrayList<>();
    try (Stream<Path> files = Files.walk(root)) {
      for (Path file : files.filter(path -> path.toString().endsWith(".java")).toList()) {
        scanned.add(file);
        String source = Files.readString(file).replaceAll("\\s+", "");
        if (!source.contains("implementsPointerStore")
            && CONSISTENCY_METHODS.stream().anyMatch(source::contains)) {
          offenders.add(root.relativize(file).toString());
        }
      }
    }

    assertThat(scanned).as("no service sources scanned under %s", root).isNotEmpty();
    assertThat(offenders)
        .as(
            "inject the authoritative PointerStore (the safe default) or @CachedPointerStore once;"
                + " do not choose consistency at every read")
        .isEmpty();
  }
}
