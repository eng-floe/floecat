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
  private static final Path SERVICE_SOURCES =
      Path.of("src/main/java/ai/floedb/floecat/service").toAbsolutePath();
  private static final List<String> CONSISTENCY_METHODS =
      List.of(
          ".getConsistent(",
          ".getBatchConsistent(",
          ".listPointersByPrefixConsistent(",
          ".countByPrefixConsistent(");

  @Test
  void serviceCodeSelectsConsistencyByStoreViewNotByMethodCall() throws IOException {
    List<String> offenders = new ArrayList<>();
    List<Path> scanned = serviceSources();
    for (Path file : scanned) {
      String source = Files.readString(file).replaceAll("\\s+", "");
      if (!source.contains("implementsPointerStore")
          && CONSISTENCY_METHODS.stream().anyMatch(source::contains)) {
        offenders.add(SERVICE_SOURCES.relativize(file).toString());
      }
    }

    assertThat(scanned).as("no service sources scanned under %s", SERVICE_SOURCES).isNotEmpty();
    assertThat(offenders)
        .as(
            "inject the authoritative PointerStore (the safe default) or @CachedPointerStore once;"
                + " do not choose consistency at every read")
        .isEmpty();
  }

  @Test
  void pointerStoreViewsStayHiddenBehindCacheAndRepositoryLayers() throws IOException {
    List<String> cachedViewOffenders = new ArrayList<>();
    List<String> rawViewOffenders = new ArrayList<>();
    for (Path file : serviceSources()) {
      String relative = SERVICE_SOURCES.relativize(file).toString();
      String source = Files.readString(file);
      if (selectsView(source, "CachedPointerStore")
          && !relative.startsWith("cache/")
          && !relative.startsWith("repo/")) {
        cachedViewOffenders.add(relative);
      }
      if (selectsView(source, "RawPointerStore") && !relative.equals("cache/MetadataCaches.java")) {
        rawViewOffenders.add(relative);
      }
    }

    assertThat(cachedViewOffenders)
        .as("services should ask repositories for data, not select a pointer-cache view")
        .isEmpty();
    assertThat(rawViewOffenders)
        .as("only pointer-cache composition may inject the raw store")
        .isEmpty();
  }

  private static List<Path> serviceSources() throws IOException {
    try (Stream<Path> files = Files.walk(SERVICE_SOURCES)) {
      return files.filter(path -> path.toString().endsWith(".java")).toList();
    }
  }

  private static boolean selectsView(String source, String qualifier) {
    return source.contains("import ai.floedb.floecat.storage.spi." + qualifier + ";");
  }
}
