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

package ai.floedb.floecat.service.repo;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/**
 * An emptiness check that a marker fences must read strongly.
 *
 * <p>A marker is sampled with a consistent point read and a prefix count or scan is not: on
 * DynamoDB {@code countByPrefix} and {@code listPointersByPrefix} pass {@code consistentRead=false}
 * while {@code get} passes true. So a relation committed just before the marker sample is already
 * counted in the marker version, and if the index has not caught up the emptiness check reads zero,
 * the CAS matches the version that write itself produced, and the delete commits over a live row.
 *
 * <p>The fence closes the window after the sample. Only a consistent read closes the one before it.
 * That makes this a property no in-memory test can catch — the in-memory store is always consistent
 * — so it is asserted against the source instead.
 */
class EmptinessReadsConsistentlyTest {

  @Test
  void theRelationCountBehindAFenceReadsStrongly() throws IOException {
    String body = bodyOf("repo/impl/NamespaceRepository.java", "int relationCount(");

    assertThat(body)
        .as("the relation count decides whether a namespace may be deleted or moved")
        .contains("countConsistent(");
    assertThat(body)
        .as(
            "an eventually-consistent count cannot be fenced against: a relation committed just"
                + " before the marker sample reads as absent while the CAS still matches")
        .doesNotContain(".count(");
  }

  @Test
  void theDescendantCheckBehindAFenceReadsStrongly() throws IOException {
    String body = bodyOf("repo/impl/NamespaceRepository.java", "hasDescendants(");

    assertThat(body)
        .as("hasDescendants decides whether a namespace may be renamed or re-parented")
        .contains("countConsistent(");
    assertThat(body)
        .as("the eventually-consistent read cannot be fenced against, for the same reason")
        .doesNotContain(" list(")
        .doesNotContain(".count(");
  }

  /**
   * The namespace count behind a catalog delete reads strongly, on the same terms.
   *
   * <p>Guarded separately from the namespace count because it is a different caller of a different
   * repository method, and a guard keyed only to the namespace path would not have noticed this one
   * arriving.
   */
  @Test
  void theNamespaceCountBehindACatalogDeleteReadsStrongly() throws IOException {
    String body =
        bodyOf("catalog/impl/CatalogServiceImpl.java", "public Uni<DeleteCatalogResponse>");

    assertThat(body)
        .as("the namespace count decides whether a catalog may be deleted")
        .contains("countConsistent(");
    // No doesNotContain guard for the namespace count: the eventually-consistent variant no longer
    // exists on that repository, so there is nothing to call by mistake.
  }

  /**
   * One method's body, from its declaration to its closing brace.
   *
   * <p>Bounded deliberately. A window running to end-of-file is satisfied by any later declaration
   * of the very method being asked about, so the assertion passes whatever the body does.
   */
  private static String bodyOf(String relativePath, String declaration) throws IOException {
    String source =
        Files.readString(Path.of("src/main/java/ai/floedb/floecat/service", relativePath));
    int start = source.indexOf(declaration);
    assertThat(start).as("found " + declaration).isNotNegative();
    int end = source.indexOf("\n  }", start);
    assertThat(end).as("found the end of " + declaration).isGreaterThan(start);
    return source.substring(start, end);
  }
}
