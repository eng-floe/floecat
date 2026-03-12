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

package ai.floedb.floecat.metagraph.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.common.rpc.ResourceId;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class TypeNodeTest {

  @Test
  void constructor_throwsWhenNamespaceIdIsNull() {
    assertThatThrownBy(() -> node(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("namespaceId");
  }

  @Test
  void constructor_throwsWhenNamespaceIdHasNoIdentity() {
    assertThatThrownBy(() -> node(ResourceId.getDefaultInstance()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("namespaceId");
  }

  @Test
  void constructor_succeedsWithValidNamespaceId() {
    ResourceId nsId = ResourceId.newBuilder().setId("pg_catalog-ns-id").build();
    TypeNode type = node(nsId);
    assertThat(type.namespaceId()).isEqualTo(nsId);
  }

  @Test
  void constructor_normalizesNullDisplayName() {
    ResourceId nsId = ResourceId.newBuilder().setId("pg_catalog-ns-id").build();
    TypeNode type =
        new TypeNode(
            ResourceId.newBuilder().setId("type-id").build(),
            1,
            Instant.EPOCH,
            "1.0",
            nsId,
            null,
            "U",
            false,
            null,
            null);
    assertThat(type.displayName()).isEmpty();
  }

  private static TypeNode node(ResourceId namespaceId) {
    return new TypeNode(
        ResourceId.newBuilder().setId("type-id").build(),
        1,
        Instant.EPOCH,
        "1.0",
        namespaceId,
        "int4",
        "U",
        false,
        null,
        Map.of());
  }
}
