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
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class FunctionNodeTest {

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
    ResourceId namespaceId = ResourceId.newBuilder().setId("pg_catalog-ns-id").build();
    FunctionNode function = node(namespaceId);
    assertThat(function.namespaceId()).isEqualTo(namespaceId);
  }

  @Test
  void constructor_normalizesNullDisplayName() {
    ResourceId namespaceId = ResourceId.newBuilder().setId("pg_catalog-ns-id").build();
    FunctionNode function =
        new FunctionNode(
            ResourceId.newBuilder().setId("fn-id").build(),
            1,
            Instant.EPOCH,
            "1.0",
            namespaceId,
            null,
            List.of(),
            null,
            false,
            false,
            null);
    assertThat(function.displayName()).isEmpty();
  }

  private static FunctionNode node(ResourceId namespaceId) {
    return new FunctionNode(
        ResourceId.newBuilder().setId("fn-id").build(),
        1,
        Instant.EPOCH,
        "1.0",
        namespaceId,
        "abs",
        List.of(),
        null,
        false,
        false,
        Map.of());
  }
}
