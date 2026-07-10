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

package ai.floedb.floecat.service.repo.impl;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.rpc.NamespacePath;
import ai.floedb.floecat.connector.rpc.SourceMapping;
import ai.floedb.floecat.connector.rpc.SourceSelector;
import org.junit.jupiter.api.Test;

class ConnectorRepositoryNormalizeTest {

  @SuppressWarnings("deprecation")
  @Test
  void legacyOnlyConnectorIsNormalizedIntoSingleMapping() {
    Connector legacy =
        Connector.newBuilder()
            .setSource(
                SourceSelector.newBuilder()
                    .setNamespace(NamespacePath.newBuilder().addSegments("db"))
                    .setTable("orders")
                    .addColumns("order_id"))
            .setDestination(
                DestinationTarget.newBuilder().setCatalogId(ResourceId.newBuilder().setId("cat-1")))
            .build();

    Connector normalized = ConnectorRepository.normalizeLegacyMapping(legacy);

    assertThat(normalized.hasSource()).isFalse();
    assertThat(normalized.hasDestination()).isFalse();
    assertThat(normalized.getMappingsCount()).isEqualTo(1);
    SourceMapping mapping = normalized.getMappings(0);
    assertThat(mapping.getSource().getNamespace().getSegmentsList()).containsExactly("db");
    assertThat(mapping.getSource().getTable()).isEqualTo("orders");
    assertThat(mapping.getSource().getColumnsList()).containsExactly("order_id");
    assertThat(mapping.getDestination().getCatalogId().getId()).isEqualTo("cat-1");
  }

  @SuppressWarnings("deprecation")
  @Test
  void legacySourceOnlyConnectorIsNormalizedIntoSingleMapping() {
    Connector legacy =
        Connector.newBuilder()
            .setSource(
                SourceSelector.newBuilder()
                    .setNamespace(NamespacePath.newBuilder().addSegments("db"))
                    .setTable("orders"))
            .build();

    Connector normalized = ConnectorRepository.normalizeLegacyMapping(legacy);

    assertThat(normalized.hasSource()).isFalse();
    assertThat(normalized.hasDestination()).isFalse();
    assertThat(normalized.getMappingsCount()).isEqualTo(1);
    SourceMapping mapping = normalized.getMappings(0);
    assertThat(mapping.getSource().getNamespace().getSegmentsList()).containsExactly("db");
    assertThat(mapping.getSource().getTable()).isEqualTo("orders");
    assertThat(mapping.getDestination()).isEqualTo(DestinationTarget.getDefaultInstance());
  }

  @SuppressWarnings("deprecation")
  @Test
  void legacyDestinationOnlyConnectorIsNormalizedIntoSingleMapping() {
    Connector legacy =
        Connector.newBuilder()
            .setDestination(
                DestinationTarget.newBuilder().setCatalogId(ResourceId.newBuilder().setId("cat-1")))
            .build();

    Connector normalized = ConnectorRepository.normalizeLegacyMapping(legacy);

    assertThat(normalized.hasSource()).isFalse();
    assertThat(normalized.hasDestination()).isFalse();
    assertThat(normalized.getMappingsCount()).isEqualTo(1);
    SourceMapping mapping = normalized.getMappings(0);
    assertThat(mapping.getSource()).isEqualTo(SourceSelector.getDefaultInstance());
    assertThat(mapping.getDestination().getCatalogId().getId()).isEqualTo("cat-1");
  }

  @Test
  void mappingsConnectorPassesThroughUnchanged() {
    Connector connector =
        Connector.newBuilder()
            .addMappings(
                SourceMapping.newBuilder()
                    .setSource(
                        SourceSelector.newBuilder()
                            .setNamespace(NamespacePath.newBuilder().addSegments("db"))))
            .build();

    assertThat(ConnectorRepository.normalizeLegacyMapping(connector)).isSameAs(connector);
  }

  @Test
  void shellConnectorWithoutSourceOrMappingsPassesThrough() {
    Connector shell = Connector.newBuilder().setDisplayName("shell").build();

    assertThat(ConnectorRepository.normalizeLegacyMapping(shell)).isSameAs(shell);
  }
}
