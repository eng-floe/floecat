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

package ai.floedb.floecat.extensions.floedb.utils;

import static ai.floedb.floecat.extensions.floedb.utils.FloePayloads.Descriptor.*;
import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.extensions.floedb.proto.FloeColumnSpecific;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineHintKey;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.systemcatalog.util.TestCatalogOverlay;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ScannerUtilsColumnPayloadTest {

  @Test
  void columnPayloadReturnsPersistedHint() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("tbl-1")
            .build();
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("ns-1")
            .build();

    long columnId = 5L;
    FloeColumnSpecific expected = FloeColumnSpecific.newBuilder().setAttnum(42).build();
    String payloadType = COLUMN.type();
    EngineHintKey key = new EngineHintKey("floedb", "1", payloadType);
    EngineHint hint = new EngineHint(payloadType, expected.toByteArray());

    Map<EngineHintKey, EngineHint> engineHints = Map.of(key, hint);
    Map<Long, Map<EngineHintKey, EngineHint>> columnHints = new LinkedHashMap<>();
    columnHints.put(columnId, engineHints);
    columnHints = Map.copyOf(columnHints);
    TableNode table = new TestTableNode(tableId, namespaceId, engineHints, columnHints);
    CatalogOverlay overlay = new TestCatalogOverlay().addNode(table);

    SchemaColumn column =
        SchemaColumn.newBuilder()
            .setName("col")
            .setFieldId((int) columnId)
            .setId(columnId)
            .setLogicalType("INT")
            .build();

    Optional<FloeColumnSpecific> persisted =
        ScannerUtils.columnPayload(
            overlay,
            tableId,
            column.getId(),
            COLUMN,
            FloeColumnSpecific.class,
            EngineContext.of("floedb", "1"));

    assertThat(persisted).isPresent().contains(expected);
  }

  private static final class TestTableNode implements TableNode {

    private final ResourceId id;
    private final ResourceId namespaceId;
    private final Map<EngineHintKey, EngineHint> engineHints;
    private final Map<Long, Map<EngineHintKey, EngineHint>> columnHints;

    private TestTableNode(
        ResourceId id,
        ResourceId namespaceId,
        Map<EngineHintKey, EngineHint> engineHints,
        Map<Long, Map<EngineHintKey, EngineHint>> columnHints) {
      this.id = id;
      this.namespaceId = namespaceId;
      this.engineHints = Map.copyOf(engineHints);
      this.columnHints = Map.copyOf(columnHints);
    }

    @Override
    public ResourceId id() {
      return id;
    }

    @Override
    public ResourceId namespaceId() {
      return namespaceId;
    }

    @Override
    public long version() {
      return 1;
    }

    @Override
    public String displayName() {
      return "test-table";
    }

    @Override
    public Instant metadataUpdatedAt() {
      return Instant.EPOCH;
    }

    @Override
    public GraphNodeOrigin origin() {
      return GraphNodeOrigin.USER;
    }

    @Override
    public Map<EngineHintKey, EngineHint> engineHints() {
      return engineHints;
    }

    @Override
    public Map<Long, Map<EngineHintKey, EngineHint>> columnHints() {
      return columnHints;
    }
  }
}
