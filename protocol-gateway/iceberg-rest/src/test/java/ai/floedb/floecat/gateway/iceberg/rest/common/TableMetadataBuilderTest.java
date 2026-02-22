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

package ai.floedb.floecat.gateway.iceberg.rest.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergRef;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSchema;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TableMetadataBuilderTest {
  private static final ObjectMapper JSON = new ObjectMapper();

  @Test
  void currentSnapshotUsesMetadataReference() {
    TrinoFixtureTestSupport.Fixture fixture = TrinoFixtureTestSupport.simpleFixture();
    Table table =
        fixture.table().toBuilder()
            .setResourceId(ResourceId.newBuilder().setId("catalog:ns:orders"))
            .build();
    Map<String, String> props = new LinkedHashMap<>(table.getPropertiesMap());
    List<Snapshot> snapshots = fixture.snapshots();
    long earliest = snapshots.stream().mapToLong(Snapshot::getSequenceNumber).min().orElse(0L);
    long earliestSnapshotId =
        snapshots.stream()
            .filter(s -> s.getSequenceNumber() == earliest)
            .findFirst()
            .map(Snapshot::getSnapshotId)
            .orElse(0L);
    props.put("current-snapshot-id", Long.toString(earliestSnapshotId));
    IcebergMetadata metadata =
        fixture.metadata().toBuilder()
            .setCurrentSnapshotId(earliestSnapshotId)
            .putRefs(
                "dev",
                IcebergRef.newBuilder()
                    .setSnapshotId(earliestSnapshotId)
                    .setType("branch")
                    .setMaxReferenceAgeMs(1234L)
                    .build())
            .build();

    TableMetadataView view =
        TableMetadataBuilder.fromCatalog("orders", table, props, metadata, snapshots);

    assertEquals(earliestSnapshotId, view.currentSnapshotId());
    assertEquals(Long.toString(earliestSnapshotId), view.properties().get("current-snapshot-id"));
    @SuppressWarnings("unchecked")
    Map<String, Object> devRef = (Map<String, Object>) view.refs().get("dev");
    assertNotNull(devRef);
    assertEquals(earliestSnapshotId, devRef.get("snapshot-id"));
    assertEquals(1234L, devRef.get("max-ref-age-ms"));
    assertFalse(devRef.containsKey("max-reference-age-ms"));
  }

  @Test
  void snapshotsAlwaysIncludeRequiredSpecFields() {
    TrinoFixtureTestSupport.Fixture fixture = TrinoFixtureTestSupport.simpleFixture();
    Table table =
        fixture.table().toBuilder()
            .setResourceId(ResourceId.newBuilder().setId("catalog:ns:orders"))
            .build();
    Map<String, String> props = new LinkedHashMap<>(table.getPropertiesMap());
    IcebergMetadata metadata = fixture.metadata();
    Snapshot sparse = Snapshot.newBuilder().setSnapshotId(11L).build();

    TableMetadataView view =
        TableMetadataBuilder.fromCatalog("orders", table, props, metadata, List.of(sparse));

    Map<String, Object> snapshot = view.snapshots().get(0);
    assertEquals(0L, snapshot.get("timestamp-ms"));
    assertEquals("", snapshot.get("manifest-list"));
    @SuppressWarnings("unchecked")
    Map<String, Object> summary = (Map<String, Object>) snapshot.get("summary");
    assertNotNull(summary);
    assertEquals("append", summary.get("operation"));
  }

  @Test
  void zeroSnapshotIdRemainsValidForCurrentRef() {
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("catalog:delta:call_center"))
            .putProperties("data_source_format", "DELTA")
            .build();
    Map<String, String> props = new LinkedHashMap<>(table.getPropertiesMap());
    IcebergMetadata metadata =
        IcebergMetadata.newBuilder()
            .setFormatVersion(2)
            .setCurrentSnapshotId(0L)
            .setCurrentSchemaId(0)
            .setLastColumnId(1)
            .putRefs("main", IcebergRef.newBuilder().setSnapshotId(0L).setType("branch").build())
            .build();
    Snapshot snapshot = Snapshot.newBuilder().setSnapshotId(0L).setSequenceNumber(0L).build();

    TableMetadataView view =
        TableMetadataBuilder.fromCatalog("call_center", table, props, metadata, List.of(snapshot));

    assertEquals(0L, view.currentSnapshotId());
    assertEquals("0", view.properties().get("current-snapshot-id"));
    @SuppressWarnings("unchecked")
    Map<String, Object> mainRef = (Map<String, Object>) view.refs().get("main");
    assertNotNull(mainRef);
    assertEquals(0L, mainRef.get("snapshot-id"));
  }

  @Test
  void refsNormalizeLegacyMaxReferenceAgeKeyFromProperties() {
    TrinoFixtureTestSupport.Fixture fixture = TrinoFixtureTestSupport.simpleFixture();
    Table table =
        fixture.table().toBuilder()
            .setResourceId(ResourceId.newBuilder().setId("catalog:ns:orders"))
            .build();
    Map<String, String> props = new LinkedHashMap<>(table.getPropertiesMap());
    long currentSnapshotId = fixture.metadata().getCurrentSnapshotId();
    props.put("current-snapshot-id", Long.toString(currentSnapshotId));
    props.put(
        RefPropertyUtil.PROPERTY_KEY,
        "{\"dev\":{\"snapshot-id\":"
            + currentSnapshotId
            + ",\"type\":\"branch\",\"max-reference-age-ms\":77}}");

    TableMetadataView view =
        TableMetadataBuilder.fromCatalog(
            "orders", table, props, fixture.metadata(), fixture.snapshots());

    @SuppressWarnings("unchecked")
    Map<String, Object> devRef = (Map<String, Object>) view.refs().get("dev");
    assertNotNull(devRef);
    assertEquals(77L, ((Number) devRef.get("max-ref-age-ms")).longValue());
    assertFalse(devRef.containsKey("max-reference-age-ms"));
  }

  @Test
  void deltaTablesPopulateDefaultNameMappingForReadersWithoutFieldIds() throws Exception {
    String schemaJson =
        "{\"schema-id\":0,\"type\":\"struct\",\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"int\",\"required\":false},"
            + "{\"id\":2,\"name\":\"v\",\"type\":\"string\",\"required\":false}],\"last-column-id\":2}";
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("catalog:delta:dv_demo_delta"))
            .putProperties("data_source_format", "DELTA")
            .build();
    Map<String, String> props = new LinkedHashMap<>(table.getPropertiesMap());
    IcebergMetadata metadata =
        IcebergMetadata.newBuilder()
            .setFormatVersion(2)
            .setCurrentSchemaId(0)
            .setLastColumnId(2)
            .addSchemas(
                IcebergSchema.newBuilder()
                    .setSchemaId(0)
                    .setSchemaJson(schemaJson)
                    .setLastColumnId(2)
                    .build())
            .build();

    TableMetadataView view =
        TableMetadataBuilder.fromCatalog("dv_demo_delta", table, props, metadata, List.of());

    String mappingJson = view.properties().get("schema.name-mapping.default");
    assertNotNull(mappingJson);
    JsonNode mapping = JSON.readTree(mappingJson);
    assertTrue(mapping.isArray());
    assertEquals(1, mapping.get(0).get("field-id").asInt());
    assertEquals("id", mapping.get(0).get("names").get(0).asText());
    assertEquals(2, mapping.get(1).get("field-id").asInt());
    assertEquals("v", mapping.get(1).get("names").get(0).asText());
  }
}
