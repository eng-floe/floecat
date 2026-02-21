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

package ai.floedb.floecat.gateway.iceberg.rest.services.compat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Timestamp;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class DeltaIcebergMetadataTranslatorTest {

  private static final ObjectMapper JSON = new ObjectMapper();
  private final DeltaIcebergMetadataTranslator translator = new DeltaIcebergMetadataTranslator();

  @Test
  void translateBuildsMetadataFromLatestSnapshot() {
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:delta_orders").build())
            .setSchemaJson(
                "{\"schema-id\":7,\"type\":\"struct\",\"fields\":[],\"last-column-id\":7}")
            .build();
    Snapshot older =
        Snapshot.newBuilder()
            .setSnapshotId(10L)
            .setSequenceNumber(1L)
            .setSchemaId(5)
            .setUpstreamCreatedAt(Timestamp.newBuilder().setSeconds(100))
            .build();
    Snapshot current =
        Snapshot.newBuilder()
            .setSnapshotId(11L)
            .setSequenceNumber(2L)
            .setSchemaId(7)
            .setSchemaJson(
                "{\"schema-id\":7,\"type\":\"struct\",\"fields\":[],\"last-column-id\":7}")
            .setUpstreamCreatedAt(Timestamp.newBuilder().setSeconds(200))
            .build();

    var metadata = translator.translate(table, List.of(current, older));

    assertEquals(11L, metadata.getCurrentSnapshotId());
    assertEquals(2L, metadata.getLastSequenceNumber());
    assertEquals("branch", metadata.getRefsOrThrow("main").getType());
    assertEquals(1, metadata.getSchemasCount());
    assertTrue(metadata.getMetadataLocation().contains("floe+delta://cat:db:delta_orders"));
    assertEquals(2, metadata.getSnapshotLogCount());
    assertEquals(7, metadata.getCurrentSchemaId());
    assertEquals(7, metadata.getSchemas(0).getLastColumnId());
  }

  @Test
  void translateAssignsFieldIdsWhenSchemaLacksIds() throws Exception {
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:delta_orders").build())
            .setSchemaJson(
                "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":false}]}")
            .build();
    Snapshot current =
        Snapshot.newBuilder()
            .setSnapshotId(11L)
            .setSequenceNumber(2L)
            .setSchemaId(3)
            .setSchemaJson(
                "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":false}]}")
            .setUpstreamCreatedAt(Timestamp.newBuilder().setSeconds(200))
            .build();

    var metadata = translator.translate(table, List.of(current));
    JsonNode schema = JSON.readTree(metadata.getSchemas(0).getSchemaJson());

    assertEquals(3, schema.get("schema-id").asInt());
    assertEquals(1, schema.get("last-column-id").asInt());
    assertEquals(1, schema.get("fields").get(0).get("id").asInt());
    assertEquals("id", schema.get("fields").get(0).get("name").asText());
    assertEquals(true, schema.get("fields").get(0).get("required").asBoolean());
  }

  @Test
  void translateUsesRealCallCenterDeltaFixtureSchema() throws Exception {
    String logPath = "delta-fixtures/call_center/_delta_log/00000000000000000000.json";
    try (InputStream in = getClass().getClassLoader().getResourceAsStream(logPath)) {
      if (in == null) {
        throw new IllegalStateException("Missing test fixture: " + logPath);
      }
      String text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
      String metadataLine =
          text.lines()
              .filter(line -> line.contains("\"metaData\""))
              .findFirst()
              .orElseThrow(() -> new IllegalStateException("metaData action not found in fixture"));
      JsonNode metadataRoot = JSON.readTree(metadataLine).get("metaData");
      String schemaString = metadataRoot.get("schemaString").asText();
      long createdMs = metadataRoot.get("createdTime").asLong();

      Table table =
          Table.newBuilder()
              .setResourceId(
                  ResourceId.newBuilder().setId("cat:examples:delta:call_center").build())
              .setSchemaJson(schemaString)
              .build();
      Snapshot current =
          Snapshot.newBuilder()
              .setSnapshotId(1L)
              .setSequenceNumber(1L)
              .setSchemaId(1)
              .setSchemaJson(schemaString)
              .setUpstreamCreatedAt(
                  Timestamp.newBuilder()
                      .setSeconds(createdMs / 1000L)
                      .setNanos((int) ((createdMs % 1000L) * 1_000_000L))
                      .build())
              .build();

      var metadata = translator.translate(table, List.of(current));
      JsonNode translatedSchema = JSON.readTree(metadata.getSchemas(0).getSchemaJson());
      JsonNode translatedFields = translatedSchema.get("fields");
      int inputFieldCount = JSON.readTree(schemaString).get("fields").size();

      assertEquals(1, metadata.getCurrentSchemaId());
      assertEquals(inputFieldCount, translatedFields.size());
      assertEquals(inputFieldCount, translatedSchema.get("last-column-id").asInt());
      assertEquals("cc_call_center_sk", translatedFields.get(0).get("name").asText());
      assertEquals(1, translatedFields.get(0).get("id").asInt());
    }
  }

  @Test
  void translateUsesLatestSnapshotForMultiSnapshotDeltaFixture() throws Exception {
    String firstLogPath =
        "delta-fixtures/my_local_delta_table/_delta_log/00000000000000000000.json";
    String secondLogPath =
        "delta-fixtures/my_local_delta_table/_delta_log/00000000000000000001.json";
    try (InputStream first = getClass().getClassLoader().getResourceAsStream(firstLogPath);
        InputStream second = getClass().getClassLoader().getResourceAsStream(secondLogPath)) {
      if (first == null || second == null) {
        throw new IllegalStateException(
            "Missing test fixtures: " + firstLogPath + " or " + secondLogPath);
      }

      String firstText = new String(first.readAllBytes(), StandardCharsets.UTF_8);
      String secondText = new String(second.readAllBytes(), StandardCharsets.UTF_8);

      JsonNode metadataRoot =
          JSON.readTree(
                  firstText
                      .lines()
                      .filter(line -> line.contains("\"metaData\""))
                      .findFirst()
                      .orElseThrow(
                          () ->
                              new IllegalStateException("metaData action not found in first log")))
              .get("metaData");
      String schemaString = metadataRoot.get("schemaString").asText();

      JsonNode commit0 =
          JSON.readTree(
                  firstText
                      .lines()
                      .filter(line -> line.contains("\"commitInfo\""))
                      .findFirst()
                      .orElseThrow(
                          () ->
                              new IllegalStateException(
                                  "commitInfo action not found in first log")))
              .get("commitInfo");
      JsonNode commit1 =
          JSON.readTree(
                  secondText
                      .lines()
                      .filter(line -> line.contains("\"commitInfo\""))
                      .findFirst()
                      .orElseThrow(
                          () ->
                              new IllegalStateException(
                                  "commitInfo action not found in second log")))
              .get("commitInfo");

      long ts0 = commit0.get("timestamp").asLong();
      long ts1 = commit1.get("timestamp").asLong();

      Table table =
          Table.newBuilder()
              .setResourceId(
                  ResourceId.newBuilder().setId("cat:examples:delta:my_local_delta_table").build())
              .setSchemaJson(schemaString)
              .build();
      Snapshot snapshot0 =
          Snapshot.newBuilder()
              .setSnapshotId(0L)
              .setSequenceNumber(0L)
              .setSchemaId(0)
              .setSchemaJson(schemaString)
              .setUpstreamCreatedAt(
                  Timestamp.newBuilder()
                      .setSeconds(ts0 / 1000L)
                      .setNanos((int) ((ts0 % 1000L) * 1_000_000L))
                      .build())
              .build();
      Snapshot snapshot1 =
          Snapshot.newBuilder()
              .setSnapshotId(1L)
              .setSequenceNumber(1L)
              .setSchemaId(0)
              .setSchemaJson(schemaString)
              .setUpstreamCreatedAt(
                  Timestamp.newBuilder()
                      .setSeconds(ts1 / 1000L)
                      .setNanos((int) ((ts1 % 1000L) * 1_000_000L))
                      .build())
              .build();

      var metadata = translator.translate(table, List.of(snapshot0, snapshot1));
      JsonNode translatedSchema = JSON.readTree(metadata.getSchemas(0).getSchemaJson());
      JsonNode translatedFields = translatedSchema.get("fields");
      int inputFieldCount = JSON.readTree(schemaString).get("fields").size();

      assertEquals(1L, metadata.getCurrentSnapshotId());
      assertEquals(1L, metadata.getLastSequenceNumber());
      assertEquals(2, metadata.getSnapshotLogCount());
      assertEquals(1L, metadata.getRefsOrThrow("main").getSnapshotId());
      assertEquals((ts1 / 1000L) * 1000L, metadata.getLastUpdatedMs());
      assertEquals(inputFieldCount, translatedFields.size());
      assertEquals("id", translatedFields.get(0).get("name").asText());
    }
  }

  @Test
  void translateKeepsNestedTypeObjectsInSchema() throws Exception {
    String nestedSchema =
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"payload\",\"nullable\":true,"
            + "\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"k\",\"type\":\"string\","
            + "\"nullable\":true}]}}]}";
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:delta_nested").build())
            .setSchemaJson(nestedSchema)
            .build();
    Snapshot snapshot =
        Snapshot.newBuilder()
            .setSnapshotId(2L)
            .setSequenceNumber(2L)
            .setSchemaId(1)
            .setSchemaJson(nestedSchema)
            .setUpstreamCreatedAt(Timestamp.newBuilder().setSeconds(200))
            .build();

    var metadata = translator.translate(table, List.of(snapshot));
    JsonNode schema = JSON.readTree(metadata.getSchemas(0).getSchemaJson());
    JsonNode typeNode = schema.get("fields").get(0).get("type");

    assertTrue(typeNode.isObject());
    assertEquals("struct", typeNode.get("type").asText());
  }

  @Test
  void translateSetsMainRefForSnapshotZero() {
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("cat:db:delta_zero").build())
            .setSchemaJson(
                "{\"schema-id\":0,\"type\":\"struct\",\"fields\":[],\"last-column-id\":0}")
            .build();
    Snapshot snapshot0 =
        Snapshot.newBuilder()
            .setSnapshotId(0L)
            .setSequenceNumber(0L)
            .setSchemaId(0)
            .setSchemaJson(
                "{\"schema-id\":0,\"type\":\"struct\",\"fields\":[],\"last-column-id\":0}")
            .setUpstreamCreatedAt(Timestamp.newBuilder().setSeconds(200))
            .build();

    var metadata = translator.translate(table, List.of(snapshot0));

    assertEquals(0L, metadata.getCurrentSnapshotId());
    assertTrue(metadata.containsRefs("main"));
    assertEquals(0L, metadata.getRefsOrThrow("main").getSnapshotId());
  }
}
