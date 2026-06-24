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

package ai.floedb.floecat.gateway.iceberg.rest.services.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.common.RefPropertyUtil;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.TablePropertyService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.metadata.TableCommitMetadataMutator;
import com.google.protobuf.FieldMask;
import jakarta.ws.rs.core.Response;
import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class TablePropertyServiceTest {
  private final TablePropertyService service = new TablePropertyService();

  TablePropertyServiceTest() {
    injectMetadataMutator(service, new TableCommitMetadataMutator());
  }

  private static void injectMetadataMutator(
      TablePropertyService service, TableCommitMetadataMutator metadataMutator) {
    try {
      Field field = TablePropertyService.class.getDeclaredField("metadataMutator");
      field.setAccessible(true);
      field.set(service, metadataMutator);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Failed to initialize TablePropertyService test", e);
    }
  }

  @Test
  void stripMetadataLocationIsNoOp() {
    Map<String, String> props = new LinkedHashMap<>();
    props.put("metadata-location", "a");
    props.put("other", "value");

    service.stripMetadataLocation(props);

    assertEquals(Map.of("other", "value"), props);
  }

  @Test
  void applyPropertyUpdatesValidatesEmptySet() {
    Map<String, String> props = new LinkedHashMap<>();
    var response = service.applyPropertyUpdates(props, List.of(Map.of("action", "set-properties")));

    assertEquals(400, response.getStatus());
    assertTrue(
        response.getEntity().toString().contains("set-properties requires updates"),
        "expected validation response");
  }

  @Test
  void applyPropertyUpdatesMergesAndRemovesProperties() {
    Map<String, String> props = new LinkedHashMap<>();
    props.put("existing", "value");

    Response response =
        service.applyPropertyUpdates(
            props,
            List.of(
                Map.of(
                    "action",
                    "set-properties",
                    "updates",
                    Map.of("foo", "bar", "metadata-location", "ignored")),
                Map.of("action", "remove-properties", "removals", List.of("existing"))));

    assertNull(response);
    assertEquals(Map.of("foo", "bar"), props);
  }

  @Test
  void applyPropertyUpdatesDoesNotRemoveReservedFormatVersion() {
    Map<String, String> props = new LinkedHashMap<>();
    props.put("format-version", "1");
    props.put("other", "value");

    Response response =
        service.applyPropertyUpdates(
            props,
            List.of(
                Map.of(
                    "action",
                    "remove-properties",
                    "removals",
                    List.of("format-version", "other"))));

    assertNull(response);
    assertEquals("1", props.get("format-version"));
    assertFalse(props.containsKey("other"));
  }

  @Test
  void applyLocationUpdateRequiresSingleLocation() {
    var response =
        service.applyLocationUpdate(
            TableSpec.newBuilder(),
            FieldMask.newBuilder(),
            tableSupplier(),
            List.of(
                Map.of("action", "set-location", "location", "s3://a"),
                Map.of("action", "set-location", "location", "s3://b")));

    assertEquals(400, response.getStatus());
    assertTrue(response.getEntity().toString().contains("may only be specified once"));
  }

  @Test
  void applyLocationUpdateSetsUpstream() {
    TableSpec.Builder spec = TableSpec.newBuilder();
    FieldMask.Builder mask = FieldMask.newBuilder();
    Table table =
        Table.newBuilder()
            .setUpstream(
                UpstreamRef.newBuilder()
                    .setUri("s3://current")
                    .setConnectorId(ResourceId.newBuilder().setId("con").build())
                    .addNamespacePath("ns")
                    .setTableDisplayName("tbl")
                    .build())
            .build();

    Response response =
        service.applyLocationUpdate(
            spec,
            mask,
            () -> table,
            List.of(Map.of("action", "set-location", "location", "s3://new/location")));

    assertNull(response);
    assertEquals(List.of("upstream.uri"), mask.getPathsList());
    assertEquals("s3://new/location", spec.getUpstream().getUri());
  }

  @Test
  void applyLocationUpdateAppliesWithoutConnector() {
    TableSpec.Builder spec = TableSpec.newBuilder();
    FieldMask.Builder mask = FieldMask.newBuilder();
    Table table =
        Table.newBuilder()
            .setUpstream(UpstreamRef.newBuilder().setUri("s3://current").build())
            .build();

    Response response =
        service.applyLocationUpdate(
            spec,
            mask,
            () -> table,
            List.of(Map.of("action", "set-location", "location", "s3://new/location")));

    assertNull(response);
    assertEquals(List.of("upstream.uri"), mask.getPathsList());
    assertEquals("s3://new/location", spec.getUpstream().getUri());
  }

  @Test
  void hasPropertyUpdatesDetectsSetAndRemove() {
    Table table = Table.newBuilder().putProperties("existing", "value").build();

    var result =
        service.applyCommitPropertyUpdates(
            () -> table,
            null,
            List.of(Map.of("action", "set-properties", "updates", Map.of("added", "yes"))));

    assertNull(result.error());
    assertEquals("yes", result.properties().get("added"));
  }

  @Test
  void ensurePropertyMapReturnsCopy() {
    Table table =
        Table.newBuilder()
            .putProperties("existing", "value")
            .putProperties("other", "prop")
            .build();

    Map<String, String> map = service.ensurePropertyMap(() -> table, null);

    assertEquals(table.getPropertiesMap(), map);
    map.put("new", "value");
    assertFalse(table.getPropertiesMap().containsKey("new"));
  }

  @Test
  void applyCommitPropertyUpdatesAppliesSnapshotRefMutationsInOrder() {
    Map<String, Map<String, Object>> initialRefs = new LinkedHashMap<>();
    initialRefs.put("main", new LinkedHashMap<>(Map.of("snapshot-id", 10L, "type", "branch")));
    Table table =
        Table.newBuilder()
            .putProperties(RefPropertyUtil.PROPERTY_KEY, RefPropertyUtil.encode(initialRefs))
            .putProperties("current-snapshot-id", "10")
            .build();

    List<Map<String, Object>> updates =
        List.of(
            Map.of(
                "action",
                "add-snapshot",
                "snapshot",
                Map.of(
                    "snapshot-id",
                    33L,
                    "timestamp-ms",
                    1772624629860L,
                    "sequence-number",
                    1L,
                    "summary",
                    Map.of("operation", "append"))),
            Map.of("action", "set-snapshot-ref", "ref-name", "main", "snapshot-id", 22L),
            Map.of("action", "remove-snapshot-ref", "ref-name", "main"),
            Map.of(
                "action",
                "set-snapshot-ref",
                "ref-name",
                "main",
                "snapshot-id",
                33L,
                "min_snapshots_to_keep",
                7));

    var result = service.applyCommitPropertyUpdates(() -> table, null, updates);

    assertNull(result.error());
    assertEquals("33", result.properties().get("current-snapshot-id"));
    Map<String, Map<String, Object>> refs =
        RefPropertyUtil.decode(result.properties().get(RefPropertyUtil.PROPERTY_KEY));
    assertEquals(1, refs.size());
    assertEquals(33L, ((Number) refs.get("main").get("snapshot-id")).longValue());
    assertEquals(7, ((Number) refs.get("main").get("min-snapshots-to-keep")).intValue());
  }

  @Test
  void applyCommitPropertyUpdatesDoesNotRewindCurrentSnapshotFromAddedSnapshotWithoutMainRef() {
    Table table =
        Table.newBuilder()
            .putProperties("current-snapshot-id", "1944648604358776794")
            .putProperties("last-sequence-number", "0")
            .build();

    List<Map<String, Object>> updates =
        List.of(
            Map.of(
                "action",
                "add-snapshot",
                "snapshot",
                Map.of(
                    "snapshot-id",
                    4652753989274070009L,
                    "timestamp-ms",
                    1781027618000L,
                    "sequence-number",
                    1L,
                    "summary",
                    Map.of("operation", "append", "total-data-files", "0"))),
            Map.of("action", "upgrade-format-version", "format-version", 2));

    var result = service.applyCommitPropertyUpdates(() -> table, null, updates);

    assertNull(result.error());
    assertEquals("1944648604358776794", result.properties().get("current-snapshot-id"));
    assertEquals("1", result.properties().get("last-sequence-number"));
  }

  @Test
  void applyCommitPropertyUpdatesIgnoresStaleMainRefWithoutAddedSnapshot() {
    Map<String, Map<String, Object>> initialRefs = new LinkedHashMap<>();
    initialRefs.put(
        "main", new LinkedHashMap<>(Map.of("snapshot-id", 2193008915892245619L, "type", "branch")));
    Table table =
        Table.newBuilder()
            .putProperties("current-snapshot-id", "2193008915892245619")
            .putProperties("last-sequence-number", "0")
            .putProperties(RefPropertyUtil.PROPERTY_KEY, RefPropertyUtil.encode(initialRefs))
            .build();

    List<Map<String, Object>> updates =
        List.of(
            Map.of("action", "upgrade-format-version", "format-version", 2),
            Map.of(
                "action",
                "set-snapshot-ref",
                "ref-name",
                "main",
                "snapshot-id",
                9180431282726110998L,
                "type",
                "branch"));

    var result = service.applyCommitPropertyUpdates(() -> table, null, updates);

    assertNull(result.error());
    assertEquals("2193008915892245619", result.properties().get("current-snapshot-id"));
    Map<String, Map<String, Object>> refs =
        RefPropertyUtil.decode(result.properties().get(RefPropertyUtil.PROPERTY_KEY));
    assertEquals(2193008915892245619L, ((Number) refs.get("main").get("snapshot-id")).longValue());
  }

  @Test
  void applyCommitPropertyUpdatesPreservesCurrentSnapshotForMetadataOnlyFormatUpgrade() {
    Table table =
        Table.newBuilder()
            .putProperties("current-snapshot-id", "5239576338451716881")
            .putProperties("last-sequence-number", "0")
            .putProperties("format-version", "1")
            .build();

    List<Map<String, Object>> updates =
        List.of(
            Map.of(
                "action",
                "remove-properties",
                "removals",
                List.of("current-snapshot-id", "format-version")),
            Map.of("action", "upgrade-format-version", "format-version", 2));

    var result = service.applyCommitPropertyUpdates(() -> table, null, updates);

    assertNull(result.error());
    assertEquals("5239576338451716881", result.properties().get("current-snapshot-id"));
    assertEquals("2", result.properties().get("format-version"));
  }

  @Test
  void applyCanonicalMetadataPropertiesNormalizesSnapshotAndRefPropertiesFromMetadata() {
    Map<String, Map<String, Object>> staleRefs = new LinkedHashMap<>();
    staleRefs.put("main", new LinkedHashMap<>(Map.of("snapshot-id", 10L, "type", "branch")));
    Table plannedTable =
        Table.newBuilder()
            .putProperties("current-snapshot-id", "10")
            .putProperties("last-sequence-number", "999")
            .putProperties(RefPropertyUtil.PROPERTY_KEY, RefPropertyUtil.encode(staleRefs))
            .build();
    TableMetadataView metadata =
        new TableMetadataView(
            2,
            "tbl-1",
            "s3://floecat/iceberg/orders",
            "s3://floecat/iceberg/orders/metadata/00002.metadata.json",
            1L,
            Map.of(),
            2,
            0,
            0,
            0,
            0,
            33L,
            7L,
            List.of(),
            List.of(),
            List.of(),
            Map.of("main", Map.of("snapshot-id", 33L, "type", "branch")),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    Table updated = service.applyCanonicalMetadataProperties(plannedTable, metadata);

    assertEquals("33", updated.getPropertiesMap().get("current-snapshot-id"));
    assertEquals("7", updated.getPropertiesMap().get("last-sequence-number"));
    Map<String, Map<String, Object>> refs =
        RefPropertyUtil.decode(updated.getPropertiesMap().get(RefPropertyUtil.PROPERTY_KEY));
    assertEquals(33L, ((Number) refs.get("main").get("snapshot-id")).longValue());
  }

  private Supplier<Table> tableSupplier() {
    return () -> Table.newBuilder().build();
  }
}
