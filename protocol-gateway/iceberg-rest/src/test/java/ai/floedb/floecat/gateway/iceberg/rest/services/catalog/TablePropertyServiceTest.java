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
import ai.floedb.floecat.gateway.iceberg.rest.common.RefPropertyUtil;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.TablePropertyService;
import com.google.protobuf.FieldMask;
import jakarta.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class TablePropertyServiceTest {
  private final TablePropertyService service = new TablePropertyService();

  @Test
  void stripMetadataLocationIsNoOp() {
    Map<String, String> props = new LinkedHashMap<>();
    props.put("metadata-location", "a");
    props.put("other", "value");

    service.stripMetadataLocation(props);

    assertEquals(Map.of("metadata-location", "a", "other", "value"), props);
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
    assertEquals(Map.of("foo", "bar", "metadata-location", "ignored"), props);
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

  private Supplier<Table> tableSupplier() {
    return () -> Table.newBuilder().build();
  }
}
