package ai.floedb.floecat.gateway.iceberg.rest.services.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
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
  void stripMetadataLocationRemovesCanonicalKey() {
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
  void applyLocationUpdateSkipsWithoutConnector() {
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
    assertTrue(mask.getPathsList().isEmpty(), "expected location update to be skipped");
    assertFalse(spec.hasUpstream(), "upstream should not be mutated without connector");
  }

  @Test
  void hasPropertyUpdatesDetectsSetAndRemove() {
    TableRequests.Commit commit =
        new TableRequests.Commit(
            null, null, null, null, null, List.of(), List.of(Map.of("action", "set-properties")));
    assertTrue(service.hasPropertyUpdates(commit));
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
  void tableWithPropertyOverridesReplacesProperties() {
    Table table = Table.newBuilder().putProperties("old", "value").build();
    Map<String, String> overrides = Map.of("new", "value");

    Table updated = service.tableWithPropertyOverrides(() -> table, overrides);

    assertSame("value", updated.getPropertiesOrThrow("new"));
    assertFalse(updated.getPropertiesMap().containsKey("old"));
  }

  private Supplier<Table> tableSupplier() {
    return () -> Table.newBuilder().build();
  }
}
