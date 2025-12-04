package ai.floedb.metacat.gateway.iceberg.rest.services.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableSpec;
import ai.floedb.metacat.gateway.iceberg.rest.api.request.TableRequests;
import com.google.protobuf.FieldMask;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class TablePropertyServiceTest {
  private final TablePropertyService service = new TablePropertyService();

  @Test
  void stripMetadataLocationRemovesBothKeyVariants() {
    Map<String, String> props = new LinkedHashMap<>();
    props.put("metadata-location", "a");
    props.put("metadata_location", "b");
    props.put("other", "value");

    service.stripMetadataLocation(props);

    assertEquals(Map.of("other", "value"), props);
  }

  @Test
  void applyPropertyUpdatesValidatesEmptySet() {
    Map<String, String> props = new LinkedHashMap<>();
    var response =
        service.applyPropertyUpdates(props, List.of(Map.of("action", "set-properties")));

    assertEquals(400, response.getStatus());
    assertTrue(
        response.getEntity().toString().contains("set-properties requires updates"),
        "expected validation response");
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
  void hasPropertyUpdatesDetectsSetAndRemove() {
    TableRequests.Commit commit =
        new TableRequests.Commit(
            null, null, null, null, null, null, List.of(Map.of("action", "set-properties")));
    assertTrue(service.hasPropertyUpdates(commit));
  }

  private Supplier<Table> tableSupplier() {
    return () -> Table.newBuilder().build();
  }
}
