package ai.floedb.floecat.gateway.iceberg.rest.services.catalog;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.support.MetadataLocationUtil;
import com.google.protobuf.FieldMask;
import java.util.LinkedHashMap;
import java.util.Map;

public final class MetadataLocationSync {

  private MetadataLocationSync() {}

  public static Table ensureMetadataLocation(
      TableLifecycleService lifecycleService,
      TableGatewaySupport tableSupport,
      ResourceId tableId,
      Table current,
      String desiredLocation) {
    if (tableId == null || desiredLocation == null || desiredLocation.isBlank()) {
      return current;
    }
    Table existing = current;
    if (existing == null) {
      existing = lifecycleService.getTable(tableId);
    }
    if (existing == null) {
      return null;
    }
    String stored = MetadataLocationUtil.metadataLocation(existing.getPropertiesMap());
    if (desiredLocation.equals(stored)) {
      return existing;
    }
    Map<String, String> props = new LinkedHashMap<>(existing.getPropertiesMap());
    MetadataLocationUtil.setMetadataLocation(props, desiredLocation);
    TableSpec.Builder spec = TableSpec.newBuilder().putAllProperties(props);
    if (tableSupport != null) {
      tableSupport.addMetadataLocationProperties(spec, desiredLocation);
    } else {
      MetadataLocationUtil.setMetadataLocation(spec::putProperties, desiredLocation);
    }
    return lifecycleService.updateTable(
        UpdateTableRequest.newBuilder()
            .setTableId(tableId)
            .setSpec(spec)
            .setUpdateMask(FieldMask.newBuilder().addPaths("properties").build())
            .build());
  }
}
