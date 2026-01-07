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

package ai.floedb.floecat.gateway.iceberg.rest.services.metadata;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.common.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
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
