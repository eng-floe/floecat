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

package ai.floedb.floecat.gateway.iceberg.rest.table.transaction;

import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.support.CommitUpdateInspector;
import ai.floedb.floecat.gateway.iceberg.rest.table.TableMetadataBuilder;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class CreateUpdateFactory {
  private CreateUpdateFactory() {}

  static List<Map<String, Object>> fromCreateRequest(TableRequests.Create request, TableSpec spec) {
    TableMetadataBuilder.CreateRequestState state =
        TableMetadataBuilder.createRequestState(null, null, request, spec.getPropertiesMap());
    Map<String, String> props = new LinkedHashMap<>(state.properties());
    String tableLocation = blankToNull(props.remove("location"));
    Integer formatVersion = state.formatVersion();

    List<Map<String, Object>> updates = new ArrayList<>();
    if (tableLocation != null) {
      updates.add(
          Map.of("action", CommitUpdateInspector.ACTION_SET_LOCATION, "location", tableLocation));
    }
    updates.add(
        Map.of(
            "action",
            CommitUpdateInspector.ACTION_UPGRADE_FORMAT_VERSION,
            "format-version",
            formatVersion));
    updates.add(
        Map.of(
            "action",
            CommitUpdateInspector.ACTION_ADD_SCHEMA,
            "schema",
            state.schema(),
            "last-column-id",
            state.lastColumnId()));
    updates.add(
        Map.of(
            "action",
            CommitUpdateInspector.ACTION_SET_CURRENT_SCHEMA,
            "schema-id",
            state.schemaId()));
    updates.add(
        Map.of("action", CommitUpdateInspector.ACTION_ADD_SPEC, "spec", state.partitionSpec()));
    updates.add(
        Map.of(
            "action",
            CommitUpdateInspector.ACTION_SET_DEFAULT_SPEC,
            "spec-id",
            state.defaultSpecId()));
    updates.add(
        Map.of(
            "action",
            CommitUpdateInspector.ACTION_ADD_SORT_ORDER,
            "sort-order",
            state.sortOrder()));
    updates.add(
        Map.of(
            "action",
            CommitUpdateInspector.ACTION_SET_DEFAULT_SORT_ORDER,
            "sort-order-id",
            state.defaultSortOrderId()));
    if (!props.isEmpty()) {
      updates.add(Map.of("action", CommitUpdateInspector.ACTION_SET_PROPERTIES, "updates", props));
    }
    return List.copyOf(updates);
  }

  private static String blankToNull(String value) {
    return value == null || value.isBlank() ? null : value;
  }
}
