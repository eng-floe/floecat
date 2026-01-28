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

package ai.floedb.floecat.gateway.iceberg.rest.api.metadata;

import ai.floedb.floecat.gateway.iceberg.rest.common.MetadataLocationUtil;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record TableMetadataView(
    @JsonProperty("format-version") Integer formatVersion,
    @JsonProperty("table-uuid") String tableUuid,
    @JsonProperty("location") String location,
    @JsonProperty(value = "metadata-location", access = JsonProperty.Access.WRITE_ONLY)
        String metadataLocation,
    @JsonProperty("last-updated-ms") Long lastUpdatedMs,
    @JsonProperty("properties") Map<String, String> properties,
    @JsonProperty("last-column-id") Integer lastColumnId,
    @JsonProperty("current-schema-id") Integer currentSchemaId,
    @JsonProperty("default-spec-id") Integer defaultSpecId,
    @JsonProperty("last-partition-id") Integer lastPartitionId,
    @JsonProperty("default-sort-order-id") Integer defaultSortOrderId,
    @JsonProperty("current-snapshot-id") Long currentSnapshotId,
    @JsonProperty("last-sequence-number") Long lastSequenceNumber,
    @JsonProperty("schemas") List<Map<String, Object>> schemas,
    @JsonProperty("partition-specs") List<Map<String, Object>> partitionSpecs,
    @JsonProperty("sort-orders") List<Map<String, Object>> sortOrders,
    @JsonProperty("refs") Map<String, Object> refs,
    @JsonProperty("snapshot-log") List<Map<String, Object>> snapshotLog,
    @JsonProperty("metadata-log") List<Map<String, Object>> metadataLog,
    @JsonProperty("statistics") List<Map<String, Object>> statistics,
    @JsonProperty("partition-statistics") List<Map<String, Object>> partitionStatistics,
    @JsonProperty("snapshots") List<Map<String, Object>> snapshots) {

  public TableMetadataView withMetadataLocation(String newLocation) {
    if (newLocation == null || newLocation.isBlank()) {
      return this;
    }
    Map<String, String> updatedProps =
        properties == null ? new LinkedHashMap<>() : new LinkedHashMap<>(properties);
    updatedProps.remove(MetadataLocationUtil.PRIMARY_KEY);
    return new TableMetadataView(
        formatVersion,
        tableUuid,
        location,
        newLocation,
        lastUpdatedMs,
        updatedProps,
        lastColumnId,
        currentSchemaId,
        defaultSpecId,
        lastPartitionId,
        defaultSortOrderId,
        currentSnapshotId,
        lastSequenceNumber,
        schemas,
        partitionSpecs,
        sortOrders,
        refs,
        snapshotLog,
        metadataLog,
        statistics,
        partitionStatistics,
        snapshots);
  }
}
