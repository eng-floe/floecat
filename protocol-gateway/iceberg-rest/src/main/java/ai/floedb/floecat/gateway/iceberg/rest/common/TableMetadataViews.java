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

import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import java.util.List;
import java.util.Map;

public final class TableMetadataViews {
  private TableMetadataViews() {}

  public static Builder copy(TableMetadataView base) {
    return new Builder(base);
  }

  public static final class Builder {
    private Integer formatVersion;
    private String tableUuid;
    private String location;
    private String metadataLocation;
    private Long lastUpdatedMs;
    private Map<String, String> properties;
    private Integer lastColumnId;
    private Integer currentSchemaId;
    private Integer defaultSpecId;
    private Integer lastPartitionId;
    private Integer defaultSortOrderId;
    private Long currentSnapshotId;
    private Long lastSequenceNumber;
    private List<Map<String, Object>> schemas;
    private List<Map<String, Object>> partitionSpecs;
    private List<Map<String, Object>> sortOrders;
    private Map<String, Object> refs;
    private List<Map<String, Object>> snapshotLog;
    private List<Map<String, Object>> metadataLog;
    private List<Map<String, Object>> statistics;
    private List<Map<String, Object>> partitionStatistics;
    private List<Map<String, Object>> snapshots;

    private Builder(TableMetadataView base) {
      this.formatVersion = base.formatVersion();
      this.tableUuid = base.tableUuid();
      this.location = base.location();
      this.metadataLocation = base.metadataLocation();
      this.lastUpdatedMs = base.lastUpdatedMs();
      this.properties = base.properties();
      this.lastColumnId = base.lastColumnId();
      this.currentSchemaId = base.currentSchemaId();
      this.defaultSpecId = base.defaultSpecId();
      this.lastPartitionId = base.lastPartitionId();
      this.defaultSortOrderId = base.defaultSortOrderId();
      this.currentSnapshotId = base.currentSnapshotId();
      this.lastSequenceNumber = base.lastSequenceNumber();
      this.schemas = base.schemas();
      this.partitionSpecs = base.partitionSpecs();
      this.sortOrders = base.sortOrders();
      this.refs = base.refs();
      this.snapshotLog = base.snapshotLog();
      this.metadataLog = base.metadataLog();
      this.statistics = base.statistics();
      this.partitionStatistics = base.partitionStatistics();
      this.snapshots = base.snapshots();
    }

    public Builder formatVersion(Integer value) {
      this.formatVersion = value;
      return this;
    }

    public Builder location(String value) {
      this.location = value;
      return this;
    }

    public Builder metadataLocation(String value) {
      this.metadataLocation = value;
      return this;
    }

    public Builder properties(Map<String, String> value) {
      this.properties = value;
      return this;
    }

    public Builder lastColumnId(Integer value) {
      this.lastColumnId = value;
      return this;
    }

    public Builder currentSchemaId(Integer value) {
      this.currentSchemaId = value;
      return this;
    }

    public Builder defaultSpecId(Integer value) {
      this.defaultSpecId = value;
      return this;
    }

    public Builder lastPartitionId(Integer value) {
      this.lastPartitionId = value;
      return this;
    }

    public Builder defaultSortOrderId(Integer value) {
      this.defaultSortOrderId = value;
      return this;
    }

    public Builder currentSnapshotId(Long value) {
      this.currentSnapshotId = value;
      return this;
    }

    public Builder lastSequenceNumber(Long value) {
      this.lastSequenceNumber = value;
      return this;
    }

    public Builder schemas(List<Map<String, Object>> value) {
      this.schemas = value;
      return this;
    }

    public Builder partitionSpecs(List<Map<String, Object>> value) {
      this.partitionSpecs = value;
      return this;
    }

    public Builder sortOrders(List<Map<String, Object>> value) {
      this.sortOrders = value;
      return this;
    }

    public Builder snapshots(List<Map<String, Object>> value) {
      this.snapshots = value;
      return this;
    }

    public TableMetadataView build() {
      return new TableMetadataView(
          formatVersion,
          tableUuid,
          location,
          metadataLocation,
          lastUpdatedMs,
          properties,
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
}
