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

package ai.floedb.floecat.gateway.iceberg.rest.services.table.metadata;

import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.common.TableMetadataViews;
import java.util.List;
import java.util.Map;

public final class TableMetadataViewSupport {
  private TableMetadataViewSupport() {}

  public static MetadataCopyBuilder copyMetadata(TableMetadataView base) {
    return new MetadataCopyBuilder(base);
  }

  public static final class MetadataCopyBuilder {
    private final TableMetadataView base;
    private Integer formatVersion;
    private String location;
    private String metadataLocation;
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

    private MetadataCopyBuilder(TableMetadataView base) {
      this.base = base;
      this.formatVersion = base.formatVersion();
      this.location = base.location();
      this.metadataLocation = base.metadataLocation();
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

    public MetadataCopyBuilder formatVersion(Integer value) {
      this.formatVersion = value;
      return this;
    }

    public MetadataCopyBuilder location(String value) {
      this.location = value;
      return this;
    }

    public MetadataCopyBuilder metadataLocation(String value) {
      this.metadataLocation = value;
      return this;
    }

    public MetadataCopyBuilder properties(Map<String, String> value) {
      this.properties = value;
      return this;
    }

    public MetadataCopyBuilder lastColumnId(Integer value) {
      this.lastColumnId = value;
      return this;
    }

    public MetadataCopyBuilder currentSchemaId(Integer value) {
      this.currentSchemaId = value;
      return this;
    }

    public MetadataCopyBuilder defaultSpecId(Integer value) {
      this.defaultSpecId = value;
      return this;
    }

    public MetadataCopyBuilder lastPartitionId(Integer value) {
      this.lastPartitionId = value;
      return this;
    }

    public MetadataCopyBuilder defaultSortOrderId(Integer value) {
      this.defaultSortOrderId = value;
      return this;
    }

    public MetadataCopyBuilder currentSnapshotId(Long value) {
      this.currentSnapshotId = value;
      return this;
    }

    public MetadataCopyBuilder lastSequenceNumber(Long value) {
      this.lastSequenceNumber = value;
      return this;
    }

    public MetadataCopyBuilder schemas(List<Map<String, Object>> value) {
      this.schemas = value;
      return this;
    }

    public MetadataCopyBuilder partitionSpecs(List<Map<String, Object>> value) {
      this.partitionSpecs = value;
      return this;
    }

    public MetadataCopyBuilder sortOrders(List<Map<String, Object>> value) {
      this.sortOrders = value;
      return this;
    }

    public MetadataCopyBuilder refs(Map<String, Object> value) {
      this.refs = value;
      return this;
    }

    public MetadataCopyBuilder snapshotLog(List<Map<String, Object>> value) {
      this.snapshotLog = value;
      return this;
    }

    public MetadataCopyBuilder metadataLog(List<Map<String, Object>> value) {
      this.metadataLog = value;
      return this;
    }

    public MetadataCopyBuilder statistics(List<Map<String, Object>> value) {
      this.statistics = value;
      return this;
    }

    public MetadataCopyBuilder partitionStatistics(List<Map<String, Object>> value) {
      this.partitionStatistics = value;
      return this;
    }

    public MetadataCopyBuilder snapshots(List<Map<String, Object>> value) {
      this.snapshots = value;
      return this;
    }

    public TableMetadataView build() {
      return TableMetadataViews.copy(base)
          .formatVersion(formatVersion)
          .location(location)
          .metadataLocation(metadataLocation)
          .properties(properties)
          .lastColumnId(lastColumnId)
          .currentSchemaId(currentSchemaId)
          .defaultSpecId(defaultSpecId)
          .lastPartitionId(lastPartitionId)
          .defaultSortOrderId(defaultSortOrderId)
          .currentSnapshotId(currentSnapshotId)
          .lastSequenceNumber(lastSequenceNumber)
          .schemas(schemas)
          .partitionSpecs(partitionSpecs)
          .sortOrders(sortOrders)
          .refs(refs)
          .snapshotLog(snapshotLog)
          .metadataLog(metadataLog)
          .statistics(statistics)
          .partitionStatistics(partitionStatistics)
          .snapshots(snapshots)
          .build();
    }
  }
}
