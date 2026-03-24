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

import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.asInteger;
import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.asLong;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.support.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.support.RefPropertyUtil;
import ai.floedb.floecat.gateway.iceberg.rest.table.IcebergMetadataService;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import java.util.Map;
import java.util.function.Function;

record CurrentTableState(
    Table table,
    IcebergMetadata metadata,
    String tableUuid,
    Integer currentSchemaId,
    Integer defaultSpecId,
    Integer defaultSortOrderId,
    Integer lastAssignedFieldId,
    Integer lastAssignedPartitionId,
    String metadataLocation) {

  static CurrentTableState load(
      Table table, TableGatewaySupport tableSupport, IcebergMetadataService metadataService) {
    IcebergMetadata metadata =
        metadataService == null
            ? null
            : metadataService.resolveCurrentIcebergMetadata(table, tableSupport);
    return from(table, metadata);
  }

  static CurrentTableState from(Table table, IcebergMetadata metadata) {
    return new CurrentTableState(
        table,
        metadata,
        resolveAuthoritativeTableUuid(table, metadata),
        resolveAuthoritativeInt(
            table,
            metadata,
            "current-schema-id",
            "current schema id",
            IcebergMetadata::getCurrentSchemaId),
        resolveAuthoritativeInt(
            table,
            metadata,
            "default-spec-id",
            "default spec id",
            IcebergMetadata::getDefaultSpecId),
        resolveAuthoritativeInt(
            table,
            metadata,
            "default-sort-order-id",
            "default sort order id",
            IcebergMetadata::getDefaultSortOrderId),
        resolveAuthoritativeInt(
            table,
            metadata,
            "last-column-id",
            "last assigned field id",
            IcebergMetadata::getLastColumnId),
        resolveAuthoritativeInt(
            table,
            metadata,
            "last-partition-id",
            "last assigned partition id",
            IcebergMetadata::getLastPartitionId),
        MetadataLocationUtil.resolveCurrentMetadataLocation(table, metadata));
  }

  Long refSnapshotId(String refName) {
    return resolveAuthoritativeRefSnapshotId(table, metadata, refName);
  }

  boolean snapshotRefExists(String refName) {
    Long snapshotId = refSnapshotId(refName);
    return snapshotId != null && snapshotId >= 0L;
  }

  private static Integer resolveAuthoritativeInt(
      Table table,
      IcebergMetadata metadata,
      String propertyKey,
      String fieldName,
      Function<IcebergMetadata, Integer> metadataExtractor) {
    Integer propertyValue =
        propertyInt(table == null ? null : table.getPropertiesMap(), propertyKey);
    if (metadata == null) {
      return propertyValue;
    }
    Integer metadataValue = metadataExtractor.apply(metadata);
    if (metadataValue == null || metadataValue < 0) {
      return propertyValue;
    }
    if (propertyValue != null && !metadataValue.equals(propertyValue)) {
      throw authoritativeMismatch(fieldName, propertyKey, metadataValue, propertyValue);
    }
    return metadataValue;
  }

  private static String resolveAuthoritativeTableUuid(Table table, IcebergMetadata metadata) {
    String propertyUuid = mirroredTableUuid(table);
    if (metadata == null || metadata.getTableUuid() == null || metadata.getTableUuid().isBlank()) {
      return propertyUuid;
    }
    String metadataUuid = metadata.getTableUuid();
    if (propertyUuid != null && !metadataUuid.equals(propertyUuid)) {
      throw authoritativeMismatch("table uuid", "table-uuid", metadataUuid, propertyUuid);
    }
    return metadataUuid;
  }

  private static Long resolveAuthoritativeRefSnapshotId(
      Table table, IcebergMetadata metadata, String refName) {
    Long propertySnapshotId = propertyRefSnapshotId(table, refName);
    Long metadataSnapshotId = metadataRefSnapshotId(metadata, refName);
    if (metadataSnapshotId != null) {
      if (propertySnapshotId != null && !metadataSnapshotId.equals(propertySnapshotId)) {
        String propertyKey =
            "main".equals(refName) ? "current-snapshot-id" : RefPropertyUtil.PROPERTY_KEY;
        throw authoritativeMismatch(
            "snapshot ref " + refName + " snapshot id",
            propertyKey,
            metadataSnapshotId,
            propertySnapshotId);
      }
      return metadataSnapshotId;
    }
    return propertySnapshotId;
  }

  private static Long metadataRefSnapshotId(IcebergMetadata metadata, String refName) {
    if (metadata == null) {
      return null;
    }
    if (metadata.getRefsMap().containsKey(refName)) {
      long snapshotId = metadata.getRefsOrThrow(refName).getSnapshotId();
      return snapshotId >= 0L ? snapshotId : null;
    }
    if ("main".equals(refName)) {
      long currentSnapshotId = metadata.getCurrentSnapshotId();
      return currentSnapshotId > 0L ? currentSnapshotId : null;
    }
    return null;
  }

  private static Long propertyRefSnapshotId(Table table, String refName) {
    if (table == null) {
      return null;
    }
    if ("main".equals(refName)) {
      return asLong(table.getPropertiesMap().get("current-snapshot-id"));
    }
    String encodedRefs = table.getPropertiesMap().get(RefPropertyUtil.PROPERTY_KEY);
    if (encodedRefs == null || encodedRefs.isBlank()) {
      return null;
    }
    Map<String, Map<String, Object>> refs = RefPropertyUtil.decode(encodedRefs);
    if (!refs.containsKey(refName)) {
      return null;
    }
    Long snapshotId = asLong(refs.get(refName).get("snapshot-id"));
    return snapshotId != null && snapshotId >= 0L ? snapshotId : null;
  }

  private static String mirroredTableUuid(Table table) {
    if (table == null) {
      return null;
    }
    String fromProps = table.getPropertiesMap().get("table-uuid");
    if (fromProps != null && !fromProps.isBlank()) {
      return fromProps;
    }
    return null;
  }

  private static IllegalStateException authoritativeMismatch(
      String fieldName, String propertyKey, Object metadataValue, Object propertyValue) {
    return new IllegalStateException(
        "backend mirrored property '"
            + propertyKey
            + "' diverged from authoritative Iceberg metadata for "
            + fieldName
            + " (metadata="
            + metadataValue
            + ", property="
            + propertyValue
            + ")");
  }

  private static Integer propertyInt(Map<String, String> props, String key) {
    if (props == null || key == null || key.isBlank()) {
      return null;
    }
    return asInteger(props.get(key));
  }
}
