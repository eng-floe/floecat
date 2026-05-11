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

import ai.floedb.floecat.catalog.rpc.PartitionField;
import ai.floedb.floecat.catalog.rpc.PartitionSpecInfo;
import ai.floedb.floecat.gateway.iceberg.rest.common.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.common.RefPropertyUtil;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergRef;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSchema;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSortField;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSortOrder;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;

@ApplicationScoped
public class TableMetadataImportMapperSupport {

  TableMetadataImportService.ImportedMetadata toImportedMetadata(
      TableMetadata metadata, String metadataLocation) {
    Map<String, String> props = properties(metadata, metadataLocation);
    return new TableMetadataImportService.ImportedMetadata(
        SchemaParser.toJson(metadata.schema()),
        props,
        metadata.location(),
        toIcebergMetadata(metadata, metadataLocation),
        currentSnapshot(metadata, props),
        snapshots(metadata));
  }

  private Map<String, String> properties(TableMetadata metadata, String metadataLocation) {
    Map<String, String> props = new LinkedHashMap<>(metadata.properties());
    props.putIfAbsent("table-uuid", metadata.uuid());
    if (metadata.location() != null && !metadata.location().isBlank()) {
      props.put("location", metadata.location());
    }
    String refsProperty = encodeRefs(metadata.refs());
    if (refsProperty != null && !refsProperty.isBlank()) {
      props.put(RefPropertyUtil.PROPERTY_KEY, refsProperty);
    }
    props.put("format-version", Integer.toString(metadata.formatVersion()));
    putInt(props, "current-schema-id", metadata.currentSchemaId());
    putInt(props, "last-column-id", metadata.lastColumnId());
    putInt(props, "default-spec-id", metadata.defaultSpecId());
    putInt(props, "last-partition-id", metadata.lastAssignedPartitionId());
    putInt(props, "default-sort-order-id", metadata.defaultSortOrderId());
    putLong(props, "last-sequence-number", metadata.lastSequenceNumber());
    return props;
  }

  private TableMetadataImportService.ImportedSnapshot currentSnapshot(
      TableMetadata metadata, Map<String, String> props) {
    Snapshot current = metadata.currentSnapshot();
    if (current == null) {
      return null;
    }
    putLong(props, "current-snapshot-id", current.snapshotId());
    return toImportedSnapshot(current);
  }

  private List<TableMetadataImportService.ImportedSnapshot> snapshots(TableMetadata metadata) {
    List<TableMetadataImportService.ImportedSnapshot> snapshotList = new ArrayList<>();
    for (Snapshot snapshot : metadata.snapshots()) {
      snapshotList.add(toImportedSnapshot(snapshot));
    }
    return List.copyOf(snapshotList);
  }

  private TableMetadataImportService.ImportedSnapshot toImportedSnapshot(Snapshot snapshot) {
    return new TableMetadataImportService.ImportedSnapshot(
        snapshot.snapshotId(),
        snapshot.parentId(),
        snapshot.sequenceNumber(),
        snapshot.timestampMillis(),
        snapshot.manifestListLocation(),
        copySummaryWithOperation(snapshot),
        snapshot.schemaId());
  }

  private Map<String, String> copySummaryWithOperation(Snapshot snapshot) {
    Map<String, String> summary = snapshot.summary() == null ? Map.of() : snapshot.summary();
    if (summary.isEmpty() && (snapshot.operation() == null || snapshot.operation().isBlank())) {
      return Map.of();
    }
    LinkedHashMap<String, String> copy = new LinkedHashMap<>(summary);
    if (snapshot.operation() != null && !snapshot.operation().isBlank()) {
      copy.putIfAbsent("operation", snapshot.operation());
    }
    return Collections.unmodifiableMap(copy);
  }

  private void putInt(Map<String, String> props, String key, int value) {
    if (value >= 0) {
      props.put(key, Integer.toString(value));
    }
  }

  private void putLong(Map<String, String> props, String key, long value) {
    if (value >= 0) {
      props.put(key, Long.toString(value));
    }
  }

  private IcebergMetadata toIcebergMetadata(TableMetadata metadata, String metadataLocation) {
    IcebergMetadata.Builder builder =
        IcebergMetadata.newBuilder()
            .setTableUuid(metadata.uuid())
            .setFormatVersion(metadata.formatVersion())
            .setLastUpdatedMs(metadata.lastUpdatedMillis())
            .setLastColumnId(metadata.lastColumnId())
            .setCurrentSchemaId(metadata.currentSchemaId())
            .setDefaultSpecId(metadata.defaultSpecId())
            .setLastPartitionId(metadata.lastAssignedPartitionId())
            .setDefaultSortOrderId(metadata.defaultSortOrderId())
            .setLastSequenceNumber(metadata.lastSequenceNumber());
    Snapshot current = metadata.currentSnapshot();
    if (current != null) {
      builder.setCurrentSnapshotId(current.snapshotId());
    }

    for (Schema schema : metadata.schemas()) {
      IcebergSchema.Builder schemaBuilder =
          IcebergSchema.newBuilder()
              .setSchemaId(schema.schemaId())
              .setSchemaJson(SchemaParser.toJson(schema));
      if (schema.identifierFieldIds() != null && !schema.identifierFieldIds().isEmpty()) {
        schemaBuilder.addAllIdentifierFieldIds(schema.identifierFieldIds());
      }
      builder.addSchemas(schemaBuilder.build());
    }

    for (PartitionSpec spec : metadata.specs()) {
      PartitionSpecInfo.Builder specBuilder =
          PartitionSpecInfo.newBuilder().setSpecId(spec.specId());
      for (org.apache.iceberg.PartitionField field : spec.fields()) {
        specBuilder.addFields(
            PartitionField.newBuilder()
                .setFieldId(field.sourceId())
                .setName(field.name())
                .setTransform(field.transform().toString())
                .build());
      }
      builder.addPartitionSpecs(specBuilder.build());
    }

    for (SortOrder order : metadata.sortOrders()) {
      IcebergSortOrder.Builder orderBuilder =
          IcebergSortOrder.newBuilder().setSortOrderId(order.orderId());
      for (SortField field : order.fields()) {
        orderBuilder.addFields(
            IcebergSortField.newBuilder()
                .setSourceFieldId(field.sourceId())
                .setTransform(field.transform().toString())
                .setDirection(field.direction().name())
                .setNullOrder(field.nullOrder().name())
                .build());
      }
      builder.addSortOrders(orderBuilder.build());
    }

    for (Map.Entry<String, SnapshotRef> entry : metadata.refs().entrySet()) {
      SnapshotRef ref = entry.getValue();
      IcebergRef.Builder refBuilder =
          IcebergRef.newBuilder()
              .setSnapshotId(ref.snapshotId())
              .setType(ref.type().name().toLowerCase(Locale.ROOT));
      if (ref.maxRefAgeMs() != null) {
        refBuilder.setMaxReferenceAgeMs(ref.maxRefAgeMs());
      }
      if (ref.maxSnapshotAgeMs() != null) {
        refBuilder.setMaxSnapshotAgeMs(ref.maxSnapshotAgeMs());
      }
      if (ref.minSnapshotsToKeep() != null) {
        refBuilder.setMinSnapshotsToKeep(ref.minSnapshotsToKeep());
      }
      builder.putRefs(entry.getKey(), refBuilder.build());
    }
    return builder.build();
  }

  private String encodeRefs(Map<String, SnapshotRef> refs) {
    if (refs == null || refs.isEmpty()) {
      return null;
    }
    Map<String, Map<String, Object>> encoded = new LinkedHashMap<>();
    refs.forEach(
        (name, ref) -> {
          if (name == null || name.isBlank() || ref == null) {
            return;
          }
          Map<String, Object> entry = new LinkedHashMap<>();
          entry.put("snapshot-id", ref.snapshotId());
          entry.put("type", ref.type().name().toLowerCase(Locale.ROOT));
          if (ref.maxRefAgeMs() != null) {
            entry.put("max-ref-age-ms", ref.maxRefAgeMs());
          }
          if (ref.maxSnapshotAgeMs() != null) {
            entry.put("max-snapshot-age-ms", ref.maxSnapshotAgeMs());
          }
          if (ref.minSnapshotsToKeep() != null) {
            entry.put("min-snapshots-to-keep", ref.minSnapshotsToKeep());
          }
          encoded.put(name, entry);
        });
    return RefPropertyUtil.encode(encoded);
  }
}
