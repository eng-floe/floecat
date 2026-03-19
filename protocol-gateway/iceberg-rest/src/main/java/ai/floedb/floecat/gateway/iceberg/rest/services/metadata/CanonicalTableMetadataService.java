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
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.common.TableMetadataBuilder;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergBlobMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergEncryptedKey;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadataLogEntry;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergPartitionStatisticsFile;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergRef;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSchema;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSnapshotLogEntry;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSortField;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSortOrder;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergStatisticsFile;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.encryption.EncryptedKey;

@ApplicationScoped
public class CanonicalTableMetadataService {
  @Inject ObjectMapper mapper;

  public void setMapper(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  public TableMetadata toTableMetadata(TableMetadataView metadata, String metadataLocation) {
    if (metadata == null) {
      return null;
    }
    try {
      JsonNode node = mapper.valueToTree(metadata);
      return TableMetadataParser.fromJson(metadataLocation, node);
    } catch (RuntimeException e) {
      throw new MaterializeMetadataException("Unable to serialize Iceberg metadata", e);
    }
  }

  public TableMetadataView toTableMetadataView(TableMetadata metadata, String metadataLocation) {
    if (metadata == null) {
      return null;
    }
    try {
      IcebergMetadata icebergMetadata = toIcebergMetadata(metadata, metadataLocation);
      TableMetadataView view =
          TableMetadataBuilder.fromCanonicalMetadata(
              metadata.uuid(), icebergMetadata, toCatalogSnapshots(metadata));
      return metadataLocation == null || metadataLocation.isBlank()
          ? view
          : view.withMetadataLocation(metadataLocation);
    } catch (Exception e) {
      throw new MaterializeMetadataException("Unable to deserialize Iceberg metadata", e);
    }
  }

  public TableMetadata bootstrapTableMetadata(
      String tableName,
      Table table,
      Map<String, String> props,
      IcebergMetadata metadata,
      List<Snapshot> snapshots) {
    String metadataLocation =
        metadata == null || metadata.getMetadataLocation().isBlank()
            ? null
            : metadata.getMetadataLocation();
    if (props != null && !props.isEmpty()) {
      String propertyLocation = props.get("metadata-location");
      if (propertyLocation != null && !propertyLocation.isBlank()) {
        metadataLocation = propertyLocation;
      }
    }
    if (metadataLocation == null || metadataLocation.isBlank()) {
      return null;
    }
    TableMetadataView view =
        TableMetadataBuilder.fromCatalog(
            tableName,
            table,
            props == null ? Map.of() : new LinkedHashMap<>(props),
            metadata,
            snapshots == null ? List.of() : snapshots);
    if (view == null) {
      return null;
    }
    return toTableMetadata(view, metadataLocation);
  }

  public IcebergMetadata toIcebergMetadata(TableMetadata metadata, String metadataLocation) {
    if (metadata == null) {
      return null;
    }
    IcebergMetadata.Builder builder =
        IcebergMetadata.newBuilder()
            .setTableUuid(metadata.uuid())
            .setFormatVersion(metadata.formatVersion())
            .setLocation(metadata.location())
            .setLastUpdatedMs(metadata.lastUpdatedMillis())
            .setLastColumnId(metadata.lastColumnId())
            .setCurrentSchemaId(metadata.currentSchemaId())
            .setDefaultSpecId(metadata.defaultSpecId())
            .setLastPartitionId(metadata.lastAssignedPartitionId())
            .setDefaultSortOrderId(metadata.defaultSortOrderId())
            .setLastSequenceNumber(metadata.lastSequenceNumber())
            .setNextRowId(metadata.nextRowId());
    if (metadataLocation != null && !metadataLocation.isBlank()) {
      builder.setMetadataLocation(metadataLocation);
    }
    if (metadata.properties() != null && !metadata.properties().isEmpty()) {
      builder.putAllProperties(metadata.properties());
    }
    org.apache.iceberg.Snapshot current = metadata.currentSnapshot();
    if (current != null) {
      builder.setCurrentSnapshotId(current.snapshotId());
    }
    for (HistoryEntry entry : metadata.snapshotLog()) {
      builder.addSnapshotLog(
          IcebergSnapshotLogEntry.newBuilder()
              .setTimestampMs(entry.timestampMillis())
              .setSnapshotId(entry.snapshotId())
              .build());
    }
    for (TableMetadata.MetadataLogEntry entry : metadata.previousFiles()) {
      builder.addMetadataLog(
          IcebergMetadataLogEntry.newBuilder()
              .setTimestampMs(entry.timestampMillis())
              .setFile(entry.file())
              .build());
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

    for (var entry : metadata.refs().entrySet()) {
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
    for (StatisticsFile file : metadata.statisticsFiles()) {
      IcebergStatisticsFile.Builder statsBuilder =
          IcebergStatisticsFile.newBuilder()
              .setSnapshotId(file.snapshotId())
              .setStatisticsPath(file.path())
              .setFileSizeInBytes(file.fileSizeInBytes())
              .setFileFooterSizeInBytes(file.fileFooterSizeInBytes());
      for (BlobMetadata blob : file.blobMetadata()) {
        IcebergBlobMetadata.Builder blobBuilder =
            IcebergBlobMetadata.newBuilder()
                .setType(blob.type())
                .setSnapshotId(blob.sourceSnapshotId())
                .setSequenceNumber(blob.sourceSnapshotSequenceNumber())
                .addAllFields(blob.fields());
        if (blob.properties() != null && !blob.properties().isEmpty()) {
          blobBuilder.putAllProperties(blob.properties());
        }
        statsBuilder.addBlobMetadata(blobBuilder.build());
      }
      builder.addStatistics(statsBuilder.build());
    }
    for (PartitionStatisticsFile file : metadata.partitionStatisticsFiles()) {
      builder.addPartitionStatistics(
          IcebergPartitionStatisticsFile.newBuilder()
              .setSnapshotId(file.snapshotId())
              .setStatisticsPath(file.path())
              .setFileSizeInBytes(file.fileSizeInBytes())
              .build());
    }
    for (EncryptedKey key : metadata.encryptionKeys()) {
      IcebergEncryptedKey.Builder keyBuilder =
          IcebergEncryptedKey.newBuilder()
              .setKeyId(key.keyId())
              .setEncryptedKeyMetadata(ByteString.copyFrom(key.encryptedKeyMetadata()));
      if (key.encryptedById() != null && !key.encryptedById().isBlank()) {
        keyBuilder.setEncryptedById(key.encryptedById());
      }
      builder.addEncryptionKeys(keyBuilder.build());
    }

    return builder.build();
  }

  private List<ai.floedb.floecat.catalog.rpc.Snapshot> toCatalogSnapshots(TableMetadata metadata) {
    if (metadata.snapshots() == null || metadata.snapshots().isEmpty()) {
      return List.of();
    }
    List<ai.floedb.floecat.catalog.rpc.Snapshot> snapshots =
        new ArrayList<>(metadata.snapshots().size());
    for (org.apache.iceberg.Snapshot snapshot : metadata.snapshots()) {
      ai.floedb.floecat.catalog.rpc.Snapshot.Builder builder =
          ai.floedb.floecat.catalog.rpc.Snapshot.newBuilder().setSnapshotId(snapshot.snapshotId());
      if (snapshot.parentId() != null) {
        builder.setParentSnapshotId(snapshot.parentId());
      }
      builder.setUpstreamCreatedAt(Timestamps.fromMillis(snapshot.timestampMillis()));
      if (snapshot.sequenceNumber() > 0) {
        builder.setSequenceNumber(snapshot.sequenceNumber());
      }
      if (snapshot.manifestListLocation() != null && !snapshot.manifestListLocation().isBlank()) {
        builder.addManifestList(snapshot.manifestListLocation());
      }
      IcebergMetadata.Builder snapshotMetadata = IcebergMetadata.newBuilder();
      if (snapshot.summary() != null && !snapshot.summary().isEmpty()) {
        snapshotMetadata.putAllSummary(snapshot.summary());
      }
      if (snapshot.operation() != null && !snapshot.operation().isBlank()) {
        snapshotMetadata.setOperation(snapshot.operation());
      }
      if (snapshotMetadata.getSummaryCount() > 0 || snapshotMetadata.hasOperation()) {
        builder.putFormatMetadata("iceberg", snapshotMetadata.build().toByteString());
      }
      Integer schemaId = snapshot.schemaId();
      if (schemaId != null && schemaId >= 0) {
        builder.setSchemaId(schemaId);
      }
      snapshots.add(builder.build());
    }
    return List.copyOf(snapshots);
  }
}
