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

package ai.floedb.floecat.gateway.iceberg.minimal.services.metadata;

import ai.floedb.floecat.catalog.rpc.PartitionField;
import ai.floedb.floecat.catalog.rpc.PartitionSpecInfo;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.minimal.api.request.TableCreateRequest;
import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergRef;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSchema;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSortField;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSortOrder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableMetadataImportService {
  private static final Logger LOG = Logger.getLogger(TableMetadataImportService.class);
  private final MinimalGatewayConfig config;

  @Inject
  public TableMetadataImportService(MinimalGatewayConfig config) {
    this.config = config;
  }

  public record ImportedMetadata(
      String schemaJson,
      Map<String, String> properties,
      String tableLocation,
      IcebergMetadata icebergMetadata,
      ImportedSnapshot currentSnapshot,
      List<ImportedSnapshot> snapshots) {}

  public record ImportedSnapshot(
      Long snapshotId,
      Long parentSnapshotId,
      Long sequenceNumber,
      Long timestampMs,
      String manifestList,
      Map<String, String> summary,
      Integer schemaId) {}

  public ImportedMetadata importMetadata(
      String metadataLocation, Map<String, String> ioProperties) {
    return importedMetadata(loadTableMetadata(metadataLocation, ioProperties), metadataLocation);
  }

  public ImportedMetadata writeInitialMetadata(
      TableCreateRequest request, String metadataLocation, Map<String, String> ioProperties) {
    TableMetadata finalMetadata = buildInitialTableMetadata(request, metadataLocation);
    FileIO fileIO = null;
    try {
      fileIO = FileIoFactory.createFileIo(ioProperties, config);
      TableMetadataParser.write(finalMetadata, fileIO.newOutputFile(metadataLocation));
      return importedMetadata(finalMetadata, metadataLocation);
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      String tableLocation = request == null ? null : request.location();
      LOG.errorf(
          e,
          "Failed to materialize staged Iceberg metadata location=%s tableLocation=%s ioProps=%s",
          metadataLocation,
          tableLocation,
          ioProperties == null ? Map.of() : ioProperties);
      throw new IllegalArgumentException("Unable to materialize staged Iceberg metadata", e);
    } finally {
      if (fileIO instanceof AutoCloseable closable) {
        try {
          closable.close();
        } catch (Exception ignored) {
        }
      }
    }
  }

  public ImportedMetadata buildInitialMetadata(
      TableCreateRequest request, String metadataLocation) {
    return importedMetadata(buildInitialTableMetadata(request, metadataLocation), metadataLocation);
  }

  public TableMetadata bootstrapMetadataFromCatalogTable(Table currentTable) {
    Schema schema;
    try {
      schema = SchemaParser.fromJson(currentTable.getSchemaJson());
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(
          "current table schema is required for metadata commits", e);
    }

    Map<String, String> properties = new LinkedHashMap<>(currentTable.getPropertiesMap());
    Integer requestedFormatVersion =
        parseRequestedFormatVersion(properties.remove("format-version"));
    String requestedUuid = blankToNull(properties.remove("table-uuid"));
    stripCanonicalMetadataProperties(properties);

    return materializeMetadata(
        schema,
        PartitionSpec.unpartitioned(),
        SortOrder.unsorted(),
        tableLocation(currentTable),
        properties,
        requestedFormatVersion,
        requestedUuid,
        "Unable to construct bootstrap Iceberg metadata");
  }

  public TableMetadata rebuildEmptyMetadataAtFormatVersion(
      TableMetadata metadata, int requestedFormatVersion) {
    if (metadata == null) {
      throw new IllegalArgumentException("metadata is required");
    }
    return materializeMetadata(
        metadata.schema(),
        metadata.spec(),
        metadata.sortOrder(),
        metadata.location(),
        new LinkedHashMap<>(metadata.properties()),
        requestedFormatVersion,
        blankToNull(metadata.uuid()),
        "Unable to rebuild bootstrap Iceberg metadata");
  }

  public Map<String, String> canonicalProperties(TableMetadata metadata, String metadataLocation) {
    Map<String, String> props = new LinkedHashMap<>(metadata.properties());
    props.put("metadata-location", metadataLocation);
    props.putIfAbsent("table-uuid", metadata.uuid());
    if (metadata.location() != null && !metadata.location().isBlank()) {
      props.put("location", metadata.location());
      props.put("storage_location", metadata.location());
    }
    props.put("format-version", Integer.toString(metadata.formatVersion()));
    putInt(props, "current-schema-id", metadata.currentSchemaId());
    putInt(props, "last-column-id", metadata.lastColumnId());
    putInt(props, "default-spec-id", metadata.defaultSpecId());
    putInt(props, "last-partition-id", metadata.lastAssignedPartitionId());
    putInt(props, "default-sort-order-id", metadata.defaultSortOrderId());
    putLong(props, "last-sequence-number", metadata.lastSequenceNumber());
    putLong(props, "last-updated-ms", metadata.lastUpdatedMillis());
    if (metadata.currentSnapshot() != null) {
      putLong(props, "current-snapshot-id", metadata.currentSnapshot().snapshotId());
    } else {
      props.remove("current-snapshot-id");
    }
    return Map.copyOf(props);
  }

  private TableMetadata buildInitialTableMetadata(
      TableCreateRequest request, String metadataLocation) {
    if (request == null) {
      throw new IllegalArgumentException("Request body is required");
    }
    if (metadataLocation == null || metadataLocation.isBlank()) {
      throw new IllegalArgumentException("metadata-location is required");
    }
    String tableLocation = request.location();
    if (tableLocation == null || tableLocation.isBlank()) {
      throw new IllegalArgumentException("location is required for stage-create");
    }

    Schema schema = SchemaParser.fromJson(request.schema().toString());
    PartitionSpec spec =
        request.partitionSpec() == null || request.partitionSpec().isNull()
            ? PartitionSpec.unpartitioned()
            : PartitionSpecParser.fromJson(schema, request.partitionSpec().toString());
    SortOrder sortOrder =
        request.writeOrder() == null || request.writeOrder().isNull()
            ? SortOrder.unsorted()
            : SortOrderParser.fromJson(schema, request.writeOrder().toString());

    Map<String, String> properties =
        request.properties() == null
            ? new LinkedHashMap<>()
            : new LinkedHashMap<>(request.properties());
    properties.putIfAbsent("metadata-location", metadataLocation);
    properties.putIfAbsent("last-sequence-number", "0");

    Integer requestedFormatVersion =
        parseRequestedFormatVersion(properties.remove("format-version"));
    String requestedUuid = blankToNull(properties.remove("table-uuid"));

    return materializeMetadata(
        schema,
        spec,
        sortOrder,
        tableLocation,
        properties,
        requestedFormatVersion,
        requestedUuid,
        "Unable to construct initial Iceberg metadata");
  }

  public TableMetadata loadTableMetadata(
      String metadataLocation, Map<String, String> ioProperties) {
    if (metadataLocation == null || metadataLocation.isBlank()) {
      throw new IllegalArgumentException("metadata-location is required");
    }
    FileIO fileIO = null;
    try {
      fileIO = FileIoFactory.createFileIo(ioProperties, config);
      return TableMetadataParser.read(fileIO, metadataLocation);
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      LOG.errorf(
          e,
          "Failed to read Iceberg metadata location=%s ioProps=%s",
          metadataLocation,
          ioProperties == null ? Map.of() : ioProperties);
      throw new IllegalArgumentException(
          "Unable to read Iceberg metadata from " + metadataLocation, e);
    } finally {
      if (fileIO instanceof AutoCloseable closable) {
        try {
          closable.close();
        } catch (Exception ignored) {
        }
      }
    }
  }

  private static void putInt(Map<String, String> props, String key, int value) {
    if (value >= 0) {
      props.put(key, Integer.toString(value));
    }
  }

  private static void putLong(Map<String, String> props, String key, long value) {
    if (value >= 0) {
      props.put(key, Long.toString(value));
    }
  }

  private ImportedMetadata importedMetadata(TableMetadata metadata, String metadataLocation) {
    String schemaJson = SchemaParser.toJson(metadata.schema());
    ImportedSnapshot currentSnapshot = null;
    if (metadata.currentSnapshot() != null) {
      currentSnapshot = toImportedSnapshot(metadata.currentSnapshot());
    }
    List<ImportedSnapshot> snapshots = new ArrayList<>();
    for (Snapshot snapshot : metadata.snapshots()) {
      snapshots.add(toImportedSnapshot(snapshot));
    }
    return new ImportedMetadata(
        schemaJson,
        canonicalProperties(metadata, metadataLocation),
        metadata.location(),
        toIcebergMetadata(metadata, metadataLocation),
        currentSnapshot,
        List.copyOf(snapshots));
  }

  private ImportedSnapshot toImportedSnapshot(Snapshot snapshot) {
    if (snapshot == null) {
      return null;
    }
    return new ImportedSnapshot(
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

  public IcebergMetadata toIcebergMetadata(TableMetadata metadata, String metadataLocation) {
    IcebergMetadata.Builder builder =
        IcebergMetadata.newBuilder()
            .setTableUuid(metadata.uuid())
            .setFormatVersion(metadata.formatVersion())
            .setMetadataLocation(metadataLocation)
            .setLastUpdatedMs(metadata.lastUpdatedMillis())
            .setLastColumnId(metadata.lastColumnId())
            .setCurrentSchemaId(metadata.currentSchemaId())
            .setDefaultSpecId(metadata.defaultSpecId())
            .setLastPartitionId(metadata.lastAssignedPartitionId())
            .setDefaultSortOrderId(metadata.defaultSortOrderId())
            .setLastSequenceNumber(metadata.lastSequenceNumber());
    if (metadata.currentSnapshot() != null) {
      builder.setCurrentSnapshotId(metadata.currentSnapshot().snapshotId());
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
    for (Map.Entry<String, org.apache.iceberg.SnapshotRef> entry : metadata.refs().entrySet()) {
      org.apache.iceberg.SnapshotRef ref = entry.getValue();
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

  private Integer parseRequestedFormatVersion(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException ignored) {
      return null;
    }
  }

  private String blankToNull(String value) {
    return value == null || value.isBlank() ? null : value;
  }

  private void stripCanonicalMetadataProperties(Map<String, String> properties) {
    properties.remove("metadata-location");
    properties.remove("last-updated-ms");
    properties.remove("last-column-id");
    properties.remove("current-schema-id");
    properties.remove("default-spec-id");
    properties.remove("last-partition-id");
    properties.remove("default-sort-order-id");
    properties.remove("last-sequence-number");
    properties.remove("current-snapshot-id");
    properties.remove("storage_location");
  }

  private String tableLocation(Table currentTable) {
    String location = currentTable.getPropertiesOrDefault("location", "");
    if (location != null && !location.isBlank()) {
      return location;
    }
    location = currentTable.getPropertiesOrDefault("storage_location", "");
    return location == null || location.isBlank() ? null : location;
  }

  private TableMetadata materializeMetadata(
      Schema schema,
      PartitionSpec spec,
      SortOrder sortOrder,
      String tableLocation,
      Map<String, String> properties,
      Integer requestedFormatVersion,
      String requestedUuid,
      String failureMessage) {
    try {
      Map<String, String> safeProperties = properties == null ? Map.of() : Map.copyOf(properties);
      var builder =
          requestedFormatVersion != null && requestedFormatVersion >= 1
              ? TableMetadata.buildFromEmpty(requestedFormatVersion)
              : TableMetadata.buildFrom(
                  TableMetadata.newTableMetadata(
                      schema, spec, sortOrder, tableLocation, safeProperties));
      if (requestedFormatVersion != null && requestedFormatVersion >= 1) {
        if (tableLocation != null && !tableLocation.isBlank()) {
          builder.setLocation(tableLocation);
        }
        if (!safeProperties.isEmpty()) {
          builder.setProperties(safeProperties);
        }
        builder.setCurrentSchema(schema, schema.highestFieldId());
        builder.setDefaultPartitionSpec(spec);
        builder.setDefaultSortOrder(sortOrder);
      } else if (requestedFormatVersion != null) {
        builder.upgradeFormatVersion(requestedFormatVersion);
      }
      if (requestedUuid != null) {
        builder.assignUUID(requestedUuid);
      }
      TableMetadata metadata = builder.build();
      if (requestedUuid == null && (metadata.uuid() == null || metadata.uuid().isBlank())) {
        builder = TableMetadata.buildFrom(metadata);
        builder.assignUUID(UUID.randomUUID().toString());
        metadata = builder.build();
      }
      return metadata;
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      LOG.errorf(e, "%s tableLocation=%s", failureMessage, tableLocation);
      throw new IllegalArgumentException(failureMessage, e);
    }
  }
}
