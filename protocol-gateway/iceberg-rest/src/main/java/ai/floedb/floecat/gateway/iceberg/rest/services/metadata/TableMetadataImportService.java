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
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.common.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.common.RefPropertyUtil;
import ai.floedb.floecat.gateway.iceberg.rest.common.TableMetadataBuilder;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableMetadataImportService {
  private static final Logger LOG = Logger.getLogger(TableMetadataImportService.class);
  @Inject CanonicalTableMetadataService canonicalTableMetadataService;

  public record ImportedMetadata(
      TableMetadata tableMetadata,
      TableMetadataView metadataView,
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
      List<String> manifestLists,
      Map<String, String> summary,
      Integer schemaId) {}

  public record ResolvedMetadata(
      TableMetadata tableMetadata,
      TableMetadataView metadataView,
      IcebergMetadata icebergMetadata) {}

  public ImportedMetadata importMetadata(
      String metadataLocation, Map<String, String> ioProperties) {
    if (metadataLocation == null || metadataLocation.isBlank()) {
      throw new IllegalArgumentException("metadata-location is required");
    }
    LOG.infof(
        "Importing Iceberg metadata location=%s fileIO=%s ioProps=%s",
        metadataLocation,
        ioProperties == null ? "<null>" : ioProperties.getOrDefault("io-impl", "<default>"),
        redactIoProperties(ioProperties));
    FileIO fileIO = null;
    try {
      fileIO = FileIoFactory.createFileIo(ioProperties, null, false);
      TableMetadata metadata = TableMetadataParser.read(fileIO, metadataLocation);
      TableMetadataView metadataView =
          canonicalTableMetadataService.toTableMetadataView(metadata, metadataLocation);
      String schemaJson = SchemaParser.toJson(metadata.schema());
      Map<String, String> props = new LinkedHashMap<>(metadata.properties());
      MetadataLocationUtil.setMetadataLocation(props, metadataLocation);
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
      Snapshot current = metadata.currentSnapshot();
      ImportedSnapshot importedSnapshot = null;
      if (current != null) {
        putLong(props, "current-snapshot-id", current.snapshotId());
        Map<String, String> summary = copySummaryWithOperation(current);
        importedSnapshot =
            new ImportedSnapshot(
                current.snapshotId(),
                current.parentId(),
                current.sequenceNumber(),
                current.timestampMillis(),
                manifestLists(current.manifestListLocation()),
                summary,
                current.schemaId());
      }
      List<ImportedSnapshot> snapshotList = new ArrayList<>();
      for (Snapshot snapshot : metadata.snapshots()) {
        Map<String, String> summary = copySummaryWithOperation(snapshot);
        snapshotList.add(
            new ImportedSnapshot(
                snapshot.snapshotId(),
                snapshot.parentId(),
                snapshot.sequenceNumber(),
                snapshot.timestampMillis(),
                manifestLists(snapshot.manifestListLocation()),
                summary,
                snapshot.schemaId()));
      }
      IcebergMetadata icebergMetadata =
          canonicalTableMetadataService.toIcebergMetadata(metadata, metadataLocation);
      return new ImportedMetadata(
          metadata,
          metadataView,
          schemaJson,
          props,
          metadata.location(),
          icebergMetadata,
          importedSnapshot,
          List.copyOf(snapshotList));
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      LOG.debugf(e, "Failed to read Iceberg metadata from %s", metadataLocation);
      throw new IllegalArgumentException(
          "Unable to read Iceberg metadata from " + metadataLocation, e);
    } finally {
      closeQuietly(fileIO);
    }
  }

  public ImportedMetadata importMetadata(
      Table table, IcebergMetadata metadata, Map<String, String> defaultFileIoProperties) {
    String metadataLocation = resolveMetadataLocation(table, metadata);
    if (metadataLocation == null || metadataLocation.isBlank()) {
      return null;
    }
    return importMetadata(metadataLocation, mergeIoProperties(table, defaultFileIoProperties));
  }

  public ResolvedMetadata resolveMetadata(
      String tableName,
      Table table,
      IcebergMetadata metadata,
      Map<String, String> defaultFileIoProperties,
      Supplier<List<ai.floedb.floecat.catalog.rpc.Snapshot>> snapshotsSupplier) {
    ImportedMetadata imported = importMetadata(table, metadata, defaultFileIoProperties);
    if (imported != null && imported.metadataView() != null) {
      return new ResolvedMetadata(
          imported.tableMetadata(), imported.metadataView(), imported.icebergMetadata());
    }
    List<ai.floedb.floecat.catalog.rpc.Snapshot> snapshots =
        snapshotsSupplier == null ? List.of() : snapshotsSupplier.get();
    Map<String, String> props =
        table == null ? Map.of() : new LinkedHashMap<>(table.getPropertiesMap());
    String metadataLocation = resolveMetadataLocation(table, metadata);
    TableMetadata tableMetadata = null;
    if (metadataLocation != null && !metadataLocation.isBlank()) {
      try {
        tableMetadata =
            canonicalTableMetadataService.bootstrapTableMetadata(
                tableName, table, props, metadata, snapshots);
      } catch (RuntimeException e) {
        LOG.debugf(
            e,
            "Unable to canonicalize fallback metadata table=%s location=%s",
            tableName,
            metadataLocation);
      }
    }
    TableMetadataView metadataView;
    if (tableMetadata != null) {
      metadataView =
          canonicalTableMetadataService.toTableMetadataView(tableMetadata, metadataLocation);
    } else {
      metadataView =
          TableMetadataBuilder.fromCatalog(
              tableName, table, props, metadata, snapshots == null ? List.of() : snapshots);
    }
    if (metadataView == null) {
      return new ResolvedMetadata(null, null, metadata);
    }
    return new ResolvedMetadata(tableMetadata, metadataView, metadata);
  }

  public ResolvedMetadata resolveMetadata(
      String tableName,
      Table table,
      TableGatewaySupport tableSupport,
      Supplier<List<ai.floedb.floecat.catalog.rpc.Snapshot>> snapshotsSupplier) {
    IcebergMetadata fallbackMetadata =
        tableSupport == null ? null : tableSupport.loadCurrentMetadata(table);
    Map<String, String> defaultFileIoProperties =
        tableSupport == null ? Map.of() : tableSupport.defaultFileIoProperties();
    IcebergMetadata currentMetadata =
        resolveCurrentIcebergMetadata(table, fallbackMetadata, defaultFileIoProperties);
    return resolveMetadata(
        tableName, table, currentMetadata, defaultFileIoProperties, snapshotsSupplier);
  }

  public IcebergMetadata resolveCurrentIcebergMetadata(
      Table table, IcebergMetadata fallbackMetadata, Map<String, String> defaultFileIoProperties) {
    ImportedMetadata imported = importMetadata(table, fallbackMetadata, defaultFileIoProperties);
    if (imported != null && imported.icebergMetadata() != null) {
      return imported.icebergMetadata();
    }
    return fallbackMetadata;
  }

  public IcebergMetadata resolveCurrentIcebergMetadata(
      Table table, TableGatewaySupport tableSupport) {
    if (tableSupport == null) {
      return null;
    }
    IcebergMetadata fallbackMetadata = tableSupport.loadCurrentMetadata(table);
    return resolveCurrentIcebergMetadata(
        table, fallbackMetadata, tableSupport.defaultFileIoProperties());
  }

  private String resolveMetadataLocation(Table table, IcebergMetadata metadata) {
    if (table != null && table.getPropertiesCount() > 0) {
      String propertyLocation = MetadataLocationUtil.metadataLocation(table.getPropertiesMap());
      if (propertyLocation != null && !propertyLocation.isBlank()) {
        return propertyLocation;
      }
    }
    if (metadata != null
        && metadata.getMetadataLocation() != null
        && !metadata.getMetadataLocation().isBlank()) {
      return metadata.getMetadataLocation();
    }
    return null;
  }

  private Map<String, String> mergeIoProperties(
      Table table, Map<String, String> defaultFileIoProperties) {
    Map<String, String> ioProps =
        defaultFileIoProperties == null
            ? new LinkedHashMap<>()
            : new LinkedHashMap<>(defaultFileIoProperties);
    if (table != null && table.getPropertiesCount() > 0) {
      ioProps.putAll(FileIoFactory.filterIoProperties(table.getPropertiesMap()));
    }
    return ioProps;
  }

  private void closeQuietly(FileIO fileIO) {
    if (fileIO instanceof AutoCloseable closable) {
      try {
        closable.close();
      } catch (Exception e) {
        LOG.debugf(e, "Failed to close FileIO %s", fileIO.getClass().getName());
      }
    }
  }

  private void putInt(Map<String, String> props, String key, int value) {
    if (value >= 0) {
      props.put(key, Integer.toString(value));
    }
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

  private void putLong(Map<String, String> props, String key, long value) {
    if (value >= 0) {
      props.put(key, Long.toString(value));
    }
  }

  private static List<String> manifestLists(String manifestList) {
    if (manifestList == null || manifestList.isBlank()) {
      return List.of();
    }
    return List.of(manifestList);
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

  private Map<String, String> redactIoProperties(Map<String, String> ioProperties) {
    if (ioProperties == null || ioProperties.isEmpty()) {
      return Map.of();
    }
    Map<String, String> redacted = new LinkedHashMap<>(ioProperties.size());
    ioProperties.forEach(
        (key, value) -> {
          if (key == null) {
            return;
          }
          String normalized = key.toLowerCase();
          if (normalized.contains("secret")
              || normalized.contains("token")
              || normalized.contains("key")) {
            redacted.put(key, "***");
          } else {
            redacted.put(key, value);
          }
        });
    return redacted;
  }
}
