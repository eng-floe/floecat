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

package ai.floedb.floecat.gateway.iceberg.rest.table;

import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.asString;
import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.firstNonBlank;

import ai.floedb.floecat.catalog.rpc.PartitionField;
import ai.floedb.floecat.catalog.rpc.PartitionSpecInfo;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.support.CommitUpdateInspector;
import ai.floedb.floecat.gateway.iceberg.rest.support.FileIoFactory;
import ai.floedb.floecat.gateway.iceberg.rest.support.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.support.RefPropertyUtil;
import ai.floedb.floecat.gateway.iceberg.rest.support.SnapshotMetadataUtil;
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
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotRefParser;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.encryption.EncryptedKey;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.jboss.logging.Logger;

@ApplicationScoped
public class IcebergMetadataService {
  private static final Logger LOG = Logger.getLogger(IcebergMetadataService.class);
  private static final Set<String> SKIPPED_MATERIALIZE_SCHEMES = Set.of("floecat");
  @Inject IcebergGatewayConfig config;
  @Inject ObjectMapper mapper;
  @Inject TableGatewaySupport tableGatewaySupport;

  public void setMapper(ObjectMapper mapper) {
    this.mapper = mapper;
  }

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
      Integer schemaId) {
    public Map<String, Object> toSnapshotUpdate(String schemaJson) {
      Map<String, Object> snapshotMap = new LinkedHashMap<>();
      if (snapshotId != null) {
        snapshotMap.put("snapshot-id", snapshotId);
      }
      if (parentSnapshotId != null) {
        snapshotMap.put("parent-snapshot-id", parentSnapshotId);
      }
      if (sequenceNumber != null) {
        snapshotMap.put("sequence-number", sequenceNumber);
      }
      if (timestampMs != null) {
        snapshotMap.put("timestamp-ms", timestampMs);
      }
      String manifestList = SnapshotMetadataUtil.firstManifestList(manifestLists);
      if (manifestList != null) {
        snapshotMap.put("manifest-list", manifestList);
      }
      if (summary != null && !summary.isEmpty()) {
        snapshotMap.put("summary", summary);
      }
      if (schemaId != null) {
        snapshotMap.put("schema-id", schemaId);
      }
      if (schemaJson != null && !schemaJson.isBlank()) {
        snapshotMap.put("schema-json", schemaJson);
      }
      return snapshotMap;
    }
  }

  public record ResolvedMetadata(
      TableMetadata tableMetadata,
      TableMetadataView metadataView,
      IcebergMetadata icebergMetadata) {}

  public static final class MaterializeResult {
    private final String metadataLocation;
    private final TableMetadata tableMetadata;

    public MaterializeResult(String metadataLocation) {
      this(metadataLocation, null);
    }

    public MaterializeResult(String metadataLocation, TableMetadata tableMetadata) {
      this.metadataLocation = metadataLocation;
      this.tableMetadata = tableMetadata;
    }

    public String metadataLocation() {
      return metadataLocation;
    }

    public TableMetadata tableMetadata() {
      return tableMetadata;
    }
  }

  public enum MaterializeMode {
    EXACT_LOCATION,
    NEXT_VERSION
  }

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
      TableMetadataView metadataView = toTableMetadataView(metadata, metadataLocation);
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
      IcebergMetadata icebergMetadata = toIcebergMetadata(metadata, metadataLocation);
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
      FileIoFactory.closeQuietly(fileIO);
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
        tableMetadata = bootstrapTableMetadata(tableName, table, props, metadata, snapshots);
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
      metadataView = toTableMetadataView(tableMetadata, metadataLocation);
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
    Map<String, String> defaultFileIoProperties =
        tableSupport == null ? Map.of() : tableSupport.defaultFileIoProperties();
    IcebergMetadata currentMetadata =
        resolveCurrentIcebergMetadata(table, null, defaultFileIoProperties);
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
    Map<String, String> defaultFileIoProperties =
        tableSupport == null ? Map.of() : tableSupport.defaultFileIoProperties();
    return resolveCurrentIcebergMetadata(table, null, defaultFileIoProperties);
  }

  public MaterializeResult materializeAtExactLocation(
      String namespaceFq, String tableName, TableMetadata metadata, String metadataLocation) {
    return materialize(
        namespaceFq, tableName, metadata, MaterializeMode.EXACT_LOCATION, metadataLocation);
  }

  public MaterializeResult materializeNextVersion(
      String namespaceFq, String tableName, TableMetadata metadata) {
    return materialize(namespaceFq, tableName, metadata, MaterializeMode.NEXT_VERSION, null);
  }

  private MaterializeResult materialize(
      String namespaceFq,
      String tableName,
      TableMetadata metadata,
      MaterializeMode mode,
      String metadataLocation) {
    if (metadata == null) {
      LOG.debugf(
          "Skipping metadata materialization for %s.%s because commit metadata was empty",
          namespaceFq, tableName);
      return new MaterializeResult(metadataLocation, null);
    }
    String requestedLocation =
        mode == MaterializeMode.EXACT_LOCATION ? metadataLocation : metadata.metadataFileLocation();
    if (requestedLocation != null && !shouldMaterialize(requestedLocation)) {
      LOG.debugf(
          "Skipping metadata materialization for %s.%s because metadata-location was %s",
          namespaceFq, tableName, requestedLocation);
      return canonicalizeMaterializeResult(requestedLocation, metadata);
    }
    String resolvedLocation = null;
    FileIO fileIO = null;
    try {
      Map<String, String> props = new LinkedHashMap<>();
      if (tableGatewaySupport != null) {
        props.putAll(tableGatewaySupport.defaultFileIoProperties());
      }
      props.putAll(sanitizeProperties(metadata.properties()));
      fileIO = FileIoFactory.createFileIo(props, config, true);
      resolvedLocation = resolveMaterializedLocation(fileIO, requestedLocation, metadata, mode);
      if (resolvedLocation == null || resolvedLocation.isBlank()) {
        LOG.debugf(
            "Skipping metadata materialization for %s.%s because metadata-location was unavailable",
            namespaceFq, tableName);
        return canonicalizeMaterializeResult(requestedLocation, metadata);
      }
      TableMetadata parsed = withMetadataLocation(metadata, resolvedLocation);
      writeMetadata(fileIO, resolvedLocation, parsed);
      LOG.infof(
          "Materialized Iceberg metadata files for %s.%s to %s",
          namespaceFq, tableName, resolvedLocation);
      return new MaterializeResult(resolvedLocation, parsed);
    } catch (Exception e) {
      LOG.warnf(
          e,
          "Materialize metadata failure namespace=%s table=%s target=%s",
          namespaceFq,
          tableName,
          resolvedLocation == null ? requestedLocation : resolvedLocation);
      throw new IllegalArgumentException(
          "Failed to materialize Iceberg metadata files to " + resolvedLocation, e);
    } finally {
      FileIoFactory.closeQuietly(fileIO);
    }
  }

  public String reserveCreateMetadataLocation(
      String tableLocation, String tableUuid, Map<String, String> fileIoProperties) {
    if (!hasText(tableLocation) || !hasText(tableUuid)) {
      throw new IllegalArgumentException(
          "stage-create requires table location and table uuid to reserve metadata location");
    }
    String directory = normalizeMetadataDirectory(tableLocation + "/metadata/");
    if (!hasText(directory)) {
      throw new IllegalStateException(
          "Unable to reserve stage-create metadata location for table location " + tableLocation);
    }
    FileIO fileIO = null;
    try {
      Map<String, String> props =
          fileIoProperties == null ? new LinkedHashMap<>() : new LinkedHashMap<>(fileIoProperties);
      fileIO = FileIoFactory.createFileIo(props, config, true);
      long nextVersion = nextMetadataVersion(fileIO, directory, null);
      return directory + metadataFileName(nextVersion, tableUuid);
    } catch (RuntimeException e) {
      LOG.debugf(
          e,
          "Unable to reserve stage-create metadata location for tableLocation=%s",
          tableLocation);
      throw new IllegalStateException(
          "Unable to reserve stage-create metadata location for " + tableLocation, e);
    } finally {
      FileIoFactory.closeQuietly(fileIO);
    }
  }

  MaterializeResult canonicalizeMaterializeResult(String metadataLocation, TableMetadata metadata) {
    if (metadata == null) {
      return new MaterializeResult(metadataLocation, null);
    }
    TableMetadata canonical =
        hasText(metadataLocation) ? withMetadataLocation(metadata, metadataLocation) : metadata;
    return new MaterializeResult(metadataLocation, canonical);
  }

  private String resolveMetadataLocation(Table table, IcebergMetadata metadata) {
    return MetadataLocationUtil.resolveCurrentMetadataLocation(table, metadata);
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

  private boolean shouldMaterialize(String metadataLocation) {
    if (metadataLocation == null || metadataLocation.isBlank()) {
      return false;
    }
    try {
      URI uri = URI.create(metadataLocation);
      String scheme = uri.getScheme();
      if (scheme == null) {
        return false;
      }
      return !SKIPPED_MATERIALIZE_SCHEMES.contains(scheme.toLowerCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      LOG.infof(
          "Skipping metadata materialization because metadata-location %s is invalid",
          metadataLocation);
      return false;
    }
  }

  private void writeMetadata(FileIO fileIO, String location, TableMetadata metadata) {
    LOG.infof("Writing Iceberg metadata file %s", location == null ? "<null>" : location);
    TableMetadataParser.write(metadata, fileIO.newOutputFile(location));
    LOG.infof(
        "Successfully wrote Iceberg metadata file %s", location == null ? "<null>" : location);
  }

  private static boolean hasText(String value) {
    return value != null && !value.isBlank();
  }

  private Map<String, String> sanitizeProperties(Map<String, String> props) {
    Map<String, String> sanitized = new LinkedHashMap<>();
    if (props != null && !props.isEmpty()) {
      props.forEach(
          (key, value) -> {
            if (key != null && value != null) {
              sanitized.put(key, value);
            }
          });
    }
    return sanitized;
  }

  private String resolveMaterializedLocation(
      FileIO fileIO, String metadataLocation, TableMetadata metadata, MaterializeMode mode) {
    if (mode == MaterializeMode.EXACT_LOCATION && isExactMetadataPath(metadataLocation)) {
      return metadataLocation;
    }
    return resolveVersionedLocation(fileIO, metadataLocation, metadata);
  }

  private String resolveVersionedLocation(
      FileIO fileIO, String metadataLocation, TableMetadata metadata) {
    String directory = null;
    if (metadataLocation != null) {
      if (metadataLocation.endsWith("/")) {
        directory = metadataLocation;
      } else {
        directory = directoryOf(metadataLocation);
      }
    }
    if (directory == null) {
      directory = metadataDirectory(metadata);
    }
    directory = normalizeMetadataDirectory(directory);
    if (directory == null || directory.isBlank()) {
      return null;
    }
    if (!directory.endsWith("/")) {
      directory = directory + "/";
    }
    return directory + nextMetadataFileName(fileIO, directory, metadata);
  }

  private String directoryOf(String metadataLocation) {
    if (metadataLocation == null || metadataLocation.isBlank()) {
      return null;
    }
    String trimmed = metadataLocation;
    while (trimmed.endsWith("/") && trimmed.length() > 1) {
      trimmed = trimmed.substring(0, trimmed.length() - 1);
    }
    int slash = trimmed.lastIndexOf('/');
    if (slash < 0) {
      return null;
    }
    return trimmed.substring(0, slash + 1);
  }

  private String metadataDirectory(TableMetadata metadata) {
    if (metadata == null) {
      return null;
    }
    String location = firstNonBlank(metadata.metadataFileLocation(), metadata.location());
    String directory = metadataDirectoryFromLocation(location);
    if (directory != null) {
      return directory;
    }

    Map<String, String> props = metadata.properties();
    if (props != null && !props.isEmpty()) {
      String candidate = firstNonBlank(props.get("metadata-location"), props.get("location"));
      directory = metadataDirectoryFromLocation(candidate);
      if (directory != null) {
        return directory;
      }
    }

    directory = directoryFromMetadataLog(metadata);
    if (directory != null) {
      return directory;
    }

    location = firstNonBlank(location, props == null ? null : props.get("location"));
    if (location == null || location.isBlank()) {
      return null;
    }
    String base = location.endsWith("/") ? location.substring(0, location.length() - 1) : location;
    return base + "/metadata/";
  }

  private String metadataDirectoryFromLocation(String location) {
    if (location == null || location.isBlank()) {
      return null;
    }
    if (looksLikeMetadataPath(location)) {
      return directoryOf(location);
    }
    return null;
  }

  private boolean looksLikeMetadataPath(String location) {
    if (location == null || location.isBlank()) {
      return false;
    }
    if (location.contains("/metadata/")) {
      return true;
    }
    return location.endsWith(".metadata.json");
  }

  private String directoryFromMetadataLog(TableMetadata metadata) {
    if (metadata == null || metadata.previousFiles() == null) {
      return null;
    }
    for (TableMetadata.MetadataLogEntry entry : metadata.previousFiles()) {
      String file = entry == null ? null : entry.file();
      if (file != null && !file.isBlank()) {
        String directory = directoryOf(file);
        if (directory != null) {
          return directory;
        }
      }
    }
    return null;
  }

  private String nextMetadataFileName(FileIO fileIO, String directory, TableMetadata metadata) {
    long nextVersion = nextMetadataVersion(fileIO, directory, metadata);
    return metadataFileName(nextVersion, UUID.randomUUID().toString());
  }

  private String metadataFileName(long version, String suffix) {
    return String.format("%05d-%s.metadata.json", version, suffix);
  }

  private long nextMetadataVersion(FileIO fileIO, String directory, TableMetadata metadata) {
    long max = parseExistingFiles(fileIO, directory);
    if (metadata != null && metadata.previousFiles() != null) {
      for (TableMetadata.MetadataLogEntry entry : metadata.previousFiles()) {
        String file = entry == null ? null : entry.file();
        long parsed = parseVersion(file);
        if (parsed > max) {
          max = parsed;
        }
      }
    }
    return max < 0 ? 0 : max + 1;
  }

  private TableMetadata withMetadataLocation(TableMetadata metadata, String metadataLocation) {
    if (metadata == null || !hasText(metadataLocation)) {
      return metadata;
    }
    Map<String, String> props = new LinkedHashMap<>();
    if (metadata.properties() != null && !metadata.properties().isEmpty()) {
      props.putAll(metadata.properties());
    }
    props.put("metadata-location", metadataLocation);
    return TableMetadata.buildFrom(metadata)
        .discardChanges()
        .setProperties(props)
        .withMetadataLocation(metadataLocation)
        .build();
  }

  private long parseExistingFiles(FileIO fileIO, String directory) {
    if (!(fileIO instanceof SupportsPrefixOperations prefixOps)) {
      return -1;
    }
    long max = -1;
    try {
      for (FileInfo info : prefixOps.listPrefix(directory)) {
        String location = info.location();
        if (location != null && location.endsWith(".metadata.json")) {
          long parsed = parseVersion(location);
          if (parsed > max) {
            max = parsed;
          }
        }
      }
    } catch (UnsupportedOperationException e) {
      return -1;
    }
    return max;
  }

  private long parseVersion(String file) {
    if (file == null || file.isBlank()) {
      return -1;
    }
    int slash = file.lastIndexOf('/');
    String name = slash >= 0 ? file.substring(slash + 1) : file;
    int dash = name.indexOf('-');
    if (dash <= 0) {
      return -1;
    }
    try {
      return Long.parseLong(name.substring(0, dash));
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  private String normalizeMetadataDirectory(String directory) {
    if (directory == null || directory.isBlank()) {
      return null;
    }
    String normalized = directory;
    while (normalized.endsWith("/") && normalized.length() > 1) {
      normalized = normalized.substring(0, normalized.length() - 1);
    }
    if (normalized.endsWith(".metadata.json")) {
      int slash = normalized.lastIndexOf('/');
      if (slash >= 0) {
        normalized = normalized.substring(0, slash);
      } else {
        return null;
      }
    }
    return normalized.endsWith("/") ? normalized : normalized + "/";
  }

  private boolean isExactMetadataPath(String metadataLocation) {
    return hasText(metadataLocation)
        && looksLikeMetadataPath(metadataLocation)
        && !metadataLocation.endsWith("/");
  }

  public TableMetadata toTableMetadata(TableMetadataView metadata, String metadataLocation) {
    if (metadata == null) {
      return null;
    }
    try {
      JsonNode node = mapper.valueToTree(metadata);
      return TableMetadataParser.fromJson(metadataLocation, node);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Unable to serialize Iceberg metadata", e);
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
      throw new IllegalArgumentException("Unable to deserialize Iceberg metadata", e);
    }
  }

  public TableMetadata bootstrapTableMetadata(
      String tableName,
      Table table,
      Map<String, String> props,
      IcebergMetadata metadata,
      List<ai.floedb.floecat.catalog.rpc.Snapshot> snapshots) {
    Map<String, String> effectiveProps =
        props == null ? new LinkedHashMap<>() : new LinkedHashMap<>(props);
    String tableUuid =
        firstNonBlank(
            effectiveProps.get("table-uuid"),
            table != null && table.hasResourceId() ? table.getResourceId().getId() : null,
            tableName);
    String tableLocation =
        firstNonBlank(
            metadata == null ? null : metadata.getLocation(),
            table != null && table.hasUpstream() ? table.getUpstream().getUri() : null,
            effectiveProps.get("location"));
    String metadataLocation =
        MetadataLocationUtil.resolveOrBootstrapMetadataLocation(
            effectiveProps,
            metadata == null || metadata.getMetadataLocation().isBlank()
                ? null
                : metadata.getMetadataLocation(),
            tableLocation,
            tableUuid);
    if (tableLocation != null && !tableLocation.isBlank()) {
      effectiveProps.putIfAbsent("location", tableLocation);
    }
    if (tableUuid != null && !tableUuid.isBlank()) {
      effectiveProps.putIfAbsent("table-uuid", tableUuid);
    }
    if (firstNonBlank(tableLocation, effectiveProps.get("location")) == null) {
      return null;
    }
    TableMetadataView view =
        TableMetadataBuilder.fromCatalog(
            tableName, table, effectiveProps, metadata, snapshots == null ? List.of() : snapshots);
    if (view == null) {
      return null;
    }
    TableMetadata tableMetadata = toTableMetadata(view, metadataLocation);
    if (tableMetadata == null) {
      return null;
    }
    if (metadataLocation != null
        && !metadataLocation.isBlank()
        && (tableMetadata.metadataFileLocation() == null
            || tableMetadata.metadataFileLocation().isBlank())) {
      tableMetadata =
          TableMetadata.buildFrom(tableMetadata)
              .discardChanges()
              .withMetadataLocation(metadataLocation)
              .build();
    }
    return tableMetadata;
  }

  public TableMetadata bootstrapTableMetadataFromCommit(Table table, TableRequests.Commit request) {
    if (table == null
        || request == null
        || request.updates() == null
        || request.updates().isEmpty()) {
      return null;
    }

    Map<String, String> props = new LinkedHashMap<>(table.getPropertiesMap());
    Schema schema = null;
    Integer currentSchemaId = null;
    PartitionSpec spec = null;
    Integer defaultSpecId = null;
    SortOrder sortOrder = null;
    Integer defaultSortOrderId = null;
    String location = firstNonBlank(props.get("location"), null);

    for (Map<String, Object> update : request.updates()) {
      if (update == null) {
        continue;
      }
      CommitUpdateInspector.UpdateAction action = CommitUpdateInspector.actionTypeOf(update);
      if (action == null) {
        continue;
      }
      switch (action) {
        case UPGRADE_FORMAT_VERSION -> {
          Integer requested = asInteger(update.get("format-version"));
          if (requested != null && requested > 0) {
            props.put("format-version", Integer.toString(requested));
          }
        }
        case SET_LOCATION -> {
          String requestedLocation = asString(update.get("location"));
          if (requestedLocation != null && !requestedLocation.isBlank()) {
            location = requestedLocation;
            props.put("location", requestedLocation);
          }
        }
        case ADD_SCHEMA -> {
          @SuppressWarnings("unchecked")
          Map<String, Object> schemaMap =
              update.get("schema") instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
          Schema parsed = parseSchema(schemaMap);
          if (parsed != null) {
            schema = parsed;
            currentSchemaId = parsed.schemaId();
          }
        }
        case SET_CURRENT_SCHEMA -> {
          Integer schemaId = asInteger(update.get("schema-id"));
          if (schemaId != null && schemaId >= 0) {
            currentSchemaId = schemaId;
          }
        }
        case ADD_SPEC -> {
          @SuppressWarnings("unchecked")
          Map<String, Object> specMap =
              update.get("spec") instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
          PartitionSpec parsed = parsePartitionSpec(specMap, schema);
          if (parsed != null) {
            spec = parsed;
            defaultSpecId = parsed.specId();
          }
        }
        case SET_DEFAULT_SPEC -> {
          Integer specId = asInteger(update.get("spec-id"));
          if (specId != null && specId >= 0) {
            defaultSpecId = specId;
          }
        }
        case ADD_SORT_ORDER -> {
          @SuppressWarnings("unchecked")
          Map<String, Object> sortOrderMap =
              update.get("sort-order") instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
          SortOrder parsed = parseSortOrder(sortOrderMap, schema);
          if (parsed != null) {
            sortOrder = parsed;
            defaultSortOrderId = parsed.orderId();
          }
        }
        case SET_DEFAULT_SORT_ORDER -> {
          Integer sortOrderId = asInteger(update.get("sort-order-id"));
          if (sortOrderId != null && sortOrderId >= 0) {
            defaultSortOrderId = sortOrderId;
          }
        }
        default -> {
          // ignore
        }
      }
    }

    if (schema == null) {
      return null;
    }
    if (currentSchemaId != null && currentSchemaId >= 0) {
      props.put("current-schema-id", Integer.toString(currentSchemaId));
    }
    if (spec == null) {
      spec = PartitionSpec.unpartitioned();
      defaultSpecId = spec.specId();
    }
    if (defaultSpecId != null && defaultSpecId >= 0) {
      props.put("default-spec-id", Integer.toString(defaultSpecId));
    }
    if (sortOrder == null) {
      sortOrder = SortOrder.unsorted();
      defaultSortOrderId = sortOrder.orderId();
    }
    if (defaultSortOrderId != null && defaultSortOrderId >= 0) {
      props.put("default-sort-order-id", Integer.toString(defaultSortOrderId));
    }
    if (location == null || location.isBlank()) {
      return null;
    }

    TableMetadata metadata =
        TableMetadata.newTableMetadata(schema, spec, sortOrder, location, props);
    String metadataLocation = MetadataLocationUtil.metadataLocation(props);
    if (metadataLocation != null && !metadataLocation.isBlank()) {
      metadata =
          TableMetadata.buildFrom(metadata)
              .discardChanges()
              .withMetadataLocation(metadataLocation)
              .build();
    }
    return metadata;
  }

  public TableMetadata applyCommitUpdates(
      TableMetadata metadata, Table table, TableRequests.Commit request) {
    if (metadata == null
        || request == null
        || request.updates() == null
        || request.updates().isEmpty()) {
      return metadata;
    }
    CommitUpdateInspector.Parsed parsed = CommitUpdateInspector.inspect(request);
    TableMetadata.Builder builder = TableMetadata.buildFrom(metadata).discardChanges();
    String metadataLocation =
        firstNonBlank(
            metadata.metadataFileLocation(), table.getPropertiesMap().get("metadata-location"));
    if (metadataLocation != null && !metadataLocation.isBlank()) {
      builder.withMetadataLocation(metadataLocation);
    }
    Map<String, String> mergedProperties = new LinkedHashMap<>(metadata.properties());
    mergedProperties.putAll(table.getPropertiesMap());
    builder.setProperties(mergedProperties);

    Map<Integer, Schema> schemasById = new LinkedHashMap<>(metadata.schemasById());
    Map<Long, org.apache.iceberg.Snapshot> snapshotsById = new LinkedHashMap<>();
    for (org.apache.iceberg.Snapshot snapshot : metadata.snapshots()) {
      if (snapshot != null) {
        snapshotsById.put(snapshot.snapshotId(), snapshot);
      }
    }
    int currentFormatVersion = metadata.formatVersion();
    long lastSequenceNumber = Math.max(0L, metadata.lastSequenceNumber());
    Integer currentSchemaId = metadata.currentSchemaId();
    Integer lastAddedSchemaId = null;
    Integer lastAddedSpecId = null;
    Integer lastAddedSortOrderId = null;

    for (Map<String, Object> update : request.updates()) {
      if (update == null) {
        continue;
      }
      CommitUpdateInspector.UpdateAction action = CommitUpdateInspector.actionTypeOf(update);
      if (action == null) {
        continue;
      }
      switch (action) {
        case UPGRADE_FORMAT_VERSION -> {
          Integer requested = asInteger(update.get("format-version"));
          if (requested != null) {
            builder.upgradeFormatVersion(requested);
            currentFormatVersion = requested;
          }
        }
        case SET_LOCATION -> {
          String requestedLocation = asString(update.get("location"));
          if (requestedLocation != null && !requestedLocation.isBlank()) {
            builder.setLocation(requestedLocation);
          }
        }
        case ADD_SCHEMA -> {
          @SuppressWarnings("unchecked")
          Map<String, Object> schemaMap =
              update.get("schema") instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
          Schema schema = parseSchema(schemaMap);
          if (schema != null) {
            builder.addSchema(schema);
            schemasById.put(schema.schemaId(), schema);
            if (schema.schemaId() >= 0) {
              lastAddedSchemaId = schema.schemaId();
            }
          }
        }
        case SET_CURRENT_SCHEMA -> {
          Integer schemaId =
              resolveLastAddedId(asInteger(update.get("schema-id")), lastAddedSchemaId);
          if (schemaId != null) {
            builder.setCurrentSchema(schemaId);
            currentSchemaId = schemaId;
          }
        }
        case ADD_SPEC -> {
          @SuppressWarnings("unchecked")
          Map<String, Object> specMap =
              update.get("spec") instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
          PartitionSpec spec =
              parsePartitionSpec(
                  specMap, activeSchema(schemasById, currentSchemaId, lastAddedSchemaId));
          if (spec != null) {
            builder.addPartitionSpec(spec);
            if (spec.specId() >= 0) {
              lastAddedSpecId = spec.specId();
            }
          }
        }
        case SET_DEFAULT_SPEC -> {
          Integer specId = resolveLastAddedId(asInteger(update.get("spec-id")), lastAddedSpecId);
          if (specId != null) {
            builder.setDefaultPartitionSpec(specId);
          }
        }
        case ADD_SORT_ORDER -> {
          @SuppressWarnings("unchecked")
          Map<String, Object> sortOrderMap =
              update.get("sort-order") instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
          SortOrder sortOrder =
              parseSortOrder(
                  sortOrderMap, activeSchema(schemasById, currentSchemaId, lastAddedSchemaId));
          if (sortOrder != null) {
            builder.addSortOrder(sortOrder);
            if (sortOrder.orderId() >= 0) {
              lastAddedSortOrderId = sortOrder.orderId();
            }
          }
        }
        case SET_DEFAULT_SORT_ORDER -> {
          Integer sortOrderId =
              resolveLastAddedId(asInteger(update.get("sort-order-id")), lastAddedSortOrderId);
          if (sortOrderId != null) {
            builder.setDefaultSortOrder(sortOrderId);
          }
        }
        case ADD_SNAPSHOT -> {
          @SuppressWarnings("unchecked")
          Map<String, Object> snapshotMap =
              update.get("snapshot") instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
          org.apache.iceberg.Snapshot snapshot =
              parseSnapshot(snapshotMap, currentFormatVersion, lastSequenceNumber);
          if (snapshot != null) {
            if (snapshotsById.containsKey(snapshot.snapshotId())) {
              org.apache.iceberg.Snapshot existing = snapshotsById.get(snapshot.snapshotId());
              if (existing != null) {
                lastSequenceNumber = Math.max(lastSequenceNumber, existing.sequenceNumber());
              }
              break;
            }
            builder.addSnapshot(snapshot);
            snapshotsById.put(snapshot.snapshotId(), snapshot);
            lastSequenceNumber = Math.max(lastSequenceNumber, snapshot.sequenceNumber());
          }
        }
        default -> {
          // handled below or ignored
        }
      }
    }

    if (!parsed.removedSnapshotIds().isEmpty()) {
      builder.removeSnapshots(parsed.removedSnapshotIds());
      parsed.removedSnapshotIds().forEach(snapshotsById::remove);
    }
    for (CommitUpdateInspector.SnapshotRefMutation mutation : parsed.snapshotRefMutations()) {
      if (mutation == null || mutation.refName() == null || mutation.refName().isBlank()) {
        continue;
      }
      if (mutation.remove()) {
        builder.removeRef(mutation.refName());
        continue;
      }
      if (!hasKnownSnapshot(snapshotsById, mutation.snapshotId())) {
        LOG.debugf(
            "Skipping pre-commit snapshot ref mutation ref=%s snapshotId=%s because snapshot is not yet present in canonical metadata",
            mutation.refName(), mutation.snapshotId());
        continue;
      }
      SnapshotRef ref = parseSnapshotRef(mutation);
      if (ref != null) {
        builder.setRef(mutation.refName(), ref);
      }
    }
    return builder.build();
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

  private Schema activeSchema(
      Map<Integer, Schema> schemasById, Integer currentSchemaId, Integer lastAddedSchemaId) {
    if (currentSchemaId != null && schemasById.containsKey(currentSchemaId)) {
      return schemasById.get(currentSchemaId);
    }
    if (lastAddedSchemaId != null && schemasById.containsKey(lastAddedSchemaId)) {
      return schemasById.get(lastAddedSchemaId);
    }
    return schemasById.values().stream().findFirst().orElse(null);
  }

  private Schema parseSchema(Map<String, Object> schemaMap) {
    if (schemaMap == null || schemaMap.isEmpty()) {
      return null;
    }
    try {
      return SchemaParser.fromJson(mapper.writeValueAsString(schemaMap));
    } catch (Exception e) {
      LOG.debugf(e, "Failed to parse schema update");
      return null;
    }
  }

  private PartitionSpec parsePartitionSpec(Map<String, Object> specMap, Schema schema) {
    if (specMap == null || specMap.isEmpty() || schema == null) {
      return null;
    }
    try {
      return PartitionSpecParser.fromJson(schema, mapper.writeValueAsString(specMap));
    } catch (Exception e) {
      LOG.debugf(e, "Failed to parse partition spec update");
      return null;
    }
  }

  private SortOrder parseSortOrder(Map<String, Object> sortOrderMap, Schema schema) {
    if (sortOrderMap == null || sortOrderMap.isEmpty() || schema == null) {
      return null;
    }
    try {
      return SortOrderParser.fromJson(schema, mapper.writeValueAsString(sortOrderMap));
    } catch (Exception e) {
      LOG.debugf(e, "Failed to parse sort order update");
      return null;
    }
  }

  private SnapshotRef parseSnapshotRef(CommitUpdateInspector.SnapshotRefMutation mutation) {
    try {
      Map<String, Object> ref = new LinkedHashMap<>();
      ref.put("snapshot-id", mutation.snapshotId());
      ref.put("type", mutation.type());
      if (mutation.maxRefAgeMs() != null) {
        ref.put("max-ref-age-ms", mutation.maxRefAgeMs());
      }
      if (mutation.maxSnapshotAgeMs() != null) {
        ref.put("max-snapshot-age-ms", mutation.maxSnapshotAgeMs());
      }
      if (mutation.minSnapshotsToKeep() != null) {
        ref.put("min-snapshots-to-keep", mutation.minSnapshotsToKeep());
      }
      return SnapshotRefParser.fromJson(mapper.writeValueAsString(ref));
    } catch (Exception e) {
      LOG.debugf(e, "Failed to parse snapshot ref mutation");
      return null;
    }
  }

  private org.apache.iceberg.Snapshot parseSnapshot(
      Map<String, Object> snapshotMap, int formatVersion, long lastSequenceNumber) {
    if (snapshotMap == null || snapshotMap.isEmpty()) {
      return null;
    }
    try {
      Map<String, Object> normalized = new LinkedHashMap<>(snapshotMap);
      normalized.remove("schema-json");
      if (!normalized.containsKey("sequence-number")) {
        long nextSequence = formatVersion >= 2 ? Math.max(0L, lastSequenceNumber) + 1L : 0L;
        normalized.put("sequence-number", nextSequence);
      }
      return SnapshotParser.fromJson(mapper.writeValueAsString(normalized));
    } catch (Exception e) {
      LOG.debugf(e, "Failed to parse snapshot update");
      return null;
    }
  }

  private Integer resolveLastAddedId(Integer requested, Integer lastAdded) {
    if (requested == null) {
      return null;
    }
    if (requested == -1) {
      return lastAdded;
    }
    return requested;
  }

  private Integer asInteger(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number number) {
      return number.intValue();
    }
    String text = value.toString();
    if (text.isBlank()) {
      return null;
    }
    try {
      return Integer.parseInt(text);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private boolean hasKnownSnapshot(
      Map<Long, org.apache.iceberg.Snapshot> snapshotsById, Long snapshotId) {
    if (snapshotsById == null || snapshotsById.isEmpty() || snapshotId == null || snapshotId <= 0) {
      return false;
    }
    return snapshotsById.containsKey(snapshotId);
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
