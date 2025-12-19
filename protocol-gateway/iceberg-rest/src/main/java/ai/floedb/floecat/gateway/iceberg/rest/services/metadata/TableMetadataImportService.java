package ai.floedb.floecat.gateway.iceberg.rest.services.metadata;

import ai.floedb.floecat.gateway.iceberg.rest.common.MetadataLocationUtil;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;
import org.jboss.logging.Logger;

@jakarta.enterprise.context.ApplicationScoped
public class TableMetadataImportService {
  private static final Logger LOG = Logger.getLogger(TableMetadataImportService.class);

  public record ImportedMetadata(
      String schemaJson,
      Map<String, String> properties,
      String tableLocation,
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
    if (metadataLocation == null || metadataLocation.isBlank()) {
      throw new IllegalArgumentException("metadata-location is required");
    }
    FileIO fileIO = null;
    try {
      fileIO = FileIoFactory.createFileIo(ioProperties, null, false);
      TableMetadata metadata = TableMetadataParser.read(fileIO, metadataLocation);
      String schemaJson = SchemaParser.toJson(metadata.schema());
      Map<String, String> props = new LinkedHashMap<>(metadata.properties());
      MetadataLocationUtil.setMetadataLocation(props, metadataLocation);
      props.putIfAbsent("table-uuid", metadata.uuid());
      if (metadata.location() != null && !metadata.location().isBlank()) {
        props.put("location", metadata.location());
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
                current.manifestListLocation(),
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
                snapshot.manifestListLocation(),
                summary,
                snapshot.schemaId()));
      }
      return new ImportedMetadata(
          schemaJson, props, metadata.location(), importedSnapshot, List.copyOf(snapshotList));
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

  private Map<String, String> copySummaryWithOperation(org.apache.iceberg.Snapshot snapshot) {
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
}
