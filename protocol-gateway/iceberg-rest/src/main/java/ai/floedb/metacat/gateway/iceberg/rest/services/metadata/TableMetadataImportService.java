package ai.floedb.metacat.gateway.iceberg.rest.services.metadata;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;
import org.jboss.logging.Logger;

@jakarta.enterprise.context.ApplicationScoped
public class TableMetadataImportService {
  private static final Logger LOG = Logger.getLogger(TableMetadataImportService.class);
  private static final String DEFAULT_IO_IMPL = "org.apache.iceberg.aws.s3.S3FileIO";
  private static final Set<String> IO_PROP_PREFIXES =
      Set.of("s3.", "s3a.", "s3n.", "fs.", "client.", "aws.", "hadoop.");

  public record ImportedMetadata(
      String schemaJson, Map<String, String> properties, String tableLocation) {}

  public ImportedMetadata importMetadata(
      String metadataLocation, Map<String, String> ioProperties) {
    if (metadataLocation == null || metadataLocation.isBlank()) {
      throw new IllegalArgumentException("metadata-location is required");
    }
    FileIO fileIO = null;
    try {
      fileIO = instantiateFileIO(ioProperties);
      TableMetadata metadata = TableMetadataParser.read(fileIO, metadataLocation);
      String schemaJson = SchemaParser.toJson(metadata.schema());
      Map<String, String> props = new LinkedHashMap<>(metadata.properties());
      props.put("metadata-location", metadataLocation);
      props.put("metadata_location", metadataLocation);
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
      if (current != null) {
        putLong(props, "current-snapshot-id", current.snapshotId());
      }
      return new ImportedMetadata(schemaJson, props, metadata.location());
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

  private FileIO instantiateFileIO(Map<String, String> props) {
    Map<String, String> normalized = props == null ? Map.of() : new LinkedHashMap<>(props);
    String impl = normalized.getOrDefault("io-impl", DEFAULT_IO_IMPL).trim();
    try {
      Class<?> clazz = Class.forName(impl);
      Object instance = clazz.getDeclaredConstructor().newInstance();
      if (!(instance instanceof FileIO fileIO)) {
        throw new IllegalArgumentException(impl + " does not implement FileIO");
      }
      fileIO.initialize(filterIoProperties(normalized));
      return fileIO;
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to instantiate FileIO " + impl, e);
    }
  }

  private Map<String, String> filterIoProperties(Map<String, String> props) {
    if (props == null || props.isEmpty()) {
      return Map.of();
    }
    Map<String, String> filtered = new LinkedHashMap<>();
    props.forEach(
        (key, value) -> {
          if (key == null || value == null) {
            return;
          }
          for (String prefix : IO_PROP_PREFIXES) {
            if (key.startsWith(prefix)) {
              filtered.put(key, value);
              return;
            }
          }
          if ("io-impl".equals(key)) {
            filtered.put(key, value);
          }
        });
    return filtered;
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

  private void putLong(Map<String, String> props, String key, long value) {
    if (value >= 0) {
      props.put(key, Long.toString(value));
    }
  }
}
