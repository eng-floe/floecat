package ai.floedb.floecat.gateway.iceberg.rest.services.metadata;

import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.common.MetadataLocationUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.jboss.logging.Logger;

@ApplicationScoped
public class MaterializeMetadataService {
  private static final Logger LOG = Logger.getLogger(MaterializeMetadataService.class);
  private static final Set<String> SKIPPED_SCHEMES = Set.of("floecat");

  @Inject ObjectMapper mapper;
  @Inject IcebergGatewayConfig config;

  public void setMapper(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  public record MaterializeResult(String metadataLocation, TableMetadataView metadata) {}

  public MaterializeResult materialize(
      String namespaceFq,
      String tableName,
      TableMetadataView metadata,
      String metadataLocationOverride) {
    if (metadata == null) {
      LOG.debugf(
          "Skipping metadata materialization for %s.%s because commit metadata was empty",
          namespaceFq, tableName);
      return new MaterializeResult(metadataLocationOverride, null);
    }
    String requestedLocation = firstNonBlank(metadataLocationOverride, metadata.metadataLocation());
    if (requestedLocation != null && !shouldMaterialize(requestedLocation)) {
      LOG.debugf(
          "Skipping metadata materialization for %s.%s because metadata-location was %s",
          namespaceFq, tableName, requestedLocation);
      return new MaterializeResult(requestedLocation, metadata);
    }
    String resolvedLocation = null;
    FileIO fileIO = null;
    try {
      Map<String, String> props = sanitizeProperties(metadata.properties());
      fileIO = newFileIo(props);
      resolvedLocation = resolveVersionedLocation(fileIO, requestedLocation, metadata);
      if (resolvedLocation == null || resolvedLocation.isBlank()) {
        LOG.debugf(
            "Skipping metadata materialization for %s.%s because metadata-location was unavailable",
            namespaceFq, tableName);
        return new MaterializeResult(requestedLocation, metadata);
      }
      TableMetadataView resolvedMetadata = metadata.withMetadataLocation(resolvedLocation);
      String canonicalJson = canonicalMetadataJson(resolvedMetadata, resolvedLocation);
      writeJson(fileIO, resolvedLocation, canonicalJson);
      LOG.infof(
          "Materialized Iceberg metadata files for %s.%s to %s",
          namespaceFq, tableName, resolvedLocation);
      return new MaterializeResult(resolvedLocation, resolvedMetadata);
    } catch (MaterializeMetadataException e) {
      throw e;
    } catch (Exception e) {
      throw new MaterializeMetadataException(
          "Failed to materialize Iceberg metadata files to " + resolvedLocation, e);
    } finally {
      closeQuietly(fileIO);
    }
  }

  protected FileIO newFileIo(Map<String, String> props) {
    return FileIoFactory.createFileIo(props, config, true);
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
      return !SKIPPED_SCHEMES.contains(scheme.toLowerCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      LOG.debugf(
          "Skipping metadata materialization because metadata-location %s is invalid",
          metadataLocation);
      return false;
    }
  }

  public void setConfig(IcebergGatewayConfig config) {
    this.config = config;
  }

  private String canonicalMetadataJson(TableMetadataView metadata, String metadataLocation) {
    try {
      JsonNode node = mapper.valueToTree(metadata);
      if (node instanceof ObjectNode objectNode) {
        long snapshotId = objectNode.path("current-snapshot-id").asLong(-1L);
        if (snapshotId <= 0) {
          objectNode.remove("current-snapshot-id");
        }
      }
      TableMetadata parsed = TableMetadataParser.fromJson(metadataLocation, node);
      return TableMetadataParser.toJson(parsed);
    } catch (RuntimeException e) {
      throw new MaterializeMetadataException("Unable to serialize Iceberg metadata", e);
    }
  }

  private void writeJson(FileIO fileIO, String location, String payload) {
    OutputFile outputFile = fileIO.newOutputFile(location);
    byte[] data = payload.getBytes(StandardCharsets.UTF_8);
    try (PositionOutputStream stream = outputFile.createOrOverwrite()) {
      stream.write(data);
    } catch (IOException e) {
      throw new MaterializeMetadataException("Failed to write metadata file " + location, e);
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

  private static String firstNonBlank(String first, String second) {
    if (first != null && !first.isBlank()) {
      return first;
    }
    return (second == null || second.isBlank()) ? null : second;
  }

  private Map<String, String> sanitizeProperties(Map<String, String> props) {
    if (props == null || props.isEmpty()) {
      return Map.of();
    }
    Map<String, String> sanitized = new LinkedHashMap<>();
    props.forEach(
        (key, value) -> {
          if (key != null && value != null) {
            sanitized.put(key, value);
          }
        });
    return sanitized;
  }

  private String resolveVersionedLocation(
      FileIO fileIO, String metadataLocation, TableMetadataView metadata) {
    String directory = metadataLocation != null ? directoryOf(metadataLocation) : null;
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

  private boolean isPointerLocation(String metadataLocation) {
    return MetadataLocationUtil.isPointer(metadataLocation);
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

  private String metadataDirectory(TableMetadataView metadata) {
    if (metadata == null) {
      return null;
    }
    String location = firstNonBlank(metadata.metadataLocation(), metadata.location());
    String directory = directoryOf(location);
    if (directory != null) {
      return directory;
    }

    Map<String, String> props = metadata.properties();
    if (props != null && !props.isEmpty()) {
      String candidate =
          firstNonBlank(
              props.get("metadata-location"),
              firstNonBlank(props.get("metadata_location"), props.get("location")));
      directory = directoryOf(candidate);
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

  private String directoryFromMetadataLog(TableMetadataView metadata) {
    if (metadata == null || metadata.metadataLog() == null) {
      return null;
    }
    for (Map<String, Object> entry : metadata.metadataLog()) {
      Object value = entry == null ? null : entry.get("metadata-file");
      if (value instanceof String file && !file.isBlank()) {
        String directory = directoryOf(file);
        if (directory != null) {
          return directory;
        }
      }
    }
    return null;
  }

  private String nextMetadataFileName(FileIO fileIO, String directory, TableMetadataView metadata) {
    long nextVersion = nextMetadataVersion(fileIO, directory, metadata);
    return String.format("%05d-%s.metadata.json", nextVersion, UUID.randomUUID());
  }

  private long nextMetadataVersion(FileIO fileIO, String directory, TableMetadataView metadata) {
    long max = parseExistingFiles(fileIO, directory);
    if (metadata != null && metadata.metadataLog() != null) {
      List<Map<String, Object>> entries = metadata.metadataLog();
      for (Map<String, Object> entry : entries) {
        Object value = entry.get("metadata-file");
        if (value instanceof String file) {
          long parsed = parseVersion(file);
          if (parsed > max) {
            max = parsed;
          }
        }
      }
    }
    return max < 0 ? 0 : max + 1;
  }

  private long parseExistingFiles(FileIO fileIO, String directory) {
    if (!(fileIO instanceof org.apache.iceberg.io.SupportsPrefixOperations prefixOps)) {
      return -1;
    }
    long max = -1;
    try {
      for (org.apache.iceberg.io.FileInfo info : prefixOps.listPrefix(directory)) {
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
}
