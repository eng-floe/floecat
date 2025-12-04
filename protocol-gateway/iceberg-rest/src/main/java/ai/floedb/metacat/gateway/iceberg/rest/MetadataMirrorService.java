package ai.floedb.metacat.gateway.iceberg.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
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

/** Handles mirroring Iceberg metadata files into the table's backing storage (e.g. S3). */
@ApplicationScoped
public class MetadataMirrorService {
  private static final Logger LOG = Logger.getLogger(MetadataMirrorService.class);
  private static final String DEFAULT_IO_IMPL = "org.apache.iceberg.aws.s3.S3FileIO";
  private static final Set<String> IO_PROP_PREFIXES =
      Set.of("s3.", "s3a.", "s3n.", "fs.", "client.", "aws.", "hadoop.");
  private static final Set<String> SKIPPED_SCHEMES = Set.of("metacat");

  @Inject ObjectMapper mapper;

  public record MirrorResult(String metadataLocation, TableMetadataView metadata) {}

  public MirrorResult mirror(
      String namespaceFq,
      String tableName,
      TableMetadataView metadata,
      String metadataLocationOverride) {
    if (metadata == null) {
      LOG.debugf(
          "Skipping metadata mirror for %s.%s because commit metadata was empty",
          namespaceFq, tableName);
      return new MirrorResult(metadataLocationOverride, null);
    }
    String requestedLocation =
        firstNonBlank(metadataLocationOverride, metadata.metadataLocation());
    if (requestedLocation != null && !shouldMirror(requestedLocation)) {
      LOG.debugf(
          "Skipping metadata mirror for %s.%s because metadata-location was %s",
          namespaceFq, tableName, requestedLocation);
      return new MirrorResult(requestedLocation, metadata);
    }
    String resolvedLocation = null;
    try {
      Map<String, String> props = sanitizeProperties(metadata.properties());
      FileIO fileIO = instantiateFileIO(props);
      resolvedLocation = resolveVersionedLocation(fileIO, requestedLocation, metadata);
      if (resolvedLocation == null || resolvedLocation.isBlank()) {
        LOG.debugf(
            "Skipping metadata mirror for %s.%s because metadata-location was unavailable",
            namespaceFq, tableName);
        return new MirrorResult(requestedLocation, metadata);
      }
      TableMetadataView resolvedMetadata = metadata.withMetadataLocation(resolvedLocation);
      String canonicalJson = canonicalMetadataJson(resolvedMetadata, resolvedLocation);
      writeJson(fileIO, resolvedLocation, canonicalJson);
      LOG.infof(
          "Mirrored Iceberg metadata files for %s.%s to %s",
          namespaceFq, tableName, resolvedLocation);
      return new MirrorResult(resolvedLocation, resolvedMetadata);
    } catch (MetadataMirrorException e) {
      throw e;
    } catch (Exception e) {
      throw new MetadataMirrorException(
          "Failed to mirror Iceberg metadata files to " + resolvedLocation, e);
    }
  }

  private boolean shouldMirror(String metadataLocation) {
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
          "Skipping metadata mirror because metadata-location %s is invalid", metadataLocation);
      return false;
    }
  }

  private FileIO instantiateFileIO(Map<String, String> props) {
    String impl = props.getOrDefault("io-impl", DEFAULT_IO_IMPL).trim();
    try {
      Class<?> clazz = Class.forName(impl);
      Object instance = clazz.getDeclaredConstructor().newInstance();
      if (!(instance instanceof FileIO fileIO)) {
        throw new MetadataMirrorException(impl + " does not implement FileIO");
      }
      fileIO.initialize(filterIoProperties(props));
      return fileIO;
    } catch (MetadataMirrorException e) {
      throw e;
    } catch (Exception e) {
      throw new MetadataMirrorException("Unable to instantiate FileIO " + impl, e);
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
        });
    return filtered;
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
      throw new MetadataMirrorException("Unable to serialize Iceberg metadata", e);
    }
  }

  private void writeJson(FileIO fileIO, String location, String payload) {
    OutputFile outputFile = fileIO.newOutputFile(location);
    byte[] data = payload.getBytes(StandardCharsets.UTF_8);
    try (PositionOutputStream stream = outputFile.createOrOverwrite()) {
      stream.write(data);
    } catch (IOException e) {
      throw new MetadataMirrorException("Failed to write metadata file " + location, e);
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
    if (metadataLocation != null
        && !metadataLocation.isBlank()
        && !isPointerLocation(metadataLocation)
        && (metadata.metadataLog() == null || metadata.metadataLog().isEmpty())
        && !fileExists(fileIO, metadataLocation)) {
      return metadataLocation;
    }
    String directory = metadataLocation != null ? directoryOf(metadataLocation) : null;
    if (directory == null) {
      directory = metadataDirectory(metadata);
    }
    if (directory == null || directory.isBlank()) {
      return null;
    }
    if (!directory.endsWith("/")) {
      directory = directory + "/";
    }
    return directory + nextMetadataFileName(metadataLocation, metadata);
  }

  private boolean isPointerLocation(String metadataLocation) {
    if (metadataLocation == null || metadataLocation.isBlank()) {
      return false;
    }
    int slash = metadataLocation.lastIndexOf('/');
    String file = slash >= 0 ? metadataLocation.substring(slash + 1) : metadataLocation;
    return "metadata.json".equals(file);
  }

  private String directoryOf(String metadataLocation) {
    if (metadataLocation == null || metadataLocation.isBlank()) {
      return null;
    }
    int slash = metadataLocation.lastIndexOf('/');
    if (slash < 0) {
      return null;
    }
    return metadataLocation.substring(0, slash + 1);
  }

  private String metadataDirectory(TableMetadataView metadata) {
    if (metadata == null) {
      return null;
    }
    String location = metadata.location();
    if ((location == null || location.isBlank()) && metadata.properties() != null) {
      location = metadata.properties().get("location");
    }
    if (location == null || location.isBlank()) {
      return null;
    }
    String base = location.endsWith("/") ? location.substring(0, location.length() - 1) : location;
    return base + "/metadata/";
  }

  private String nextMetadataFileName(String currentLocation, TableMetadataView metadata) {
    long nextVersion = nextMetadataVersion(currentLocation, metadata);
    return String.format("%05d-%s.metadata.json", nextVersion, UUID.randomUUID());
  }

  private long nextMetadataVersion(String currentLocation, TableMetadataView metadata) {
    long max = parseVersion(currentLocation);
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

  private boolean fileExists(FileIO fileIO, String location) {
    if (fileIO == null || location == null || location.isBlank()) {
      return false;
    }
    try {
      return fileIO.newInputFile(location).exists();
    } catch (UnsupportedOperationException e) {
      return false;
    }
  }
}
