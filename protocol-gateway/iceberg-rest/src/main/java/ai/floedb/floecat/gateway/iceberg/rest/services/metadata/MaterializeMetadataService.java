package ai.floedb.floecat.gateway.iceberg.rest.services.metadata;

import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.support.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.support.MirrorLocationUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.io.SupportsPrefixOperations;
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
      boolean overrideProvided =
          metadataLocationOverride != null && !metadataLocationOverride.isBlank();
      resolvedLocation = requestedLocation;
      if (overrideProvided) {
        if (isPointerLocation(resolvedLocation) || fileExists(fileIO, resolvedLocation)) {
          resolvedLocation = resolveVersionedLocation(fileIO, resolvedLocation, metadata);
        }
      } else {
        if (resolvedLocation == null || resolvedLocation.isBlank()) {
          resolvedLocation = resolveVersionedLocation(fileIO, requestedLocation, metadata);
        } else if (needsVersionedName(resolvedLocation, metadata)
            || (resolvedLocation != null
                && !MirrorLocationUtil.isMirrorMetadataLocation(resolvedLocation)
                && fileExists(fileIO, resolvedLocation))) {
          resolvedLocation = resolveVersionedLocation(fileIO, resolvedLocation, metadata);
        }
      }
      if (resolvedLocation == null || resolvedLocation.isBlank()) {
        LOG.debugf(
            "Skipping metadata materialization for %s.%s because metadata-location was unavailable",
            namespaceFq, tableName);
        return new MaterializeResult(requestedLocation, metadata);
      }
      boolean mirrorLocation = MirrorLocationUtil.isMirrorMetadataLocation(resolvedLocation);
      String canonicalLocation = MirrorLocationUtil.stripMetadataMirrorPrefix(resolvedLocation);
      if (canonicalLocation == null || canonicalLocation.isBlank()) {
        canonicalLocation = resolvedLocation;
      }
      if (!mirrorLocation) {
        canonicalLocation = ensureCanonicalVersion(fileIO, canonicalLocation);
        resolvedLocation = canonicalLocation;
      }
      NormalizationResult normalization =
          mirrorLocation
              ? new NormalizationResult(metadata, Map.of())
              : normalizeMetadata(metadata, canonicalLocation);
      TableMetadataView normalizedMetadata =
          normalization.metadata() != null ? normalization.metadata() : metadata;
      TableMetadataView resolvedMetadata =
          normalizedMetadata == null
              ? null
              : normalizedMetadata.withMetadataLocation(canonicalLocation);
      if (!mirrorLocation) {
        copyMirrorArtifacts(fileIO, normalization.mirrorCopies(), canonicalLocation);
        copyMirrorDirectory(fileIO, canonicalLocation);
      }
      String canonicalJson = canonicalMetadataJson(resolvedMetadata, canonicalLocation);
      if (!mirrorLocation) {
        canonicalJson = canonicalJson.replace("/.floecat-metadata", "");
      }
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
    if (isPointerLocation(metadataLocation)) {
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
    directory = normalizeMetadataDirectory(directory);
    if (directory == null || directory.isBlank()) {
      return null;
    }
    if (!directory.endsWith("/")) {
      directory = directory + "/";
    }
    return directory + nextMetadataFileName(metadataLocation, metadata);
  }

  private boolean isPointerLocation(String metadataLocation) {
    return MetadataLocationUtil.isPointer(metadataLocation);
  }

  private boolean needsVersionedName(String metadataLocation, TableMetadataView metadata) {
    return metadataLocation == null
        || metadataLocation.isBlank()
        || isPointerLocation(metadataLocation)
        || (metadata.metadataLog() != null && !metadata.metadataLog().isEmpty());
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
    } catch (UnsupportedOperationException | NotFoundException e) {
      return false;
    } catch (RuntimeException e) {
      return false;
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

  private NormalizationResult normalizeMetadata(
      TableMetadataView metadata, String resolvedLocation) throws IOException {
    if (metadata == null) {
      return new NormalizationResult(null, Map.of());
    }
    ObjectNode node = mapper.valueToTree(metadata);
    Map<String, String> mirrorCopies = new LinkedHashMap<>();
    replaceMirrorStrings(node, mirrorCopies);
    if (resolvedLocation != null && !resolvedLocation.isBlank()) {
      node.put("metadata-location", resolvedLocation);
    }
    alignCurrentSnapshot(node);
    TableMetadataView normalized = mapper.treeToValue(node, TableMetadataView.class);
    return new NormalizationResult(normalized, mirrorCopies);
  }

  private void replaceMirrorStrings(JsonNode node, Map<String, String> mirrorCopies) {
    if (node == null) {
      return;
    }
    if (node.isObject()) {
      ObjectNode objectNode = (ObjectNode) node;
      var fields = objectNode.fields();
      while (fields.hasNext()) {
        var entry = fields.next();
        JsonNode child = entry.getValue();
        if (child.isTextual()) {
          String updated = stripMirror(child.asText(), mirrorCopies);
          if (updated != null) {
            objectNode.put(entry.getKey(), updated);
          }
        } else {
          replaceMirrorStrings(child, mirrorCopies);
        }
      }
    } else if (node.isArray()) {
      for (int i = 0; i < node.size(); i++) {
        JsonNode child = node.get(i);
        if (child.isTextual()) {
          String updated = stripMirror(child.asText(), mirrorCopies);
          if (updated != null) {
            ((com.fasterxml.jackson.databind.node.ArrayNode) node)
                .set(i, com.fasterxml.jackson.databind.node.TextNode.valueOf(updated));
          }
        } else {
          replaceMirrorStrings(child, mirrorCopies);
        }
      }
    }
  }

  private String stripMirror(String value, Map<String, String> mirrorCopies) {
    if (value == null || value.isBlank()) {
      return null;
    }
    if (!MirrorLocationUtil.isMirrorMetadataLocation(value)) {
      return null;
    }
    String canonical = MirrorLocationUtil.stripMetadataMirrorPrefix(value);
    mirrorCopies.putIfAbsent(canonical, value);
    return canonical;
  }

  private void copyMirrorArtifacts(
      FileIO fileIO, Map<String, String> mirrorCopies, String metadataLocation) {
    if (mirrorCopies == null || mirrorCopies.isEmpty()) {
      return;
    }
    mirrorCopies.forEach(
        (canonical, mirror) -> {
          if (canonical == null
              || mirror == null
              || canonical.equals(metadataLocation)
              || !isLikelyFile(canonical)) {
            return;
          }
          copyFile(fileIO, mirror, canonical);
        });
  }

  private void copyMirrorDirectory(FileIO fileIO, String canonicalLocation) {
    if (!(fileIO instanceof SupportsPrefixOperations prefixOps)) {
      return;
    }
    if (canonicalLocation == null || canonicalLocation.isBlank()) {
      return;
    }
    String canonicalDir = directoryOf(canonicalLocation);
    if (canonicalDir == null) {
      return;
    }
    String placeholder = canonicalDir + UUID.randomUUID() + ".metadata.json";
    String mirrorPlaceholder = MirrorLocationUtil.mirrorMetadataLocation(placeholder);
    String mirrorDir = directoryOf(mirrorPlaceholder);
    if (mirrorDir == null) {
      return;
    }
    try {
      Iterable<FileInfo> files = prefixOps.listPrefix(mirrorDir);
      for (FileInfo info : files) {
        if (info == null || info.location() == null) {
          continue;
        }
        String canonical = MirrorLocationUtil.stripMetadataMirrorPrefix(info.location());
        if (canonical == null || canonical.isBlank()) {
          continue;
        }
        if (canonical.equals(canonicalLocation)) {
          continue;
        }
        copyFile(fileIO, info.location(), canonical);
      }
    } catch (UnsupportedOperationException e) {
      LOG.debugf(e, "FileIO does not support prefix listing; skipping mirror copy");
    } catch (RuntimeException e) {
      LOG.debugf(e, "Failed to copy mirrored directory %s", mirrorDir);
    }
  }

  private boolean isLikelyFile(String path) {
    if (path == null || path.isBlank()) {
      return false;
    }
    if (path.endsWith("/")) {
      return false;
    }
    int slash = path.lastIndexOf('/');
    String name = slash >= 0 ? path.substring(slash + 1) : path;
    return name.contains(".");
  }

  private void copyFile(FileIO fileIO, String source, String destination) {
    try {
      InputFile input = fileIO.newInputFile(source);
      if (!input.exists()) {
        return;
      }
      OutputFile output = fileIO.newOutputFile(destination);
      try (SeekableInputStream inputStream = input.newStream();
          PositionOutputStream outputStream = output.createOrOverwrite()) {
        byte[] buffer = new byte[8192];
        int bytesRead;
        while ((bytesRead = inputStream.read(buffer)) >= 0) {
          if (bytesRead == 0) {
            continue;
          }
          outputStream.write(buffer, 0, bytesRead);
        }
      }
    } catch (IOException e) {
      throw new MaterializeMetadataException(
          "Failed to copy mirrored artifact from " + source + " to " + destination, e);
    } catch (RuntimeException e) {
      LOG.debugf(
          e,
          "Failed to copy mirrored artifact from %s to %s (continuing with commit)",
          source,
          destination);
    }
  }

  private record NormalizationResult(
      TableMetadataView metadata, Map<String, String> mirrorCopies) {}

  private String ensureCanonicalVersion(FileIO fileIO, String canonicalLocation) {
    if (canonicalLocation == null || canonicalLocation.isBlank()) {
      return canonicalLocation;
    }
    String directory = directoryOf(canonicalLocation);
    if (directory == null || directory.isBlank()) {
      return canonicalLocation;
    }
    long currentVersion = parseVersion(canonicalLocation);
    if (currentVersion < 0) {
      currentVersion = -1;
    }
    long maxExisting = maxExistingVersion(fileIO, directory);
    long requiredVersion = Math.max(maxExisting + 1, currentVersion);
    if (currentVersion >= 0 && currentVersion >= requiredVersion) {
      return canonicalLocation;
    }
    long nextVersion = Math.max(requiredVersion, 0);
    return directory + metadataFileName(nextVersion);
  }

  private long maxExistingVersion(FileIO fileIO, String directory) {
    if (!(fileIO instanceof SupportsPrefixOperations prefixOps)) {
      return -1;
    }
    try {
      Iterable<FileInfo> files = prefixOps.listPrefix(directory);
      long max = -1;
      for (FileInfo info : files) {
        if (info == null || info.location() == null) {
          continue;
        }
        long parsed = parseVersion(info.location());
        if (parsed > max) {
          max = parsed;
        }
      }
      return max;
    } catch (UnsupportedOperationException e) {
      return -1;
    } catch (RuntimeException e) {
      LOG.debugf(e, "Failed to list metadata directory %s", directory);
      return -1;
    }
  }

  private String metadataFileName(long version) {
    long normalized = Math.max(version, 0);
    return String.format("%05d-%s.metadata.json", normalized, UUID.randomUUID());
  }

  private void alignCurrentSnapshot(ObjectNode node) {
    if (node == null) {
      return;
    }
    JsonNode snapshotsNode = node.get("snapshots");
    if (!(snapshotsNode instanceof com.fasterxml.jackson.databind.node.ArrayNode snapshots)
        || snapshots.isEmpty()) {
      return;
    }
    long bestSnapshotId = -1L;
    long bestSequence = Long.MIN_VALUE;
    for (JsonNode snapshot : snapshots) {
      if (!(snapshot instanceof ObjectNode)) {
        continue;
      }
      long snapshotId = snapshot.path("snapshot-id").asLong(-1L);
      if (snapshotId <= 0) {
        continue;
      }
      long sequence = snapshot.path("sequence-number").asLong(Long.MIN_VALUE);
      if (sequence > bestSequence || (sequence == bestSequence && snapshotId > bestSnapshotId)) {
        bestSequence = sequence;
        bestSnapshotId = snapshotId;
      }
    }
    if (bestSnapshotId <= 0) {
      return;
    }
    node.put("current-snapshot-id", bestSnapshotId);
    JsonNode propsNode = node.get("properties");
    if (propsNode instanceof ObjectNode props) {
      props.put("current-snapshot-id", Long.toString(bestSnapshotId));
    }
    ObjectNode refs = asObjectNode(node.get("refs"));
    if (refs == null) {
      refs = node.putObject("refs");
    }
    ObjectNode mainRef = asObjectNode(refs.get("main"));
    if (mainRef == null) {
      mainRef = refs.putObject("main");
      mainRef.put("type", "branch");
    }
    mainRef.put("snapshot-id", bestSnapshotId);
  }

  private ObjectNode asObjectNode(JsonNode node) {
    return node instanceof ObjectNode objectNode ? objectNode : null;
  }
}
