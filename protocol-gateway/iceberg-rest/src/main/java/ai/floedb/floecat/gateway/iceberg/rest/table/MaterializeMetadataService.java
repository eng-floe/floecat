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

import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.firstNonBlank;

import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.support.FileIoFactory;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.jboss.logging.Logger;

@ApplicationScoped
public class MaterializeMetadataService {
  private static final Logger LOG = Logger.getLogger(MaterializeMetadataService.class);
  private static final Set<String> SKIPPED_SCHEMES = Set.of("floecat");

  @Inject IcebergGatewayConfig config;
  @Inject TableGatewaySupport tableGatewaySupport;

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

  public MaterializeResult materialize(
      String namespaceFq,
      String tableName,
      TableMetadata metadata,
      String metadataLocationOverride) {
    if (metadata == null) {
      LOG.debugf(
          "Skipping metadata materialization for %s.%s because commit metadata was empty",
          namespaceFq, tableName);
      return new MaterializeResult(metadataLocationOverride, null);
    }
    String requestedLocation =
        firstNonBlank(metadataLocationOverride, metadata.metadataFileLocation());
    if (requestedLocation != null && !shouldMaterialize(requestedLocation)) {
      LOG.debugf(
          "Skipping metadata materialization for %s.%s because metadata-location was %s",
          namespaceFq, tableName, requestedLocation);
      return canonicalizeResult(requestedLocation, metadata);
    }
    String resolvedLocation = null;
    FileIO fileIO = null;
    try {
      Map<String, String> props = new LinkedHashMap<>();
      if (tableGatewaySupport != null) {
        props.putAll(tableGatewaySupport.defaultFileIoProperties());
      }
      props.putAll(sanitizeProperties(metadata.properties()));
      fileIO = newFileIo(props);
      resolvedLocation = resolveVersionedLocation(fileIO, requestedLocation, metadata);
      if (resolvedLocation == null || resolvedLocation.isBlank()) {
        LOG.debugf(
            "Skipping metadata materialization for %s.%s because metadata-location was unavailable",
            namespaceFq, tableName);
        return canonicalizeResult(requestedLocation, metadata);
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

  protected FileIO newFileIo(Map<String, String> props) {
    return FileIoFactory.createFileIo(props, config, true);
  }

  MaterializeResult canonicalizeResult(String metadataLocation, TableMetadata metadata) {
    if (metadata == null) {
      return new MaterializeResult(metadataLocation, null);
    }
    TableMetadata canonical =
        hasText(metadataLocation) ? withMetadataLocation(metadata, metadataLocation) : metadata;
    return new MaterializeResult(metadataLocation, canonical);
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
    return String.format("%05d-%s.metadata.json", nextVersion, UUID.randomUUID());
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
}
