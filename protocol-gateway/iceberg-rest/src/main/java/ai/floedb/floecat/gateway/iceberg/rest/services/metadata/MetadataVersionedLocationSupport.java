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

import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.SupportsPrefixOperations;

@ApplicationScoped
public class MetadataVersionedLocationSupport {

  String resolveVersionedLocation(
      FileIO fileIO, String metadataLocation, TableMetadataView metadata) {
    String directory =
        metadataLocation == null
            ? null
            : (metadataLocation.endsWith("/") ? metadataLocation : directoryOf(metadataLocation));
    directory = normalizeMetadataDirectory(directory);
    if (directory == null || directory.isBlank()) {
      return null;
    }
    if (!directory.endsWith("/")) {
      directory = directory + "/";
    }
    return directory + nextMetadataFileName(fileIO, directory, metadata);
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
}
