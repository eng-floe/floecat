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

package ai.floedb.floecat.service.storage.impl;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;

final class TableStorageLocationResolver {
  private TableStorageLocationResolver() {}

  static String resolveRequestedOrTableLocation(
      Table table, String requestedLocation, SnapshotRepository snapshotRepo) {
    String resolvedRequested = resolveStorageUri(requestedLocation);
    return resolvedRequested != null
        ? resolvedRequested
        : resolveTableLocation(table, snapshotRepo);
  }

  static String resolveTableLocation(Table table, SnapshotRepository snapshotRepo) {
    if (table == null) {
      return null;
    }
    String storageLocation = resolveStorageUri(table.getPropertiesMap().get("storage_location"));
    if (storageLocation != null) {
      return storageLocation;
    }
    String location = resolveStorageUri(table.getPropertiesMap().get("location"));
    if (location != null) {
      return location;
    }
    String deltaTableRoot = resolveStorageUri(table.getPropertiesMap().get("delta.table-root"));
    if (deltaTableRoot != null) {
      return deltaTableRoot;
    }
    String externalLocation = resolveStorageUri(table.getPropertiesMap().get("external.location"));
    if (externalLocation != null) {
      return externalLocation;
    }
    String sourceMetadataLocation =
        resolveStorageUri(table.getPropertiesMap().get("source_metadata_location"));
    if (sourceMetadataLocation != null) {
      return deriveTableRootLocation(sourceMetadataLocation);
    }
    String snapshotMetadataLocation = resolveCurrentSnapshotMetadataLocation(table, snapshotRepo);
    if (snapshotMetadataLocation != null) {
      return deriveTableRootLocation(snapshotMetadataLocation);
    }
    String upstreamUri = table.hasUpstream() ? table.getUpstream().getUri() : null;
    return resolveStorageUri(upstreamUri);
  }

  private static String resolveCurrentSnapshotMetadataLocation(
      Table table, SnapshotRepository snapshotRepo) {
    if (table == null || snapshotRepo == null) {
      return null;
    }
    return snapshotRepo
        .getCurrentSnapshot(table.getResourceId())
        .map(SnapshotRepository::metadataLocation)
        .orElse(null);
  }

  static String deriveTableRootLocation(String location) {
    String normalized = resolveStorageUri(location);
    if (normalized == null) {
      return null;
    }
    String root = stripLocationSegment(normalized, "/metadata/");
    if (root != null) {
      return root;
    }
    root = stripLocationSegment(normalized, "/_delta_log/");
    if (root != null) {
      return root;
    }
    root = stripLocationSuffix(normalized, "/metadata");
    if (root != null) {
      return root;
    }
    root = stripLocationSuffix(normalized, "/_delta_log");
    if (root != null) {
      return root;
    }
    return normalized;
  }

  private static String stripLocationSegment(String location, String segment) {
    int idx = location.indexOf(segment);
    return idx > 0 ? location.substring(0, idx) : null;
  }

  private static String stripLocationSuffix(String location, String suffix) {
    return location.endsWith(suffix)
        ? location.substring(0, location.length() - suffix.length())
        : null;
  }

  static String resolveStorageUri(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    String trimmed = value.trim();
    String lower = trimmed.toLowerCase(java.util.Locale.ROOT);
    if (lower.startsWith("s3://")
        || lower.startsWith("s3a://")
        || lower.startsWith("s3n://")
        || lower.startsWith("abfs://")
        || lower.startsWith("abfss://")
        || lower.startsWith("gs://")
        || lower.startsWith("gcs://")
        || lower.startsWith("wasb://")
        || lower.startsWith("wasbs://")
        || lower.startsWith("adl://")
        || lower.startsWith("oss://")
        || lower.startsWith("cos://")
        || lower.startsWith("file://")) {
      return trimmed;
    }
    return null;
  }
}
