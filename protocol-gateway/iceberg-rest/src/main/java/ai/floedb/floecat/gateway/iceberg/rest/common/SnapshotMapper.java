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

package ai.floedb.floecat.gateway.iceberg.rest.common;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class SnapshotMapper {
  private SnapshotMapper() {}

  static List<Map<String, Object>> snapshotLog(List<Snapshot> snapshots) {
    if (snapshots == null || snapshots.isEmpty()) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>();
    for (Snapshot snapshot : snapshots) {
      Map<String, Object> log = new LinkedHashMap<>();
      log.put(
          "timestamp-ms",
          snapshot.hasUpstreamCreatedAt()
              ? snapshot.getUpstreamCreatedAt().getSeconds() * 1000L
              : 0L);
      log.put("snapshot-id", snapshot.getSnapshotId());
      out.add(log);
    }
    return out;
  }

  static List<Map<String, Object>> metadataLog() {
    return List.of();
  }

  static List<Map<String, Object>> statistics() {
    return List.of();
  }

  static List<Map<String, Object>> sanitizeStatistics(List<Map<String, Object>> statistics) {
    if (statistics == null || statistics.isEmpty()) {
      return List.of();
    }
    List<Map<String, Object>> sanitized = new ArrayList<>(statistics.size());
    for (Map<String, Object> entry : statistics) {
      Map<String, Object> copy = entry == null ? new LinkedHashMap<>() : new LinkedHashMap<>(entry);
      Object blobs = copy.get("blob-metadata");
      if (!(blobs instanceof List<?>)) {
        copy.put("blob-metadata", List.of());
      }
      sanitized.add(copy);
    }
    return sanitized;
  }

  static List<Map<String, Object>> partitionStatistics() {
    return List.of();
  }

  static List<Map<String, Object>> snapshots(List<Snapshot> snapshots) {
    if (snapshots == null || snapshots.isEmpty()) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>();
    for (Snapshot snapshot : snapshots) {
      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("snapshot-id", snapshot.getSnapshotId());
      if (snapshot.hasParentSnapshotId()) {
        entry.put("parent-snapshot-id", snapshot.getParentSnapshotId());
      }
      long sequenceNumber = snapshot.getSequenceNumber();
      if (sequenceNumber > 0) {
        entry.put("sequence-number", sequenceNumber);
      }
      long timestampMs =
          snapshot.hasUpstreamCreatedAt()
              ? snapshot.getUpstreamCreatedAt().getSeconds() * 1000L
              : 0L;
      entry.put("timestamp-ms", timestampMs);
      entry.put(
          "manifest-list", snapshot.getManifestList().isBlank() ? "" : snapshot.getManifestList());
      int schemaId = snapshot.getSchemaId();
      if (schemaId >= 0) {
        entry.put("schema-id", schemaId);
      }
      Map<String, String> summary = new LinkedHashMap<>();
      if (!snapshot.getSummaryMap().isEmpty()) {
        summary.putAll(snapshot.getSummaryMap());
      }
      summary.putIfAbsent("operation", "append");
      entry.put("summary", summary);
      out.add(entry);
    }
    return out;
  }

  static List<Map<String, Object>> nonNullMapList(List<Map<String, Object>> value) {
    if (value == null || value.isEmpty()) {
      return List.of();
    }
    List<Map<String, Object>> copy = new ArrayList<>(value.size());
    for (Map<String, Object> entry : value) {
      copy.add(entry == null ? Map.of() : new LinkedHashMap<>(entry));
    }
    return copy;
  }
}
