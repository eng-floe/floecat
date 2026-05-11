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

package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableIdentifierDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.rest.common.CommitUpdateInspector;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService.ImportedMetadata;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService.ImportedSnapshot;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class TableRegisterRequestBuilder {
  TransactionCommitRequest buildRegisterTransactionRequest(
      List<String> namespacePath,
      String tableName,
      Map<String, String> mergedProps,
      String metadataLocation,
      ImportedMetadata importedMetadata,
      List<Long> existingSnapshotIds,
      boolean assertCreate) {
    List<Map<String, Object>> requirements =
        assertCreate
            ? new ArrayList<>(CommitUpdateInspector.assertCreateRequirements())
            : List.of();
    List<Map<String, Object>> updates = new ArrayList<>();
    Map<String, String> props = mergedProps == null ? Map.of() : new LinkedHashMap<>(mergedProps);
    if (!props.isEmpty()) {
      updates.add(Map.of("action", CommitUpdateInspector.ACTION_SET_PROPERTIES, "updates", props));
    }
    String location = props.get("location");
    if (location != null && !location.isBlank()) {
      updates.add(
          Map.of("action", CommitUpdateInspector.ACTION_SET_LOCATION, "location", location));
    }

    List<Long> importedSnapshotIds = new ArrayList<>();
    for (ImportedSnapshot snapshot : snapshotsToImport(importedMetadata)) {
      Map<String, Object> snapshotMap =
          toSnapshotUpdate(snapshot, importedMetadata, metadataLocation);
      if (!snapshotMap.isEmpty()) {
        updates.add(
            Map.of("action", CommitUpdateInspector.ACTION_ADD_SNAPSHOT, "snapshot", snapshotMap));
      }
      if (snapshot != null && snapshot.snapshotId() != null) {
        importedSnapshotIds.add(snapshot.snapshotId());
      }
    }
    if (existingSnapshotIds != null
        && !existingSnapshotIds.isEmpty()
        && !importedSnapshotIds.isEmpty()) {
      List<Long> removals =
          existingSnapshotIds.stream().filter(id -> !importedSnapshotIds.contains(id)).toList();
      if (!removals.isEmpty()) {
        updates.add(
            Map.of(
                "action", CommitUpdateInspector.ACTION_REMOVE_SNAPSHOTS, "snapshot-ids", removals));
      }
    }
    return new TransactionCommitRequest(
        List.of(
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(namespacePath, tableName), requirements, updates)));
  }

  Map<String, String> mergeImportedProperties(
      Map<String, String> existing,
      ImportedMetadata importedMetadata,
      Map<String, String> registerIoProperties) {
    Map<String, String> merged = new LinkedHashMap<>();
    if (existing != null && !existing.isEmpty()) {
      merged.putAll(existing);
    }
    if (importedMetadata != null && importedMetadata.properties() != null) {
      merged.putAll(importedMetadata.properties());
    }
    if (importedMetadata != null
        && importedMetadata.tableLocation() != null
        && !importedMetadata.tableLocation().isBlank()) {
      merged.put("location", importedMetadata.tableLocation());
    }
    if (registerIoProperties != null && !registerIoProperties.isEmpty()) {
      registerIoProperties.forEach(
          (key, value) -> {
            if (!isSecretBearingProperty(key) && value != null) {
              merged.put(key, value);
            }
          });
    }
    return merged;
  }

  private boolean isSecretBearingProperty(String key) {
    if (key == null || key.isBlank()) {
      return false;
    }
    String normalized = key.toLowerCase();
    return normalized.contains("secret")
        || normalized.contains("access-key")
        || normalized.contains("session-token")
        || normalized.contains("token");
  }

  private List<ImportedSnapshot> snapshotsToImport(ImportedMetadata importedMetadata) {
    if (importedMetadata == null) {
      return List.of();
    }
    if (importedMetadata.snapshots() != null) {
      return importedMetadata.snapshots();
    }
    if (importedMetadata.currentSnapshot() == null) {
      return List.of();
    }
    return Collections.singletonList(importedMetadata.currentSnapshot());
  }

  private Map<String, Object> toSnapshotUpdate(
      ImportedSnapshot snapshot, ImportedMetadata importedMetadata, String metadataLocation) {
    if (snapshot == null) {
      return Map.of();
    }
    Map<String, Object> snapshotMap = new LinkedHashMap<>();
    if (snapshot.snapshotId() != null) {
      snapshotMap.put("snapshot-id", snapshot.snapshotId());
    }
    if (snapshot.parentSnapshotId() != null) {
      snapshotMap.put("parent-snapshot-id", snapshot.parentSnapshotId());
    }
    if (snapshot.sequenceNumber() != null) {
      snapshotMap.put("sequence-number", snapshot.sequenceNumber());
    }
    if (snapshot.timestampMs() != null) {
      snapshotMap.put("timestamp-ms", snapshot.timestampMs());
    }
    if (snapshot.manifestList() != null && !snapshot.manifestList().isBlank()) {
      snapshotMap.put("manifest-list", snapshot.manifestList());
    }
    if (snapshot.summary() != null && !snapshot.summary().isEmpty()) {
      snapshotMap.put("summary", snapshot.summary());
    }
    if (snapshot.schemaId() != null) {
      snapshotMap.put("schema-id", snapshot.schemaId());
    }
    if (importedMetadata != null
        && importedMetadata.schemaJson() != null
        && !importedMetadata.schemaJson().isBlank()) {
      snapshotMap.put("schema-json", importedMetadata.schemaJson());
    }
    if (isCurrentImportedSnapshot(snapshot, importedMetadata)
        && metadataLocation != null
        && !metadataLocation.isBlank()) {
      snapshotMap.put("metadata-location", metadataLocation);
    }
    return snapshotMap;
  }

  private boolean isCurrentImportedSnapshot(
      ImportedSnapshot snapshot, ImportedMetadata importedMetadata) {
    if (snapshot == null
        || snapshot.snapshotId() == null
        || importedMetadata == null
        || importedMetadata.currentSnapshot() == null
        || importedMetadata.currentSnapshot().snapshotId() == null) {
      return false;
    }
    return snapshot.snapshotId().equals(importedMetadata.currentSnapshot().snapshotId());
  }
}
