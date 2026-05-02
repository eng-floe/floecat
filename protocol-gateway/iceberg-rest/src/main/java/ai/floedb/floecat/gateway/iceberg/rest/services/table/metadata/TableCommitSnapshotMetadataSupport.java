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

package ai.floedb.floecat.gateway.iceberg.rest.services.table.metadata;

import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.normalizeFormatVersionForSnapshots;

import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.common.CommitUpdateInspector;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class TableCommitSnapshotMetadataSupport {

  TableMetadataView mergeSnapshotUpdates(
      TableMetadataView metadata, CommitUpdateInspector.Parsed parsed) {
    List<Map<String, Object>> addedSnapshots = parsed.addedSnapshots();
    var removedSnapshotIds = parsed.removedSnapshotIdsSet();
    if (addedSnapshots.isEmpty() && (removedSnapshotIds == null || removedSnapshotIds.isEmpty())) {
      return metadata;
    }
    List<Map<String, Object>> existing =
        metadata.snapshots() == null ? List.of() : metadata.snapshots();
    Map<Long, Map<String, Object>> merged = new LinkedHashMap<>();
    for (Map<String, Object> snapshot : existing) {
      Long id = snapshotId(snapshot);
      if (id != null) {
        if (removedSnapshotIds != null && removedSnapshotIds.contains(id)) {
          continue;
        }
        merged.put(id, new LinkedHashMap<>(snapshot));
      }
    }
    for (Map<String, Object> snapshot : addedSnapshots) {
      Long id = snapshotId(snapshot);
      if (id == null) {
        continue;
      }
      if (removedSnapshotIds != null && removedSnapshotIds.contains(id)) {
        continue;
      }
      merged.put(id, new LinkedHashMap<>(snapshot));
    }
    List<Map<String, Object>> updatedSnapshots =
        merged.isEmpty() ? List.of() : List.copyOf(merged.values());
    Long requestSequence = parsed.maxSnapshotSequenceNumber();
    Long snapshotSequence = maxSequenceFromSnapshots(updatedSnapshots);
    Long maxSequence = maxNonNull(snapshotSequence, requestSequence);
    Integer formatVersion =
        normalizeFormatVersionForSnapshots(metadata.formatVersion(), requestSequence);
    Map<String, String> props =
        metadata.properties() == null
            ? new LinkedHashMap<>()
            : new LinkedHashMap<>(metadata.properties());
    Long currentSnapshotId = desiredCurrentSnapshotId(metadata, parsed, updatedSnapshots, props);
    Map<String, Object> refs = mergeSnapshotRefs(metadata.refs(), parsed, currentSnapshotId);
    if (maxSequence != null && maxSequence > 0) {
      props.put("last-sequence-number", Long.toString(maxSequence));
    }
    if (currentSnapshotId != null && currentSnapshotId >= 0) {
      props.put("current-snapshot-id", Long.toString(currentSnapshotId));
    }
    return TableMetadataViewSupport.copyMetadata(metadata)
        .formatVersion(formatVersion)
        .properties(Map.copyOf(props))
        .currentSnapshotId(
            currentSnapshotId != null ? currentSnapshotId : metadata.currentSnapshotId())
        .lastSequenceNumber(maxSequence != null ? maxSequence : metadata.lastSequenceNumber())
        .refs(refs)
        .snapshots(updatedSnapshots)
        .build();
  }

  private Long desiredCurrentSnapshotId(
      TableMetadataView metadata,
      CommitUpdateInspector.Parsed parsed,
      List<Map<String, Object>> updatedSnapshots,
      Map<String, String> props) {
    Long requestedMainRefSnapshotId = parsed == null ? null : parsed.requestedMainRefSnapshotId();
    if (requestedMainRefSnapshotId != null && requestedMainRefSnapshotId >= 0) {
      return requestedMainRefSnapshotId;
    }
    Long propertyCurrentSnapshotId = parseLong(props.get("current-snapshot-id"));
    if (propertyCurrentSnapshotId != null && propertyCurrentSnapshotId >= 0) {
      return propertyCurrentSnapshotId;
    }
    if (metadata.currentSnapshotId() == null) {
      return latestSnapshotId(updatedSnapshots);
    }
    return metadata.currentSnapshotId();
  }

  private Map<String, Object> mergeSnapshotRefs(
      Map<String, Object> existingRefs,
      CommitUpdateInspector.Parsed parsed,
      Long currentSnapshotId) {
    Map<String, Object> refs = new LinkedHashMap<>();
    if (existingRefs != null && !existingRefs.isEmpty()) {
      refs.putAll(existingRefs);
    }
    if (parsed != null) {
      for (CommitUpdateInspector.SnapshotRefMutation mutation : parsed.snapshotRefMutations()) {
        if (mutation == null) {
          continue;
        }
        String refName = mutation.refName();
        if (refName == null || refName.isBlank()) {
          continue;
        }
        if (mutation.remove()) {
          refs.remove(refName);
          continue;
        }
        Long snapshotId = mutation.snapshotId();
        if (snapshotId == null || snapshotId < 0) {
          continue;
        }
        Map<String, Object> ref = new LinkedHashMap<>();
        ref.put("snapshot-id", snapshotId);
        ref.put(
            "type",
            mutation.type() == null || mutation.type().isBlank() ? "branch" : mutation.type());
        if (mutation.maxRefAgeMs() != null) {
          ref.put("max-ref-age-ms", mutation.maxRefAgeMs());
        }
        if (mutation.maxSnapshotAgeMs() != null) {
          ref.put("max-snapshot-age-ms", mutation.maxSnapshotAgeMs());
        }
        if (mutation.minSnapshotsToKeep() != null) {
          ref.put("min-snapshots-to-keep", mutation.minSnapshotsToKeep());
        }
        refs.put(refName, Map.copyOf(ref));
      }
    }
    if (currentSnapshotId != null && currentSnapshotId >= 0) {
      refs.put("main", Map.of("snapshot-id", currentSnapshotId, "type", "branch"));
    }
    return Map.copyOf(refs);
  }

  private Long latestSnapshotId(List<Map<String, Object>> snapshots) {
    Long latest = null;
    for (Map<String, Object> snapshot : snapshots) {
      Long id = snapshotId(snapshot);
      if (id != null && id >= 0) {
        latest = id;
      }
    }
    return latest;
  }

  private Long snapshotId(Map<String, Object> snapshot) {
    if (snapshot == null) {
      return null;
    }
    return parseLong(snapshot.get("snapshot-id"));
  }

  private Long maxSequenceFromSnapshots(List<Map<String, Object>> snapshots) {
    Long max = null;
    for (Map<String, Object> snapshot : snapshots) {
      if (snapshot == null) {
        continue;
      }
      Long seq = parseLong(snapshot.get("sequence-number"));
      if (seq == null || seq <= 0) {
        continue;
      }
      max = max == null ? seq : Math.max(max, seq);
    }
    return max;
  }

  private Long maxNonNull(Long... values) {
    Long max = null;
    if (values == null) {
      return null;
    }
    for (Long value : values) {
      if (value == null || value <= 0) {
        continue;
      }
      max = max == null ? value : Math.max(max, value);
    }
    return max;
  }

  private Long parseLong(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number number) {
      return number.longValue();
    }
    String text = value.toString();
    if (text.isBlank()) {
      return null;
    }
    try {
      return Long.parseLong(text);
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
