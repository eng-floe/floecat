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

import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class CommitUpdateInspector {
  public static final String REQUIREMENT_ASSERT_CREATE = "assert-create";

  private static final List<Map<String, Object>> ASSERT_CREATE_REQUIREMENTS =
      List.of(Map.of("type", REQUIREMENT_ASSERT_CREATE));

  private static final Set<String> SUPPORTED_REQUIREMENT_TYPES =
      Set.of(
          REQUIREMENT_ASSERT_CREATE,
          "assert-table-uuid",
          "assert-current-schema-id",
          "assert-last-assigned-field-id",
          "assert-last-assigned-partition-id",
          "assert-default-spec-id",
          "assert-default-sort-order-id",
          "assert-ref-snapshot-id");

  private static final Set<String> SUPPORTED_UPDATE_ACTIONS =
      Set.of(
          "set-properties",
          "remove-properties",
          "set-location",
          "add-snapshot",
          "remove-snapshots",
          "set-snapshot-ref",
          "remove-snapshot-ref",
          "assign-uuid",
          "upgrade-format-version",
          "add-schema",
          "set-current-schema",
          "add-spec",
          "set-default-spec",
          "add-sort-order",
          "set-default-sort-order",
          "remove-partition-specs",
          "remove-schemas",
          "set-statistics",
          "remove-statistics",
          "set-partition-statistics",
          "remove-partition-statistics",
          "add-encryption-key",
          "remove-encryption-key");

  private static final Set<String> CREATE_INITIALIZATION_ACTIONS =
      Set.of(
          "add-schema",
          "set-current-schema",
          "add-spec",
          "set-default-spec",
          "add-sort-order",
          "set-default-sort-order");

  private CommitUpdateInspector() {}

  public static List<Map<String, Object>> assertCreateRequirements() {
    return ASSERT_CREATE_REQUIREMENTS;
  }

  public static boolean isSupportedRequirementType(String type) {
    return type != null && SUPPORTED_REQUIREMENT_TYPES.contains(type);
  }

  public static boolean isSupportedUpdateAction(String action) {
    return action != null && SUPPORTED_UPDATE_ACTIONS.contains(action);
  }

  public static boolean isCreateInitializationAction(String action) {
    return action != null && CREATE_INITIALIZATION_ACTIONS.contains(action);
  }

  public record SnapshotRefMutation(
      boolean remove,
      String refName,
      Long snapshotId,
      String type,
      Long maxRefAgeMs,
      Long maxSnapshotAgeMs,
      Integer minSnapshotsToKeep) {}

  public record Parsed(
      List<Map<String, Object>> addedSnapshots,
      List<Long> addedSnapshotIds,
      Long latestAddedSnapshotId,
      List<Long> removedSnapshotIds,
      Set<Long> removedSnapshotIdsSet,
      List<SnapshotRefMutation> snapshotRefMutations,
      boolean containsSnapshotUpdates,
      String requestedMetadataLocation,
      Long requestedSequenceNumber,
      Long maxSnapshotSequenceNumber,
      Long requestedMainRefSnapshotId) {}

  public static Parsed inspect(TableRequests.Commit req) {
    return req == null ? inspectUpdates(List.of()) : inspectUpdates(req.updates());
  }

  public static Parsed inspectUpdates(List<Map<String, Object>> updates) {
    if (updates == null || updates.isEmpty()) {
      return empty();
    }

    List<Map<String, Object>> addedSnapshots = new ArrayList<>();
    List<Long> addedSnapshotIds = new ArrayList<>();
    Long latestAddedSnapshotId = null;
    List<Long> removedSnapshotIds = new ArrayList<>();
    Set<Long> removedSnapshotIdsSet = new LinkedHashSet<>();
    List<SnapshotRefMutation> snapshotRefMutations = new ArrayList<>();
    boolean containsSnapshotUpdates = false;
    String requestedMetadataLocation = null;
    Long requestedSequenceNumber = null;
    Long maxSnapshotSequenceNumber = null;
    Long requestedMainRefSnapshotId = null;

    for (Map<String, Object> update : updates) {
      if (update == null) {
        continue;
      }
      String action = TableMappingUtil.asString(update.get("action"));
      if (action == null || action.isBlank()) {
        continue;
      }
      switch (action) {
        case "add-snapshot" -> {
          containsSnapshotUpdates = true;
          Map<String, Object> snapshot = TableMappingUtil.asObjectMap(update.get("snapshot"));
          if (snapshot == null || snapshot.isEmpty()) {
            continue;
          }
          addedSnapshots.add(snapshot);
          Long snapshotId = TableMappingUtil.asLong(snapshot.get("snapshot-id"));
          if (snapshotId != null && snapshotId >= 0L) {
            addedSnapshotIds.add(snapshotId);
            if (snapshotId > 0L) {
              latestAddedSnapshotId = snapshotId;
            }
          }
          Long sequenceNumber = TableMappingUtil.asLong(snapshot.get("sequence-number"));
          if (sequenceNumber != null && sequenceNumber > 0L) {
            maxSnapshotSequenceNumber =
                maxSnapshotSequenceNumber == null
                    ? sequenceNumber
                    : Math.max(maxSnapshotSequenceNumber, sequenceNumber);
          }
        }
        case "remove-snapshots" -> {
          containsSnapshotUpdates = true;
          Object raw = update.get("snapshot-ids");
          if (!(raw instanceof List<?> ids)) {
            continue;
          }
          for (Object id : ids) {
            Long value = TableMappingUtil.asLong(id);
            if (value != null && value >= 0L) {
              removedSnapshotIds.add(value);
              removedSnapshotIdsSet.add(value);
            }
          }
        }
        case "set-snapshot-ref" -> {
          containsSnapshotUpdates = true;
          String refName = TableMappingUtil.asString(update.get("ref-name"));
          Long snapshotId = TableMappingUtil.asLong(update.get("snapshot-id"));
          String type = TableMappingUtil.asString(update.get("type"));
          Long maxRefAgeMs =
              TableMappingUtil.asLong(
                  TableMappingUtil.firstNonNull(
                      update.get("max-ref-age-ms"), update.get("max_ref_age_ms")));
          Long maxSnapshotAgeMs =
              TableMappingUtil.asLong(
                  TableMappingUtil.firstNonNull(
                      update.get("max-snapshot-age-ms"), update.get("max_snapshot_age_ms")));
          Integer minSnapshotsToKeep =
              TableMappingUtil.asInteger(
                  TableMappingUtil.firstNonNull(
                      update.get("min-snapshots-to-keep"), update.get("min_snapshots_to_keep")));
          snapshotRefMutations.add(
              new SnapshotRefMutation(
                  false,
                  refName,
                  snapshotId,
                  type,
                  maxRefAgeMs,
                  maxSnapshotAgeMs,
                  minSnapshotsToKeep));
          if (requestedMainRefSnapshotId != null) {
            continue;
          }
          if (!"main".equals(refName)) {
            continue;
          }
          requestedMainRefSnapshotId = snapshotId;
        }
        case "remove-snapshot-ref" -> {
          containsSnapshotUpdates = true;
          snapshotRefMutations.add(
              new SnapshotRefMutation(
                  true,
                  TableMappingUtil.asString(update.get("ref-name")),
                  null,
                  null,
                  null,
                  null,
                  null));
        }
        case "set-properties" -> {
          Map<String, Object> props = TableMappingUtil.asObjectMap(update.get("updates"));
          if (props == null || props.isEmpty()) {
            continue;
          }
          if (requestedMetadataLocation == null || requestedMetadataLocation.isBlank()) {
            String location = TableMappingUtil.asString(props.get("metadata-location"));
            if (location != null && !location.isBlank()) {
              requestedMetadataLocation = location;
            }
          }
          Long sequence = TableMappingUtil.asLong(props.get("last-sequence-number"));
          if (sequence != null && sequence > 0L) {
            requestedSequenceNumber =
                requestedSequenceNumber == null
                    ? sequence
                    : Math.max(requestedSequenceNumber, sequence);
          }
        }
        default -> {
          // Ignore other actions.
        }
      }
    }

    return new Parsed(
        addedSnapshots.isEmpty() ? List.of() : List.copyOf(addedSnapshots),
        addedSnapshotIds.isEmpty() ? List.of() : List.copyOf(addedSnapshotIds),
        latestAddedSnapshotId,
        removedSnapshotIds.isEmpty() ? List.of() : List.copyOf(removedSnapshotIds),
        removedSnapshotIdsSet.isEmpty() ? Set.of() : Set.copyOf(removedSnapshotIdsSet),
        snapshotRefMutations.isEmpty() ? List.of() : List.copyOf(snapshotRefMutations),
        containsSnapshotUpdates,
        requestedMetadataLocation,
        requestedSequenceNumber,
        maxSnapshotSequenceNumber,
        requestedMainRefSnapshotId);
  }

  private static Parsed empty() {
    return new Parsed(
        List.of(), List.of(), null, List.of(), Set.of(), List.of(), false, null, null, null, null);
  }
}
