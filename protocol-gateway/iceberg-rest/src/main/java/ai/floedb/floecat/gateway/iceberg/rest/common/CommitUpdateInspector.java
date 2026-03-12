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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class CommitUpdateInspector {
  public static final String REQUIREMENT_ASSERT_CREATE = "assert-create";
  public static final String ACTION_SET_PROPERTIES = "set-properties";
  public static final String ACTION_REMOVE_PROPERTIES = "remove-properties";
  public static final String ACTION_SET_LOCATION = "set-location";
  public static final String ACTION_ADD_SNAPSHOT = "add-snapshot";
  public static final String ACTION_REMOVE_SNAPSHOTS = "remove-snapshots";
  public static final String ACTION_SET_SNAPSHOT_REF = "set-snapshot-ref";
  public static final String ACTION_REMOVE_SNAPSHOT_REF = "remove-snapshot-ref";
  public static final String ACTION_ASSIGN_UUID = "assign-uuid";
  public static final String ACTION_UPGRADE_FORMAT_VERSION = "upgrade-format-version";
  public static final String ACTION_ADD_SCHEMA = "add-schema";
  public static final String ACTION_SET_CURRENT_SCHEMA = "set-current-schema";
  public static final String ACTION_ADD_SPEC = "add-spec";
  public static final String ACTION_SET_DEFAULT_SPEC = "set-default-spec";
  public static final String ACTION_ADD_SORT_ORDER = "add-sort-order";
  public static final String ACTION_SET_DEFAULT_SORT_ORDER = "set-default-sort-order";
  public static final String ACTION_REMOVE_PARTITION_SPECS = "remove-partition-specs";
  public static final String ACTION_REMOVE_SCHEMAS = "remove-schemas";
  public static final String ACTION_SET_STATISTICS = "set-statistics";
  public static final String ACTION_REMOVE_STATISTICS = "remove-statistics";
  public static final String ACTION_SET_PARTITION_STATISTICS = "set-partition-statistics";
  public static final String ACTION_REMOVE_PARTITION_STATISTICS = "remove-partition-statistics";
  public static final String ACTION_ADD_ENCRYPTION_KEY = "add-encryption-key";
  public static final String ACTION_REMOVE_ENCRYPTION_KEY = "remove-encryption-key";

  public enum UpdateAction {
    SET_PROPERTIES(ACTION_SET_PROPERTIES, true, false, false),
    REMOVE_PROPERTIES(ACTION_REMOVE_PROPERTIES, true, false, false),
    SET_LOCATION(ACTION_SET_LOCATION, false, false, false),
    ADD_SNAPSHOT(ACTION_ADD_SNAPSHOT, false, false, false),
    REMOVE_SNAPSHOTS(ACTION_REMOVE_SNAPSHOTS, false, false, false),
    SET_SNAPSHOT_REF(ACTION_SET_SNAPSHOT_REF, false, false, false),
    REMOVE_SNAPSHOT_REF(ACTION_REMOVE_SNAPSHOT_REF, false, false, false),
    ASSIGN_UUID(ACTION_ASSIGN_UUID, false, false, false),
    UPGRADE_FORMAT_VERSION(ACTION_UPGRADE_FORMAT_VERSION, false, true, false),
    ADD_SCHEMA(ACTION_ADD_SCHEMA, false, true, true),
    SET_CURRENT_SCHEMA(ACTION_SET_CURRENT_SCHEMA, false, true, true),
    ADD_SPEC(ACTION_ADD_SPEC, false, true, true),
    SET_DEFAULT_SPEC(ACTION_SET_DEFAULT_SPEC, false, true, true),
    ADD_SORT_ORDER(ACTION_ADD_SORT_ORDER, false, true, true),
    SET_DEFAULT_SORT_ORDER(ACTION_SET_DEFAULT_SORT_ORDER, false, true, true),
    REMOVE_PARTITION_SPECS(ACTION_REMOVE_PARTITION_SPECS, false, false, false),
    REMOVE_SCHEMAS(ACTION_REMOVE_SCHEMAS, false, false, false),
    SET_STATISTICS(ACTION_SET_STATISTICS, false, false, false),
    REMOVE_STATISTICS(ACTION_REMOVE_STATISTICS, false, false, false),
    SET_PARTITION_STATISTICS(ACTION_SET_PARTITION_STATISTICS, false, false, false),
    REMOVE_PARTITION_STATISTICS(ACTION_REMOVE_PARTITION_STATISTICS, false, false, false),
    ADD_ENCRYPTION_KEY(ACTION_ADD_ENCRYPTION_KEY, false, false, false),
    REMOVE_ENCRYPTION_KEY(ACTION_REMOVE_ENCRYPTION_KEY, false, false, false);

    private static final Map<String, UpdateAction> BY_WIRE_NAME;

    static {
      Map<String, UpdateAction> byName = new HashMap<>();
      for (UpdateAction action : values()) {
        byName.put(action.wireName, action);
      }
      BY_WIRE_NAME = Collections.unmodifiableMap(byName);
    }

    private final String wireName;
    private final boolean propertyAction;
    private final boolean tableDefinitionAction;
    private final boolean createInitializationAction;

    UpdateAction(
        String wireName,
        boolean propertyAction,
        boolean tableDefinitionAction,
        boolean createInitializationAction) {
      this.wireName = wireName;
      this.propertyAction = propertyAction;
      this.tableDefinitionAction = tableDefinitionAction;
      this.createInitializationAction = createInitializationAction;
    }

    public static UpdateAction fromWireName(String wireName) {
      if (wireName == null) {
        return null;
      }
      return BY_WIRE_NAME.get(wireName);
    }

    public boolean isPropertyAction() {
      return propertyAction;
    }

    public boolean isTableDefinitionAction() {
      return tableDefinitionAction;
    }

    public boolean isCreateInitializationAction() {
      return createInitializationAction;
    }
  }

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

  private CommitUpdateInspector() {}

  public static List<Map<String, Object>> assertCreateRequirements() {
    return ASSERT_CREATE_REQUIREMENTS;
  }

  public static boolean isSupportedRequirementType(String type) {
    return type != null && SUPPORTED_REQUIREMENT_TYPES.contains(type);
  }

  public static boolean isSupportedUpdateAction(String action) {
    return actionTypeOf(action) != null;
  }

  public static boolean isCreateInitializationAction(String action) {
    UpdateAction actionType = actionTypeOf(action);
    return actionType != null && actionType.isCreateInitializationAction();
  }

  public static boolean isPropertyAction(String action) {
    UpdateAction actionType = actionTypeOf(action);
    return actionType != null && actionType.isPropertyAction();
  }

  public static boolean isTableDefinitionAction(String action) {
    UpdateAction actionType = actionTypeOf(action);
    return actionType != null && actionType.isTableDefinitionAction();
  }

  public static String actionOf(Map<String, Object> update) {
    return TableMappingUtil.asString(update == null ? null : update.get("action"));
  }

  public static UpdateAction actionTypeOf(String action) {
    return UpdateAction.fromWireName(action);
  }

  public static UpdateAction actionTypeOf(Map<String, Object> update) {
    return actionTypeOf(actionOf(update));
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
      UpdateAction action = actionTypeOf(update);
      if (action == null) {
        continue;
      }
      switch (action) {
        case ADD_SNAPSHOT -> {
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
        case REMOVE_SNAPSHOTS -> {
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
        case SET_SNAPSHOT_REF -> {
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
        case REMOVE_SNAPSHOT_REF -> {
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
        case SET_PROPERTIES -> {
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
