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

package ai.floedb.floecat.delta.identity;

import ai.floedb.floecat.catalog.rpc.ColumnIdentityHistoryTier;
import ai.floedb.floecat.catalog.rpc.ColumnIdentityMap;
import ai.floedb.floecat.schema.identity.ColumnIdentityMaps;
import ai.floedb.floecat.schema.identity.ColumnPath;
import ai.floedb.floecat.schema.identity.HistoryCoverage;
import ai.floedb.floecat.schema.identity.HistoryTier;
import ai.floedb.floecat.schema.identity.IdentityMode;
import ai.floedb.floecat.schema.identity.NodeKind;
import ai.floedb.floecat.schema.identity.PreferredCanonicalIds;
import ai.floedb.floecat.schema.identity.SchemaIdentityEntry;
import ai.floedb.floecat.schema.identity.SchemaIdentityReconciler;
import ai.floedb.floecat.schema.identity.SchemaIdentityState;
import io.delta.kernel.Snapshot;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Shared snapshot and current-table identity reconciliation for every Delta ingestion path. */
public final class DeltaIdentityService {

  public IdentityResult reconcileSnapshots(
      DeltaHistorySource history, PersistedIdentityLookup persisted, List<Long> versions) {
    Objects.requireNonNull(history, "history");
    Objects.requireNonNull(persisted, "persisted");
    Objects.requireNonNull(versions, "versions");
    LinkedHashMap<Long, ColumnIdentityMap> maps = new LinkedHashMap<>();
    List<IdentityResult.Failure> failures = new ArrayList<>();
    versions.stream()
        .distinct()
        .sorted()
        .forEach(version -> reconcileVersion(history, persisted, version, maps, failures));
    return new IdentityResult(maps, failures);
  }

  public IdentityResult reconcileCurrent(
      DeltaHistorySource history, PersistedIdentityLookup persisted) {
    return reconcileSnapshots(history, persisted, List.of(history.headVersion()));
  }

  private void reconcileVersion(
      DeltaHistorySource history,
      PersistedIdentityLookup persisted,
      long version,
      Map<Long, ColumnIdentityMap> maps,
      List<IdentityResult.Failure> failures) {
    try {
      Snapshot snapshot;
      try {
        Optional<Snapshot> loaded = history.snapshot(version);
        if (loaded.isEmpty()) {
          return;
        }
        snapshot = loaded.orElseThrow();
      } catch (RuntimeException unavailable) {
        if (isTruncatedHistory(unavailable)) {
          return;
        }
        throw unavailable;
      }
      Optional<ColumnIdentityMap> exactSnapshot = persisted.exact(version);
      Optional<ColumnIdentityMap> exact =
          exactSnapshot.isPresent() ? exactSnapshot : exactTableMap(persisted, version);
      if (exact.isPresent()) {
        try {
          validatePersisted(history.tableName(), version, snapshot, exact.orElseThrow());
          maps.put(version, exact.orElseThrow());
          return;
        } catch (InvalidPersistedIdentityException mismatch) {
          if (exactSnapshot.isPresent() || history.historyTier().provesContinuity()) {
            throw mismatch;
          }
          // The map itself was restored successfully, but a current-schema-only catalog now
          // describes a different schema or mode. That is an unprovable history boundary, not
          // corrupt persisted state, so allocate a new generation above the old high-water mark.
          maps.put(
              version,
              reset(
                      snapshot,
                      version,
                      exact.orElseThrow(),
                      persisted.highWaterMark(),
                      history.historyTier())
                  .identityMap());
          return;
        }
      }

      Optional<Map.Entry<Long, ColumnIdentityMap>> successor = persisted.ceiling(version + 1L);
      Optional<ColumnIdentityMap> dataOnly =
          successor.flatMap(entry -> dataOnlyBackfill(history, version, snapshot, entry));
      if (dataOnly.isPresent()) {
        maps.put(version, bindSourceAnchor(dataOnly.orElseThrow(), history));
        return;
      }

      Optional<Map.Entry<Long, ColumnIdentityMap>> predecessor =
          predecessor(persisted, maps, version);
      long highWaterMark =
          maps.values().stream()
              .mapToLong(ColumnIdentityMap::getHighWaterMark)
              .max()
              .orElse(persisted.highWaterMark());
      highWaterMark = Math.max(highWaterMark, persisted.highWaterMark());
      PreferredCanonicalIds preferred =
          successor
              .map(entry -> preferredSuccessorIds(history, version, snapshot, entry))
              .orElse(PreferredCanonicalIds.NONE);
      ColumnIdentityMap reconciled =
          reconcileFromPredecessor(
              history, version, snapshot, predecessor, highWaterMark, preferred);
      maps.put(version, bindSourceAnchor(reconciled, history));
    } catch (RuntimeException failure) {
      failures.add(toFailure(history.tableName(), version, failure));
    }
  }

  private static Optional<ColumnIdentityMap> exactTableMap(
      PersistedIdentityLookup persisted, long version) {
    return persisted
        .tableMap()
        .filter(map -> ColumnIdentityMaps.restore(map).sourceVersion() == version);
  }

  private static Optional<Map.Entry<Long, ColumnIdentityMap>> predecessor(
      PersistedIdentityLookup persisted, Map<Long, ColumnIdentityMap> reconciled, long version) {
    Optional<Map.Entry<Long, ColumnIdentityMap>> floor = persisted.floor(version - 1L);
    Optional<Map.Entry<Long, ColumnIdentityMap>> generatedFloor =
        reconciled.entrySet().stream()
            .filter(entry -> entry.getKey() < version)
            .max(Map.Entry.comparingByKey());
    if (generatedFloor.isPresent()
        && (floor.isEmpty()
            || generatedFloor.orElseThrow().getKey() > floor.orElseThrow().getKey())) {
      floor = generatedFloor;
    }
    Optional<ColumnIdentityMap> tableMap = persisted.tableMap();
    if (tableMap.isEmpty()) {
      return floor;
    }
    SchemaIdentityState tableState = ColumnIdentityMaps.restore(tableMap.orElseThrow());
    if (tableState.sourceVersion() >= version
        || (floor.isPresent() && floor.orElseThrow().getKey() >= tableState.sourceVersion())) {
      return floor;
    }
    return Optional.of(Map.entry(tableState.sourceVersion(), tableMap.orElseThrow()));
  }

  private static Optional<ColumnIdentityMap> dataOnlyBackfill(
      DeltaHistorySource history,
      long version,
      Snapshot snapshot,
      Map.Entry<Long, ColumnIdentityMap> successor) {
    DeltaHistorySource.MetadataHistory range =
        history.metadataVersions(version + 1L, successor.getKey());
    if (!range.complete() || !range.versions().isEmpty()) {
      return Optional.empty();
    }
    Snapshot successorSnapshot = history.snapshot(successor.getKey()).orElse(null);
    if (successorSnapshot == null) {
      return Optional.empty();
    }
    validatePersisted(
        history.tableName(), successor.getKey(), successorSnapshot, successor.getValue());
    HistoryTier provenance =
        weakerTier(
            ColumnIdentityMaps.restore(successor.getValue()).historyTier(), history.historyTier());
    return Optional.of(carryForward(successor.getValue(), snapshot, version, provenance));
  }

  private static ColumnIdentityMap reconcileFromPredecessor(
      DeltaHistorySource history,
      long version,
      Snapshot selected,
      Optional<Map.Entry<Long, ColumnIdentityMap>> predecessor,
      long highWaterMark,
      PreferredCanonicalIds preferred) {
    if (predecessor.isEmpty()) {
      return reconcile(selected, version, null, highWaterMark, preferred, history.historyTier())
          .identityMap();
    }

    Map.Entry<Long, ColumnIdentityMap> entry = predecessor.orElseThrow();
    SchemaIdentityState state = ColumnIdentityMaps.restore(entry.getValue());
    if ((!state.sourceVersionAnchored() && history.sourceVersionAnchored())
        || (!state.historyTier().provesContinuity() && history.historyTier().provesContinuity())) {
      return reset(selected, version, entry.getValue(), highWaterMark, history.historyTier())
          .identityMap();
    }
    if (!history.historyTier().provesContinuity()) {
      return reconcileWithoutOrderedHistory(
          history, version, selected, entry.getValue(), highWaterMark, preferred);
    }
    DeltaHistorySource.MetadataHistory metadata =
        history.metadataVersions(state.sourceVersion() + 1L, version);
    if (!metadata.complete()) {
      return reset(selected, version, entry.getValue(), highWaterMark, history.historyTier())
          .identityMap();
    }

    ColumnIdentityMap current = entry.getValue();
    long previousNativeHighWaterMark = persistedNativeHighWaterMark(current);
    boolean selectedReconciled = false;
    for (long metadataVersion : metadata.versions()) {
      Snapshot metadataSnapshot =
          metadataVersion == version ? selected : history.snapshot(metadataVersion).orElse(null);
      if (metadataSnapshot == null) {
        return reset(selected, version, current, highWaterMark, history.historyTier())
            .identityMap();
      }
      IdentityMode currentMode = ColumnIdentityMaps.restore(current).mode();
      IdentityMode nextMode = mode(metadataSnapshot);
      long nextNativeHighWaterMark = nativeHighWaterMark(metadataSnapshot);
      boolean generationBoundary =
          currentMode != nextMode
              || (nextMode == IdentityMode.NATIVE_FIELD_ID
                  && nextNativeHighWaterMark < previousNativeHighWaterMark);
      current =
          generationBoundary
              ? reset(
                      metadataSnapshot,
                      metadataVersion,
                      current,
                      highWaterMark,
                      history.historyTier())
                  .identityMap()
              : reconcile(
                      metadataSnapshot,
                      metadataVersion,
                      current,
                      highWaterMark,
                      metadataVersion == version ? preferred : PreferredCanonicalIds.NONE,
                      weakerTier(
                          ColumnIdentityMaps.restore(current).historyTier(), history.historyTier()))
                  .identityMap();
      previousNativeHighWaterMark = nextNativeHighWaterMark;
      selectedReconciled = metadataVersion == version;
    }
    if (selectedReconciled) {
      return current;
    }
    return carryForward(
        current,
        selected,
        version,
        weakerTier(ColumnIdentityMaps.restore(current).historyTier(), history.historyTier()));
  }

  private static ColumnIdentityMap reconcileWithoutOrderedHistory(
      DeltaHistorySource history,
      long version,
      Snapshot selected,
      ColumnIdentityMap previous,
      long highWaterMark,
      PreferredCanonicalIds preferred) {
    SchemaIdentityState state = ColumnIdentityMaps.restore(previous);
    IdentityMode selectedMode = mode(selected);
    HistoryTier provenance = weakerTier(state.historyTier(), history.historyTier());
    boolean generationBoundary = state.mode() != selectedMode;

    if (!generationBoundary && history.historyTier() == HistoryTier.UNORDERED_QUERY_HISTORY) {
      DeltaHistorySource.UnorderedHistory unordered = history.unorderedHistory();
      generationBoundary =
          selectedMode == IdentityMode.NATIVE_FIELD_ID
              && (unordered.metadata().stream()
                      .map(DeltaHistorySource.HistoricalMetadata::configuration)
                      .map(ColumnMappingMode::fromTableProperties)
                      .anyMatch(mode -> !mode.isEnabled())
                  || unordered.protocols().stream()
                      .anyMatch(protocol -> !protocol.supportsColumnMapping()));
    }

    if (!generationBoundary && selectedMode == IdentityMode.NATIVE_FIELD_ID) {
      long previousNativeHighWaterMark = persistedNativeHighWaterMark(previous);
      long selectedNativeHighWaterMark = nativeHighWaterMark(selected);
      generationBoundary =
          selectedNativeHighWaterMark < previousNativeHighWaterMark
              || reusesUnownedNativeId(selected, state, previousNativeHighWaterMark);
    }

    return generationBoundary
        ? reset(selected, version, previous, highWaterMark, history.historyTier()).identityMap()
        : reconcile(selected, version, previous, highWaterMark, preferred, provenance)
            .identityMap();
  }

  private static boolean reusesUnownedNativeId(
      Snapshot snapshot, SchemaIdentityState previous, long previousNativeHighWaterMark) {
    Set<Integer> previousNativeIds = new LinkedHashSet<>();
    previous.entries().stream()
        .filter(entry -> isField(entry.path()))
        .forEach(entry -> entry.nativeFieldId().ifPresent(previousNativeIds::add));
    return DeltaColumnMapping.resolveSchema(snapshot).schema().nodes().stream()
        .filter(node -> node.kind() == NodeKind.FIELD)
        .map(node -> node.nativeFieldId().orElseThrow())
        .anyMatch(
            nativeId ->
                nativeId <= previousNativeHighWaterMark && !previousNativeIds.contains(nativeId));
  }

  private static HistoryTier weakerTier(HistoryTier previous, HistoryTier current) {
    if (previous == HistoryTier.NONE || current == HistoryTier.NONE) {
      return HistoryTier.NONE;
    }
    if (previous == HistoryTier.UNORDERED_QUERY_HISTORY
        || current == HistoryTier.UNORDERED_QUERY_HISTORY) {
      return HistoryTier.UNORDERED_QUERY_HISTORY;
    }
    return current;
  }

  private static ColumnIdentityMap bindSourceAnchor(
      ColumnIdentityMap map, DeltaHistorySource history) {
    return history.sourceVersionAnchored()
        ? map
        : ColumnIdentityMaps.withoutAnchoredSourceVersion(map);
  }

  private static DeltaCanonicalIdentity.Reconciled reconcile(
      Snapshot snapshot,
      long version,
      ColumnIdentityMap previous,
      long highWaterMark,
      PreferredCanonicalIds preferred,
      HistoryTier historyTier) {
    DeltaResolvedSchema resolved = DeltaColumnMapping.resolveSchema(snapshot);
    IdentityMode mode = DeltaCanonicalIdentity.identityMode(resolved);
    Optional<SchemaIdentityState> previousState =
        previous == null ? Optional.empty() : Optional.of(ColumnIdentityMaps.restore(previous));
    long floor =
        mode == IdentityMode.NATIVE_FIELD_ID
            ? Math.max(highWaterMark, DeltaColumnMapping.maxColumnId(snapshot))
            : highWaterMark;
    SchemaIdentityReconciler.Result identity =
        SchemaIdentityReconciler.reconcile(
            resolved.schema(),
            version,
            mode,
            previousState,
            HistoryCoverage.COMPLETE_METADATA_HISTORY,
            floor,
            preferred,
            historyTier);
    return new DeltaCanonicalIdentity.Reconciled(
        resolved,
        identity,
        withNativeHighWaterMark(ColumnIdentityMaps.toProto(identity.state()), snapshot));
  }

  private static DeltaCanonicalIdentity.Reconciled reset(
      Snapshot snapshot,
      long version,
      ColumnIdentityMap previous,
      long highWaterMark,
      HistoryTier historyTier) {
    DeltaResolvedSchema resolved = DeltaColumnMapping.resolveSchema(snapshot);
    IdentityMode mode = DeltaCanonicalIdentity.identityMode(resolved);
    long floor =
        Math.max(
            highWaterMark,
            previous == null ? 0L : ColumnIdentityMaps.restore(previous).highWaterMark());
    if (mode == IdentityMode.NATIVE_FIELD_ID) {
      floor = Math.max(floor, DeltaColumnMapping.maxColumnId(snapshot));
    }
    SchemaIdentityReconciler.Result identity =
        SchemaIdentityReconciler.reset(resolved.schema(), version, mode, floor, historyTier);
    return new DeltaCanonicalIdentity.Reconciled(
        resolved,
        identity,
        withNativeHighWaterMark(ColumnIdentityMaps.toProto(identity.state()), snapshot));
  }

  private static PreferredCanonicalIds preferredSuccessorIds(
      DeltaHistorySource history,
      long version,
      Snapshot selected,
      Map.Entry<Long, ColumnIdentityMap> successor) {
    DeltaHistorySource.MetadataHistory range =
        history.metadataVersions(version + 1L, successor.getKey());
    if (!range.complete()) {
      return PreferredCanonicalIds.NONE;
    }
    Snapshot successorSnapshot = history.snapshot(successor.getKey()).orElse(null);
    if (successorSnapshot == null) {
      return PreferredCanonicalIds.NONE;
    }
    validatePersisted(
        history.tableName(), successor.getKey(), successorSnapshot, successor.getValue());
    IdentityMode selectedMode = mode(selected);
    SchemaIdentityState successorState = ColumnIdentityMaps.restore(successor.getValue());
    if (selectedMode != successorState.mode()) {
      return PreferredCanonicalIds.NONE;
    }

    Set<ColumnPath> paths = new LinkedHashSet<>();
    Set<Integer> nativeIds = new LinkedHashSet<>();
    successorState.entries().stream()
        .filter(entry -> isField(entry.path()))
        .forEach(
            entry -> {
              paths.add(entry.path());
              entry.nativeFieldId().ifPresent(nativeIds::add);
            });
    long previousNativeHighWaterMark = nativeHighWaterMark(selected);
    List<Long> versions = new ArrayList<>(range.versions());
    if (!versions.contains(successor.getKey())) {
      versions.add(successor.getKey());
    }
    versions.sort(Comparator.naturalOrder());
    for (long futureVersion : versions) {
      Snapshot future = history.snapshot(futureVersion).orElse(null);
      if (future == null || mode(future) != selectedMode) {
        return PreferredCanonicalIds.NONE;
      }
      long nextNativeHighWaterMark = nativeHighWaterMark(future);
      if (selectedMode == IdentityMode.NATIVE_FIELD_ID
          && nextNativeHighWaterMark < previousNativeHighWaterMark) {
        return PreferredCanonicalIds.NONE;
      }
      previousNativeHighWaterMark = nextNativeHighWaterMark;
      Set<ColumnPath> futurePaths = new LinkedHashSet<>();
      Set<Integer> futureNativeIds = new LinkedHashSet<>();
      DeltaColumnMapping.resolveSchema(future).schema().nodes().stream()
          .filter(node -> node.kind() == NodeKind.FIELD)
          .forEach(
              node -> {
                futurePaths.add(node.path());
                node.nativeFieldId().ifPresent(futureNativeIds::add);
              });
      paths.retainAll(futurePaths);
      nativeIds.retainAll(futureNativeIds);
    }

    Map<ColumnPath, Long> byPath = new LinkedHashMap<>();
    Map<Integer, Long> byNativeId = new LinkedHashMap<>();
    for (SchemaIdentityEntry entry : successorState.entries()) {
      if (!isField(entry.path())) {
        continue;
      }
      if (paths.contains(entry.path())) {
        byPath.put(entry.path(), entry.canonicalId());
      }
      if (entry.nativeFieldId().isPresent()
          && nativeIds.contains(entry.nativeFieldId().getAsInt())) {
        byNativeId.put(entry.nativeFieldId().getAsInt(), entry.canonicalId());
      }
    }
    return selectedMode == IdentityMode.NATIVE_FIELD_ID
        ? PreferredCanonicalIds.byNativeId(byNativeId)
        : PreferredCanonicalIds.byPath(byPath);
  }

  private static boolean isField(ColumnPath path) {
    return path.elements().getLast().kind() == NodeKind.FIELD;
  }

  private static IdentityMode mode(Snapshot snapshot) {
    return DeltaCanonicalIdentity.identityMode(DeltaColumnMapping.resolveSchema(snapshot));
  }

  private static long nativeHighWaterMark(Snapshot snapshot) {
    if (snapshot == null || mode(snapshot) != IdentityMode.NATIVE_FIELD_ID) {
      return 0L;
    }
    return DeltaColumnMapping.maxColumnId(snapshot);
  }

  private static long persistedNativeHighWaterMark(ColumnIdentityMap map) {
    SchemaIdentityState state = ColumnIdentityMaps.restore(map);
    if (state.mode() != IdentityMode.NATIVE_FIELD_ID) {
      return 0L;
    }
    if (!map.hasNativeHighWaterMark()) {
      throw new IllegalArgumentException("Native identity map has no native high-water mark");
    }
    return map.getNativeHighWaterMark();
  }

  private static ColumnIdentityMap carryForward(
      ColumnIdentityMap map, Snapshot snapshot, long version, HistoryTier historyTier) {
    SchemaIdentityState rebound =
        SchemaIdentityReconciler.bindSourceVersion(ColumnIdentityMaps.restore(map), version);
    ColumnIdentityMap result =
        withNativeHighWaterMark(
            ColumnIdentityMaps.toProto(rebound).toBuilder()
                .setHistoryTier(historyTier(historyTier))
                .build(),
            snapshot);
    DeltaCanonicalIdentity.validateSnapshot(snapshot, result);
    return result;
  }

  private static ColumnIdentityMap withNativeHighWaterMark(
      ColumnIdentityMap map, Snapshot snapshot) {
    if (mode(snapshot) == IdentityMode.NATIVE_FIELD_ID) {
      return ColumnIdentityMaps.withNativeHighWaterMark(
          map, DeltaColumnMapping.maxColumnId(snapshot));
    }
    return map;
  }

  private static ColumnIdentityHistoryTier historyTier(HistoryTier tier) {
    return switch (tier) {
      case VERSIONED_LOG -> ColumnIdentityHistoryTier.COLUMN_IDENTITY_HISTORY_TIER_VERSIONED_LOG;
      case VERSIONED_CHANGE_FEED ->
          ColumnIdentityHistoryTier.COLUMN_IDENTITY_HISTORY_TIER_VERSIONED_CHANGE_FEED;
      case UNORDERED_QUERY_HISTORY ->
          ColumnIdentityHistoryTier.COLUMN_IDENTITY_HISTORY_TIER_UNORDERED_QUERY_HISTORY;
      case NONE -> ColumnIdentityHistoryTier.COLUMN_IDENTITY_HISTORY_TIER_NONE;
    };
  }

  private static void validatePersisted(
      String table, long version, Snapshot snapshot, ColumnIdentityMap map) {
    try {
      DeltaCanonicalIdentity.validateSnapshot(snapshot, map);
      SchemaIdentityState state = ColumnIdentityMaps.restore(map);
      if (state.mode() == IdentityMode.NATIVE_FIELD_ID
          && (!map.hasNativeHighWaterMark()
              || map.getNativeHighWaterMark() != DeltaColumnMapping.maxColumnId(snapshot))) {
        throw new IllegalArgumentException(
            "Snapshot native high-water mark does not match its column identity map");
      }
    } catch (IllegalArgumentException invalid) {
      throw new InvalidPersistedIdentityException(
          "Invalid persisted column identity map for table "
              + table
              + " snapshot "
              + version
              + ": "
              + invalid.getMessage(),
          invalid);
    }
  }

  private static IdentityResult.Failure toFailure(
      String table, long version, RuntimeException failure) {
    IdentityResult.Failure.Kind kind;
    if (failure instanceof InvalidPersistedIdentityException) {
      kind = IdentityResult.Failure.Kind.INVALID_PERSISTED_STATE;
    } else if (KernelAdapter.isUnsupportedVoid(failure)) {
      kind = IdentityResult.Failure.Kind.UNSUPPORTED;
    } else {
      kind = IdentityResult.Failure.Kind.SOURCE_ERROR;
    }
    return new IdentityResult.Failure(table, version, kind, failure.getMessage());
  }

  private static boolean isTruncatedHistory(Throwable failure) {
    return KernelAdapter.isTruncatedHistory(failure);
  }

  private static final class InvalidPersistedIdentityException extends IllegalArgumentException {
    private InvalidPersistedIdentityException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
