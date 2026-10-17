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

import ai.floedb.floecat.catalog.rpc.ColumnIdentityMap;
import ai.floedb.floecat.schema.identity.ColumnIdentityMaps;
import ai.floedb.floecat.schema.identity.ColumnPath;
import ai.floedb.floecat.schema.identity.HistoryTier;
import ai.floedb.floecat.schema.identity.IdentityMode;
import ai.floedb.floecat.schema.identity.NodeKind;
import ai.floedb.floecat.schema.identity.PreferredCanonicalIds;
import ai.floedb.floecat.schema.identity.SchemaIdentityEntry;
import ai.floedb.floecat.schema.identity.SchemaIdentityReconciler;
import ai.floedb.floecat.schema.identity.SchemaIdentityState;
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
      Optional<DeltaIdentitySnapshot> loaded = history.snapshot(version);
      if (loaded.isEmpty()) {
        if (history.snapshotExclusionReason(version).isPresent()) {
          return;
        }
        throw new IllegalStateException(
            "Delta source did not return requested snapshot " + version);
      }
      DeltaIdentitySnapshot snapshot = loaded.orElseThrow();
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
              startGeneration(
                  snapshot,
                  version,
                  exact.orElseThrow(),
                  persisted.highWaterMark(),
                  PreferredCanonicalIds.NONE,
                  history.historyTier()));
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
        .filter(
            map -> {
              SchemaIdentityState state = ColumnIdentityMaps.restore(map);
              return state.sourceVersionAnchored() && state.sourceVersion() == version;
            });
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
    if (tableState.sourceVersion() > version
        || (tableState.sourceVersion() == version && tableState.sourceVersionAnchored())
        || (floor.isPresent() && floor.orElseThrow().getKey() >= tableState.sourceVersion())) {
      return floor;
    }
    return Optional.of(Map.entry(tableState.sourceVersion(), tableMap.orElseThrow()));
  }

  private static Optional<ColumnIdentityMap> dataOnlyBackfill(
      DeltaHistorySource history,
      long version,
      DeltaIdentitySnapshot snapshot,
      Map.Entry<Long, ColumnIdentityMap> successor) {
    DeltaHistorySource.MetadataHistory range =
        history.metadataVersions(version + 1L, successor.getKey());
    if (!range.complete() || !range.versions().isEmpty()) {
      return Optional.empty();
    }
    DeltaIdentitySnapshot successorSnapshot = history.snapshot(successor.getKey()).orElse(null);
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
      DeltaIdentitySnapshot selected,
      Optional<Map.Entry<Long, ColumnIdentityMap>> predecessor,
      long highWaterMark,
      PreferredCanonicalIds preferred) {
    if (predecessor.isEmpty()) {
      return reconcile(selected, version, null, highWaterMark, preferred, history.historyTier());
    }

    Map.Entry<Long, ColumnIdentityMap> entry = predecessor.orElseThrow();
    SchemaIdentityState state = ColumnIdentityMaps.restore(entry.getValue());
    if ((!state.sourceVersionAnchored() && history.sourceVersionAnchored())
        || (!state.historyTier().provesContinuity() && history.historyTier().provesContinuity())) {
      return startGeneration(
          selected, version, entry.getValue(), highWaterMark, preferred, history.historyTier());
    }
    if (!history.historyTier().provesContinuity()) {
      return reconcileWithoutOrderedHistory(
          history, version, selected, entry.getValue(), highWaterMark, preferred);
    }
    DeltaHistorySource.MetadataHistory metadata =
        history.metadataVersions(state.sourceVersion() + 1L, version);
    if (!metadata.complete()) {
      return reconcileAcrossHistoryGap(
          history,
          version,
          selected,
          entry.getValue(),
          highWaterMark,
          preferred,
          metadata.reason());
    }

    List<MetadataStep> steps = new ArrayList<>(metadata.versions().size());
    for (long metadataVersion : metadata.versions()) {
      DeltaIdentitySnapshot metadataSnapshot =
          metadataVersion == version ? selected : history.snapshot(metadataVersion).orElse(null);
      if (metadataSnapshot == null) {
        return reconcileAcrossHistoryGap(
            history,
            version,
            selected,
            entry.getValue(),
            highWaterMark,
            preferred,
            "metadata snapshot " + metadataVersion + " is unavailable");
      }
      steps.add(new MetadataStep(metadataVersion, metadataSnapshot));
    }
    Map<Long, PreferredCanonicalIds> preferredByVersion =
        preferredByMetadataVersion(steps, version, selected, preferred);

    ColumnIdentityMap current = entry.getValue();
    long previousNativeHighWaterMark = persistedNativeHighWaterMark(current);
    boolean selectedReconciled = false;
    for (MetadataStep step : steps) {
      SchemaIdentityState currentState = ColumnIdentityMaps.restore(current);
      long metadataVersion = step.version();
      DeltaIdentitySnapshot metadataSnapshot = step.snapshot();
      long nextNativeHighWaterMark = nativeHighWaterMark(metadataSnapshot);
      boolean generationBoundary =
          generationBoundary(currentState.mode(), previousNativeHighWaterMark, metadataSnapshot);
      current =
          generationBoundary
              ? reconcile(
                  metadataSnapshot,
                  metadataVersion,
                  null,
                  Math.max(highWaterMark, currentState.highWaterMark()),
                  preferredByVersion.getOrDefault(metadataVersion, PreferredCanonicalIds.NONE),
                  history.historyTier())
              : reconcile(
                  metadataSnapshot,
                  metadataVersion,
                  current,
                  highWaterMark,
                  preferredByVersion.getOrDefault(metadataVersion, PreferredCanonicalIds.NONE),
                  weakerTier(currentState.historyTier(), history.historyTier()));
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

  private static ColumnIdentityMap reconcileAcrossHistoryGap(
      DeltaHistorySource history,
      long version,
      DeltaIdentitySnapshot selected,
      ColumnIdentityMap previous,
      long highWaterMark,
      PreferredCanonicalIds preferred,
      String reason) {
    SchemaIdentityState state = ColumnIdentityMaps.restore(previous);
    if (state.mode() == IdentityMode.STRUCTURED_PATH
        && mode(selected) == IdentityMode.STRUCTURED_PATH) {
      if (!preferred.equals(PreferredCanonicalIds.NONE)) {
        // A complete selected-to-successor range can prove the selected identity even when the
        // predecessor-to-selected range has been retained away.
        return startGeneration(
            selected, version, previous, highWaterMark, preferred, history.historyTier());
      }
      throw new IllegalStateException(
          "Cannot reconcile unmapped Delta column identity across incomplete metadata history"
              + (reason == null || reason.isBlank() ? "" : ": " + reason));
    }
    // Native field IDs remain authoritative across a history gap. Mode changes and unsafe native
    // counter reuse are still handled as generation boundaries by the no-history reconciler.
    return reconcileWithoutOrderedHistory(
        history, version, selected, previous, highWaterMark, preferred);
  }

  static Map<Long, PreferredCanonicalIds> preferredByMetadataVersion(
      List<MetadataStep> metadataSteps,
      long selectedVersion,
      DeltaIdentitySnapshot selected,
      PreferredCanonicalIds preferred) {
    if (preferred.equals(PreferredCanonicalIds.NONE) || metadataSteps.isEmpty()) {
      return Map.of();
    }
    List<MetadataStep> continuity = new ArrayList<>(metadataSteps);
    if (continuity.stream().noneMatch(step -> step.version() == selectedVersion)) {
      continuity.add(new MetadataStep(selectedVersion, selected));
    }
    continuity.sort(Comparator.comparingLong(MetadataStep::version));

    Map<Long, PreferredCanonicalIds> result = new LinkedHashMap<>();
    PreferredCanonicalIds remaining = filterPreferred(preferred, selected);
    MetadataStep later = continuity.getLast();
    result.put(later.version(), remaining);
    for (int index = continuity.size() - 2; index >= 0; index--) {
      MetadataStep current = continuity.get(index);
      if (generationBoundary(current.snapshot(), later.snapshot())) {
        remaining = PreferredCanonicalIds.NONE;
      } else {
        remaining = filterPreferred(remaining, current.snapshot());
      }
      result.put(current.version(), remaining);
      later = current;
    }
    return Map.copyOf(result);
  }

  private static PreferredCanonicalIds filterPreferred(
      PreferredCanonicalIds preferred, DeltaIdentitySnapshot snapshot) {
    if (preferred.equals(PreferredCanonicalIds.NONE)) {
      return preferred;
    }
    IdentityMode snapshotMode = mode(snapshot);
    FieldIdentities fields = fieldIdentities(snapshot);
    if (snapshotMode == IdentityMode.NATIVE_FIELD_ID) {
      Map<Integer, Long> retained = new LinkedHashMap<>(preferred.byNativeId());
      retained.keySet().retainAll(fields.nativeIds());
      return PreferredCanonicalIds.byNativeId(retained);
    }
    Map<ColumnPath, Long> retained = new LinkedHashMap<>(preferred.byPath());
    retained.keySet().retainAll(fields.paths());
    return PreferredCanonicalIds.byPath(retained);
  }

  private static boolean generationBoundary(
      DeltaIdentitySnapshot current, DeltaIdentitySnapshot next) {
    return generationBoundary(mode(current), nativeHighWaterMark(current), next);
  }

  private static boolean generationBoundary(
      IdentityMode currentMode, long currentNativeHighWaterMark, DeltaIdentitySnapshot next) {
    IdentityMode nextMode = mode(next);
    return currentMode != nextMode
        || (nextMode == IdentityMode.NATIVE_FIELD_ID
            && nativeHighWaterMark(next) < currentNativeHighWaterMark);
  }

  record MetadataStep(long version, DeltaIdentitySnapshot snapshot) {}

  private static ColumnIdentityMap reconcileWithoutOrderedHistory(
      DeltaHistorySource history,
      long version,
      DeltaIdentitySnapshot selected,
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
        ? startGeneration(
            selected,
            version,
            previous,
            Math.max(highWaterMark, state.highWaterMark()),
            preferred,
            history.historyTier())
        : reconcile(selected, version, previous, highWaterMark, preferred, provenance);
  }

  private static boolean reusesUnownedNativeId(
      DeltaIdentitySnapshot snapshot,
      SchemaIdentityState previous,
      long previousNativeHighWaterMark) {
    Set<Integer> previousNativeIds = new LinkedHashSet<>();
    previous.entries().stream()
        .filter(entry -> isField(entry.path()))
        .forEach(entry -> entry.nativeFieldId().ifPresent(previousNativeIds::add));
    return snapshot.resolved().schema().nodes().stream()
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

  private static ColumnIdentityMap reconcile(
      DeltaIdentitySnapshot snapshot,
      long version,
      ColumnIdentityMap previous,
      long highWaterMark,
      PreferredCanonicalIds preferred,
      HistoryTier historyTier) {
    DeltaResolvedSchema resolved = snapshot.resolved();
    IdentityMode mode = DeltaCanonicalIdentity.identityMode(resolved);
    Optional<SchemaIdentityState> previousState =
        previous == null ? Optional.empty() : Optional.of(ColumnIdentityMaps.restore(previous));
    long floor =
        mode == IdentityMode.NATIVE_FIELD_ID
            ? Math.max(highWaterMark, snapshot.nativeHighWaterMark())
            : highWaterMark;
    SchemaIdentityState identity =
        SchemaIdentityReconciler.reconcile(
            resolved.schema(), version, mode, previousState, floor, preferred, historyTier);
    return withNativeHighWaterMark(ColumnIdentityMaps.toProto(identity), snapshot);
  }

  private static ColumnIdentityMap startGeneration(
      DeltaIdentitySnapshot snapshot,
      long version,
      ColumnIdentityMap previous,
      long highWaterMark,
      PreferredCanonicalIds preferred,
      HistoryTier historyTier) {
    long floor =
        Math.max(
            highWaterMark,
            previous == null ? 0L : ColumnIdentityMaps.restore(previous).highWaterMark());
    return reconcile(snapshot, version, null, floor, preferred, historyTier);
  }

  private static PreferredCanonicalIds preferredSuccessorIds(
      DeltaHistorySource history,
      long version,
      DeltaIdentitySnapshot selected,
      Map.Entry<Long, ColumnIdentityMap> successor) {
    DeltaHistorySource.MetadataHistory range =
        history.metadataVersions(version + 1L, successor.getKey());
    if (!range.complete()) {
      return PreferredCanonicalIds.NONE;
    }
    DeltaIdentitySnapshot successorSnapshot = history.snapshot(successor.getKey()).orElse(null);
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
      DeltaIdentitySnapshot future = history.snapshot(futureVersion).orElse(null);
      if (future == null || generationBoundary(selectedMode, previousNativeHighWaterMark, future)) {
        return PreferredCanonicalIds.NONE;
      }
      long nextNativeHighWaterMark = nativeHighWaterMark(future);
      previousNativeHighWaterMark = nextNativeHighWaterMark;
      FieldIdentities futureFields = fieldIdentities(future);
      paths.retainAll(futureFields.paths());
      nativeIds.retainAll(futureFields.nativeIds());
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

  private static FieldIdentities fieldIdentities(DeltaIdentitySnapshot snapshot) {
    return new FieldIdentities(snapshot.fieldPaths(), snapshot.nativeIds());
  }

  private record FieldIdentities(Set<ColumnPath> paths, Set<Integer> nativeIds) {}

  private static IdentityMode mode(DeltaIdentitySnapshot snapshot) {
    return snapshot.mode();
  }

  private static long nativeHighWaterMark(DeltaIdentitySnapshot snapshot) {
    if (snapshot == null || mode(snapshot) != IdentityMode.NATIVE_FIELD_ID) {
      return 0L;
    }
    return snapshot.nativeHighWaterMark();
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
      ColumnIdentityMap map,
      DeltaIdentitySnapshot snapshot,
      long version,
      HistoryTier historyTier) {
    SchemaIdentityState rebound =
        SchemaIdentityReconciler.bindSourceVersion(ColumnIdentityMaps.restore(map), version);
    ColumnIdentityMap result =
        withNativeHighWaterMark(
            ColumnIdentityMaps.withHistoryTier(ColumnIdentityMaps.toProto(rebound), historyTier),
            snapshot);
    DeltaCanonicalIdentity.validateSnapshot(snapshot, result);
    return result;
  }

  private static ColumnIdentityMap withNativeHighWaterMark(
      ColumnIdentityMap map, DeltaIdentitySnapshot snapshot) {
    if (mode(snapshot) == IdentityMode.NATIVE_FIELD_ID) {
      return ColumnIdentityMaps.withNativeHighWaterMark(map, snapshot.nativeHighWaterMark());
    }
    return map;
  }

  private static void validatePersisted(
      String table, long version, DeltaIdentitySnapshot snapshot, ColumnIdentityMap map) {
    try {
      SchemaIdentityState state = DeltaCanonicalIdentity.validateSnapshot(snapshot, map);
      if (state.mode() == IdentityMode.NATIVE_FIELD_ID
          && (!map.hasNativeHighWaterMark()
              || map.getNativeHighWaterMark() != snapshot.nativeHighWaterMark())) {
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

  private static final class InvalidPersistedIdentityException extends IllegalArgumentException {
    private InvalidPersistedIdentityException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
