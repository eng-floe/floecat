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
import ai.floedb.floecat.schema.identity.HistoryCoverage;
import ai.floedb.floecat.schema.identity.HistoryTier;
import ai.floedb.floecat.schema.identity.IdentityMode;
import ai.floedb.floecat.schema.identity.LegacyDottedKeyIndex;
import ai.floedb.floecat.schema.identity.PreferredCanonicalIds;
import ai.floedb.floecat.schema.identity.SchemaIdentityReconciler;
import ai.floedb.floecat.schema.identity.SchemaIdentityState;
import io.delta.kernel.Snapshot;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Converts Delta schema identity reconciliation state to and from the snapshot wire contract. */
public final class DeltaCanonicalIdentity {
  private DeltaCanonicalIdentity() {}

  public static Reconciled reconcile(
      Snapshot snapshot,
      long sourceVersion,
      ColumnIdentityMap previousIdentityMap,
      HistoryCoverage historyCoverage,
      long highWaterMarkFloor,
      PreferredCanonicalIds preferredIds,
      HistoryTier historyTier) {
    Objects.requireNonNull(snapshot, "snapshot");
    DeltaResolvedSchema resolved = DeltaColumnMapping.resolveSchema(snapshot);
    IdentityMode mode = identityMode(resolved);
    Optional<SchemaIdentityState> previous =
        previousIdentityMap == null
                || previousIdentityMap.equals(ColumnIdentityMap.getDefaultInstance())
            ? Optional.empty()
            : Optional.of(fromProto(previousIdentityMap));
    long floor =
        mode == IdentityMode.NATIVE_FIELD_ID
            ? Math.max(highWaterMarkFloor, DeltaColumnMapping.maxColumnId(snapshot))
            : highWaterMarkFloor;
    SchemaIdentityReconciler.Result result =
        SchemaIdentityReconciler.reconcile(
            resolved.schema(),
            sourceVersion,
            mode,
            previous,
            historyCoverage,
            floor,
            preferredIds,
            historyTier);
    return new Reconciled(resolved, result, toProto(result.state(), snapshot));
  }

  /** Starts a new generation after unavailable history while preserving ID monotonicity. */
  public static Reconciled reset(
      Snapshot snapshot,
      long sourceVersion,
      ColumnIdentityMap previousIdentityMap,
      long highWaterMarkFloor,
      HistoryTier historyTier) {
    Objects.requireNonNull(snapshot, "snapshot");
    DeltaResolvedSchema resolved = DeltaColumnMapping.resolveSchema(snapshot);
    IdentityMode mode = identityMode(resolved);
    long previousHighWaterMark =
        previousIdentityMap == null
                || previousIdentityMap.equals(ColumnIdentityMap.getDefaultInstance())
            ? 0L
            : fromProto(previousIdentityMap).highWaterMark();
    previousHighWaterMark = Math.max(previousHighWaterMark, highWaterMarkFloor);
    if (mode == IdentityMode.NATIVE_FIELD_ID) {
      previousHighWaterMark =
          Math.max(previousHighWaterMark, DeltaColumnMapping.maxColumnId(snapshot));
    }
    SchemaIdentityReconciler.Result result =
        SchemaIdentityReconciler.reset(
            resolved.schema(), sourceVersion, mode, previousHighWaterMark, historyTier);
    return new Reconciled(resolved, result, toProto(result.state(), snapshot));
  }

  /** Advances a reconciled map across commits known not to contain metadata changes. */
  public static ColumnIdentityMap stampSourceVersion(
      Snapshot snapshot,
      long sourceVersion,
      ColumnIdentityMap identityMap,
      long highWaterMarkFloor) {
    Objects.requireNonNull(snapshot, "snapshot");
    SchemaIdentityState state = fromProto(identityMap);
    SchemaIdentityState stamped =
        SchemaIdentityReconciler.stampSourceVersion(
            state, sourceVersion, HistoryCoverage.COMPLETE_METADATA_HISTORY, highWaterMarkFloor);
    ColumnIdentityMap result = toProto(stamped, snapshot);
    validateSnapshot(snapshot, result);
    return result;
  }

  /**
   * The identity mode this table reconciles under.
   *
   * <p>Mapped StructFields use their protocol field IDs. Collection interiors use deterministic IDs
   * derived from the nearest StructField, independent of whether nested provenance IDs are present.
   * The protocol requires those nested IDs only while IcebergCompatV2 is active.
   */
  public static IdentityMode identityMode(DeltaResolvedSchema resolved) {
    return resolved.effectiveMappingMode().isEnabled()
        ? IdentityMode.NATIVE_FIELD_ID
        : IdentityMode.STRUCTURED_PATH;
  }

  public static SchemaIdentityState fromProto(ColumnIdentityMap value) {
    return ColumnIdentityMaps.restore(value);
  }

  public static void validateSnapshot(Snapshot snapshot, ColumnIdentityMap identityMap) {
    SchemaIdentityState state = fromProto(identityMap);
    if (state.sourceVersion() != snapshot.getVersion()) {
      throw new IllegalArgumentException(
          "Column identity map version "
              + state.sourceVersion()
              + " does not match Delta snapshot "
              + snapshot.getVersion());
    }
    DeltaResolvedSchema resolved = DeltaColumnMapping.resolveSchema(snapshot);
    if ((state.mode() == IdentityMode.NATIVE_FIELD_ID)
        != resolved.effectiveMappingMode().isEnabled()) {
      throw new IllegalArgumentException("Snapshot and column identity map use different modes");
    }
    Set<ColumnPath> sourcePaths =
        resolved.schema().nodes().stream().map(node -> node.path()).collect(Collectors.toSet());
    Set<ColumnPath> mappedPaths =
        state.entries().stream().map(entry -> entry.path()).collect(Collectors.toSet());
    if (!sourcePaths.equals(mappedPaths)) {
      throw new IllegalArgumentException("Snapshot schema does not match its column identity map");
    }
    if (state.mode() == IdentityMode.NATIVE_FIELD_ID) {
      SchemaIdentityState expected =
          SchemaIdentityReconciler.reconcile(
                  resolved.schema(),
                  snapshot.getVersion() + 1L,
                  IdentityMode.NATIVE_FIELD_ID,
                  Optional.of(state),
                  HistoryCoverage.COMPLETE_METADATA_HISTORY,
                  state.highWaterMark(),
                  PreferredCanonicalIds.NONE,
                  state.historyTier())
              .state();
      resolved
          .schema()
          .nodes()
          .forEach(
              node -> {
                var mapped = state.byPath(node.path()).orElseThrow();
                var source = node.nativeFieldId();
                if (mapped.canonicalId() != expected.byPath(node.path()).orElseThrow().canonicalId()
                    || !mapped.nativeFieldId().equals(source)) {
                  throw new IllegalArgumentException(
                      "Mapped Delta identity changed for " + node.path().display());
                }
              });
    }
  }

  /** Returns canonical IDs keyed by the unambiguous logical names used by the stats engine. */
  public static java.util.Map<String, Long> canonicalIdsByStatsKey(
      Snapshot snapshot, ColumnIdentityMap identityMap) {
    validateSnapshot(snapshot, identityMap);
    SchemaIdentityState state = fromProto(identityMap);
    DeltaResolvedSchema resolved = DeltaColumnMapping.resolveSchema(snapshot);
    LegacyDottedKeyIndex<Long> index = LegacyDottedKeyIndex.create();
    resolved
        .schema()
        .nodes()
        .forEach(
            node -> index.add(node.path(), state.byPath(node.path()).orElseThrow().canonicalId()));
    return new LinkedHashMap<>(index.values());
  }

  /** Returns canonical IDs keyed by unambiguous logical dotted paths from persisted state. */
  public static java.util.Map<String, Long> canonicalIdsByLogicalKey(
      ColumnIdentityMap identityMap) {
    fromProto(identityMap);
    LegacyDottedKeyIndex<Long> index = LegacyDottedKeyIndex.create();
    identityMap
        .getEntriesList()
        .forEach(
            entry ->
                index.add(
                    ColumnIdentityMaps.pathFromProto(entry.getPathList()), entry.getColumnId()));
    return new LinkedHashMap<>(index.values());
  }

  public static java.util.Map<ColumnPath, Long> canonicalIdsByPath(ColumnIdentityMap identityMap) {
    SchemaIdentityState state = fromProto(identityMap);
    java.util.Map<ColumnPath, Long> result = new LinkedHashMap<>();
    state.entries().forEach(entry -> result.put(entry.path(), entry.canonicalId()));
    return java.util.Map.copyOf(result);
  }

  /** Returns source ordinals keyed by the same unambiguous logical names as stats. */
  public static java.util.Map<String, Integer> ordinalsByStatsKey(Snapshot snapshot) {
    LegacyDottedKeyIndex<Integer> index = LegacyDottedKeyIndex.create();
    DeltaColumnMapping.resolveSchema(snapshot)
        .schema()
        .nodes()
        .forEach(node -> index.add(node.path(), node.ordinal()));
    return new LinkedHashMap<>(index.values());
  }

  public static ColumnIdentityMap toProto(SchemaIdentityState state) {
    return ColumnIdentityMaps.toProto(state);
  }

  private static ColumnIdentityMap toProto(SchemaIdentityState state, Snapshot snapshot) {
    if (state.mode() == IdentityMode.NATIVE_FIELD_ID) {
      return ColumnIdentityMaps.withNativeHighWaterMark(
          toProto(state), DeltaColumnMapping.maxColumnId(snapshot));
    }
    return toProto(state);
  }

  public record Reconciled(
      DeltaResolvedSchema resolvedSchema,
      SchemaIdentityReconciler.Result identity,
      ColumnIdentityMap identityMap) {}
}
