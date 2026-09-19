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

package ai.floedb.floecat.connector.delta.uc.impl;

import ai.floedb.floecat.catalog.rpc.ColumnIdentityEntry;
import ai.floedb.floecat.catalog.rpc.ColumnIdentityMap;
import ai.floedb.floecat.catalog.rpc.ColumnIdentityMode;
import ai.floedb.floecat.catalog.rpc.ColumnIdentityPathElement;
import ai.floedb.floecat.catalog.rpc.ColumnIdentityPathElementKind;
import ai.floedb.floecat.connector.delta.identity.DeltaResolvedSchema;
import ai.floedb.floecat.schema.identity.ColumnPath;
import ai.floedb.floecat.schema.identity.HistoryCoverage;
import ai.floedb.floecat.schema.identity.IdentityMode;
import ai.floedb.floecat.schema.identity.LegacyDottedKeyIndex;
import ai.floedb.floecat.schema.identity.SchemaIdentityReconciler;
import ai.floedb.floecat.schema.identity.SchemaIdentityState;
import io.delta.kernel.Snapshot;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

/** Converts Delta schema identity reconciliation state to and from the snapshot wire contract. */
final class DeltaCanonicalIdentity {
  private static final int FORMAT_VERSION = 1;

  private DeltaCanonicalIdentity() {}

  static Reconciled reconcile(
      Snapshot snapshot,
      long sourceVersion,
      ColumnIdentityMap previousIdentityMap,
      HistoryCoverage historyCoverage) {
    Objects.requireNonNull(snapshot, "snapshot");
    DeltaResolvedSchema resolved = DeltaColumnMapping.resolveSchema(snapshot);
    IdentityMode mode = identityMode(resolved);
    Optional<SchemaIdentityState> previous =
        previousIdentityMap == null
                || previousIdentityMap.equals(ColumnIdentityMap.getDefaultInstance())
            ? Optional.empty()
            : Optional.of(fromProto(previousIdentityMap));
    SchemaIdentityReconciler.Result result;
    if (mode == IdentityMode.NATIVE_FIELD_ID) {
      previous.ifPresent(
          state -> {
            if (state.mode() != mode) {
              throw new IllegalArgumentException(
                  "Identity mode changed; a clean identity reset is required");
            }
            if (sourceVersion <= state.sourceVersion()) {
              throw new IllegalArgumentException(
                  "Source version " + sourceVersion + " does not follow " + state.sourceVersion());
            }
          });
      long highWaterMark =
          Math.max(
              previous.map(SchemaIdentityState::highWaterMark).orElse(0L),
              DeltaColumnMapping.maxColumnId(snapshot));
      result =
          SchemaIdentityReconciler.reset(resolved.schema(), sourceVersion, mode, highWaterMark);
    } else {
      result =
          SchemaIdentityReconciler.reconcile(
              resolved.schema(), sourceVersion, mode, previous, historyCoverage);
    }
    return new Reconciled(resolved, result, toProto(result.state()));
  }

  /** Starts a new generation after unavailable history while preserving ID monotonicity. */
  static Reconciled reset(
      Snapshot snapshot, long sourceVersion, ColumnIdentityMap previousIdentityMap) {
    Objects.requireNonNull(snapshot, "snapshot");
    DeltaResolvedSchema resolved = DeltaColumnMapping.resolveSchema(snapshot);
    IdentityMode mode = identityMode(resolved);
    long previousHighWaterMark =
        previousIdentityMap == null
                || previousIdentityMap.equals(ColumnIdentityMap.getDefaultInstance())
            ? 0L
            : fromProto(previousIdentityMap).highWaterMark();
    if (mode == IdentityMode.NATIVE_FIELD_ID) {
      previousHighWaterMark =
          Math.max(previousHighWaterMark, DeltaColumnMapping.maxColumnId(snapshot));
    }
    SchemaIdentityReconciler.Result result =
        SchemaIdentityReconciler.reset(
            resolved.schema(), sourceVersion, mode, previousHighWaterMark);
    return new Reconciled(resolved, result, toProto(result.state()));
  }

  /** Advances a reconciled map across commits known not to contain metadata changes. */
  static ColumnIdentityMap stampSourceVersion(
      Snapshot snapshot, long sourceVersion, ColumnIdentityMap identityMap) {
    Objects.requireNonNull(snapshot, "snapshot");
    SchemaIdentityState state = fromProto(identityMap);
    SchemaIdentityState stamped =
        SchemaIdentityReconciler.stampSourceVersion(
            state, sourceVersion, HistoryCoverage.COMPLETE_METADATA_HISTORY);
    ColumnIdentityMap result = toProto(stamped);
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
  static IdentityMode identityMode(DeltaResolvedSchema resolved) {
    return resolved.effectiveMappingMode().isEnabled()
        ? IdentityMode.NATIVE_FIELD_ID
        : IdentityMode.STRUCTURED_PATH;
  }

  static SchemaIdentityState fromProto(ColumnIdentityMap value) {
    Objects.requireNonNull(value, "value");
    if (value.getFormatVersion() != FORMAT_VERSION) {
      throw new IllegalArgumentException(
          "Unsupported column identity map format " + value.getFormatVersion());
    }
    IdentityMode mode =
        switch (value.getMode()) {
          case COLUMN_IDENTITY_MODE_NATIVE_FIELD_ID -> IdentityMode.NATIVE_FIELD_ID;
          case COLUMN_IDENTITY_MODE_STRUCTURED_PATH -> IdentityMode.STRUCTURED_PATH;
          default -> throw new IllegalArgumentException("Column identity map has no mode");
        };
    List<ai.floedb.floecat.schema.identity.SchemaIdentityEntry> entries = new ArrayList<>();
    for (ColumnIdentityEntry entry : value.getEntriesList()) {
      entries.add(
          new ai.floedb.floecat.schema.identity.SchemaIdentityEntry(
              pathFromProto(entry.getPathList()),
              entry.hasNativeFieldId()
                  ? OptionalInt.of(entry.getNativeFieldId())
                  : OptionalInt.empty(),
              entry.getColumnId()));
    }
    return SchemaIdentityState.restore(
        value.getSourceVersion(),
        value.getHighWaterMark(),
        mode,
        entries,
        value.getFingerprint(),
        value.getStateChecksum());
  }

  static void validateSnapshot(Snapshot snapshot, ColumnIdentityMap identityMap) {
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
          SchemaIdentityReconciler.reset(
                  resolved.schema(),
                  snapshot.getVersion(),
                  IdentityMode.NATIVE_FIELD_ID,
                  DeltaColumnMapping.maxColumnId(snapshot))
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
  static java.util.Map<String, Long> canonicalIdsByStatsKey(
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
  static java.util.Map<String, Long> canonicalIdsByLogicalKey(ColumnIdentityMap identityMap) {
    fromProto(identityMap);
    LegacyDottedKeyIndex<Long> index = LegacyDottedKeyIndex.create();
    identityMap
        .getEntriesList()
        .forEach(entry -> index.add(pathFromProto(entry.getPathList()), entry.getColumnId()));
    return new LinkedHashMap<>(index.values());
  }

  /** Returns source ordinals keyed by the same unambiguous logical names as stats. */
  static java.util.Map<String, Integer> ordinalsByStatsKey(Snapshot snapshot) {
    LegacyDottedKeyIndex<Integer> index = LegacyDottedKeyIndex.create();
    DeltaColumnMapping.resolveSchema(snapshot)
        .schema()
        .nodes()
        .forEach(node -> index.add(node.path(), node.ordinal()));
    return new LinkedHashMap<>(index.values());
  }

  static ColumnIdentityMap toProto(SchemaIdentityState state) {
    ColumnIdentityMap.Builder out =
        ColumnIdentityMap.newBuilder()
            .setFormatVersion(FORMAT_VERSION)
            .setSourceVersion(state.sourceVersion())
            .setHighWaterMark(state.highWaterMark())
            .setMode(
                state.mode() == IdentityMode.NATIVE_FIELD_ID
                    ? ColumnIdentityMode.COLUMN_IDENTITY_MODE_NATIVE_FIELD_ID
                    : ColumnIdentityMode.COLUMN_IDENTITY_MODE_STRUCTURED_PATH)
            .setFingerprint(state.fingerprint())
            .setStateChecksum(state.stateChecksum());
    for (ai.floedb.floecat.schema.identity.SchemaIdentityEntry entry : state.entries()) {
      ColumnIdentityEntry.Builder mapped =
          ColumnIdentityEntry.newBuilder()
              .setColumnId(entry.canonicalId())
              .addAllPath(pathToProto(entry.path()));
      entry.nativeFieldId().ifPresent(mapped::setNativeFieldId);
      out.addEntries(mapped);
    }
    return out.build();
  }

  private static List<ColumnIdentityPathElement> pathToProto(ColumnPath path) {
    return path.elements().stream()
        .map(
            element -> {
              ColumnIdentityPathElement.Builder out =
                  ColumnIdentityPathElement.newBuilder()
                      .setKind(
                          switch (element.kind()) {
                            case FIELD ->
                                ColumnIdentityPathElementKind
                                    .COLUMN_IDENTITY_PATH_ELEMENT_KIND_FIELD;
                            case ARRAY_ELEMENT ->
                                ColumnIdentityPathElementKind
                                    .COLUMN_IDENTITY_PATH_ELEMENT_KIND_ARRAY_ELEMENT;
                            case MAP_KEY ->
                                ColumnIdentityPathElementKind
                                    .COLUMN_IDENTITY_PATH_ELEMENT_KIND_MAP_KEY;
                            case MAP_VALUE ->
                                ColumnIdentityPathElementKind
                                    .COLUMN_IDENTITY_PATH_ELEMENT_KIND_MAP_VALUE;
                          });
              if (element.name() != null) {
                out.setName(element.name());
              }
              return out.build();
            })
        .toList();
  }

  private static ColumnPath pathFromProto(List<ColumnIdentityPathElement> elements) {
    ColumnPath path = ColumnPath.ROOT;
    for (ColumnIdentityPathElement element : elements) {
      path =
          switch (element.getKind()) {
            case COLUMN_IDENTITY_PATH_ELEMENT_KIND_FIELD -> path.field(element.getName());
            case COLUMN_IDENTITY_PATH_ELEMENT_KIND_ARRAY_ELEMENT -> path.arrayElement();
            case COLUMN_IDENTITY_PATH_ELEMENT_KIND_MAP_KEY -> path.mapKey();
            case COLUMN_IDENTITY_PATH_ELEMENT_KIND_MAP_VALUE -> path.mapValue();
            default ->
                throw new IllegalArgumentException("Column identity path has no element kind");
          };
    }
    if (path.isRoot()) {
      throw new IllegalArgumentException("Column identity path cannot be empty");
    }
    return path;
  }

  record Reconciled(
      DeltaResolvedSchema resolvedSchema,
      SchemaIdentityReconciler.Result identity,
      ColumnIdentityMap identityMap) {}
}
