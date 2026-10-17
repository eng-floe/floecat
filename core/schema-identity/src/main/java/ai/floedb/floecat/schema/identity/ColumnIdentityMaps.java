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

package ai.floedb.floecat.schema.identity;

import ai.floedb.floecat.catalog.rpc.ColumnIdentityEntry;
import ai.floedb.floecat.catalog.rpc.ColumnIdentityHistoryTier;
import ai.floedb.floecat.catalog.rpc.ColumnIdentityMap;
import ai.floedb.floecat.catalog.rpc.ColumnIdentityMode;
import ai.floedb.floecat.catalog.rpc.ColumnIdentityPathElement;
import ai.floedb.floecat.catalog.rpc.ColumnIdentityPathElementKind;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

/** The single protobuf boundary for the canonical column-identity contract. */
public final class ColumnIdentityMaps {
  public static final int FORMAT_VERSION = 1;

  private ColumnIdentityMaps() {}

  /** Restores and fully validates a persisted map, including its fingerprints. */
  public static SchemaIdentityState restore(ColumnIdentityMap value) {
    Objects.requireNonNull(value, "value");
    if (value.equals(ColumnIdentityMap.getDefaultInstance())) {
      throw new IllegalArgumentException("Column identity map is required");
    }
    if (value.getFormatVersion() != FORMAT_VERSION) {
      throw new IllegalArgumentException(
          "Unsupported column identity map format " + value.getFormatVersion());
    }
    IdentityMode mode = identityMode(value.getMode());
    HistoryTier historyTier = historyTier(value.getHistoryTier());
    List<SchemaIdentityEntry> entries = new ArrayList<>(value.getEntriesCount());
    for (ColumnIdentityEntry entry : value.getEntriesList()) {
      entries.add(
          new SchemaIdentityEntry(
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
        historyTier,
        value.hasNativeHighWaterMark()
            ? OptionalLong.of(value.getNativeHighWaterMark())
            : OptionalLong.empty(),
        value.getSourceVersionAnchored(),
        entries,
        value.getFingerprint(),
        value.getStateChecksum());
  }

  /** Validates a map and binds it to the snapshot version that carries it. */
  public static SchemaIdentityState restoreForSnapshot(ColumnIdentityMap value, long snapshotId) {
    SchemaIdentityState state = restore(value);
    if (state.sourceVersion() != snapshotId) {
      throw new IllegalArgumentException(
          "Column identity map version "
              + state.sourceVersion()
              + " does not match snapshot "
              + snapshotId);
    }
    return state;
  }

  /** Returns a validated fingerprint when a snapshot carries canonical identity state. */
  public static Optional<String> fingerprint(Snapshot snapshot) {
    Objects.requireNonNull(snapshot, "snapshot");
    if (!snapshot.hasColumnIdentityMap()) {
      return Optional.empty();
    }
    return Optional.of(
        restoreForSnapshot(snapshot.getColumnIdentityMap(), snapshot.getSnapshotId())
            .fingerprint());
  }

  public static boolean matchesFingerprint(Optional<String> expected, String actual) {
    Objects.requireNonNull(expected, "expected");
    return expected
        .map(fingerprint -> fingerprint.equals(actual))
        .orElseGet(() -> actual == null || actual.isBlank());
  }

  public static ColumnIdentityMap toProto(SchemaIdentityState state) {
    Objects.requireNonNull(state, "state");
    ColumnIdentityMap.Builder out =
        ColumnIdentityMap.newBuilder()
            .setFormatVersion(FORMAT_VERSION)
            .setSourceVersion(state.sourceVersion())
            .setHighWaterMark(state.highWaterMark())
            .setMode(
                state.mode() == IdentityMode.NATIVE_FIELD_ID
                    ? ColumnIdentityMode.COLUMN_IDENTITY_MODE_NATIVE_FIELD_ID
                    : ColumnIdentityMode.COLUMN_IDENTITY_MODE_STRUCTURED_PATH)
            .setHistoryTier(historyTier(state.historyTier()))
            .setSourceVersionAnchored(state.sourceVersionAnchored())
            .setFingerprint(state.fingerprint())
            .setStateChecksum(state.stateChecksum());
    state.nativeHighWaterMark().ifPresent(out::setNativeHighWaterMark);
    for (SchemaIdentityEntry entry : state.entries()) {
      ColumnIdentityEntry.Builder mapped =
          ColumnIdentityEntry.newBuilder()
              .setColumnId(entry.canonicalId())
              .addAllPath(pathToProto(entry.path()));
      entry.nativeFieldId().ifPresent(mapped::setNativeFieldId);
      out.addEntries(mapped);
    }
    return out.build();
  }

  /** Binds Delta's native allocation counter into the integrity-protected state. */
  public static ColumnIdentityMap withNativeHighWaterMark(
      ColumnIdentityMap value, long nativeHighWaterMark) {
    SchemaIdentityState state = restore(value);
    if (state.mode() != IdentityMode.NATIVE_FIELD_ID) {
      throw new IllegalArgumentException("Native high-water mark requires native identity mode");
    }
    CanonicalColumnId.checkAllocatedRange(nativeHighWaterMark);
    SchemaIdentityState bound =
        SchemaIdentityState.restore(
            state.sourceVersion(),
            state.highWaterMark(),
            state.mode(),
            state.historyTier(),
            OptionalLong.of(nativeHighWaterMark),
            state.sourceVersionAnchored(),
            state.entries(),
            state.fingerprint(),
            SchemaIdentityReconciler.stateChecksum(
                state.fingerprint(),
                state.highWaterMark(),
                state.sourceVersion(),
                OptionalLong.of(nativeHighWaterMark),
                state.sourceVersionAnchored()));
    return toProto(bound);
  }

  /** Marks a current-schema map as lacking a confirmed Delta log version. */
  public static ColumnIdentityMap withoutAnchoredSourceVersion(ColumnIdentityMap value) {
    SchemaIdentityState state = restore(value);
    if (!state.sourceVersionAnchored()) {
      return value;
    }
    SchemaIdentityState unanchored =
        SchemaIdentityState.restore(
            state.sourceVersion(),
            state.highWaterMark(),
            state.mode(),
            state.historyTier(),
            state.nativeHighWaterMark(),
            false,
            state.entries(),
            state.fingerprint(),
            SchemaIdentityReconciler.stateChecksum(
                state.fingerprint(),
                state.highWaterMark(),
                state.sourceVersion(),
                state.nativeHighWaterMark(),
                false));
    return toProto(unanchored);
  }

  /** Whether two maps describe the same identity state, ignoring only their source cursor. */
  public static boolean sameIdentityContent(ColumnIdentityMap left, ColumnIdentityMap right) {
    Objects.requireNonNull(left, "left");
    Objects.requireNonNull(right, "right");
    return left.getFingerprint().equals(right.getFingerprint())
        && left.getHighWaterMark() == right.getHighWaterMark()
        && left.getMode() == right.getMode()
        && left.getHistoryTier() == right.getHistoryTier()
        && left.getSourceVersionAnchored() == right.getSourceVersionAnchored()
        && left.hasNativeHighWaterMark() == right.hasNativeHighWaterMark()
        && (!left.hasNativeHighWaterMark()
            || left.getNativeHighWaterMark() == right.getNativeHighWaterMark());
  }

  public static ColumnPath pathFromProto(List<ColumnIdentityPathElement> elements) {
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

  private static IdentityMode identityMode(ColumnIdentityMode mode) {
    return switch (mode) {
      case COLUMN_IDENTITY_MODE_NATIVE_FIELD_ID -> IdentityMode.NATIVE_FIELD_ID;
      case COLUMN_IDENTITY_MODE_STRUCTURED_PATH -> IdentityMode.STRUCTURED_PATH;
      default -> throw new IllegalArgumentException("Column identity map has no mode");
    };
  }

  private static HistoryTier historyTier(ColumnIdentityHistoryTier tier) {
    return switch (tier) {
      case COLUMN_IDENTITY_HISTORY_TIER_VERSIONED_LOG -> HistoryTier.VERSIONED_LOG;
      case COLUMN_IDENTITY_HISTORY_TIER_VERSIONED_CHANGE_FEED -> HistoryTier.VERSIONED_CHANGE_FEED;
      case COLUMN_IDENTITY_HISTORY_TIER_UNORDERED_QUERY_HISTORY ->
          HistoryTier.UNORDERED_QUERY_HISTORY;
      case COLUMN_IDENTITY_HISTORY_TIER_NONE -> HistoryTier.NONE;
      default -> throw new IllegalArgumentException("Column identity map has no history tier");
    };
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
}
