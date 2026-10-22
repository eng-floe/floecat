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
import ai.floedb.floecat.catalog.rpc.ColumnIdentityMapRef;
import ai.floedb.floecat.catalog.rpc.ColumnIdentityMode;
import ai.floedb.floecat.catalog.rpc.ColumnIdentityPathElement;
import ai.floedb.floecat.catalog.rpc.ColumnIdentityPathElementKind;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import com.google.protobuf.ByteString;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

/** The single protobuf boundary for the canonical column-identity contract. */
public final class ColumnIdentityMaps {
  public static final int FORMAT_VERSION = 2;
  public static final long MAX_PAYLOAD_BYTES = 16L * 1024L * 1024L;

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

  /** Validates the bounded descriptor without reading its immutable payload. */
  public static void validateReference(ColumnIdentityMapRef value) {
    Objects.requireNonNull(value, "value");
    if (value.equals(ColumnIdentityMapRef.getDefaultInstance())) {
      throw new IllegalArgumentException("Column identity map reference is required");
    }
    if (value.getUri().isBlank()) {
      throw new IllegalArgumentException("Column identity map reference URI is required");
    }
    if (value.getPayloadBytes() == 0L || value.getPayloadBytes() > MAX_PAYLOAD_BYTES) {
      throw new IllegalArgumentException("Column identity map reference size is invalid");
    }
    if (value.getPayloadSha256().size() != 32) {
      throw new IllegalArgumentException("Column identity map reference digest must be SHA-256");
    }
    if (value.getFormatVersion() != FORMAT_VERSION) {
      throw new IllegalArgumentException(
          "Unsupported column identity map format " + value.getFormatVersion());
    }
    if (value.getSourceVersion() < 0L || value.getHighWaterMark() < 0L) {
      throw new IllegalArgumentException("Column identity map reference version is invalid");
    }
    identityMode(value.getMode());
    historyTier(value.getHistoryTier());
    if (value.getFingerprint().isBlank() || value.getStateChecksum().isBlank()) {
      throw new IllegalArgumentException("Column identity map reference checksums are required");
    }
  }

  /** Builds an integrity-protected immutable-blob reference for a serialized identity map. */
  public static ColumnIdentityMapRef reference(String uri, ColumnIdentityMap value) {
    return serialize(value).reference(uri);
  }

  /** Serializes and hashes a validated map once for immutable publication. */
  public static SerializedMap serialize(ColumnIdentityMap value) {
    restore(value);
    ByteString payload = value.toByteString();
    return new SerializedMap(value, payload, sha256(payload));
  }

  public record SerializedMap(
      ColumnIdentityMap value, ByteString payload, ByteString payloadSha256) {
    public SerializedMap {
      Objects.requireNonNull(value, "value");
      Objects.requireNonNull(payload, "payload");
      Objects.requireNonNull(payloadSha256, "payloadSha256");
    }

    public String sha256Hex() {
      return HexFormat.of().formatHex(payloadSha256.toByteArray());
    }

    public ColumnIdentityMapRef reference(String uri) {
      Objects.requireNonNull(uri, "uri");
      ColumnIdentityMapRef.Builder ref =
          ColumnIdentityMapRef.newBuilder()
              .setUri(uri)
              .setPayloadBytes(payload.size())
              .setPayloadSha256(payloadSha256)
              .setFormatVersion(value.getFormatVersion())
              .setSourceVersion(value.getSourceVersion())
              .setHighWaterMark(value.getHighWaterMark())
              .setMode(value.getMode())
              .setFingerprint(value.getFingerprint())
              .setStateChecksum(value.getStateChecksum())
              .setHistoryTier(value.getHistoryTier())
              .setSourceVersionAnchored(value.getSourceVersionAnchored());
      if (value.hasNativeHighWaterMark()) {
        ref.setNativeHighWaterMark(value.getNativeHighWaterMark());
      }
      return ref.build();
    }
  }

  /** Validates that decoded immutable bytes agree with every summary field in their reference. */
  public static SchemaIdentityState restore(ColumnIdentityMap value, ColumnIdentityMapRef ref) {
    validateReference(ref);
    SchemaIdentityState state = restore(value);
    if (value.getFormatVersion() != ref.getFormatVersion()
        || state.sourceVersion() != ref.getSourceVersion()
        || state.highWaterMark() != ref.getHighWaterMark()
        || value.getMode() != ref.getMode()
        || !state.fingerprint().equals(ref.getFingerprint())
        || !state.stateChecksum().equals(ref.getStateChecksum())
        || value.getHistoryTier() != ref.getHistoryTier()
        || value.hasNativeHighWaterMark() != ref.hasNativeHighWaterMark()
        || (value.hasNativeHighWaterMark()
            && value.getNativeHighWaterMark() != ref.getNativeHighWaterMark())
        || state.sourceVersionAnchored() != ref.getSourceVersionAnchored()) {
      throw new IllegalArgumentException("Column identity map does not match its reference");
    }
    return state;
  }

  /** Returns a validated fingerprint when a snapshot carries canonical identity state. */
  public static Optional<String> fingerprint(Snapshot snapshot) {
    Objects.requireNonNull(snapshot, "snapshot");
    if (!snapshot.hasColumnIdentityMapRef()) {
      return Optional.empty();
    }
    ColumnIdentityMapRef ref = snapshot.getColumnIdentityMapRef();
    validateReference(ref);
    if (ref.getSourceVersion() != snapshot.getSnapshotId()) {
      throw new IllegalArgumentException(
          "Column identity map version "
              + ref.getSourceVersion()
              + " does not match snapshot "
              + snapshot.getSnapshotId());
    }
    return Optional.of(ref.getFingerprint());
  }

  /**
   * Compares wire fingerprints without using an empty string as an in-memory absence sentinel. When
   * identity is not applicable, only a record with no fingerprint is compatible.
   */
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
        state.copy(
            state.sourceVersion(),
            state.highWaterMark(),
            state.historyTier(),
            OptionalLong.of(nativeHighWaterMark),
            state.sourceVersionAnchored());
    return toProto(bound);
  }

  /** Marks a current-schema map as lacking a confirmed Delta log version. */
  public static ColumnIdentityMap withoutAnchoredSourceVersion(ColumnIdentityMap value) {
    SchemaIdentityState state = restore(value);
    if (!state.sourceVersionAnchored()) {
      return value;
    }
    SchemaIdentityState unanchored =
        state.copy(
            state.sourceVersion(),
            state.highWaterMark(),
            state.historyTier(),
            state.nativeHighWaterMark(),
            false);
    return toProto(unanchored);
  }

  /** Changes provenance while preserving identity content and recomputing protected state. */
  public static ColumnIdentityMap withHistoryTier(
      ColumnIdentityMap value, HistoryTier historyTier) {
    Objects.requireNonNull(historyTier, "historyTier");
    SchemaIdentityState state = restore(value);
    if (state.historyTier() == historyTier) {
      return value;
    }
    SchemaIdentityState rebound =
        state.copy(
            state.sourceVersion(),
            state.highWaterMark(),
            historyTier,
            state.nativeHighWaterMark(),
            state.sourceVersionAnchored());
    return toProto(rebound);
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

  private static ByteString sha256(ByteString payload) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      payload.asReadOnlyByteBufferList().forEach(digest::update);
      return ByteString.copyFrom(digest.digest());
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 is unavailable", impossible);
    }
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
