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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Assigns canonical IDs by reconciling one source schema against its predecessor. Path identity
 * requires complete metadata history. Native identity additionally requires that both states are in
 * the same mapping generation; callers start a new generation by reconciling with no predecessor
 * above the previous high-water mark.
 */
public final class SchemaIdentityReconciler {
  private SchemaIdentityReconciler() {}

  /**
   * Reconciles with authoritative IDs from a newer state for identities proven continuous to it.
   * Preferred IDs never lower the high-water mark or replace an identity retained from the
   * predecessor.
   */
  public static SchemaIdentityState reconcile(
      ResolvedSchema schema,
      long sourceVersion,
      IdentityMode mode,
      Optional<SchemaIdentityState> previous,
      long highWaterMarkFloor,
      PreferredCanonicalIds preferredIds,
      HistoryTier historyTier) {
    Objects.requireNonNull(schema, "schema");
    Objects.requireNonNull(mode, "mode");
    Objects.requireNonNull(previous, "previous");
    Objects.requireNonNull(preferredIds, "preferredIds");
    Objects.requireNonNull(historyTier, "historyTier");
    CanonicalColumnId.checkAllocatedRange(highWaterMarkFloor);
    validatePreferredIds(mode, preferredIds);
    previous.ifPresent(
        state -> {
          validateSourceVersion(sourceVersion, state);
          validateMode(mode, state);
        });
    if (sourceVersion < 0L) {
      throw new IllegalArgumentException("Source version must be non-negative");
    }
    long highWaterMark =
        Math.max(highWaterMarkFloor, previous.map(SchemaIdentityState::highWaterMark).orElse(0L));
    Map<ColumnPath, SchemaIdentityEntry> previousByPath = new HashMap<>();
    Map<Integer, SchemaIdentityEntry> previousByNativeId = new HashMap<>();
    Map<Long, SchemaIdentityEntry> previousById = new HashMap<>();
    previous.ifPresent(
        state ->
            state
                .entries()
                .forEach(
                    entry -> {
                      previousByPath.put(entry.path(), entry);
                      if (isField(entry.path()) && entry.nativeFieldId().isPresent()) {
                        previousByNativeId.put(entry.nativeFieldId().getAsInt(), entry);
                      }
                      previousById.put(entry.canonicalId(), entry);
                    }));

    List<SchemaIdentityEntry> entries = new ArrayList<>(schema.nodes().size());
    Set<Long> usedIds = new HashSet<>();
    Map<ColumnPath, Long> canonicalIdsByPath = new HashMap<>();

    for (SchemaNode node : schema.nodes()) {
      long canonicalId;
      if (node.kind() != NodeKind.FIELD) {
        canonicalId = CanonicalColumnId.collectionInteriorId(node, canonicalIdsByPath);
      } else {
        int nativeId =
            mode == IdentityMode.NATIVE_FIELD_ID ? CanonicalColumnId.nativeFieldId(node) : 0;
        SchemaIdentityEntry prior =
            mode == IdentityMode.NATIVE_FIELD_ID
                ? previousByNativeId.get(nativeId)
                : previousByPath.get(node.path());
        if (prior != null) {
          canonicalId = prior.canonicalId();
        } else {
          Long preferredId =
              mode == IdentityMode.NATIVE_FIELD_ID
                  ? preferredIds.byNativeId().get(nativeId)
                  : preferredIds.byPath().get(node.path());
          SchemaIdentityEntry previousOwner =
              preferredId == null ? null : previousById.get(preferredId);
          boolean ownedBySameIdentity =
              previousOwner == null
                  || (mode == IdentityMode.NATIVE_FIELD_ID
                      ? previousOwner.nativeFieldId().orElse(0) == nativeId
                      : previousOwner.path().equals(node.path()));
          if (preferredId != null && ownedBySameIdentity && !usedIds.contains(preferredId)) {
            canonicalId = preferredId;
          } else {
            if (highWaterMark == CanonicalColumnId.MAX_ALLOCATED_ID) {
              throw new IllegalStateException("Canonical column ID space exhausted");
            }
            canonicalId = ++highWaterMark;
          }
        }
      }
      if (!usedIds.add(canonicalId)) {
        throw new IllegalArgumentException("Duplicate canonical column ID " + canonicalId);
      }
      if (!CanonicalColumnId.isDerived(canonicalId)) {
        highWaterMark = Math.max(highWaterMark, canonicalId);
      }
      entries.add(new SchemaIdentityEntry(node.path(), node.nativeFieldId(), canonicalId));
      canonicalIdsByPath.put(node.path(), canonicalId);
    }

    String fingerprint = fingerprint(mode, entries);
    String stateChecksum =
        stateChecksum(
            fingerprint,
            highWaterMark,
            sourceVersion,
            OptionalLong.empty(),
            true,
            historyTier,
            entries);
    return new SchemaIdentityState(
        sourceVersion,
        highWaterMark,
        mode,
        historyTier,
        OptionalLong.empty(),
        true,
        entries,
        fingerprint,
        stateChecksum);
  }

  /** Binds current-schema identity to a source version after the caller validates that schema. */
  public static SchemaIdentityState bindSourceVersion(
      SchemaIdentityState state, long sourceVersion) {
    Objects.requireNonNull(state, "state");
    if (sourceVersion < 0L) {
      throw new IllegalArgumentException("Source version must be non-negative");
    }
    return state.copy(
        sourceVersion,
        state.highWaterMark(),
        state.historyTier(),
        state.nativeHighWaterMark(),
        state.sourceVersionAnchored());
  }

  private static boolean isField(ColumnPath path) {
    return path.elements().getLast().kind() == NodeKind.FIELD;
  }

  private static void validatePreferredIds(IdentityMode mode, PreferredCanonicalIds preferredIds) {
    if (mode == IdentityMode.STRUCTURED_PATH && !preferredIds.byNativeId().isEmpty()) {
      throw new IllegalArgumentException("Native preferred IDs require native identity mode");
    }
    if (mode == IdentityMode.NATIVE_FIELD_ID && !preferredIds.byPath().isEmpty()) {
      throw new IllegalArgumentException(
          "Path preferred IDs require structured-path identity mode");
    }
    Set<Long> ids = new HashSet<>();
    preferredIds
        .byPath()
        .forEach((path, id) -> validatePreferredId(path != null && !path.isRoot(), id, ids));
    preferredIds
        .byNativeId()
        .forEach((nativeId, id) -> validatePreferredId(nativeId != null && nativeId > 0, id, ids));
  }

  private static void validatePreferredId(boolean validKey, Long id, Set<Long> ids) {
    if (!validKey || id == null || id <= 0L || CanonicalColumnId.isDerived(id)) {
      throw new IllegalArgumentException("Preferred identity must use an allocated column ID");
    }
    CanonicalColumnId.checkAllocatedRange(id);
    if (!ids.add(id)) {
      throw new IllegalArgumentException("Duplicate preferred canonical column ID " + id);
    }
  }

  private static void validateSourceVersion(long sourceVersion, SchemaIdentityState previous) {
    if (sourceVersion < previous.sourceVersion()
        || (sourceVersion == previous.sourceVersion() && previous.sourceVersionAnchored())) {
      throw new IllegalArgumentException(
          "Source version " + sourceVersion + " does not follow " + previous.sourceVersion());
    }
  }

  private static void validateMode(IdentityMode mode, SchemaIdentityState previous) {
    Objects.requireNonNull(mode, "mode");
    if (mode != previous.mode()) {
      throw new IllegalArgumentException(
          "Identity mode changed; a clean identity reset is required");
    }
  }

  static String fingerprint(IdentityMode mode, List<SchemaIdentityEntry> entries) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      updateString(digest, mode.stableToken());
      entries.stream()
          .sorted(Comparator.comparing(entry -> entry.path().stableKey()))
          .forEach(
              entry -> {
                updateString(digest, entry.path().stableKey());
                digest.update(ByteBuffer.allocate(Long.BYTES).putLong(entry.canonicalId()).array());
              });
      return "sha256:" + HexFormat.of().formatHex(digest.digest());
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }

  static String stateChecksum(
      String fingerprint,
      long highWaterMark,
      long sourceVersion,
      OptionalLong nativeHighWaterMark,
      boolean sourceVersionAnchored,
      HistoryTier historyTier,
      List<SchemaIdentityEntry> entries) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      updateString(digest, fingerprint);
      digest.update(ByteBuffer.allocate(Long.BYTES).putLong(highWaterMark).array());
      digest.update(ByteBuffer.allocate(Long.BYTES).putLong(sourceVersion).array());
      digest.update((byte) (sourceVersionAnchored ? 1 : 0));
      updateString(digest, historyTier.name());
      nativeHighWaterMark.ifPresent(
          value -> digest.update(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
      entries.stream()
          .sorted(Comparator.comparing(entry -> entry.path().stableKey()))
          .forEach(
              entry -> {
                updateString(digest, entry.path().stableKey());
                digest.update((byte) (entry.nativeFieldId().isPresent() ? 1 : 0));
                entry
                    .nativeFieldId()
                    .ifPresent(
                        value ->
                            digest.update(
                                ByteBuffer.allocate(Integer.BYTES).putInt(value).array()));
              });
      return "sha256:" + HexFormat.of().formatHex(digest.digest());
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }

  private static void updateString(MessageDigest digest, String value) {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
    digest.update(bytes);
  }
}
