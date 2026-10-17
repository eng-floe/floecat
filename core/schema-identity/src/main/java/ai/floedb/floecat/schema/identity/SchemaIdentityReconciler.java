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
 * the same mapping generation; callers represent a generation boundary as a reset.
 */
public final class SchemaIdentityReconciler {
  private SchemaIdentityReconciler() {}

  /**
   * Reconciles with authoritative IDs from a newer state for identities proven continuous to it.
   * Preferred IDs never lower the high-water mark or replace an identity retained from the
   * predecessor.
   */
  public static Result reconcile(
      ResolvedSchema schema,
      long sourceVersion,
      IdentityMode mode,
      Optional<SchemaIdentityState> previous,
      HistoryCoverage historyCoverage,
      long highWaterMarkFloor,
      PreferredCanonicalIds preferredIds,
      HistoryTier historyTier) {
    Objects.requireNonNull(previous, "previous");
    Objects.requireNonNull(historyCoverage, "historyCoverage");
    Objects.requireNonNull(preferredIds, "preferredIds");
    Objects.requireNonNull(historyTier, "historyTier");
    CanonicalColumnId.checkAllocatedRange(highWaterMarkFloor);
    validatePreferredIds(mode, preferredIds);
    previous.ifPresent(
        state -> {
          validateSourceVersion(sourceVersion, state);
          validateMode(mode, state);
        });
    long retainedHighWaterMark =
        Math.max(highWaterMarkFloor, previous.map(SchemaIdentityState::highWaterMark).orElse(0L));
    Optional<SchemaIdentityState> effectivePrevious =
        historyCoverage == HistoryCoverage.GAP ? Optional.empty() : previous;
    return reconcileInternal(
        schema,
        sourceVersion,
        mode,
        effectivePrevious,
        retainedHighWaterMark,
        preferredIds,
        historyTier);
  }

  /** Starts a new identity generation without allowing the canonical ID counter to regress. */
  public static Result reset(
      ResolvedSchema schema,
      long sourceVersion,
      IdentityMode mode,
      long previousHighWaterMark,
      HistoryTier historyTier) {
    CanonicalColumnId.checkAllocatedRange(previousHighWaterMark);
    return reconcileInternal(
        schema,
        sourceVersion,
        mode,
        Optional.empty(),
        previousHighWaterMark,
        PreferredCanonicalIds.NONE,
        historyTier);
  }

  /** Binds current-schema identity to a source version after the caller validates that schema. */
  public static SchemaIdentityState bindSourceVersion(
      SchemaIdentityState state, long sourceVersion) {
    Objects.requireNonNull(state, "state");
    if (sourceVersion < 0L) {
      throw new IllegalArgumentException("Source version must be non-negative");
    }
    if (sourceVersion == state.sourceVersion()) {
      return state;
    }
    return SchemaIdentityState.restore(
        sourceVersion,
        state.highWaterMark(),
        state.mode(),
        state.historyTier(),
        state.nativeHighWaterMark(),
        state.sourceVersionAnchored(),
        state.entries(),
        state.fingerprint(),
        stateChecksum(
            state.fingerprint(),
            state.highWaterMark(),
            sourceVersion,
            state.nativeHighWaterMark(),
            state.sourceVersionAnchored()));
  }

  /** Advances a state while retaining an independently persisted high-water mark. */
  public static SchemaIdentityState stampSourceVersion(
      SchemaIdentityState state,
      long sourceVersion,
      HistoryCoverage historyCoverage,
      long highWaterMarkFloor) {
    Objects.requireNonNull(state, "state");
    Objects.requireNonNull(historyCoverage, "historyCoverage");
    CanonicalColumnId.checkAllocatedRange(highWaterMarkFloor);
    if (historyCoverage != HistoryCoverage.COMPLETE_METADATA_HISTORY) {
      throw new IllegalArgumentException("Cannot stamp across a metadata history gap");
    }
    if (sourceVersion < state.sourceVersion()) {
      throw new IllegalArgumentException(
          "Cannot stamp source version " + sourceVersion + " before " + state.sourceVersion());
    }
    long highWaterMark = Math.max(state.highWaterMark(), highWaterMarkFloor);
    if (sourceVersion == state.sourceVersion() && highWaterMark == state.highWaterMark()) {
      return state;
    }
    return new SchemaIdentityState(
        sourceVersion,
        highWaterMark,
        state.mode(),
        state.historyTier(),
        state.nativeHighWaterMark(),
        state.sourceVersionAnchored(),
        state.entries(),
        state.fingerprint(),
        stateChecksum(
            state.fingerprint(),
            highWaterMark,
            sourceVersion,
            state.nativeHighWaterMark(),
            state.sourceVersionAnchored()));
  }

  private static Result reconcileInternal(
      ResolvedSchema schema,
      long sourceVersion,
      IdentityMode mode,
      Optional<SchemaIdentityState> previous,
      long initialHighWaterMark,
      PreferredCanonicalIds preferredIds,
      HistoryTier historyTier) {
    Objects.requireNonNull(schema, "schema");
    Objects.requireNonNull(mode, "mode");
    Objects.requireNonNull(previous, "previous");
    CanonicalColumnId.checkAllocatedRange(initialHighWaterMark);
    if (sourceVersion < 0L) {
      throw new IllegalArgumentException("Source version must be non-negative");
    }

    long highWaterMark =
        Math.max(initialHighWaterMark, previous.map(SchemaIdentityState::highWaterMark).orElse(0L));
    Map<ColumnPath, SchemaIdentityEntry> previousByPath = new HashMap<>();
    Map<Integer, SchemaIdentityEntry> previousByNativeId = new HashMap<>();
    Map<Long, ColumnPath> previousPathById = new HashMap<>();
    Map<Long, Integer> previousNativeIdById = new HashMap<>();
    previous.ifPresent(
        state ->
            state
                .entries()
                .forEach(
                    entry -> {
                      previousByPath.put(entry.path(), entry);
                      if (isField(entry.path()) && entry.nativeFieldId().isPresent()) {
                        SchemaIdentityEntry duplicate =
                            previousByNativeId.put(entry.nativeFieldId().getAsInt(), entry);
                        if (duplicate != null) {
                          throw new IllegalArgumentException(
                              "Duplicate native field ID " + entry.nativeFieldId().getAsInt());
                        }
                        previousNativeIdById.put(
                            entry.canonicalId(), entry.nativeFieldId().getAsInt());
                      }
                      previousPathById.put(entry.canonicalId(), entry.path());
                    }));

    List<CanonicalSchemaNode> nodes = new ArrayList<>(schema.nodes().size());
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
          ColumnPath previousOwner = preferredId == null ? null : previousPathById.get(preferredId);
          Integer previousNativeOwner =
              preferredId == null ? null : previousNativeIdById.get(preferredId);
          boolean ownedBySameIdentity =
              mode == IdentityMode.NATIVE_FIELD_ID
                  ? previousNativeOwner == null || previousNativeOwner == nativeId
                  : previousOwner == null || previousOwner.equals(node.path());
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
      nodes.add(new CanonicalSchemaNode(node, canonicalId));
      entries.add(new SchemaIdentityEntry(node.path(), node.nativeFieldId(), canonicalId));
      canonicalIdsByPath.put(node.path(), canonicalId);
    }

    String fingerprint = fingerprint(mode, entries);
    String stateChecksum =
        stateChecksum(fingerprint, highWaterMark, sourceVersion, OptionalLong.empty(), true);
    SchemaIdentityState state =
        new SchemaIdentityState(
            sourceVersion,
            highWaterMark,
            mode,
            historyTier,
            OptionalLong.empty(),
            true,
            entries,
            fingerprint,
            stateChecksum);
    return new Result(nodes, state);
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
    if (sourceVersion <= previous.sourceVersion()) {
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
      boolean sourceVersionAnchored) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      updateString(digest, fingerprint);
      digest.update(ByteBuffer.allocate(Long.BYTES).putLong(highWaterMark).array());
      digest.update(ByteBuffer.allocate(Long.BYTES).putLong(sourceVersion).array());
      digest.update((byte) (sourceVersionAnchored ? 1 : 0));
      nativeHighWaterMark.ifPresent(
          value -> digest.update(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
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

  public record Result(List<CanonicalSchemaNode> nodes, SchemaIdentityState state) {
    public Result {
      nodes = List.copyOf(Objects.requireNonNull(nodes, "nodes"));
      Objects.requireNonNull(state, "state");
    }

    public Map<ColumnPath, Long> idsByPath() {
      Map<ColumnPath, Long> result = new HashMap<>();
      nodes.forEach(node -> result.put(node.source().path(), node.canonicalId()));
      return Map.copyOf(result);
    }
  }
}
