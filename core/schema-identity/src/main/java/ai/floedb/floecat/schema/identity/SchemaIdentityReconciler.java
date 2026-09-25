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
import java.util.Set;

/**
 * Assigns canonical IDs by reconciling one source schema against its predecessor. Structured-path
 * identity cannot distinguish a drop and re-add of the same path unless the caller observes every
 * intervening metadata change, so the caller must attest whether that history is complete.
 */
public final class SchemaIdentityReconciler {
  private SchemaIdentityReconciler() {}

  public static Result reconcile(
      ResolvedSchema schema,
      long sourceVersion,
      IdentityMode mode,
      Optional<SchemaIdentityState> previous,
      HistoryCoverage historyCoverage) {
    Objects.requireNonNull(previous, "previous");
    Objects.requireNonNull(historyCoverage, "historyCoverage");
    previous.ifPresent(
        state -> {
          validateSourceVersion(sourceVersion, state);
          validateMode(mode, state);
        });
    long retainedHighWaterMark = previous.map(SchemaIdentityState::highWaterMark).orElse(0L);
    Optional<SchemaIdentityState> effectivePrevious =
        historyCoverage == HistoryCoverage.GAP ? Optional.empty() : previous;
    return reconcileInternal(schema, sourceVersion, mode, effectivePrevious, retainedHighWaterMark);
  }

  /** Starts a new identity generation without allowing the canonical ID counter to regress. */
  public static Result reset(
      ResolvedSchema schema, long sourceVersion, IdentityMode mode, long previousHighWaterMark) {
    CanonicalColumnId.checkAllocatedRange(previousHighWaterMark);
    return reconcileInternal(schema, sourceVersion, mode, Optional.empty(), previousHighWaterMark);
  }

  /**
   * Advances a state after the caller attests that the interval contains no unobserved metadata
   * changes.
   */
  public static SchemaIdentityState stampSourceVersion(
      SchemaIdentityState state, long sourceVersion, HistoryCoverage historyCoverage) {
    Objects.requireNonNull(state, "state");
    Objects.requireNonNull(historyCoverage, "historyCoverage");
    if (historyCoverage != HistoryCoverage.COMPLETE_METADATA_HISTORY) {
      throw new IllegalArgumentException("Cannot stamp across a metadata history gap");
    }
    if (sourceVersion < state.sourceVersion()) {
      throw new IllegalArgumentException(
          "Cannot stamp source version " + sourceVersion + " before " + state.sourceVersion());
    }
    if (sourceVersion == state.sourceVersion()) {
      return state;
    }
    String stateChecksum = stateChecksum(state.fingerprint(), state.highWaterMark(), sourceVersion);
    return new SchemaIdentityState(
        sourceVersion,
        state.highWaterMark(),
        state.mode(),
        state.entries(),
        state.fingerprint(),
        stateChecksum);
  }

  private static Result reconcileInternal(
      ResolvedSchema schema,
      long sourceVersion,
      IdentityMode mode,
      Optional<SchemaIdentityState> previous,
      long initialHighWaterMark) {
    Objects.requireNonNull(schema, "schema");
    Objects.requireNonNull(mode, "mode");
    Objects.requireNonNull(previous, "previous");
    CanonicalColumnId.checkAllocatedRange(initialHighWaterMark);
    if (sourceVersion < 0) {
      throw new IllegalArgumentException("Source version must be non-negative");
    }
    long highWaterMark =
        Math.max(initialHighWaterMark, previous.map(SchemaIdentityState::highWaterMark).orElse(0L));
    Map<ColumnPath, SchemaIdentityEntry> previousByPath = new HashMap<>();
    previous.ifPresent(
        state -> state.entries().forEach(entry -> previousByPath.put(entry.path(), entry)));
    List<CanonicalSchemaNode> nodes = new ArrayList<>(schema.nodes().size());
    List<SchemaIdentityEntry> entries = new ArrayList<>(schema.nodes().size());
    Set<Long> usedIds = new HashSet<>();

    for (SchemaNode node : schema.nodes()) {
      long canonicalId;
      if (mode == IdentityMode.NATIVE_FIELD_ID) {
        canonicalId =
            node.kind() == NodeKind.FIELD
                ? CanonicalColumnId.nativeFieldId(node)
                : CanonicalColumnId.collectionInteriorId(schema, node);
      } else {
        SchemaIdentityEntry prior = previousByPath.get(node.path());
        if (prior != null) {
          canonicalId = prior.canonicalId();
        } else {
          if (highWaterMark == CanonicalColumnId.MAX_ALLOCATED_ID) {
            throw new IllegalStateException("Canonical column ID space exhausted");
          }
          canonicalId = ++highWaterMark;
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
    }

    String fingerprint = fingerprint(mode, entries);
    String stateChecksum = stateChecksum(fingerprint, highWaterMark, sourceVersion);
    SchemaIdentityState state =
        new SchemaIdentityState(
            sourceVersion, highWaterMark, mode, entries, fingerprint, stateChecksum);
    return new Result(nodes, state);
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
      updateString(digest, mode.name());
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

  static String stateChecksum(String fingerprint, long highWaterMark, long sourceVersion) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      updateString(digest, fingerprint);
      digest.update(ByteBuffer.allocate(Long.BYTES).putLong(highWaterMark).array());
      digest.update(ByteBuffer.allocate(Long.BYTES).putLong(sourceVersion).array());
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
