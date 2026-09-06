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
import java.util.OptionalInt;
import java.util.Set;

/**
 * Assigns canonical IDs by reconciling one source schema against its immediate predecessor.
 * Structured-path identity cannot distinguish a drop and re-add of the same path when both occur
 * entirely within one source version; the intermediate absence must be observable to allocate a new
 * ID.
 */
public final class SchemaIdentityReconciler {
  private SchemaIdentityReconciler() {}

  public static Result reconcile(
      ResolvedSchema schema,
      long sourceVersion,
      IdentityMode mode,
      Optional<SchemaIdentityState> previous) {
    return reconcileInternal(schema, sourceVersion, mode, previous, 0L);
  }

  /** Starts a new identity generation without allowing the canonical ID counter to regress. */
  public static Result reset(
      ResolvedSchema schema, long sourceVersion, IdentityMode mode, long previousHighWaterMark) {
    if (previousHighWaterMark < 0L) {
      throw new IllegalArgumentException("Previous high-water mark must be non-negative");
    }
    return reconcileInternal(schema, sourceVersion, mode, Optional.empty(), previousHighWaterMark);
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
    if (sourceVersion < 0) {
      throw new IllegalArgumentException("Source version must be non-negative");
    }
    previous.ifPresent(state -> validatePredecessor(sourceVersion, mode, state));

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
        canonicalId = requiredNativeId(node);
      } else {
        SchemaIdentityEntry prior = previousByPath.get(node.path());
        if (prior != null) {
          canonicalId = prior.canonicalId();
        } else {
          if (highWaterMark == Long.MAX_VALUE) {
            throw new IllegalStateException("Canonical column ID space exhausted");
          }
          canonicalId = ++highWaterMark;
        }
      }
      if (!usedIds.add(canonicalId)) {
        throw new IllegalArgumentException("Duplicate canonical column ID " + canonicalId);
      }
      highWaterMark = Math.max(highWaterMark, canonicalId);
      nodes.add(new CanonicalSchemaNode(node, canonicalId));
      entries.add(new SchemaIdentityEntry(node.path(), node.nativeFieldId(), canonicalId));
    }

    String fingerprint = fingerprint(mode, highWaterMark, entries);
    SchemaIdentityState state =
        new SchemaIdentityState(sourceVersion, highWaterMark, mode, entries, fingerprint);
    return new Result(nodes, state);
  }

  private static void validatePredecessor(
      long sourceVersion, IdentityMode mode, SchemaIdentityState previous) {
    if (sourceVersion <= previous.sourceVersion()) {
      throw new IllegalArgumentException(
          "Source version " + sourceVersion + " does not follow " + previous.sourceVersion());
    }
    if (mode != previous.mode()) {
      throw new IllegalArgumentException(
          "Identity mode changed; a clean identity reset is required");
    }
    if (mode == IdentityMode.STRUCTURED_PATH && sourceVersion != previous.sourceVersion() + 1) {
      throw new IllegalArgumentException(
          "Unmapped identity reconciliation cannot skip source versions: expected "
              + (previous.sourceVersion() + 1)
              + " but received "
              + sourceVersion);
    }
  }

  private static long requiredNativeId(SchemaNode node) {
    OptionalInt nativeId = node.nativeFieldId();
    if (nativeId.isEmpty() || nativeId.getAsInt() <= 0) {
      throw new IllegalArgumentException(
          "Mapped node " + node.path().display() + " has no positive native field ID");
    }
    return nativeId.getAsInt();
  }

  static String fingerprint(
      IdentityMode mode, long highWaterMark, List<SchemaIdentityEntry> entries) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      updateString(digest, mode.name());
      digest.update(ByteBuffer.allocate(Long.BYTES).putLong(highWaterMark).array());
      entries.stream()
          .sorted(Comparator.comparing(entry -> structuredPath(entry.path())))
          .forEach(
              entry -> {
                updateString(digest, structuredPath(entry.path()));
                digest.update(
                    ByteBuffer.allocate(Integer.BYTES)
                        .putInt(entry.nativeFieldId().orElse(0))
                        .array());
                digest.update(ByteBuffer.allocate(Long.BYTES).putLong(entry.canonicalId()).array());
              });
      return "sha256:" + HexFormat.of().formatHex(digest.digest());
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }

  public static String structuredPath(ColumnPath path) {
    StringBuilder out = new StringBuilder();
    for (ColumnPath.Element element : path.elements()) {
      out.append(element.kind().ordinal()).append(':');
      String name = element.name() == null ? "" : element.name();
      out.append(name.length()).append(':').append(name).append(';');
    }
    return out.toString();
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
