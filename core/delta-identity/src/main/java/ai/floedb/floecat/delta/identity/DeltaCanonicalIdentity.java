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
import ai.floedb.floecat.schema.identity.IdentityMode;
import ai.floedb.floecat.schema.identity.LegacyDottedKeyIndex;
import ai.floedb.floecat.schema.identity.PreferredCanonicalIds;
import ai.floedb.floecat.schema.identity.SchemaIdentityReconciler;
import ai.floedb.floecat.schema.identity.SchemaIdentityState;
import io.delta.kernel.Snapshot;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Converts Delta schema identity reconciliation state to and from the snapshot wire contract. */
public final class DeltaCanonicalIdentity {
  private DeltaCanonicalIdentity() {}

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

  public static void validateSnapshot(Snapshot snapshot, ColumnIdentityMap identityMap) {
    validateSnapshot(DeltaIdentitySnapshot.from(snapshot), identityMap);
  }

  public static SchemaIdentityState validateSnapshot(
      DeltaIdentitySnapshot snapshot, ColumnIdentityMap identityMap) {
    SchemaIdentityState state = ColumnIdentityMaps.restore(identityMap);
    if (state.sourceVersion() != snapshot.version()) {
      throw new IllegalArgumentException(
          "Column identity map version "
              + state.sourceVersion()
              + " does not match Delta snapshot "
              + snapshot.version());
    }
    DeltaResolvedSchema resolved = snapshot.resolved();
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
              snapshot.version() + 1L,
              IdentityMode.NATIVE_FIELD_ID,
              Optional.of(state),
              state.highWaterMark(),
              PreferredCanonicalIds.NONE,
              state.historyTier());
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
    return state;
  }

  /** Resolves and validates once for all schema and statistics consumers. */
  public static ValidatedIdentity resolve(Snapshot snapshot, ColumnIdentityMap identityMap) {
    return resolve(DeltaIdentitySnapshot.from(snapshot), identityMap);
  }

  public static ValidatedIdentity resolve(
      DeltaIdentitySnapshot source, ColumnIdentityMap identityMap) {
    SchemaIdentityState state = validateSnapshot(source, identityMap);
    LegacyDottedKeyIndex<Long> ids = LegacyDottedKeyIndex.create();
    LegacyDottedKeyIndex<Integer> ordinals = LegacyDottedKeyIndex.create();
    source
        .resolved()
        .schema()
        .nodes()
        .forEach(
            node -> {
              ids.add(node.path(), state.byPath(node.path()).orElseThrow().canonicalId());
              ordinals.add(node.path(), node.ordinal());
            });
    return new ValidatedIdentity(
        source.resolved(),
        state,
        java.util.Map.copyOf(ids.values()),
        java.util.Map.copyOf(ordinals.values()));
  }

  public record ValidatedIdentity(
      DeltaResolvedSchema schema,
      SchemaIdentityState state,
      java.util.Map<String, Long> canonicalIdsByStatsKey,
      java.util.Map<String, Integer> ordinalsByStatsKey) {}

  public static java.util.Map<ColumnPath, Long> canonicalIdsByPath(ColumnIdentityMap identityMap) {
    SchemaIdentityState state = ColumnIdentityMaps.restore(identityMap);
    java.util.Map<ColumnPath, Long> result = new java.util.LinkedHashMap<>();
    state.entries().forEach(entry -> result.put(entry.path(), entry.canonicalId()));
    return java.util.Map.copyOf(result);
  }
}
