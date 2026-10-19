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

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.rpc.ColumnIdentityHistoryTier;
import ai.floedb.floecat.catalog.rpc.ColumnIdentityMap;
import ai.floedb.floecat.schema.identity.ColumnPath;
import ai.floedb.floecat.schema.identity.HistoryTier;
import ai.floedb.floecat.schema.identity.IdentityMode;
import ai.floedb.floecat.schema.identity.PreferredCanonicalIds;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class DeltaIdentityServiceTest {
  private static final String UNMAPPED_A = schema(field("a", "long", null));
  private static final String UNMAPPED_AB =
      schema(field("a", "long", null), field("b", "string", null));

  private final DeltaIdentityService service = new DeltaIdentityService();

  @Test
  void noHistoryUnmappedAdditionAndRemovalReconcileByPath() {
    ColumnIdentityMap first = reconcile(source(1L, UNMAPPED_A, Map.of()), Optional.empty());
    ColumnIdentityMap added = reconcile(source(2L, UNMAPPED_AB, Map.of()), Optional.of(first));
    ColumnIdentityMap removed = reconcile(source(3L, UNMAPPED_A, Map.of()), Optional.of(added));

    assertThat(columnId(added, "a")).isEqualTo(columnId(first, "a"));
    assertThat(columnId(added, "b")).isGreaterThan(first.getHighWaterMark());
    assertThat(removed.getEntriesList()).hasSize(1);
    assertThat(columnId(removed, "a")).isEqualTo(columnId(first, "a"));
  }

  @Test
  void noHistoryMappedAdditionDropAndRenameReconcileByNativeId() {
    ColumnIdentityMap first =
        reconcile(
            source(1L, mappedSchema(field("a", "long", 1)), mappedProperties(5)), Optional.empty());
    ColumnIdentityMap added =
        reconcile(
            source(
                2L,
                mappedSchema(field("a", "long", 1), field("b", "string", 6)),
                mappedProperties(6)),
            Optional.of(first));
    ColumnIdentityMap dropped =
        reconcile(
            source(3L, mappedSchema(field("b", "string", 6)), mappedProperties(6)),
            Optional.of(added));
    ColumnIdentityMap renamed =
        reconcile(
            source(4L, mappedSchema(field("renamed", "string", 6)), mappedProperties(6)),
            Optional.of(dropped));

    assertThat(columnId(added, "a")).isEqualTo(columnId(first, "a"));
    assertThat(columnId(dropped, "b")).isEqualTo(columnId(added, "b"));
    assertThat(columnId(renamed, "renamed")).isEqualTo(columnId(added, "b"));
  }

  @Test
  void noHistoryMappedDropAndReaddWithNewNativeIdAllocatesNewIdentity() {
    ColumnIdentityMap present =
        reconcile(
            source(1L, mappedSchema(field("x", "long", 1)), mappedProperties(5)), Optional.empty());
    ColumnIdentityMap dropped =
        reconcile(source(2L, mappedSchema(), mappedProperties(5)), Optional.of(present));
    ColumnIdentityMap readded =
        reconcile(
            source(3L, mappedSchema(field("x", "long", 6)), mappedProperties(6)),
            Optional.of(dropped));

    assertThat(dropped.getEntriesList()).isEmpty();
    assertThat(dropped.getHighWaterMark()).isEqualTo(present.getHighWaterMark());
    assertThat(columnId(readded, "x")).isNotEqualTo(columnId(present, "x"));
    assertThat(columnId(readded, "x")).isGreaterThan(dropped.getHighWaterMark());
  }

  @Test
  void noHistoryMappedCounterRollbackStartsANewGeneration() {
    ColumnIdentityMap first =
        reconcile(
            source(1L, mappedSchema(field("a", "long", 1)), mappedProperties(5)), Optional.empty());
    ColumnIdentityMap reset =
        reconcile(
            source(2L, mappedSchema(field("a", "long", 1)), mappedProperties(4)),
            Optional.of(first));

    assertThat(columnId(reset, "a")).isGreaterThan(first.getHighWaterMark());
  }

  @Test
  void noHistoryMappedReuseOfAnAllocatedNativeIdStartsANewGeneration() {
    ColumnIdentityMap first =
        reconcile(
            source(1L, mappedSchema(field("a", "long", 1)), mappedProperties(5)), Optional.empty());
    ColumnIdentityMap reset =
        reconcile(
            source(
                2L,
                mappedSchema(field("a", "long", 1), field("reused", "string", 3)),
                mappedProperties(5)),
            Optional.of(first));

    assertThat(reset.getEntriesList())
        .allMatch(entry -> entry.getColumnId() > first.getHighWaterMark());
  }

  @Test
  void unorderedHistoryDetectsAnInterveningMappingDisablement() {
    ColumnIdentityMap first =
        reconcile(
            source(10L, mappedSchema(field("a", "long", 1)), mappedProperties(5)),
            Optional.empty());
    DeltaIdentitySnapshot disabled = snapshot(20L, UNMAPPED_A, Map.of());
    DeltaIdentitySnapshot current =
        snapshot(30L, mappedSchema(field("a", "long", 1)), mappedProperties(5));
    DeltaHistorySource history =
        new TestHistorySource(
            30L,
            HistoryTier.UNORDERED_QUERY_HISTORY,
            Map.of(20L, disabled, 30L, current),
            List.of(20L, 30L));

    ColumnIdentityMap reset = reconcile(history, Optional.of(first));

    assertThat(columnId(reset, "a")).isGreaterThan(first.getHighWaterMark());
  }

  @Test
  void strongerHistoryRecreatesAnUnprovenGeneration() {
    ColumnIdentityMap first = reconcile(source(1L, UNMAPPED_A, Map.of()), Optional.empty());
    DeltaHistorySource versioned =
        new TestHistorySource(
            2L,
            HistoryTier.VERSIONED_LOG,
            Map.of(2L, snapshot(2L, UNMAPPED_AB, Map.of())),
            List.of(2L));

    ColumnIdentityMap next = reconcile(versioned, Optional.of(first));

    assertThat(next.getHistoryTier())
        .isEqualTo(ColumnIdentityHistoryTier.COLUMN_IDENTITY_HISTORY_TIER_VERSIONED_LOG);
    assertThat(columnId(next, "a")).isGreaterThan(first.getHighWaterMark());
  }

  @Test
  void noHistorySourceVersionIsUnanchored() {
    ColumnIdentityMap map = reconcile(source(0L, UNMAPPED_A, Map.of()), Optional.empty());

    assertThat(map.getSourceVersionAnchored()).isFalse();
    ColumnIdentityMap unchanged = reconcile(source(0L, UNMAPPED_A, Map.of()), Optional.of(map));
    assertThat(unchanged.getFingerprint()).isEqualTo(map.getFingerprint());
  }

  @Test
  void unanchoredConstantVersionReconcilesSchemaChanges() {
    ColumnIdentityMap first = reconcile(source(0L, UNMAPPED_A, Map.of()), Optional.empty());

    ColumnIdentityMap added = reconcile(source(0L, UNMAPPED_AB, Map.of()), Optional.of(first));

    assertThat(columnId(added, "a")).isEqualTo(columnId(first, "a"));
    assertThat(columnId(added, "b")).isGreaterThan(first.getHighWaterMark());
  }

  @Test
  void schemaOnlyReplacementOfAnchoredVersionZeroRemainsUnanchored() {
    var first =
        reconcile(
            new TestHistorySource(
                0L,
                HistoryTier.VERSIONED_LOG,
                Map.of(0L, snapshot(0L, UNMAPPED_A, Map.of())),
                List.of()),
            Optional.empty());
    var replaced = reconcile(source(0L, UNMAPPED_AB, Map.of()), Optional.of(first));
    assertThat(columnId(replaced, "a")).isGreaterThan(first.getHighWaterMark());
    assertThat(replaced.getSourceVersionAnchored()).isFalse();
  }

  @Test
  void aMissingHeadIsASourceFailure() {
    DeltaHistorySource missing =
        new TestHistorySource(7L, HistoryTier.VERSIONED_LOG, Map.of(), List.of());

    IdentityResult result = service.reconcileCurrent(missing, lookup(Optional.empty()));

    assertThat(result.maps()).isEmpty();
    assertThat(result.failures())
        .singleElement()
        .satisfies(
            failure -> {
              assertThat(failure.kind()).isEqualTo(IdentityResult.Failure.Kind.SOURCE_ERROR);
              assertThat(failure.snapshotVersion()).isEqualTo(7L);
              assertThat(failure.message()).contains("did not return requested snapshot 7");
            });
  }

  @Test
  void anExplicitStructuralExclusionIsNotAFailure() {
    DeltaHistorySource retainedHistory =
        new TestHistorySource(7L, HistoryTier.VERSIONED_LOG, Map.of(), List.of()) {
          @Override
          public Optional<String> snapshotExclusionReason(long version) {
            return Optional.of("version is before earliest recreatable version 10");
          }
        };

    IdentityResult result =
        service.reconcileSnapshots(retainedHistory, lookup(Optional.empty()), List.of(7L));

    assertThat(result.maps()).isEmpty();
    assertThat(result.failures()).isEmpty();
  }

  @Test
  void metadataReadFailureDoesNotBecomeAGenerationReset() {
    DeltaIdentitySnapshot previousSnapshot = snapshot(1L, UNMAPPED_A, Map.of());
    ColumnIdentityMap previous =
        reconcile(
            new TestHistorySource(
                1L, HistoryTier.VERSIONED_LOG, Map.of(1L, previousSnapshot), List.of(1L)),
            Optional.empty());
    DeltaIdentitySnapshot current = snapshot(2L, UNMAPPED_AB, Map.of());
    DeltaHistorySource failing =
        new TestHistorySource(2L, HistoryTier.VERSIONED_LOG, Map.of(2L, current), List.of(2L)) {
          @Override
          public MetadataHistory metadataVersions(long firstVersion, long lastVersion) {
            throw new IllegalStateException("simulated object-store timeout");
          }
        };

    IdentityResult result = service.reconcileCurrent(failing, lookup(Optional.of(previous)));

    assertThat(result.maps()).isEmpty();
    assertThat(result.failures())
        .singleElement()
        .satisfies(
            failure -> {
              assertThat(failure.kind()).isEqualTo(IdentityResult.Failure.Kind.SOURCE_ERROR);
              assertThat(failure.message()).contains("object-store timeout");
            });
  }

  @Test
  void unmappedHistoryGapIsRejected() {
    Snapshot previousSnapshot = snapshot(1L, UNMAPPED_A, Map.of());
    ColumnIdentityMap previous =
        reconcile(
            new TestHistorySource(
                1L, HistoryTier.VERSIONED_LOG, Map.of(1L, previousSnapshot), List.of(1L)),
            Optional.empty());
    Snapshot current = snapshot(3L, UNMAPPED_AB, Map.of());
    DeltaHistorySource gapped =
        new TestHistorySource(3L, HistoryTier.VERSIONED_LOG, Map.of(3L, current), List.of(3L)) {
          @Override
          public MetadataHistory metadataVersions(long firstVersion, long lastVersion) {
            return MetadataHistory.gap("history-unavailable");
          }
        };

    IdentityResult result = service.reconcileCurrent(gapped, lookup(Optional.of(previous)));

    assertThat(result.maps()).isEmpty();
    assertThat(result.failures())
        .singleElement()
        .satisfies(
            failure -> {
              assertThat(failure.kind()).isEqualTo(IdentityResult.Failure.Kind.SOURCE_ERROR);
              assertThat(failure.message())
                  .contains("Cannot reconcile unmapped Delta column identity")
                  .contains("history-unavailable");
            });
  }

  @Test
  void mappedHistoryGapReconcilesByNativeFieldId() {
    Snapshot previousSnapshot =
        snapshot(1L, mappedSchema(field("a", "long", 1)), mappedProperties(1));
    ColumnIdentityMap previous =
        reconcile(
            new TestHistorySource(
                1L, HistoryTier.VERSIONED_LOG, Map.of(1L, previousSnapshot), List.of(1L)),
            Optional.empty());
    Snapshot current =
        snapshot(
            3L,
            mappedSchema(field("renamed", "long", 1), field("b", "string", 2)),
            mappedProperties(2));
    DeltaHistorySource gapped =
        new TestHistorySource(3L, HistoryTier.VERSIONED_LOG, Map.of(3L, current), List.of(3L)) {
          @Override
          public MetadataHistory metadataVersions(long firstVersion, long lastVersion) {
            return MetadataHistory.gap("history-unavailable");
          }
        };

    IdentityResult result = service.reconcileCurrent(gapped, lookup(Optional.of(previous)));

    assertThat(result.failures()).isEmpty();
    ColumnIdentityMap reconciled = result.maps().get(3L);
    assertThat(columnId(reconciled, "renamed")).isEqualTo(columnId(previous, "a"));
    assertThat(columnId(reconciled, "b")).isGreaterThan(previous.getHighWaterMark());
  }

  @Test
  void predecessorGapStillUsesIdentityProvenContinuousToSuccessor() {
    DeltaIdentitySnapshot version10 = snapshot(10L, UNMAPPED_A, Map.of());
    DeltaIdentitySnapshot version20 = snapshot(20L, UNMAPPED_A, Map.of());
    DeltaIdentitySnapshot version25 = snapshot(25L, UNMAPPED_A, Map.of());
    DeltaIdentitySnapshot version30 = snapshot(30L, UNMAPPED_A, Map.of());
    ColumnIdentityMap predecessor =
        reconcile(
            new TestHistorySource(
                10L, HistoryTier.VERSIONED_LOG, Map.of(10L, version10), List.of(10L)),
            Optional.empty());
    IdentityResult successorResult =
        service.reconcileSnapshots(
            new TestHistorySource(
                30L, HistoryTier.VERSIONED_LOG, Map.of(30L, version30), List.of(30L)),
            lookup(Map.of(), Optional.empty(), 5L),
            List.of(30L));
    ColumnIdentityMap successor = successorResult.maps().get(30L);
    DeltaHistorySource history =
        new TestHistorySource(
            30L,
            HistoryTier.VERSIONED_LOG,
            Map.of(10L, version10, 20L, version20, 25L, version25, 30L, version30),
            List.of()) {
          @Override
          public MetadataHistory metadataVersions(long firstVersion, long lastVersion) {
            if (firstVersion == 11L && lastVersion == 20L) {
              return MetadataHistory.gap("history before selected version was cleaned up");
            }
            if (firstVersion == 21L && lastVersion == 30L) {
              return MetadataHistory.complete(List.of(25L));
            }
            return MetadataHistory.complete(List.of());
          }
        };

    IdentityResult result =
        service.reconcileSnapshots(
            history,
            lookup(Map.of(10L, predecessor, 30L, successor), Optional.empty(), 6L),
            List.of(20L));

    assertThat(result.failures()).isEmpty();
    assertThat(columnId(result.maps().get(20L), "a")).isEqualTo(columnId(successor, "a"));
  }

  @Test
  void intermediatePreferencesRequirePresenceThroughTheSelectedVersion() {
    DeltaIdentitySnapshot introduced = snapshot(50L, UNMAPPED_AB, Map.of());
    DeltaIdentitySnapshot dropped = snapshot(55L, UNMAPPED_A, Map.of());
    DeltaIdentitySnapshot selected = snapshot(60L, UNMAPPED_A, Map.of());
    PreferredCanonicalIds successor =
        PreferredCanonicalIds.byPath(Map.of(ColumnPath.ROOT.field("b"), 3L));

    Map<Long, PreferredCanonicalIds> preferences =
        DeltaIdentityService.preferredByMetadataVersion(
            List.of(
                new DeltaIdentityService.MetadataStep(50L, introduced),
                new DeltaIdentityService.MetadataStep(55L, dropped)),
            60L,
            selected,
            successor);

    assertThat(preferences.get(50L).byPath()).isEmpty();
    assertThat(preferences.get(55L).byPath()).isEmpty();
    assertThat(preferences.get(60L).byPath()).isEmpty();
  }

  private ColumnIdentityMap reconcile(
      DeltaHistorySource source, Optional<ColumnIdentityMap> tableMap) {
    IdentityResult result = service.reconcileCurrent(source, lookup(tableMap));
    assertThat(result.failures()).isEmpty();
    return result.maps().get(source.headVersion());
  }

  private static DeltaHistorySource source(
      long version, String schemaJson, Map<String, String> properties) {
    return new TestHistorySource(
        version,
        HistoryTier.NONE,
        Map.of(version, snapshot(version, schemaJson, properties)),
        List.of());
  }

  private static DeltaIdentitySnapshot snapshot(
      long version, String schemaJson, Map<String, String> properties) {
    return KernelAdapter.currentSchemaIdentity(version, schemaJson, properties);
  }

  private static PersistedIdentityLookup lookup(Optional<ColumnIdentityMap> tableMap) {
    return lookup(Map.of(), tableMap, tableMap.map(ColumnIdentityMap::getHighWaterMark).orElse(0L));
  }

  private static PersistedIdentityLookup lookup(
      Map<Long, ColumnIdentityMap> maps, Optional<ColumnIdentityMap> tableMap, long highWaterMark) {
    return new PersistedIdentityLookup() {
      @Override
      public Optional<ColumnIdentityMap> exact(long version) {
        return Optional.ofNullable(maps.get(version));
      }

      @Override
      public Optional<Map.Entry<Long, ColumnIdentityMap>> floor(long version) {
        return maps.entrySet().stream()
            .filter(entry -> entry.getKey() <= version)
            .max(Map.Entry.comparingByKey());
      }

      @Override
      public Optional<Map.Entry<Long, ColumnIdentityMap>> ceiling(long version) {
        return maps.entrySet().stream()
            .filter(entry -> entry.getKey() >= version)
            .min(Map.Entry.comparingByKey());
      }

      @Override
      public Optional<ColumnIdentityMap> tableMap() {
        return tableMap;
      }

      @Override
      public long highWaterMark() {
        return highWaterMark;
      }
    };
  }

  private static long columnId(ColumnIdentityMap map, String name) {
    return map.getEntriesList().stream()
        .filter(entry -> entry.getPath(0).getName().equals(name))
        .findFirst()
        .orElseThrow()
        .getColumnId();
  }

  private static Map<String, String> mappedProperties(long maxColumnId) {
    return Map.of(
        ColumnMappingMode.PROPERTY,
        "name",
        DeltaColumnMapping.MAX_COLUMN_ID,
        Long.toString(maxColumnId));
  }

  private static String mappedSchema(String... fields) {
    return schema(fields);
  }

  private static String schema(String... fields) {
    return "{\"type\":\"struct\",\"fields\":[" + String.join(",", fields) + "]}";
  }

  private static String field(String name, String type, Integer nativeId) {
    String metadata =
        nativeId == null
            ? "{}"
            : "{\"delta.columnMapping.id\":"
                + nativeId
                + ",\"delta.columnMapping.physicalName\":\"col-"
                + nativeId
                + "\"}";
    return "{\"name\":\""
        + name
        + "\",\"type\":\""
        + type
        + "\",\"nullable\":true,\"metadata\":"
        + metadata
        + "}";
  }

  private static class TestHistorySource implements DeltaHistorySource {
    private final long headVersion;
    private final HistoryTier historyTier;
    private final Map<Long, DeltaIdentitySnapshot> snapshots;
    private final List<Long> metadataVersions;

    private TestHistorySource(
        long headVersion,
        HistoryTier historyTier,
        Map<Long, DeltaIdentitySnapshot> snapshots,
        List<Long> metadataVersions) {
      this.headVersion = headVersion;
      this.historyTier = historyTier;
      this.snapshots = snapshots;
      this.metadataVersions = metadataVersions;
    }

    @Override
    public long headVersion() {
      return headVersion;
    }

    @Override
    public HistoryTier historyTier() {
      return historyTier;
    }

    @Override
    public String tableName() {
      return "catalog.schema.table";
    }

    @Override
    public Optional<DeltaIdentitySnapshot> snapshot(long version) {
      return Optional.ofNullable(snapshots.get(version));
    }

    @Override
    public boolean sourceVersionAnchored() {
      return historyTier != HistoryTier.NONE;
    }

    @Override
    public MetadataHistory metadataVersions(long firstVersion, long lastVersion) {
      return MetadataHistory.complete(
          metadataVersions.stream()
              .filter(version -> version >= firstVersion && version <= lastVersion)
              .toList());
    }

    @Override
    public UnorderedHistory unorderedHistory() {
      List<Boolean> mapped =
          metadataVersions.stream()
              .map(snapshots::get)
              .filter(java.util.Objects::nonNull)
              .map(snapshot -> snapshot.mode() == IdentityMode.NATIVE_FIELD_ID)
              .toList();
      return new UnorderedHistory(
          mapped.stream()
              .map(
                  enabled ->
                      new HistoricalMetadata(
                          "", Map.of(ColumnMappingMode.PROPERTY, enabled ? "name" : "none")))
              .toList(),
          mapped.stream()
              .map(
                  enabled ->
                      enabled
                          ? new HistoricalProtocol(
                              3,
                              7,
                              java.util.Set.of("columnMapping"),
                              java.util.Set.of("columnMapping"))
                          : new HistoricalProtocol(1, 2, java.util.Set.of(), java.util.Set.of()))
              .toList());
    }
  }
}
