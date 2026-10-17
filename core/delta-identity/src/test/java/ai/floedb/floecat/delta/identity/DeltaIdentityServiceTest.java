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
import ai.floedb.floecat.schema.identity.HistoryTier;
import io.delta.kernel.Operation;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.statistics.SnapshotStatistics;
import io.delta.kernel.transaction.UpdateTableTransactionBuilder;
import io.delta.kernel.types.StructType;
import java.io.IOException;
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
    Snapshot disabled = snapshot(20L, UNMAPPED_A, Map.of());
    Snapshot current = snapshot(30L, mappedSchema(field("a", "long", 1)), mappedProperties(5));
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

  private static Snapshot snapshot(
      long version, String schemaJson, Map<String, String> properties) {
    boolean mapped = ColumnMappingMode.fromTableProperties(properties).isEnabled();
    Protocol protocol =
        mapped
            ? new Protocol(
                3, 7, java.util.Set.of("columnMapping"), java.util.Set.of("columnMapping"))
            : new Protocol(1, 2, java.util.Set.of(), java.util.Set.of());
    return new TestSnapshot(
        version, KernelAdapter.deserializeSchema(schemaJson), properties, protocol);
  }

  private static PersistedIdentityLookup lookup(Optional<ColumnIdentityMap> tableMap) {
    return new PersistedIdentityLookup() {
      @Override
      public Optional<ColumnIdentityMap> exact(long version) {
        return Optional.empty();
      }

      @Override
      public Optional<Map.Entry<Long, ColumnIdentityMap>> floor(long version) {
        return Optional.empty();
      }

      @Override
      public Optional<Map.Entry<Long, ColumnIdentityMap>> ceiling(long version) {
        return Optional.empty();
      }

      @Override
      public Optional<ColumnIdentityMap> tableMap() {
        return tableMap;
      }

      @Override
      public long highWaterMark() {
        return tableMap.map(ColumnIdentityMap::getHighWaterMark).orElse(0L);
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

  private record TestHistorySource(
      long headVersion,
      HistoryTier historyTier,
      Map<Long, Snapshot> snapshots,
      List<Long> metadataVersions)
      implements DeltaHistorySource {
    @Override
    public String tableName() {
      return "catalog.schema.table";
    }

    @Override
    public Optional<Snapshot> snapshot(long version) {
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
      return new UnorderedHistory(
          metadataVersions.stream()
              .map(snapshots::get)
              .filter(java.util.Objects::nonNull)
              .map(snapshot -> new HistoricalMetadata("", snapshot.getTableProperties()))
              .toList(),
          metadataVersions.stream()
              .map(snapshots::get)
              .filter(java.util.Objects::nonNull)
              .map(snapshot -> ((TestSnapshot) snapshot).protocol())
              .map(
                  protocol ->
                      new HistoricalProtocol(
                          protocol.getMinReaderVersion(),
                          protocol.getMinWriterVersion(),
                          protocol.getReaderFeatures(),
                          protocol.getWriterFeatures()))
              .toList());
    }
  }

  private record TestSnapshot(
      long version, StructType schema, Map<String, String> properties, Protocol protocol)
      implements Snapshot, KernelAdapter.ProtocolSnapshot {
    @Override
    public String getPath() {
      return "s3://bucket/table";
    }

    @Override
    public long getVersion() {
      return version;
    }

    @Override
    public List<String> getPartitionColumnNames() {
      return List.of();
    }

    @Override
    public long getTimestamp(Engine engine) {
      return version;
    }

    @Override
    public StructType getSchema() {
      return schema;
    }

    @Override
    public Optional<String> getDomainMetadata(String domain) {
      return Optional.empty();
    }

    @Override
    public Map<String, String> getTableProperties() {
      return properties;
    }

    @Override
    public SnapshotStatistics getStatistics() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ScanBuilder getScanBuilder() {
      throw new UnsupportedOperationException();
    }

    @Override
    public UpdateTableTransactionBuilder buildUpdateTableTransaction(
        String engineInfo, Operation operation) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Snapshot publish(Engine engine) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void writeChecksum(Engine engine, ChecksumWriteMode checksumWriteMode) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void writeCheckpoint(Engine engine) throws IOException {
      throw new UnsupportedOperationException();
    }
  }
}
