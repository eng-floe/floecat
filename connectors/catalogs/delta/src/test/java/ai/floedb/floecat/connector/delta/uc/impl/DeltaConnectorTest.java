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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.ConstraintType;
import ai.floedb.floecat.catalog.rpc.SnapshotConstraints;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import io.delta.kernel.Operation;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Snapshot.ChecksumWriteMode;
import io.delta.kernel.Table;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.CheckpointAlreadyExistsException;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.statistics.SnapshotStatistics;
import io.delta.kernel.transaction.UpdateTableTransactionBuilder;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StructType;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class DeltaConnectorTest {
  private static final String TEST_SCHEMA_JSON =
      """
      {
        "type": "struct",
        "fields": [
          {"name": "id", "type": "long", "nullable": false, "metadata": {}}
        ]
      }
      """;

  @Test
  void enumerateSnapshotsHonorsExplicitTargetVersions() {
    Snapshot latest = snapshot(7L, 7000L);
    Snapshot v3 = snapshot(3L, 3000L);
    Snapshot v5 = snapshot(5L, 5000L);
    Table table = new StubTable(latest, Map.of(3L, v3, 5L, v5));

    TestDeltaConnector connector = new TestDeltaConnector(table);

    List<FloecatConnector.SnapshotBundle> bundles =
        connector.enumerateSnapshots(
            "ns",
            "tbl",
            ResourceId.getDefaultInstance(),
            FloecatConnector.SnapshotEnumerationOptions.fullExplicit(true, Set.of(3L, 5L)));

    List<Long> snapshotIds =
        bundles.stream()
            .map(FloecatConnector.SnapshotBundle::snapshotId)
            .collect(Collectors.toList());

    assertEquals(List.of(3L, 5L), snapshotIds);
  }

  @Test
  void enumerateSnapshotsReturnsAllUnknownVersionsForIncrementalRuns() {
    Snapshot latest = snapshot(5L, 5000L);
    Table table =
        new StubTable(
            latest,
            Map.of(
                0L, snapshot(0L, 0L),
                1L, snapshot(1L, 1000L),
                2L, snapshot(2L, 2000L),
                3L, snapshot(3L, 3000L),
                4L, snapshot(4L, 4000L),
                5L, latest));

    TestDeltaConnector connector = new TestDeltaConnector(table);

    List<FloecatConnector.SnapshotBundle> bundles =
        connector.enumerateSnapshots(
            "ns",
            "tbl",
            ResourceId.getDefaultInstance(),
            FloecatConnector.SnapshotEnumerationOptions.incremental(Set.of(1L, 4L)));

    List<Long> snapshotIds =
        bundles.stream()
            .map(FloecatConnector.SnapshotBundle::snapshotId)
            .collect(Collectors.toList());
    assertEquals(List.of(0L, 2L, 3L, 5L), snapshotIds);

    List<Long> timestamps =
        bundles.stream()
            .map(FloecatConnector.SnapshotBundle::upstreamCreatedAtMs)
            .collect(Collectors.toList());
    assertEquals(List.of(0L, 2000L, 3000L, 5000L), timestamps);
  }

  @Test
  void enumerateSnapshotsSkipsTruncatedHistoryBeforeEarliestAvailableVersion() {
    Snapshot latest = snapshot(107805L, 107805000L);
    Table table =
        new StubTable(
            latest,
            Map.of(
                107800L, snapshot(107800L, 107800000L),
                107801L, snapshot(107801L, 107801000L),
                107802L, snapshot(107802L, 107802000L),
                107803L, snapshot(107803L, 107803000L),
                107804L, snapshot(107804L, 107804000L),
                107805L, latest),
            Map.of(0L, truncatedHistory(), 1L, truncatedHistory(), 107799L, truncatedHistory()));

    TestDeltaConnector connector = new TestDeltaConnector(table);

    List<FloecatConnector.SnapshotBundle> bundles =
        connector.enumerateSnapshots(
            "ns",
            "tbl",
            ResourceId.getDefaultInstance(),
            FloecatConnector.SnapshotEnumerationOptions.incremental(Set.of()));

    List<Long> snapshotIds =
        bundles.stream()
            .map(FloecatConnector.SnapshotBundle::snapshotId)
            .collect(Collectors.toList());
    assertEquals(List.of(107800L, 107801L, 107802L, 107803L, 107804L, 107805L), snapshotIds);
  }

  @Test
  void enumerateSnapshotsRequiresSnapshotMetadataSchemaJson() {
    Snapshot latest = snapshot(2L, 2000L);
    Table table = new StubTable(latest, Map.of(2L, latest));

    StrictSchemaConnector connector = new StrictSchemaConnector(table);

    assertThrows(
        IllegalStateException.class,
        () ->
            connector.enumerateSnapshots(
                "ns",
                "tbl",
                ResourceId.getDefaultInstance(),
                FloecatConnector.SnapshotEnumerationOptions.fullExplicit(true, Set.of(2L))));
  }

  @Test
  void snapshotConstraintsUsesFallbackTablePropertiesWhenSnapshotPropertiesUnavailable() {
    Snapshot latest = snapshot(7L, 7000L);
    Table table = new StubTable(latest, Map.of(7L, latest));

    TestDeltaConnector connector = new TestDeltaConnector(table);
    connector.setFallbackTableProperties(Map.of("delta.constraints.ck_id_positive", "id > 0"));

    Optional<SnapshotConstraints> constraints =
        connector.snapshotConstraints(
            "ns", "tbl", ResourceId.getDefaultInstance(), snapshotBundle(7L, TEST_SCHEMA_JSON));

    assertTrue(constraints.isPresent());
    assertEquals(2, constraints.get().getConstraintsCount());
    assertEquals(
        List.of(ConstraintType.CT_CHECK, ConstraintType.CT_NOT_NULL),
        constraints.get().getConstraintsList().stream().map(c -> c.getType()).sorted().toList());
    assertTrue(
        constraints.get().getConstraintsList().stream()
            .anyMatch(
                c ->
                    c.getType() == ConstraintType.CT_CHECK
                        && c.getName().equals("ck_id_positive")));
  }

  @Test
  void snapshotConstraintsPrefersSnapshotPropertiesOverFallbackProperties() {
    Snapshot latest = snapshot(8L, 8000L);
    Table table = new StubTable(latest, Map.of(8L, latest));

    TestDeltaConnector connector = new TestDeltaConnector(table);
    connector.setFallbackTableProperties(
        Map.of(
            "delta.constraints.ck_fallback", "id < 100",
            "delta.constraints.ck_snapshot", "id > 0"));

    Optional<SnapshotConstraints> constraints =
        connector.snapshotConstraints(
            "ns", "tbl", ResourceId.getDefaultInstance(), snapshotBundle(8L, TEST_SCHEMA_JSON));

    assertTrue(constraints.isPresent());
    assertEquals(3, constraints.get().getConstraintsCount());
    assertTrue(
        constraints.get().getConstraintsList().stream()
            .map(c -> c.getName())
            .anyMatch(name -> name.startsWith("nn_")));
    assertEquals(
        List.of("ck_fallback", "ck_snapshot"),
        constraints.get().getConstraintsList().stream()
            .map(c -> c.getName())
            .filter(name -> !name.startsWith("nn_"))
            .sorted()
            .toList());
    assertTrue(connector.fallbackCalled.get(), "fallback properties should still be consulted");
  }

  @Test
  void snapshotConstraintsDoesNotReopenSnapshotWhenBundleIsProvided() {
    Snapshot latest = snapshot(9L, 9000L);
    StubTable table = new StubTable(latest, Map.of(9L, latest));

    TestDeltaConnector connector = new TestDeltaConnector(table);
    connector.setFallbackTableProperties(Map.of("delta.constraints.ck_amount", "amount > 0"));

    Optional<SnapshotConstraints> constraints =
        connector.snapshotConstraints(
            "ns", "tbl", ResourceId.getDefaultInstance(), snapshotBundle(9L, TEST_SCHEMA_JSON));

    assertTrue(constraints.isPresent());
    assertEquals(0, table.snapshotAsOfVersionCalls);
  }

  @Test
  void snapshotConstraintsBundleKeepsStableTopLevelColumnIdWithoutSnapshotReload() {
    Snapshot latest = snapshot(10L, 10000L, new StructType().add("id", LongType.LONG, false));
    StubTable table = new StubTable(latest, Map.of(10L, latest));

    TestDeltaConnector connector = new TestDeltaConnector(table);

    Optional<SnapshotConstraints> constraints =
        connector.snapshotConstraints(
            "ns", "tbl", ResourceId.getDefaultInstance(), snapshotBundle(10L, TEST_SCHEMA_JSON));

    assertTrue(constraints.isPresent());
    assertEquals(1, constraints.get().getConstraintsCount());
    assertEquals("id", constraints.get().getConstraints(0).getColumns(0).getColumnName());
    assertTrue(
        constraints.get().getConstraints(0).getColumns(0).getColumnId() != 0L,
        "top-level non-null constraint should retain a stable column id");
    assertEquals(0, table.snapshotAsOfVersionCalls);
  }

  @Test
  void captureSnapshotTargetStatsReturnsEmptyForUnknownSnapshot() {
    Snapshot latest = snapshot(7L, 7000L);
    Table table = new StubTable(latest, Map.of(7L, latest));

    TestDeltaConnector connector = new TestDeltaConnector(table);

    List<ai.floedb.floecat.catalog.rpc.TargetStatsRecord> stats =
        connector.captureSnapshotTargetStats(
            "ns", "tbl", ResourceId.getDefaultInstance(), 999L, Set.of());

    assertTrue(stats.isEmpty(), "unknown snapshot should return empty stats");
  }

  @Test
  void directStatsIncludeColumnsDefaultsToFirstThirtyTwoSchemaColumns() {
    List<String> availableColumns = new java.util.ArrayList<>();
    for (int i = 0; i < 40; i++) {
      availableColumns.add("c" + i);
    }

    Set<String> includeNames =
        FloecatConnector.resolveIncludedColumns(availableColumns, Set.of(), null);

    assertEquals(32, includeNames.size());
    assertEquals(
        Set.copyOf(java.util.stream.IntStream.range(0, 32).mapToObj(i -> "c" + i).toList()),
        includeNames);
  }

  @Test
  void directStatsIncludeColumnsKeepsExplicitColumnSelectorsBeyondThirtyTwo() {
    List<String> availableColumns = new java.util.ArrayList<>();
    for (int i = 0; i < 40; i++) {
      availableColumns.add("c" + i);
    }

    Set<String> includeNames =
        FloecatConnector.resolveIncludedColumns(
            availableColumns, new java.util.LinkedHashSet<>(List.of("c39", " c35 ", "c2")), null);

    assertEquals(Set.of("c39", "c35", "c2"), includeNames);
  }

  @Test
  void describeFromDeltaUsesRealSnapshotSchemaJson() {
    Snapshot latest = snapshot(11L, 11000L, new StructType().add("ignored", LongType.LONG, true));
    Table table = new StubTable(latest, Map.of(11L, latest));
    TestDeltaConnector connector = new TestDeltaConnector(table);
    String realSchemaJson =
        """
        {
          "type":"struct",
          "fields":[
            {
              "name":"payload",
              "type":{"type":"struct","fields":[
                {"name":"id","type":"long","nullable":false,"metadata":{}}
              ]},
              "nullable":true,
              "metadata":{}
            }
          ]
        }
        """;
    connector.setSnapshotSchemaJson(realSchemaJson);

    FloecatConnector.TableDescriptor descriptor =
        connector.describeFromDelta("s3://bucket/table", "ns", "tbl");

    assertEquals(realSchemaJson, descriptor.schemaJson());
  }

  private static Snapshot snapshot(long version, long timestampMs) {
    return snapshot(version, timestampMs, new StructType());
  }

  private static Snapshot snapshot(long version, long timestampMs, StructType schema) {
    return new Snapshot() {
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
        return timestampMs;
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
        return Map.of();
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
      public void writeCheckpoint(Engine engine)
          throws IOException, CheckpointAlreadyExistsException {
        throw new UnsupportedOperationException();
      }
    };
  }

  private static FloecatConnector.SnapshotBundle snapshotBundle(
      long snapshotId, String schemaJson) {
    return new FloecatConnector.SnapshotBundle(
        snapshotId, 0L, 0L, schemaJson, null, 0L, null, Map.of(), 0, null);
  }

  private static final class TestDeltaConnector extends DeltaConnector {
    private final Table table;
    private String snapshotSchemaJson = TEST_SCHEMA_JSON;
    private Map<String, String> fallbackTableProperties = Map.of();
    private final AtomicBoolean fallbackCalled = new AtomicBoolean(false);

    TestDeltaConnector(Table table) {
      super("delta-test", null, path -> null, false, 0.0d, 0L);
      this.table = table;
    }

    @Override
    protected String storageLocation(String namespaceFq, String tableName) {
      return "ignored";
    }

    @Override
    protected Table loadTable(String storageLocation) {
      return table;
    }

    @Override
    protected String snapshotSchemaJson(Snapshot snapshot) {
      return snapshotSchemaJson;
    }

    @Override
    protected Map<String, String> fallbackTablePropertiesForConstraints(
        String namespaceFq, String tableName) {
      fallbackCalled.set(true);
      return fallbackTableProperties;
    }

    @Override
    public List<String> listTables(String namespaceFq) {
      return List.of();
    }

    @Override
    public List<String> listNamespaces() {
      return List.of();
    }

    @Override
    public TableDescriptor describe(String namespaceFq, String tableName) {
      throw new UnsupportedOperationException();
    }

    void setFallbackTableProperties(Map<String, String> fallbackTableProperties) {
      this.fallbackTableProperties = fallbackTableProperties;
    }

    void setSnapshotSchemaJson(String snapshotSchemaJson) {
      this.snapshotSchemaJson = snapshotSchemaJson;
    }
  }

  private static final class StrictSchemaConnector extends DeltaConnector {
    private final Table table;

    StrictSchemaConnector(Table table) {
      super("delta-test-strict", null, path -> null, false, 0.0d, 0L);
      this.table = table;
    }

    @Override
    protected String storageLocation(String namespaceFq, String tableName) {
      return "ignored";
    }

    @Override
    protected Table loadTable(String storageLocation) {
      return table;
    }

    @Override
    public List<String> listTables(String namespaceFq) {
      return List.of();
    }

    @Override
    public List<String> listNamespaces() {
      return List.of();
    }

    @Override
    public TableDescriptor describe(String namespaceFq, String tableName) {
      throw new UnsupportedOperationException();
    }
  }

  private static final class StubTable implements Table {
    private final Snapshot latest;
    private final Map<Long, Snapshot> snapshots;
    private final Map<Long, RuntimeException> failures;
    private int snapshotAsOfVersionCalls;

    private StubTable(Snapshot latest, Map<Long, Snapshot> snapshots) {
      this(latest, snapshots, Map.of());
    }

    private StubTable(
        Snapshot latest, Map<Long, Snapshot> snapshots, Map<Long, RuntimeException> failures) {
      this.latest = latest;
      this.snapshots = snapshots;
      this.failures = failures;
    }

    @Override
    public String getPath(Engine engine) {
      return "ignored";
    }

    @Override
    public Snapshot getLatestSnapshot(Engine engine) throws TableNotFoundException {
      return latest;
    }

    @Override
    public Snapshot getSnapshotAsOfVersion(Engine engine, long version)
        throws TableNotFoundException {
      snapshotAsOfVersionCalls++;
      RuntimeException failure = failures.get(version);
      if (failure != null) {
        throw failure;
      }
      return snapshots.get(version);
    }

    @Override
    public Snapshot getSnapshotAsOfTimestamp(Engine engine, long timestamp)
        throws TableNotFoundException {
      throw new UnsupportedOperationException();
    }

    @Override
    public TransactionBuilder createTransactionBuilder(
        Engine engine, String engineInfo, Operation operation) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void checkpoint(Engine engine, long version)
        throws TableNotFoundException, CheckpointAlreadyExistsException, IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void checksum(Engine engine, long version) throws TableNotFoundException, IOException {
      throw new UnsupportedOperationException();
    }
  }

  private static KernelException truncatedHistory() {
    return new KernelException(
        "s3://bucket/table: Cannot load table version 0 as the transaction log has been truncated"
            + " due to manual deletion or the log/checkpoint retention policy. The earliest"
            + " available version is 107800.");
  }
}
