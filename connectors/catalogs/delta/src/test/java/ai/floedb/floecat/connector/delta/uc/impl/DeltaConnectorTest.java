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

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import io.delta.kernel.Operation;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.CheckpointAlreadyExistsException;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.types.StructType;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class DeltaConnectorTest {

  @Test
  void enumerateSnapshotsWithStatsHonorsExplicitTargetVersions() {
    Snapshot latest = snapshot(7L, 7000L);
    Snapshot v3 = snapshot(3L, 3000L);
    Snapshot v5 = snapshot(5L, 5000L);
    Table table = new StubTable(latest, Map.of(3L, v3, 5L, v5));

    TestDeltaConnector connector = new TestDeltaConnector(table);

    List<FloecatConnector.SnapshotBundle> bundles =
        connector.enumerateSnapshotsWithStats(
            "ns",
            "tbl",
            ResourceId.getDefaultInstance(),
            Set.of(),
            new FloecatConnector.SnapshotEnumerationOptions(false, true, Set.of(), Set.of(3L, 5L)));

    List<Long> snapshotIds =
        bundles.stream()
            .map(FloecatConnector.SnapshotBundle::snapshotId)
            .collect(Collectors.toList());

    assertEquals(List.of(3L, 5L), snapshotIds);
  }

  @Test
  void enumerateSnapshotsWithStatsReturnsAllUnknownVersionsForIncrementalRuns() {
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
        connector.enumerateSnapshotsWithStats(
            "ns",
            "tbl",
            ResourceId.getDefaultInstance(),
            Set.of(),
            new FloecatConnector.SnapshotEnumerationOptions(
                false, false, Set.of(1L, 4L), Set.of()));

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

  private static Snapshot snapshot(long version, long timestampMs) {
    return new Snapshot() {
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
        return new StructType();
      }

      @Override
      public Optional<String> getDomainMetadata(String domain) {
        return Optional.empty();
      }

      @Override
      public ScanBuilder getScanBuilder() {
        throw new UnsupportedOperationException();
      }
    };
  }

  private static final class TestDeltaConnector extends DeltaConnector {
    private final Table table;

    TestDeltaConnector(Table table) {
      super("delta-test", null, path -> null, false, 0.0d, 0L);
      this.table = table;
    }

    @Override
    protected String storageLocation(String namespaceFq, String tableName) {
      return "ignored";
    }

    @Override
    protected Table loadTable(String tableRoot) {
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

    private StubTable(Snapshot latest, Map<Long, Snapshot> snapshots) {
      this.latest = latest;
      this.snapshots = snapshots;
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
}
