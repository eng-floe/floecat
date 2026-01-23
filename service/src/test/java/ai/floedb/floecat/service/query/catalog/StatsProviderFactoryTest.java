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

package ai.floedb.floecat.service.query.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.Ndv;
import ai.floedb.floecat.catalog.rpc.TableStats;
import ai.floedb.floecat.catalog.rpc.UpstreamStamp;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.query.rpc.SnapshotSet;
import ai.floedb.floecat.service.query.catalog.testsupport.UserObjectBundleTestSupport;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import ai.floedb.floecat.storage.InMemoryBlobStore;
import ai.floedb.floecat.storage.InMemoryPointerStore;
import com.google.protobuf.Timestamp;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class StatsProviderFactoryTest {

  private static final ResourceId CATALOG =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("catalog")
          .setKind(ResourceKind.RK_CATALOG)
          .build();

  private static final ResourceId TABLE =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("users")
          .setKind(ResourceKind.RK_TABLE)
          .build();

  @Test
  void tableStatsAreOptionalWhenMissing() {
    CountingStatsRepository repository = new CountingStatsRepository();
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsProviderFactory factory = new StatsProviderFactory(repository, store);
    QueryContext ctx = queryContextWithoutPin();
    store.seed(ctx);
    var provider = factory.forQuery(ctx, "corr");

    assertTrue(provider.tableStats(TABLE).isEmpty());
    assertEquals(0, repository.tableStatsCalls());
  }

  @Test
  void cachesTableStatsPerSnapshot() {
    CountingStatsRepository repository = new CountingStatsRepository();
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsProviderFactory factory = new StatsProviderFactory(repository, store);
    long snapshotId = 10L;
    long fetchedAtMs = 10_123L;
    TableStats stats =
        TableStats.newBuilder()
            .setTableId(TABLE)
            .setSnapshotId(snapshotId)
            .setRowCount(5)
            .setTotalSizeBytes(7_777)
            .setUpstream(
                UpstreamStamp.newBuilder()
                    .setFetchedAt(
                        Timestamp.newBuilder()
                            .setSeconds(fetchedAtMs / 1000)
                            .setNanos((int) ((fetchedAtMs % 1000) * 1_000_000))
                            .build())
                    .build())
            .build();
    repository.putTableStats(TABLE, snapshotId, stats);

    QueryContext ctx = queryContextWithPin(snapshotId);
    store.seed(ctx);
    var provider = factory.forQuery(ctx, "corr");
    var view = provider.tableStats(TABLE).orElseThrow();
    assertEquals(stats.getRowCount(), view.rowCountValue().orElseThrow());
    assertEquals(stats.getTotalSizeBytes(), view.totalSizeBytesValue().orElseThrow());
    assertEquals(stats.getSnapshotId(), view.snapshotId());
    assertEquals(1, repository.tableStatsCalls());

    long newSnapshot = snapshotId + 1;
    TableStats otherStats =
        TableStats.newBuilder()
            .setTableId(TABLE)
            .setSnapshotId(newSnapshot)
            .setRowCount(7)
            .setTotalSizeBytes(9)
            .build();
    repository.putTableStats(TABLE, newSnapshot, otherStats);

    QueryContext otherCtx = queryContextWithPin(newSnapshot);
    store.seed(otherCtx);
    var freshProvider = factory.forQuery(otherCtx, "corr");
    var freshView = freshProvider.tableStats(TABLE).orElseThrow();
    assertEquals(otherStats.getRowCount(), freshView.rowCountValue().orElseThrow());
    assertEquals(otherStats.getTotalSizeBytes(), freshView.totalSizeBytesValue().orElseThrow());
    assertEquals(2, repository.tableStatsCalls());
  }

  @Test
  void totalSizeBytesIsReportedEvenWhenZero() {
    CountingStatsRepository repository = new CountingStatsRepository();
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsProviderFactory factory = new StatsProviderFactory(repository, store);
    long snapshotId = 33L;
    TableStats stats =
        TableStats.newBuilder().setTableId(TABLE).setSnapshotId(snapshotId).setRowCount(11).build();
    repository.putTableStats(TABLE, snapshotId, stats);

    QueryContext ctx = queryContextWithPin(snapshotId);
    store.seed(ctx);
    var provider = factory.forQuery(ctx, "corr");
    var view = provider.tableStats(TABLE).orElseThrow();
    assertEquals(stats.getRowCount(), view.rowCountValue().orElseThrow());
    assertEquals(stats.getTotalSizeBytes(), view.totalSizeBytesValue().orElseThrow());
  }

  @Test
  void columnStatsBestEffortWithoutPin() {
    CountingStatsRepository repository = new CountingStatsRepository();
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsProviderFactory factory = new StatsProviderFactory(repository, store);
    long snapshotId = 22L;
    long columnId = 1L;
    Ndv ndv = Ndv.newBuilder().setExact(5L).build();
    ColumnStats stats =
        ColumnStats.newBuilder()
            .setTableId(TABLE)
            .setSnapshotId(snapshotId)
            .setColumnId(columnId)
            .setColumnName("col")
            .setValueCount(77)
            .setNullCount(2)
            .setNanCount(3)
            .setLogicalType("int64")
            .setMin("1")
            .setMax("5")
            .setNdv(ndv)
            .setUpstream(
                UpstreamStamp.newBuilder()
                    .setFetchedAt(Timestamp.newBuilder().setSeconds(1).build())
                    .build())
            .build();
    repository.putColumnStats(TABLE, snapshotId, stats);

    QueryContext ctx = queryContextWithPin(snapshotId);
    store.seed(ctx);
    var provider = factory.forQuery(ctx, "corr");
    var view = provider.columnStats(TABLE, columnId).orElseThrow();
    assertEquals(TABLE, view.tableId());
    assertEquals(columnId, view.columnId());
    assertEquals("col", view.columnName());
    assertEquals(77, view.valueCount());
    assertEquals(2, view.nullCountValue().orElseThrow());
    assertEquals(3, view.nanCountValue().orElseThrow());
    assertEquals("int64", view.logicalType());
    assertEquals("1", view.minValue().orElseThrow());
    assertEquals("5", view.maxValue().orElseThrow());
    assertEquals(ndv, view.ndv().get());
    provider.columnStats(TABLE, columnId);
    assertEquals(2, repository.columnStatsCalls());

    var missingProvider = factory.forQuery(queryContextWithoutPin(), "corr");
    assertTrue(missingProvider.columnStats(TABLE, columnId).isEmpty());
  }

  @Test
  void pinnedSnapshotIdReflectsStoredPin() {
    CountingStatsRepository repository = new CountingStatsRepository();
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsProviderFactory factory = new StatsProviderFactory(repository, store);

    long snapshotId = 99L;
    QueryContext pinned = queryContextWithPin("query-pin", snapshotId);
    store.seed(pinned);
    var provider = factory.forQuery(pinned, "corr");
    assertEquals(snapshotId, provider.pinnedSnapshotId(TABLE).orElseThrow());

    QueryContext noPin = queryContextWithoutPin();
    store.seed(noPin);
    var noPinProvider = factory.forQuery(noPin, "corr");
    assertTrue(noPinProvider.pinnedSnapshotId(TABLE).isEmpty());
  }

  @Test
  void statsAppearWhenPinAddedDuringBundle() {
    CountingStatsRepository repository = new CountingStatsRepository();
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsProviderFactory factory = new StatsProviderFactory(repository, store);
    long snapshotId = 66L;
    TableStats stats =
        TableStats.newBuilder()
            .setTableId(TABLE)
            .setSnapshotId(snapshotId)
            .setRowCount(12)
            .setTotalSizeBytes(34)
            .build();
    repository.putTableStats(TABLE, snapshotId, stats);

    QueryContext ctx = queryContextWithoutPin();
    store.seed(ctx);
    var provider = factory.forQuery(ctx, "corr");

    assertTrue(provider.tableStats(TABLE).isEmpty());
    assertEquals(0, repository.tableStatsCalls());

    QueryContext pinned = queryContextWithPin(ctx.getQueryId(), snapshotId);
    store.replace(pinned);
    var view = provider.tableStats(TABLE).orElseThrow();
    assertEquals(stats.getRowCount(), view.rowCountValue().orElseThrow());
    assertEquals(stats.getTotalSizeBytes(), view.totalSizeBytesValue().orElseThrow());
    assertEquals(snapshotId, view.snapshotId());
    assertEquals(1, repository.tableStatsCalls());
  }

  private static QueryContext queryContextWithPin(long snapshotId) {
    return queryContextWithPin("query-" + snapshotId, snapshotId);
  }

  private static QueryContext queryContextWithPin(String queryId, long snapshotId) {
    SnapshotPin pin = SnapshotPin.newBuilder().setTableId(TABLE).setSnapshotId(snapshotId).build();
    SnapshotSet set = SnapshotSet.newBuilder().addPins(pin).build();
    PrincipalContext principal =
        PrincipalContext.newBuilder()
            .setAccountId(TABLE.getAccountId())
            .setSubject("tester")
            .build();

    return QueryContext.builder()
        .queryId(queryId)
        .principal(principal)
        .snapshotSet(set.toByteArray())
        .createdAtMs(1)
        .expiresAtMs(1_000)
        .state(QueryContext.State.ACTIVE)
        .version(1)
        .queryDefaultCatalogId(CATALOG)
        .build();
  }

  private static QueryContext queryContextWithoutPin() {
    PrincipalContext principal =
        PrincipalContext.newBuilder()
            .setAccountId(TABLE.getAccountId())
            .setSubject("tester")
            .build();

    return QueryContext.builder()
        .queryId("query-no-pin")
        .principal(principal)
        .createdAtMs(1)
        .expiresAtMs(1_000)
        .state(QueryContext.State.ACTIVE)
        .version(1)
        .queryDefaultCatalogId(CATALOG)
        .build();
  }

  private static final class CountingStatsRepository extends StatsRepository {

    private int tableStatsCalls;
    private int columnStatsCalls;

    private CountingStatsRepository() {
      super(new InMemoryPointerStore(), new InMemoryBlobStore());
    }

    @Override
    public Optional<ColumnStats> getColumnStats(
        ResourceId tableId, long snapshotId, long columnId) {
      columnStatsCalls++;
      return super.getColumnStats(tableId, snapshotId, columnId);
    }

    @Override
    public Optional<StatsRepository.TableStatsView> getTableStatsView(
        ResourceId tableId, long snapshotId) {
      tableStatsCalls++;
      return super.getTableStatsView(tableId, snapshotId);
    }

    private int tableStatsCalls() {
      return tableStatsCalls;
    }

    private int columnStatsCalls() {
      return columnStatsCalls;
    }
  }
}
