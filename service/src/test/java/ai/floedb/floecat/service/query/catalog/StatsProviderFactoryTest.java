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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Ndv;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.catalog.rpc.UpstreamStamp;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.query.rpc.SnapshotSet;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.service.query.catalog.testsupport.UserObjectBundleTestSupport;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.statistics.StatsOrchestrator;
import ai.floedb.floecat.service.statistics.engine.StatsEngineRegistry;
import ai.floedb.floecat.stats.identity.TargetStatsRecords;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.testing.TestStatsCaptureEngine;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import com.google.protobuf.Timestamp;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

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
    StatsProviderFactory factory = factory(repository, store);
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
    StatsProviderFactory factory = factory(repository, store);
    long snapshotId = 10L;
    long fetchedAtMs = 10_123L;
    TableValueStats stats =
        TableValueStats.newBuilder()
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
    repository.putTargetStats(TargetStatsRecords.tableRecord(TABLE, snapshotId, stats, null));

    QueryContext ctx = queryContextWithPin(snapshotId);
    store.seed(ctx);
    var provider = factory.forQuery(ctx, "corr");
    var view = provider.tableStats(TABLE).orElseThrow();
    assertEquals(stats.getRowCount(), view.rowCountValue().orElseThrow());
    assertEquals(stats.getTotalSizeBytes(), view.totalSizeBytesValue().orElseThrow());
    assertEquals(snapshotId, view.snapshotId());
    assertEquals(1, repository.tableStatsCalls());

    long newSnapshot = snapshotId + 1;
    TableValueStats otherStats =
        TableValueStats.newBuilder().setRowCount(7).setTotalSizeBytes(9).build();
    repository.putTargetStats(TargetStatsRecords.tableRecord(TABLE, newSnapshot, otherStats, null));

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
    StatsProviderFactory factory = factory(repository, store);
    long snapshotId = 33L;
    TableValueStats stats = TableValueStats.newBuilder().setRowCount(11).build();
    repository.putTargetStats(TargetStatsRecords.tableRecord(TABLE, snapshotId, stats, null));

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
    StatsProviderFactory factory = factory(repository, store);
    long snapshotId = 22L;
    long columnId = 1L;
    Ndv ndv = Ndv.newBuilder().setExact(5L).build();
    ScalarStats stats =
        ScalarStats.newBuilder()
            .setDisplayName("col")
            .setValueCount(77)
            .setNullCount(2)
            .setNanCount(3)
            .setLogicalType("int64")
            .setMin("1")
            .setMax("5")
            .setNdv(ndv)
            .putProperties("column_id", Long.toString(columnId))
            .setUpstream(
                UpstreamStamp.newBuilder()
                    .setFetchedAt(Timestamp.newBuilder().setSeconds(1).build())
                    .build())
            .build();
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(TABLE, snapshotId, columnId, stats, null));

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
    StatsProviderFactory factory = factory(repository, store);

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
    StatsProviderFactory factory = factory(repository, store);
    long snapshotId = 66L;
    TableValueStats stats =
        TableValueStats.newBuilder().setRowCount(12).setTotalSizeBytes(34).build();
    repository.putTargetStats(TargetStatsRecords.tableRecord(TABLE, snapshotId, stats, null));

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

  @Test
  void tableLookupPassesResolvedConnectorTypeToOrchestrator() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    StatsOrchestrator orchestrator = Mockito.mock(StatsOrchestrator.class);
    StatsProviderFactory factory = new StatsProviderFactory(orchestrator, tableRepository, store);

    long snapshotId = 500L;
    QueryContext ctx = queryContextWithPin(snapshotId);
    store.seed(ctx);
    when(tableRepository.getById(TABLE))
        .thenReturn(
            Optional.of(
                Table.newBuilder()
                    .setResourceId(TABLE)
                    .setUpstream(
                        ai.floedb.floecat.catalog.rpc.UpstreamRef.newBuilder()
                            .setFormat(TableFormat.TF_ICEBERG)
                            .build())
                    .build()));
    when(orchestrator.resolve(any()))
        .thenReturn(
            Optional.of(
                TargetStatsRecords.tableRecord(
                    TABLE,
                    snapshotId,
                    TableValueStats.newBuilder().setRowCount(1).setTotalSizeBytes(2).build(),
                    null)));

    var provider = factory.forQuery(ctx, "corr");
    assertTrue(provider.tableStats(TABLE).isPresent());

    ArgumentCaptor<StatsCaptureRequest> requestCaptor =
        ArgumentCaptor.forClass(StatsCaptureRequest.class);
    Mockito.verify(orchestrator).resolve(requestCaptor.capture());
    assertEquals("iceberg", requestCaptor.getValue().connectorType());
  }

  private static QueryContext queryContextWithPin(long snapshotId) {
    return queryContextWithPin("query-" + snapshotId, snapshotId);
  }

  private static StatsProviderFactory factory(
      CountingStatsRepository repository, UserObjectBundleTestSupport.TestQueryContextStore store) {
    StatsEngineRegistry registry =
        new StatsEngineRegistry(
            List.of(TestStatsCaptureEngine.builder("noop").fixed(Optional.empty()).build()));
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(repository, jobStore, tableRepository, registry);
    return new StatsProviderFactory(orchestrator, tableRepository, store);
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
    public Optional<TargetStatsRecord> getTargetStats(
        ResourceId tableId, long snapshotId, StatsTarget target) {
      switch (target.getTargetCase()) {
        case TABLE -> tableStatsCalls++;
        case COLUMN -> columnStatsCalls++;
        case TARGET_NOT_SET, EXPRESSION, FILE -> {
          // no-op
        }
      }
      return super.getTargetStats(tableId, snapshotId, target);
    }

    private int tableStatsCalls() {
      return tableStatsCalls;
    }

    private int columnStatsCalls() {
      return columnStatsCalls;
    }
  }
}
