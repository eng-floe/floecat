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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.Ndv;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.query.rpc.ColumnStatsBatch;
import ai.floedb.floecat.query.rpc.ColumnStatsBundleChunk;
import ai.floedb.floecat.query.rpc.ColumnStatsBundleEnd;
import ai.floedb.floecat.query.rpc.ColumnStatsInfo;
import ai.floedb.floecat.query.rpc.ColumnStatsResult;
import ai.floedb.floecat.query.rpc.FetchColumnStatsRequest;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.query.rpc.SnapshotSet;
import ai.floedb.floecat.query.rpc.StatsStatus;
import ai.floedb.floecat.query.rpc.StatsWarning;
import ai.floedb.floecat.query.rpc.TableColumnStatsRequest;
import ai.floedb.floecat.service.query.catalog.testsupport.UserObjectBundleTestSupport;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import ai.floedb.floecat.service.repo.util.PointerOverlay;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class PlannerStatsBundleServiceTest {

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
  void emitsHeaderThenEnd() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 10, /* maxTables= */ 10, /* maxColumns= */ 10);
    QueryContext ctx = queryContextWithPin("query-1", 100L);
    store.seed(ctx);

    repository.putColumnStats(TABLE, 100L, sampleStats(TABLE, 100L, 1L));

    FetchColumnStatsRequest request = requestFor("query-1", TABLE, List.of(1L));
    List<ColumnStatsBundleChunk> chunks =
        service.stream("corr", ctx, request).collect().asList().await().indefinitely();

    assertTrue(chunks.get(0).hasHeader());
    assertTrue(chunks.get(chunks.size() - 1).hasEnd());
    ColumnStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(1L, end.getRequestedTables());
    assertEquals(1L, end.getRequestedColumns());
    assertEquals(1L, end.getReturnedColumns());
    assertEquals(0L, end.getNotFoundColumns());
  }

  @Test
  void respectsChunkSizeLimit() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 3, /* maxTables= */ 10, /* maxColumns= */ 10);
    QueryContext ctx = queryContextWithPin("query-2", 101L);
    store.seed(ctx);

    for (long columnId = 1; columnId <= 4; columnId++) {
      repository.putColumnStats(TABLE, 101L, sampleStats(TABLE, 101L, columnId));
    }

    FetchColumnStatsRequest request = requestFor("query-2", TABLE, List.of(1L, 2L, 3L, 4L));
    List<ColumnStatsBundleChunk> chunks =
        service.stream("corr", ctx, request).collect().asList().await().indefinitely();

    List<ColumnStatsBundleChunk> batches = new ArrayList<>();
    for (ColumnStatsBundleChunk chunk : chunks) {
      if (chunk.hasBatch()) {
        batches.add(chunk);
      }
    }

    assertEquals(2, batches.size());
    assertEquals(3, batches.get(0).getBatch().getColumnsCount());
    assertEquals(1, batches.get(1).getBatch().getColumnsCount());

    ColumnStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(1L, end.getRequestedTables());
    assertEquals(4L, end.getRequestedColumns());
    assertEquals(4L, end.getReturnedColumns());
    assertEquals(0L, end.getNotFoundColumns());
  }

  @Test
  void missingSnapshotPinYieldsError() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 10, /* maxColumns= */ 10);
    QueryContext ctx = queryContextWithoutPin("query-3");
    store.seed(ctx);

    FetchColumnStatsRequest request = requestFor("query-3", TABLE, List.of(1L));
    List<ColumnStatsBundleChunk> chunks =
        service.stream("corr", ctx, request).collect().asList().await().indefinitely();

    List<ColumnStatsResult> results = flatten(chunks);
    assertEquals(1, results.size());
    assertEquals(
        results.get(0).getStatus(), ai.floedb.floecat.query.rpc.StatsStatus.STATS_STATUS_ERROR);
    assertEquals("planner_stats.pin.missing", results.get(0).getFailure().getCode());

    ColumnStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(1L, end.getErrorColumns());
    assertEquals(0L, end.getNotFoundColumns());
  }

  @Test
  void missingColumnStatsMarkedNotFound() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 10, /* maxColumns= */ 10);
    QueryContext ctx = queryContextWithPin("query-4", 102L);
    store.seed(ctx);

    repository.putColumnStats(TABLE, 102L, sampleStats(TABLE, 102L, 1L));

    FetchColumnStatsRequest request = requestFor("query-4", TABLE, List.of(1L, 2L));
    List<ColumnStatsBundleChunk> chunks =
        service.stream("corr", ctx, request).collect().asList().await().indefinitely();

    List<ColumnStatsResult> results = flatten(chunks);
    assertEquals(2, results.size());
    assertEquals(
        1,
        results.stream()
            .filter(
                r ->
                    r.getStatus()
                        .equals(ai.floedb.floecat.query.rpc.StatsStatus.STATS_STATUS_FOUND))
            .count());
    ColumnStatsResult missing =
        results.stream().filter(r -> r.getColumnId() == 2).findFirst().orElseThrow();
    assertEquals(
        ai.floedb.floecat.query.rpc.StatsStatus.STATS_STATUS_NOT_FOUND, missing.getStatus());
    assertEquals("planner_stats.column_stats.missing", missing.getFailure().getCode());

    ColumnStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(1L, end.getReturnedColumns());
    assertEquals(1L, end.getNotFoundColumns());
  }

  @Test
  void endCountersReflectDedupedColumns() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 10, /* maxColumns= */ 10);
    QueryContext ctx = queryContextWithPin("query-5", 103L);
    store.seed(ctx);

    repository.putColumnStats(TABLE, 103L, sampleStats(TABLE, 103L, 1L));

    FetchColumnStatsRequest request = requestFor("query-5", TABLE, List.of(1L, 1L, 2L));
    List<ColumnStatsBundleChunk> chunks =
        service.stream("corr", ctx, request).collect().asList().await().indefinitely();

    List<ColumnStatsResult> results = flatten(chunks);
    assertEquals(2, results.size());

    ColumnStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(1L, end.getRequestedTables());
    assertEquals(2L, end.getRequestedColumns());
    assertEquals(1L, end.getReturnedColumns());
    assertEquals(1L, end.getNotFoundColumns());
  }

  @Test
  void smartScanBranchReturnsAllColumns() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository =
        new SmartScanOnlyStatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 10, /* maxTables= */ 10, /* maxColumns= */ 10);
    QueryContext ctx = queryContextWithPin("query-smart", 200L);
    store.seed(ctx);

    for (long columnId = 1; columnId <= 3; columnId++) {
      repository.putColumnStats(TABLE, 200L, sampleStats(TABLE, 200L, columnId));
    }

    FetchColumnStatsRequest request = requestFor(ctx.getQueryId(), TABLE, List.of(1L, 2L, 3L));
    List<ColumnStatsBundleChunk> chunks =
        service.stream("corr", ctx, request).collect().asList().await().indefinitely();
    List<ColumnStatsResult> results = flatten(chunks);

    assertEquals(3, results.size());
    assertEquals(
        3,
        results.stream().filter(r -> r.getStatus().equals(StatsStatus.STATS_STATUS_FOUND)).count());
    ColumnStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(3L, end.getRequestedColumns());
    assertEquals(3L, end.getReturnedColumns());
    assertEquals(0L, end.getNotFoundColumns());
    assertEquals(0L, end.getErrorColumns());
  }

  @Test
  void cappedScanFallsBackToPerColumnReads() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    CappingStatsRepository repository =
        new CappingStatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 10, /* maxTables= */ 10, /* maxColumns= */ 10);
    QueryContext ctx = queryContextWithPin("query-cap", 300L);
    store.seed(ctx);

    for (long columnId = 1; columnId <= 4; columnId++) {
      repository.putColumnStats(TABLE, 300L, sampleStats(TABLE, 300L, columnId));
    }

    FetchColumnStatsRequest request = requestFor(ctx.getQueryId(), TABLE, List.of(1L, 2L, 3L, 4L));
    List<ColumnStatsBundleChunk> chunks =
        service.stream("corr", ctx, request).collect().asList().await().indefinitely();
    List<ColumnStatsResult> results = flatten(chunks);

    assertEquals(4, results.size());
    assertEquals(
        4,
        results.stream().filter(r -> r.getStatus().equals(StatsStatus.STATS_STATUS_FOUND)).count());
    ColumnStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(4L, end.getRequestedColumns());
    assertEquals(4L, end.getReturnedColumns());
    assertEquals(0L, end.getNotFoundColumns());
    assertEquals(0L, end.getErrorColumns());
    assertTrue(repository.batchCallCount() > 0);
  }

  @Test
  void cappedScanEmitsWarningsInsideBatch() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    CappingStatsRepository repository =
        new CappingStatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 10, /* maxTables= */ 10, /* maxColumns= */ 10);
    QueryContext ctx = queryContextWithPin("query-cap-warn", 310L);
    store.seed(ctx);

    for (long columnId = 1; columnId <= 3; columnId++) {
      repository.putColumnStats(TABLE, 310L, sampleStats(TABLE, 310L, columnId));
    }

    FetchColumnStatsRequest request = requestFor(ctx.getQueryId(), TABLE, List.of(1L, 2L, 3L));
    List<ColumnStatsBundleChunk> chunks =
        service.stream("corr", ctx, request).collect().asList().await().indefinitely();

    ColumnStatsBatch batch =
        chunks.stream()
            .filter(ColumnStatsBundleChunk::hasBatch)
            .findFirst()
            .orElseThrow()
            .getBatch();
    assertEquals(1, batch.getWarningsCount());
    StatsWarning warning = batch.getWarnings(0);
    assertEquals("planner_stats.column_stats.scan_capped", warning.getCode());
    assertTrue(warning.getDetailsMap().containsKey("pages_scanned"));
  }

  @Test
  void repoFailureEmitsErrorResult() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository =
        new ThrowingStatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 10, /* maxColumns= */ 10);
    QueryContext ctx = queryContextWithPin("query-error", 108L);
    store.seed(ctx);

    FetchColumnStatsRequest request = requestFor(ctx.getQueryId(), TABLE, List.of(1L));
    List<ColumnStatsBundleChunk> chunks =
        service.stream("corr", ctx, request).collect().asList().await().indefinitely();

    List<ColumnStatsResult> results = flatten(chunks);
    assertEquals(1, results.size());
    ColumnStatsResult result = results.get(0);
    assertEquals(StatsStatus.STATS_STATUS_ERROR, result.getStatus());
    assertEquals("planner_stats.column_stats.error", result.getFailure().getCode());

    ColumnStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(0L, end.getReturnedColumns());
    assertEquals(0L, end.getNotFoundColumns());
    assertEquals(1L, end.getErrorColumns());
  }

  @Test
  void optionalColumnStatsFieldsRespectPresence() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 10, /* maxColumns= */ 10);
    ResourceId table = TABLE;
    long snapshotId = 400L;
    ColumnStats stats =
        ColumnStats.newBuilder()
            .setTableId(table)
            .setSnapshotId(snapshotId)
            .setColumnId(42L)
            .setColumnName("optionable")
            .setValueCount(99L)
            .setNullCount(7L)
            .setNanCount(3L)
            .setMin("foo")
            .setMax("bar")
            .setNdv(Ndv.newBuilder().setExact(13L).build())
            .build();
    repository.putColumnStats(table, snapshotId, stats);
    QueryContext ctx = queryContextWithPin("query-optionals", snapshotId);
    store.seed(ctx);
    FetchColumnStatsRequest request = requestFor(ctx.getQueryId(), table, List.of(42L));
    List<ColumnStatsBundleChunk> chunks =
        service.stream("corr", ctx, request).collect().asList().await().indefinitely();
    ColumnStatsInfo info = flatten(chunks).get(0).getStats();
    assertTrue(info.hasNullCount());
    assertEquals(7L, info.getNullCount());
    assertTrue(info.hasNanCount());
    assertEquals(3L, info.getNanCount());
    assertTrue(info.hasMin());
    assertEquals("foo", info.getMin());
    assertTrue(info.hasMax());
    assertEquals("bar", info.getMax());
    assertTrue(info.hasNdv());
    assertTrue(info.getNdv().hasExact());
    assertEquals(13L, info.getNdv().getExact());
  }

  @Test
  void optionalColumnStatsFieldsAreAbsentWhenNotProvided() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 10, /* maxColumns= */ 10);
    ResourceId table = TABLE;
    long snapshotId = 410L;
    ColumnStats stats =
        ColumnStats.newBuilder()
            .setTableId(table)
            .setSnapshotId(snapshotId)
            .setColumnId(43L)
            .setColumnName("bare")
            .setValueCount(12L)
            .build();
    repository.putColumnStats(table, snapshotId, stats);
    QueryContext ctx = queryContextWithPin("query-absent", snapshotId);
    store.seed(ctx);
    FetchColumnStatsRequest request = requestFor(ctx.getQueryId(), table, List.of(43L));
    List<ColumnStatsBundleChunk> chunks =
        service.stream("corr", ctx, request).collect().asList().await().indefinitely();
    ColumnStatsInfo info = flatten(chunks).get(0).getStats();
    assertFalse(info.hasNullCount());
    assertFalse(info.hasNanCount());
    assertFalse(info.hasMin());
    assertFalse(info.hasMax());
    assertFalse(info.hasNdv());
    assertEquals(12L, info.getValueCount());
  }

  @Test
  void enforcesLimitsBeforeProcessing() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 1, /* maxColumns= */ 2);
    QueryContext ctx = queryContextWithPin("query-limits", 110L);
    store.seed(ctx);

    FetchColumnStatsRequest tooManyTables =
        FetchColumnStatsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .addTables(tableRequest(TABLE, List.of(1L)))
            .addTables(tableRequest(TABLE, List.of(2L)))
            .build();
    assertThrows(
        io.grpc.StatusRuntimeException.class,
        () -> service.stream("corr", ctx, tooManyTables).collect().asList().await().indefinitely());

    FetchColumnStatsRequest tooManyColumns =
        FetchColumnStatsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .addTables(tableRequest(TABLE, List.of(1L, 2L, 3L)))
            .build();
    assertThrows(
        io.grpc.StatusRuntimeException.class,
        () ->
            service.stream("corr", ctx, tooManyColumns).collect().asList().await().indefinitely());
  }

  private static PlannerStatsBundleService createService(
      StatsRepository repository,
      UserObjectBundleTestSupport.TestQueryContextStore store,
      int chunkSize,
      int maxTables,
      int maxColumns) {
    StatsProviderFactory factory = new StatsProviderFactory(repository, store);
    return PlannerStatsBundleService.forTesting(
        factory, repository, maxTables, maxColumns, chunkSize);
  }

  private static StatsRepository createRepository() {
    return new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
  }

  private static ColumnStats sampleStats(ResourceId tableId, long snapshotId, long columnId) {
    return ColumnStats.newBuilder()
        .setTableId(tableId)
        .setSnapshotId(snapshotId)
        .setColumnId(columnId)
        .setColumnName("col" + columnId)
        .setValueCount(columnId * 10)
        .setNullCount(columnId)
        .build();
  }

  private static FetchColumnStatsRequest requestFor(
      String queryId, ResourceId tableId, List<Long> columnIds) {
    return FetchColumnStatsRequest.newBuilder()
        .setQueryId(queryId)
        .addTables(tableRequest(tableId, columnIds))
        .build();
  }

  private static TableColumnStatsRequest tableRequest(ResourceId tableId, List<Long> columnIds) {
    return TableColumnStatsRequest.newBuilder()
        .setTableId(tableId)
        .addAllColumnIds(columnIds)
        .build();
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

  private static QueryContext queryContextWithoutPin(String queryId) {
    PrincipalContext principal =
        PrincipalContext.newBuilder()
            .setAccountId(TABLE.getAccountId())
            .setSubject("tester")
            .build();
    return QueryContext.builder()
        .queryId(queryId)
        .principal(principal)
        .createdAtMs(1)
        .expiresAtMs(1_000)
        .state(QueryContext.State.ACTIVE)
        .version(1)
        .queryDefaultCatalogId(CATALOG)
        .build();
  }

  private static List<ColumnStatsResult> flatten(List<ColumnStatsBundleChunk> chunks) {
    List<ColumnStatsResult> results = new ArrayList<>();
    for (ColumnStatsBundleChunk chunk : chunks) {
      if (chunk.hasBatch()) {
        results.addAll(chunk.getBatch().getColumnsList());
      }
    }
    return results;
  }

  private static final class SmartScanOnlyStatsRepository extends StatsRepository {
    private SmartScanOnlyStatsRepository(PointerStore pointerStore, BlobStore blobStore) {
      super(
          pointerStore,
          blobStore,
          PointerOverlay.NOOP,
          /* smartThreshold= */ 2,
          /* scanColumnsPerPage= */ 5,
          /* maxScanPages= */ 5);
    }

    @Override
    public List<ColumnStats> getColumnStatsBatch(
        ResourceId tableId, long snapshotId, List<Long> columnIds) {
      throw new AssertionError("smart scan path should not require per-column batches");
    }
  }

  private static final class CappingStatsRepository extends StatsRepository {
    private final AtomicInteger batchCalls = new AtomicInteger();

    private CappingStatsRepository(PointerStore pointerStore, BlobStore blobStore) {
      super(
          pointerStore,
          blobStore,
          PointerOverlay.NOOP,
          /* smartThreshold= */ 2,
          /* scanColumnsPerPage= */ 1,
          /* maxScanPages= */ 1);
    }

    @Override
    public List<ColumnStats> getColumnStatsBatch(
        ResourceId tableId, long snapshotId, List<Long> columnIds) {
      batchCalls.incrementAndGet();
      return super.getColumnStatsBatch(tableId, snapshotId, columnIds);
    }

    int batchCallCount() {
      return batchCalls.get();
    }
  }

  private static final class ThrowingStatsRepository extends StatsRepository {
    private ThrowingStatsRepository(
        InMemoryPointerStore pointerStore, InMemoryBlobStore blobStore) {
      super(pointerStore, blobStore);
    }

    @Override
    public List<ColumnStats> getColumnStatsBatch(
        ResourceId tableId, long snapshotId, List<Long> columnIds) {
      throw new RuntimeException("boom");
    }

    @Override
    public ColumnStatsBatchResult getColumnStatsBatchSmart(
        ResourceId tableId, long snapshotId, List<Long> columnIds) {
      throw new RuntimeException("boom");
    }
  }
}
