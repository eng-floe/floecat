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
import ai.floedb.floecat.query.rpc.BundleResultStatus;
import ai.floedb.floecat.query.rpc.ColumnStatsBatch;
import ai.floedb.floecat.query.rpc.ColumnStatsBundleChunk;
import ai.floedb.floecat.query.rpc.ColumnStatsBundleEnd;
import ai.floedb.floecat.query.rpc.ColumnStatsInfo;
import ai.floedb.floecat.query.rpc.ColumnStatsResult;
import ai.floedb.floecat.query.rpc.FetchColumnStatsRequest;
import ai.floedb.floecat.query.rpc.StatsWarning;
import ai.floedb.floecat.service.query.catalog.testsupport.UserObjectBundleTestSupport;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class PlannerStatsBundleServiceTest extends PlannerStatsBundleServiceTestSupport {

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
    assertFalse(request.getIncludeConstraints());
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
    assertEquals(BundleResultStatus.BUNDLE_RESULT_STATUS_ERROR, results.get(0).getStatus());
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
            .filter(r -> r.getStatus().equals(BundleResultStatus.BUNDLE_RESULT_STATUS_FOUND))
            .count());
    ColumnStatsResult missing =
        results.stream().filter(r -> r.getColumnId() == 2).findFirst().orElseThrow();
    assertEquals(BundleResultStatus.BUNDLE_RESULT_STATUS_NOT_FOUND, missing.getStatus());
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
        results.stream()
            .filter(r -> r.getStatus().equals(BundleResultStatus.BUNDLE_RESULT_STATUS_FOUND))
            .count());
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
        results.stream()
            .filter(r -> r.getStatus().equals(BundleResultStatus.BUNDLE_RESULT_STATUS_FOUND))
            .count());
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
    assertEquals(BundleResultStatus.BUNDLE_RESULT_STATUS_ERROR, result.getStatus());
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
    long snapshotId = 400L;
    ColumnStats stats =
        ColumnStats.newBuilder()
            .setTableId(TABLE)
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
    repository.putColumnStats(TABLE, snapshotId, stats);
    QueryContext ctx = queryContextWithPin("query-optionals", snapshotId);
    store.seed(ctx);
    FetchColumnStatsRequest request = requestFor(ctx.getQueryId(), TABLE, List.of(42L));
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
    long snapshotId = 410L;
    ColumnStats stats =
        ColumnStats.newBuilder()
            .setTableId(TABLE)
            .setSnapshotId(snapshotId)
            .setColumnId(43L)
            .setColumnName("bare")
            .setValueCount(12L)
            .build();
    repository.putColumnStats(TABLE, snapshotId, stats);
    QueryContext ctx = queryContextWithPin("query-absent", snapshotId);
    store.seed(ctx);
    FetchColumnStatsRequest request = requestFor(ctx.getQueryId(), TABLE, List.of(43L));
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

  /**
   * A ColumnStats row that exists in storage but has no metrics (no value_count, no null_count, no
   * min/max) must still be returned as FOUND — not NOT_FOUND. This is exactly the shape that
   * GenericStatsEngine.compute() emits for columns whose format (e.g. Iceberg) doesn't provide
   * per-column metrics; the planner must receive a FOUND result and apply its own default estimates
   * rather than treating the column as if stats were never ingested.
   */
  @Test
  void columnWithNoMetricsIsFoundWithEmptyStats() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 10, /* maxColumns= */ 10);
    long snapshotId = 420L;

    // Simulate a row produced by compute() for a column with no Iceberg metrics:
    // only column_id and column_name are set; all stat fields are absent.
    ColumnStats emptyStats =
        ColumnStats.newBuilder()
            .setTableId(TABLE)
            .setSnapshotId(snapshotId)
            .setColumnId(99L)
            .setColumnName("ts_col")
            .build();
    repository.putColumnStats(TABLE, snapshotId, emptyStats);

    QueryContext ctx = queryContextWithPin("query-empty-metrics", snapshotId);
    store.seed(ctx);
    FetchColumnStatsRequest request = requestFor(ctx.getQueryId(), TABLE, List.of(99L));
    List<ColumnStatsBundleChunk> chunks =
        service.stream("corr", ctx, request).collect().asList().await().indefinitely();

    List<ColumnStatsResult> results = flatten(chunks);
    assertEquals(1, results.size());
    // Row exists → must be FOUND, not NOT_FOUND
    assertEquals(BundleResultStatus.BUNDLE_RESULT_STATUS_FOUND, results.get(0).getStatus());
    assertEquals(99L, results.get(0).getColumnId());

    ColumnStatsInfo info = results.get(0).getStats();
    // No metrics were stored — all optional fields must be absent
    assertFalse(info.hasNullCount(), "null_count must not be set when no metrics");
    assertFalse(info.hasNanCount(), "nan_count must not be set when no metrics");
    assertFalse(info.hasMin(), "min must not be set when no metrics");
    assertFalse(info.hasMax(), "max must not be set when no metrics");
    assertFalse(info.hasNdv(), "ndv must not be set when no metrics");

    ColumnStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(1L, end.getReturnedColumns());
    assertEquals(0L, end.getNotFoundColumns());
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
}
