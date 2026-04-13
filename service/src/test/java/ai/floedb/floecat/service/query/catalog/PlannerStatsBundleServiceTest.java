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

import ai.floedb.floecat.catalog.rpc.ColumnStatsTarget;
import ai.floedb.floecat.catalog.rpc.EngineExpressionStatsTarget;
import ai.floedb.floecat.catalog.rpc.FileStatsTarget;
import ai.floedb.floecat.catalog.rpc.Ndv;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TableStatsTarget;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.query.rpc.BundleResultStatus;
import ai.floedb.floecat.query.rpc.FetchTargetStatsRequest;
import ai.floedb.floecat.query.rpc.TableTargetStatsRequest;
import ai.floedb.floecat.query.rpc.TargetStatsBatch;
import ai.floedb.floecat.query.rpc.TargetStatsBundleChunk;
import ai.floedb.floecat.query.rpc.TargetStatsBundleEnd;
import ai.floedb.floecat.query.rpc.TargetStatsResult;
import ai.floedb.floecat.service.query.catalog.testsupport.UserObjectBundleTestSupport;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import ai.floedb.floecat.stats.identity.TargetStatsRecords;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import com.google.protobuf.ByteString;
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
            repository, store, /* chunkSize= */ 10, /* maxTables= */ 10, /* maxTargets= */ 10);
    QueryContext ctx = queryContextWithPin("query-1", 100L);
    store.seed(ctx);

    repository.putTargetStats(
        TargetStatsRecords.columnRecord(TABLE, 100L, 1L, sampleStats(TABLE, 100L, 1L), null));

    FetchTargetStatsRequest request = requestFor("query-1", TABLE, List.of(1L));
    assertFalse(request.getIncludeConstraints());
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    assertTrue(chunks.get(0).hasHeader());
    assertTrue(chunks.get(chunks.size() - 1).hasEnd());
    TargetStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(1L, end.getRequestedTables());
    assertEquals(1L, end.getRequestedTargets());
    assertEquals(1L, end.getReturnedTargets());
    assertEquals(0L, end.getNotFoundTargets());
  }

  @Test
  void respectsChunkSizeLimit() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 3, /* maxTables= */ 10, /* maxTargets= */ 10);
    QueryContext ctx = queryContextWithPin("query-2", 101L);
    store.seed(ctx);

    for (long columnId = 1; columnId <= 4; columnId++) {
      repository.putTargetStats(
          TargetStatsRecords.columnRecord(
              TABLE, 101L, columnId, sampleStats(TABLE, 101L, columnId), null));
    }

    FetchTargetStatsRequest request = requestFor("query-2", TABLE, List.of(1L, 2L, 3L, 4L));
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    List<TargetStatsBundleChunk> batches = new ArrayList<>();
    for (TargetStatsBundleChunk chunk : chunks) {
      if (chunk.hasBatch()) {
        batches.add(chunk);
      }
    }

    assertEquals(2, batches.size());
    assertEquals(3, batches.get(0).getBatch().getTargetsCount());
    assertEquals(1, batches.get(1).getBatch().getTargetsCount());

    TargetStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(1L, end.getRequestedTables());
    assertEquals(4L, end.getRequestedTargets());
    assertEquals(4L, end.getReturnedTargets());
    assertEquals(0L, end.getNotFoundTargets());
  }

  @Test
  void missingSnapshotPinYieldsError() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 10, /* maxTargets= */ 10);
    QueryContext ctx = queryContextWithoutPin("query-3");
    store.seed(ctx);

    FetchTargetStatsRequest request = requestFor("query-3", TABLE, List.of(1L));
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    List<TargetStatsResult> results = flatten(chunks);
    assertEquals(1, results.size());
    assertEquals(BundleResultStatus.BUNDLE_RESULT_STATUS_ERROR, results.get(0).getStatus());
    assertEquals("planner_stats.pin.missing", results.get(0).getFailure().getCode());

    TargetStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(1L, end.getErrorTargets());
    assertEquals(0L, end.getNotFoundTargets());
  }

  @Test
  void missingColumnStatsMarkedNotFound() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 10, /* maxTargets= */ 10);
    QueryContext ctx = queryContextWithPin("query-4", 102L);
    store.seed(ctx);

    repository.putTargetStats(
        TargetStatsRecords.columnRecord(TABLE, 102L, 1L, sampleStats(TABLE, 102L, 1L), null));

    FetchTargetStatsRequest request = requestFor("query-4", TABLE, List.of(1L, 2L));
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    List<TargetStatsResult> results = flatten(chunks);
    assertEquals(2, results.size());
    assertEquals(
        1,
        results.stream()
            .filter(r -> r.getStatus().equals(BundleResultStatus.BUNDLE_RESULT_STATUS_FOUND))
            .count());
    TargetStatsResult missing =
        results.stream()
            .filter(r -> r.getTarget().getColumn().getColumnId() == 2)
            .findFirst()
            .orElseThrow();
    assertEquals(BundleResultStatus.BUNDLE_RESULT_STATUS_NOT_FOUND, missing.getStatus());
    assertEquals("planner_stats.target_stats.missing", missing.getFailure().getCode());

    TargetStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(1L, end.getReturnedTargets());
    assertEquals(1L, end.getNotFoundTargets());
  }

  @Test
  void mixedTargetsReturnPartialStableResults() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 10, /* maxTables= */ 10, /* maxTargets= */ 20);
    QueryContext ctx = queryContextWithPin("query-mixed", 107L);
    store.seed(ctx);

    repository.putTargetStats(
        TargetStatsRecords.tableRecord(
            TABLE, 107L, TableValueStats.newBuilder().setRowCount(100L).build(), null));
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(TABLE, 107L, 1L, sampleStats(TABLE, 107L, 1L), null));

    FetchTargetStatsRequest request =
        FetchTargetStatsRequest.newBuilder()
            .setQueryId("query-mixed")
            .addTables(
                TableTargetStatsRequest.newBuilder()
                    .setTableId(TABLE)
                    .addTargets(
                        StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()))
                    .addTargets(
                        StatsTarget.newBuilder()
                            .setColumn(ColumnStatsTarget.newBuilder().setColumnId(1L)))
                    .addTargets(
                        StatsTarget.newBuilder()
                            .setFile(
                                FileStatsTarget.newBuilder()
                                    .setFilePath("/warehouse/part-001.parquet")))
                    .addTargets(
                        StatsTarget.newBuilder()
                            .setExpression(
                                EngineExpressionStatsTarget.newBuilder()
                                    .setEngineKind("duckdb")
                                    .setEngineExpressionKey(ByteString.copyFromUtf8("expr-1")))))
            .build();

    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    List<TargetStatsResult> results = flatten(chunks);
    assertEquals(4, results.size());
    assertEquals(
        2,
        results.stream()
            .filter(r -> r.getStatus().equals(BundleResultStatus.BUNDLE_RESULT_STATUS_FOUND))
            .count());
    assertEquals(
        2,
        results.stream()
            .filter(r -> r.getStatus().equals(BundleResultStatus.BUNDLE_RESULT_STATUS_NOT_FOUND))
            .count());

    TargetStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(2L, end.getReturnedTargets());
    assertEquals(2L, end.getNotFoundTargets());
    assertEquals(0L, end.getErrorTargets());
  }

  @Test
  void endCountersReflectDedupedColumns() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 10, /* maxTargets= */ 10);
    QueryContext ctx = queryContextWithPin("query-5", 103L);
    store.seed(ctx);

    repository.putTargetStats(
        TargetStatsRecords.columnRecord(TABLE, 103L, 1L, sampleStats(TABLE, 103L, 1L), null));

    FetchTargetStatsRequest request = requestFor("query-5", TABLE, List.of(1L, 1L, 2L));
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    List<TargetStatsResult> results = flatten(chunks);
    assertEquals(2, results.size());

    TargetStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(1L, end.getRequestedTables());
    assertEquals(2L, end.getRequestedTargets());
    assertEquals(1L, end.getReturnedTargets());
    assertEquals(1L, end.getNotFoundTargets());
  }

  @Test
  void smartScanBranchReturnsAllColumns() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository =
        new SmartScanOnlyStatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 10, /* maxTables= */ 10, /* maxTargets= */ 10);
    QueryContext ctx = queryContextWithPin("query-smart", 200L);
    store.seed(ctx);

    for (long columnId = 1; columnId <= 3; columnId++) {
      repository.putTargetStats(
          TargetStatsRecords.columnRecord(
              TABLE, 200L, columnId, sampleStats(TABLE, 200L, columnId), null));
    }

    FetchTargetStatsRequest request = requestFor(ctx.getQueryId(), TABLE, List.of(1L, 2L, 3L));
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();
    List<TargetStatsResult> results = flatten(chunks);

    assertEquals(3, results.size());
    assertEquals(
        3,
        results.stream()
            .filter(r -> r.getStatus().equals(BundleResultStatus.BUNDLE_RESULT_STATUS_FOUND))
            .count());
    TargetStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(3L, end.getRequestedTargets());
    assertEquals(3L, end.getReturnedTargets());
    assertEquals(0L, end.getNotFoundTargets());
    assertEquals(0L, end.getErrorTargets());
  }

  @Test
  void cappedScanFallsBackToPerColumnReads() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    CappingStatsRepository repository =
        new CappingStatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 10, /* maxTables= */ 10, /* maxTargets= */ 10);
    QueryContext ctx = queryContextWithPin("query-cap", 300L);
    store.seed(ctx);

    for (long columnId = 1; columnId <= 4; columnId++) {
      repository.putTargetStats(
          TargetStatsRecords.columnRecord(
              TABLE, 300L, columnId, sampleStats(TABLE, 300L, columnId), null));
    }

    FetchTargetStatsRequest request = requestFor(ctx.getQueryId(), TABLE, List.of(1L, 2L, 3L, 4L));
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();
    List<TargetStatsResult> results = flatten(chunks);

    assertEquals(4, results.size());
    assertEquals(
        4,
        results.stream()
            .filter(r -> r.getStatus().equals(BundleResultStatus.BUNDLE_RESULT_STATUS_FOUND))
            .count());
    TargetStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(4L, end.getRequestedTargets());
    assertEquals(4L, end.getReturnedTargets());
    assertEquals(0L, end.getNotFoundTargets());
    assertEquals(0L, end.getErrorTargets());
  }

  @Test
  void cappedScanDoesNotEmitWarningsInsideBatch() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    CappingStatsRepository repository =
        new CappingStatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 10, /* maxTables= */ 10, /* maxTargets= */ 10);
    QueryContext ctx = queryContextWithPin("query-cap-warn", 310L);
    store.seed(ctx);

    for (long columnId = 1; columnId <= 3; columnId++) {
      repository.putTargetStats(
          TargetStatsRecords.columnRecord(
              TABLE, 310L, columnId, sampleStats(TABLE, 310L, columnId), null));
    }

    FetchTargetStatsRequest request = requestFor(ctx.getQueryId(), TABLE, List.of(1L, 2L, 3L));
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    TargetStatsBatch batch =
        chunks.stream()
            .filter(TargetStatsBundleChunk::hasBatch)
            .findFirst()
            .orElseThrow()
            .getBatch();
    assertEquals(0, batch.getWarningsCount());
  }

  @Test
  void repoFailureEmitsErrorResult() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository =
        new ThrowingStatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 10, /* maxTargets= */ 10);
    QueryContext ctx = queryContextWithPin("query-error", 108L);
    store.seed(ctx);

    FetchTargetStatsRequest request = requestFor(ctx.getQueryId(), TABLE, List.of(1L));
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    List<TargetStatsResult> results = flatten(chunks);
    assertEquals(1, results.size());
    TargetStatsResult result = results.get(0);
    assertEquals(BundleResultStatus.BUNDLE_RESULT_STATUS_ERROR, result.getStatus());
    assertEquals("planner_stats.target_stats.error", result.getFailure().getCode());

    TargetStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(0L, end.getReturnedTargets());
    assertEquals(0L, end.getNotFoundTargets());
    assertEquals(1L, end.getErrorTargets());
  }

  @Test
  void optionalColumnStatsFieldsRespectPresence() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 10, /* maxTargets= */ 10);
    long snapshotId = 400L;
    ScalarStats stats =
        ScalarStats.newBuilder()
            .setDisplayName("optionable")
            .setValueCount(99L)
            .setNullCount(7L)
            .setNanCount(3L)
            .setMin("foo")
            .setMax("bar")
            .setNdv(Ndv.newBuilder().setExact(13L).build())
            .putProperties("column_id", "42")
            .build();
    repository.putTargetStats(TargetStatsRecords.columnRecord(TABLE, snapshotId, 42L, stats, null));
    QueryContext ctx = queryContextWithPin("query-optionals", snapshotId);
    store.seed(ctx);
    FetchTargetStatsRequest request = requestFor(ctx.getQueryId(), TABLE, List.of(42L));
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();
    ScalarStats info = flatten(chunks).get(0).getStats().getScalar();
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
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 10, /* maxTargets= */ 10);
    long snapshotId = 410L;
    ScalarStats stats =
        ScalarStats.newBuilder()
            .setDisplayName("bare")
            .setValueCount(12L)
            .putProperties("column_id", "43")
            .build();
    repository.putTargetStats(TargetStatsRecords.columnRecord(TABLE, snapshotId, 43L, stats, null));
    QueryContext ctx = queryContextWithPin("query-absent", snapshotId);
    store.seed(ctx);
    FetchTargetStatsRequest request = requestFor(ctx.getQueryId(), TABLE, List.of(43L));
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();
    ScalarStats info = flatten(chunks).get(0).getStats().getScalar();
    assertFalse(info.hasNullCount());
    assertFalse(info.hasNanCount());
    assertFalse(info.hasMin());
    assertFalse(info.hasMax());
    assertFalse(info.hasNdv());
    assertEquals(12L, info.getValueCount());
  }

  /**
   * A ScalarStats row that exists in storage but has no metrics (no value_count, no null_count, no
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
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 10, /* maxTargets= */ 10);
    long snapshotId = 420L;

    // Simulate a row produced by compute() for a column with no Iceberg metrics:
    // only column_id and column_name are set; all stat fields are absent.
    ScalarStats emptyStats =
        ScalarStats.newBuilder().setDisplayName("ts_col").putProperties("column_id", "99").build();
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(TABLE, snapshotId, 99L, emptyStats, null));

    QueryContext ctx = queryContextWithPin("query-empty-metrics", snapshotId);
    store.seed(ctx);
    FetchTargetStatsRequest request = requestFor(ctx.getQueryId(), TABLE, List.of(99L));
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    List<TargetStatsResult> results = flatten(chunks);
    assertEquals(1, results.size());
    // Row exists → must be FOUND, not NOT_FOUND
    assertEquals(BundleResultStatus.BUNDLE_RESULT_STATUS_FOUND, results.get(0).getStatus());
    assertEquals(99L, results.get(0).getTarget().getColumn().getColumnId());

    ScalarStats info = results.get(0).getStats().getScalar();
    // No metrics were stored — all optional fields must be absent
    assertFalse(info.hasNullCount(), "null_count must not be set when no metrics");
    assertFalse(info.hasNanCount(), "nan_count must not be set when no metrics");
    assertFalse(info.hasMin(), "min must not be set when no metrics");
    assertFalse(info.hasMax(), "max must not be set when no metrics");
    assertFalse(info.hasNdv(), "ndv must not be set when no metrics");

    TargetStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(1L, end.getReturnedTargets());
    assertEquals(0L, end.getNotFoundTargets());
  }

  @Test
  void enforcesLimitsBeforeProcessing() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 1, /* maxTargets= */ 2);
    QueryContext ctx = queryContextWithPin("query-limits", 110L);
    store.seed(ctx);

    FetchTargetStatsRequest tooManyTables =
        FetchTargetStatsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .addTables(tableRequest(TABLE, List.of(1L)))
            .addTables(tableRequest(TABLE, List.of(2L)))
            .build();
    assertThrows(
        io.grpc.StatusRuntimeException.class,
        () ->
            service
                .streamTargets("corr", ctx, tooManyTables)
                .collect()
                .asList()
                .await()
                .indefinitely());

    FetchTargetStatsRequest tooManyColumns =
        FetchTargetStatsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .addTables(tableRequest(TABLE, List.of(1L, 2L, 3L)))
            .build();
    assertThrows(
        io.grpc.StatusRuntimeException.class,
        () ->
            service
                .streamTargets("corr", ctx, tooManyColumns)
                .collect()
                .asList()
                .await()
                .indefinitely());
  }
}
