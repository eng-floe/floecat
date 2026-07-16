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
import ai.floedb.floecat.catalog.rpc.Ndv;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.SketchPayload;
import ai.floedb.floecat.catalog.rpc.SketchRole;
import ai.floedb.floecat.catalog.rpc.StatsCompleteness;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.query.rpc.FetchTargetStatsRequest;
import ai.floedb.floecat.query.rpc.RequestedStat;
import ai.floedb.floecat.query.rpc.ReturnedStat;
import ai.floedb.floecat.query.rpc.StatRole;
import ai.floedb.floecat.query.rpc.StatsResultDegradation;
import ai.floedb.floecat.query.rpc.StatsResultStatus;
import ai.floedb.floecat.query.rpc.StatsServingOptions;
import ai.floedb.floecat.query.rpc.TableStatsRequest;
import ai.floedb.floecat.query.rpc.TargetStatsBatch;
import ai.floedb.floecat.query.rpc.TargetStatsBundleChunk;
import ai.floedb.floecat.query.rpc.TargetStatsBundleEnd;
import ai.floedb.floecat.query.rpc.TargetStatsNeed;
import ai.floedb.floecat.query.rpc.TargetStatsResult;
import ai.floedb.floecat.service.query.catalog.testsupport.UserObjectBundleTestSupport;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.statistics.StatsOrchestrator;
import ai.floedb.floecat.stats.identity.TargetStatsRecords;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class PlannerStatsBundleServiceTest extends PlannerStatsBundleServiceTestSupport {

  private static final String THETA_SKETCH_TYPE = "apache-datasketches-theta-v1";
  private static final String TUPLE_SKETCH_TYPE = "floedb-tuple-v2";

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
  void legacyUnspecifiedKindRequestDefaultsToTableIdentity() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 10, /* maxTables= */ 10, /* maxTargets= */ 10);
    QueryContext ctx = queryContextWithPin("query-legacy-kind", 100L);
    store.seed(ctx);

    repository.putTargetStats(
        TargetStatsRecords.columnRecord(TABLE, 100L, 1L, sampleStats(TABLE, 100L, 1L), null));

    ResourceId legacyTableId = TABLE.toBuilder().setKind(ResourceKind.RK_UNSPECIFIED).build();
    FetchTargetStatsRequest request = requestFor(ctx.getQueryId(), legacyTableId, List.of(1L));
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    List<TargetStatsResult> results = flatten(chunks);
    assertEquals(1, results.size());
    assertEquals(StatsResultStatus.STATS_RESULT_HIT_COMPLETE, results.get(0).getStatus());
    assertEquals(ResourceKind.RK_TABLE, results.get(0).getTableId().getKind());

    TargetStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(1L, end.getReturnedTargets());
    assertEquals(0L, end.getErrorTargets());
  }

  @Test
  void explicitViewKindDoesNotMatchTablePin() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 10, /* maxTables= */ 10, /* maxTargets= */ 10);
    QueryContext ctx = queryContextWithPin("query-view-kind", 100L);
    store.seed(ctx);

    repository.putTargetStats(
        TargetStatsRecords.columnRecord(TABLE, 100L, 1L, sampleStats(TABLE, 100L, 1L), null));

    ResourceId viewId = TABLE.toBuilder().setKind(ResourceKind.RK_VIEW).build();
    FetchTargetStatsRequest request = requestFor(ctx.getQueryId(), viewId, List.of(1L));
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    List<TargetStatsResult> results = flatten(chunks);
    assertEquals(1, results.size());
    assertEquals(StatsResultStatus.STATS_RESULT_ERROR, results.get(0).getStatus());
    assertEquals("planner_stats.pin.missing", results.get(0).getFailure().getCode());

    TargetStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(0L, end.getReturnedTargets());
    assertEquals(1L, end.getErrorTargets());
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
    assertEquals(StatsResultStatus.STATS_RESULT_ERROR, results.get(0).getStatus());
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
            .filter(r -> r.getStatus().equals(StatsResultStatus.STATS_RESULT_HIT_COMPLETE))
            .count());
    TargetStatsResult missing =
        results.stream()
            .filter(r -> r.getTarget().getColumn().getColumnId() == 2)
            .findFirst()
            .orElseThrow();
    assertEquals(StatsResultStatus.STATS_RESULT_NOT_FOUND, missing.getStatus());
    assertEquals("planner_stats.target_stats.missing", missing.getFailure().getCode());

    TargetStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(1L, end.getReturnedTargets());
    assertEquals(1L, end.getNotFoundTargets());
  }

  @Test
  void partialColumnsReturnFoundAndNotFound() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 10, /* maxTables= */ 10, /* maxTargets= */ 20);
    QueryContext ctx = queryContextWithPin("query-partial", 107L);
    store.seed(ctx);

    // Only column 1 exists; columns 2 and 3 are missing.
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(TABLE, 107L, 1L, sampleStats(TABLE, 107L, 1L), null));

    FetchTargetStatsRequest request = requestFor("query-partial", TABLE, List.of(1L, 2L, 3L));
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    List<TargetStatsResult> results = flatten(chunks);
    assertEquals(3, results.size());
    assertEquals(
        1,
        results.stream()
            .filter(r -> r.getStatus().equals(StatsResultStatus.STATS_RESULT_HIT_COMPLETE))
            .count());
    assertEquals(
        2,
        results.stream()
            .filter(r -> r.getStatus().equals(StatsResultStatus.STATS_RESULT_NOT_FOUND))
            .count());

    TargetStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(1L, end.getReturnedTargets());
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
            .filter(r -> r.getStatus().equals(StatsResultStatus.STATS_RESULT_HIT_COMPLETE))
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
            .filter(r -> r.getStatus().equals(StatsResultStatus.STATS_RESULT_HIT_COMPLETE))
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
    assertEquals(StatsResultStatus.STATS_RESULT_ERROR, result.getStatus());
    assertEquals("planner_stats.target_stats.error", result.getFailure().getCode());

    TargetStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(0L, end.getReturnedTargets());
    assertEquals(0L, end.getNotFoundTargets());
    assertEquals(1L, end.getErrorTargets());
  }

  @Test
  void batchFailureIsIsolatedToFailingTarget() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    TargetedThrowingStatsRepository repository =
        new TargetedThrowingStatsRepository(
            new InMemoryPointerStore(), new InMemoryBlobStore(), 2L);
    PlannerStatsBundleService service =
        createServiceWithRealLookup(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 10, /* maxTargets= */ 10);
    QueryContext ctx = queryContextWithPin("query-target-error", 109L);
    store.seed(ctx);

    repository.putTargetStats(
        TargetStatsRecords.columnRecord(TABLE, 109L, 1L, sampleStats(TABLE, 109L, 1L), null));
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(TABLE, 109L, 3L, sampleStats(TABLE, 109L, 3L), null));

    FetchTargetStatsRequest request = requestFor(ctx.getQueryId(), TABLE, List.of(1L, 2L, 3L));
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    List<TargetStatsResult> results = flatten(chunks);
    assertEquals(3, results.size());
    assertEquals(
        2,
        results.stream()
            .filter(r -> r.getStatus().equals(StatsResultStatus.STATS_RESULT_HIT_COMPLETE))
            .count(),
        results::toString);
    TargetStatsResult failed =
        results.stream()
            .filter(r -> r.getTarget().getColumn().getColumnId() == 2L)
            .findFirst()
            .orElseThrow();
    assertEquals(StatsResultStatus.STATS_RESULT_ERROR, failed.getStatus());
    assertEquals("planner_stats.target_stats.error", failed.getFailure().getCode());

    TargetStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(2L, end.getReturnedTargets());
    assertEquals(0L, end.getNotFoundTargets());
    assertEquals(1L, end.getErrorTargets());
  }

  @Test
  void invalidExpressionTargetReturnsInvalidArgument() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 10, /* maxTargets= */ 10);
    QueryContext ctx = queryContextWithPin("query-invalid-expression", 111L);
    store.seed(ctx);

    FetchTargetStatsRequest request =
        FetchTargetStatsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .addTables(
                TableStatsRequest.newBuilder()
                    .setTableId(TABLE)
                    .addTargets(
                        TargetStatsNeed.newBuilder()
                            .setTarget(
                                StatsTarget.newBuilder()
                                    .setExpression(
                                        EngineExpressionStatsTarget.newBuilder()
                                            .setEngineKind("duckdb")))
                            .setPriority(1)))
            .build();

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .streamTargets("corr", ctx, request)
                    .collect()
                    .asList()
                    .await()
                    .indefinitely());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), error.getStatus().getCode());
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
            .setRowCount(99L)
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
            .setRowCount(12L)
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
    assertEquals(12L, info.getRowCount());
  }

  /**
   * Stats are served from the query's pinned snapshot, not from whatever other snapshots happen to
   * have stats in storage. Guards the downstream-consumes-the-pin contract for the stats path.
   */
  @Test
  void statsResolvedAtPinnedSnapshotNotOtherSnapshots() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 10, /* maxTargets= */ 10);
    long pinnedSnapshot = 500L;
    long otherSnapshot = 999L;
    // Distinct stats exist at both snapshots; only the pinned snapshot's stats must be served.
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(
            TABLE,
            pinnedSnapshot,
            42L,
            ScalarStats.newBuilder().setRowCount(111L).putProperties("column_id", "42").build(),
            null));
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(
            TABLE,
            otherSnapshot,
            42L,
            ScalarStats.newBuilder().setRowCount(222L).putProperties("column_id", "42").build(),
            null));
    QueryContext ctx = queryContextWithPin("query-pinned-snap", pinnedSnapshot);
    store.seed(ctx);
    FetchTargetStatsRequest request = requestFor(ctx.getQueryId(), TABLE, List.of(42L));

    List<TargetStatsResult> results =
        flatten(
            service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely());

    TargetStatsResult hit =
        results.stream()
            .filter(r -> r.getStatus() == StatsResultStatus.STATS_RESULT_HIT_COMPLETE)
            .findFirst()
            .orElseThrow();
    assertEquals(pinnedSnapshot, hit.getSnapshotId());
    assertEquals(111L, hit.getStats().getScalar().getRowCount());
    // The response echoes the query's pinned snapshot so the planner can detect staleness.
    assertTrue(hit.hasPinnedSnapshotId());
    assertEquals(pinnedSnapshot, hit.getPinnedSnapshotId());
  }

  /**
   * A ScalarStats row that exists in storage with only required row_count must still be returned as
   * FOUND — not NOT_FOUND. Sparse connectors may omit optional metrics while still reporting the
   * enclosing row count.
   */
  @Test
  void columnWithOnlyRequiredRowCountIsFound() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 10, /* maxTargets= */ 10);
    long snapshotId = 420L;

    ScalarStats emptyStats =
        ScalarStats.newBuilder()
            .setDisplayName("ts_col")
            .setRowCount(420L)
            .putProperties("column_id", "99")
            .build();
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(TABLE, snapshotId, 99L, emptyStats, null));

    QueryContext ctx = queryContextWithPin("query-required-row-count", snapshotId);
    store.seed(ctx);
    FetchTargetStatsRequest request = requestFor(ctx.getQueryId(), TABLE, List.of(99L));
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    List<TargetStatsResult> results = flatten(chunks);
    assertEquals(1, results.size());
    // Row exists → must be FOUND, not NOT_FOUND
    assertEquals(StatsResultStatus.STATS_RESULT_HIT_COMPLETE, results.get(0).getStatus());
    assertEquals(99L, results.get(0).getTarget().getColumn().getColumnId());

    ScalarStats info = results.get(0).getStats().getScalar();
    assertEquals(420L, info.getRowCount());
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

    // Over-cap columns: maxTargets=2 but 3 requested — excess dropped as OMITTED_BY_BUDGET,
    // no exception.  The end chunk must report omitted_by_budget=1.
    FetchTargetStatsRequest tooManyColumns =
        FetchTargetStatsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .addTables(tableRequest(TABLE, List.of(1L, 2L, 3L)))
            .build();
    List<TargetStatsBundleChunk> overCapChunks =
        service
            .streamTargets("corr", ctx, tooManyColumns)
            .collect()
            .asList()
            .await()
            .indefinitely();
    TargetStatsBundleEnd overCapEnd =
        overCapChunks.stream()
            .filter(TargetStatsBundleChunk::hasEnd)
            .map(TargetStatsBundleChunk::getEnd)
            .findFirst()
            .orElseThrow();
    assertEquals(1L, overCapEnd.getOmittedByBudget(), "one column must be omitted by budget");
    // Two targets were within cap and should have been served (HIT_COMPLETE or NOT_FOUND).
    long served =
        overCapChunks.stream()
            .filter(TargetStatsBundleChunk::hasBatch)
            .flatMap(c -> c.getBatch().getTargetsList().stream())
            .filter(r -> r.getStatus() != StatsResultStatus.STATS_RESULT_OMITTED_BY_BUDGET)
            .count();
    assertEquals(2L, served, "two targets within cap must be served");
  }

  @Test
  void maxResponseBytesOmitsOversizedFoundRecord() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 10, /* maxTargets= */ 10);
    long snapshotId = 430L;
    ScalarStats largeStats =
        ScalarStats.newBuilder()
            .setDisplayName("large")
            .setRowCount(10L)
            .putProperties("padding", "x".repeat(4096))
            .build();
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(TABLE, snapshotId, 7L, largeStats, null));
    QueryContext ctx = queryContextWithPin("query-byte-cap-found", snapshotId);
    store.seed(ctx);

    FetchTargetStatsRequest request =
        FetchTargetStatsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .setOptions(StatsServingOptions.newBuilder().setMaxResponseBytes(256))
            .addTables(tableRequest(TABLE, List.of(7L)))
            .build();
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    List<TargetStatsResult> results = flatten(chunks);
    assertEquals(1, results.size());
    assertEquals(StatsResultStatus.STATS_RESULT_OMITTED_BY_BUDGET, results.get(0).getStatus());
    TargetStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(0L, end.getReturnedTargets());
    assertEquals(1L, end.getOmittedByBudget());
  }

  @Test
  void maxResponseBytesAccountsForNotFoundResults() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 10, /* maxTargets= */ 10);
    QueryContext ctx = queryContextWithPin("query-byte-cap-missing", 440L);
    store.seed(ctx);

    FetchTargetStatsRequest request =
        FetchTargetStatsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .setOptions(StatsServingOptions.newBuilder().setMaxResponseBytes(1))
            .addTables(tableRequest(TABLE, List.of(1L, 2L, 3L)))
            .build();
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    assertTrue(flatten(chunks).isEmpty(), "no per-target result should be emitted past byte cap");
    TargetStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(0L, end.getNotFoundTargets());
    assertEquals(3L, end.getOmittedByBudget());
  }

  @Test
  void sketchTargetCapDowngradesAndStripsSketchPayloads() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 10, /* maxTargets= */ 10);
    long snapshotId = 450L;
    SketchPayload sketch =
        SketchPayload.newBuilder()
            .setRole(SketchRole.SKETCH_ROLE_NDV)
            .setSketchType(THETA_SKETCH_TYPE)
            .setData(ByteString.copyFromUtf8("sketch-bytes"))
            .setCompleteness(StatsCompleteness.SC_COMPLETE)
            .build();
    for (long columnId : List.of(1L, 2L)) {
      ScalarStats stats =
          ScalarStats.newBuilder()
              .setDisplayName("col" + columnId)
              .setRowCount(100L)
              .setNdv(Ndv.newBuilder().setExact(10L).addSketches(sketch))
              .addSketches(sketch)
              .putProperties("column_id", Long.toString(columnId))
              .build();
      repository.putTargetStats(
          TargetStatsRecords.columnRecord(TABLE, snapshotId, columnId, stats, null));
    }
    QueryContext ctx = queryContextWithPin("query-sketch-cap", snapshotId);
    store.seed(ctx);

    FetchTargetStatsRequest request =
        FetchTargetStatsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .setOptions(StatsServingOptions.newBuilder().setMaxSketchTargets(1))
            .addTables(
                ai.floedb.floecat.query.rpc.TableStatsRequest.newBuilder()
                    .setTableId(TABLE)
                    .addTargets(sketchNeed(1L, 1))
                    .addTargets(sketchNeed(2L, 2)))
            .build();
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    List<TargetStatsResult> results = flatten(chunks);
    assertEquals(2, results.size());
    TargetStatsResult first = results.get(0);
    TargetStatsResult second = results.get(1);
    assertEquals(StatsResultStatus.STATS_RESULT_HIT_COMPLETE, first.getStatus());
    assertEquals(1, first.getStats().getScalar().getSketchesCount());
    assertEquals(1, first.getStats().getScalar().getNdv().getSketchesCount());
    assertEquals(StatsResultStatus.STATS_RESULT_HIT_PARTIAL, second.getStatus());
    assertEquals(0, second.getStats().getScalar().getSketchesCount());
    assertEquals(0, second.getStats().getScalar().getNdv().getSketchesCount());
    ReturnedStat omitted = second.getReturnedStats(0);
    assertEquals(StatsResultStatus.STATS_RESULT_OMITTED_BY_BUDGET, omitted.getStatus());
    assertEquals("max_sketch_targets", omitted.getReason());
    assertTrue(
        omitted
            .getDegradationsList()
            .contains(StatsResultDegradation.STATS_DEGRADATION_REQUESTED_STAT_OMITTED_BY_BUDGET));
    TargetStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(2L, end.getReturnedTargets());
    assertEquals(1L, end.getPartialTargets());
  }

  @Test
  void duplicateSketchTargetsDoNotConsumeBudgetBeforeDedupe() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 10, /* maxTargets= */ 10);
    long snapshotId = 452L;
    SketchPayload sketch =
        SketchPayload.newBuilder()
            .setRole(SketchRole.SKETCH_ROLE_NDV)
            .setSketchType(THETA_SKETCH_TYPE)
            .setData(ByteString.copyFromUtf8("sketch-bytes"))
            .setCompleteness(StatsCompleteness.SC_COMPLETE)
            .build();
    SketchPayload tupleSketch =
        SketchPayload.newBuilder()
            .setRole(SketchRole.SKETCH_ROLE_TUPLE_NDV)
            .setSketchType(TUPLE_SKETCH_TYPE)
            .setData(ByteString.copyFromUtf8("tuple-bytes"))
            .setCompleteness(StatsCompleteness.SC_COMPLETE)
            .build();
    for (long columnId : List.of(1L, 2L)) {
      ScalarStats stats =
          ScalarStats.newBuilder()
              .setDisplayName("col" + columnId)
              .setRowCount(100L)
              .setNdv(Ndv.newBuilder().setExact(10L).addSketches(sketch))
              .addSketches(sketch)
              .addSketches(tupleSketch)
              .putProperties("column_id", Long.toString(columnId))
              .build();
      repository.putTargetStats(
          TargetStatsRecords.columnRecord(TABLE, snapshotId, columnId, stats, null));
    }
    QueryContext ctx = queryContextWithPin("query-full-dedupe-cap", snapshotId);
    store.seed(ctx);

    FetchTargetStatsRequest request =
        FetchTargetStatsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .setOptions(StatsServingOptions.newBuilder().setMaxSketchTargets(2))
            .addTables(
                ai.floedb.floecat.query.rpc.TableStatsRequest.newBuilder()
                    .setTableId(TABLE)
                    .addTargets(fullNeed(1L, 1))
                    .addTargets(fullNeed(1L, 1))
                    .addTargets(fullNeed(2L, 2)))
            .build();
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    List<TargetStatsResult> results = flatten(chunks);
    assertEquals(2, results.size(), "duplicate target should be served once");
    assertTrue(
        results.stream()
            .allMatch(r -> r.getStatus() == StatsResultStatus.STATS_RESULT_HIT_COMPLETE));
    assertTrue(
        results.stream()
            .allMatch(
                r ->
                    r.getStats().getScalar().getSketchesCount() == 2
                        && r.getStats().getScalar().getNdv().getSketchesCount() == 1));
    TargetStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(2L, end.getReturnedTargets());
    assertEquals(0L, end.getPartialTargets());
  }

  @Test
  void sketchRequestDowngradesWhenStoredRecordHasNoSketchPayloads() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 10, /* maxTargets= */ 10);
    long snapshotId = 455L;
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(
            TABLE,
            snapshotId,
            1L,
            ScalarStats.newBuilder()
                .setDisplayName("scalar_only")
                .setRowCount(100L)
                .setNdv(Ndv.newBuilder().setExact(10L))
                .build(),
            null));
    QueryContext ctx = queryContextWithPin("query-sketch-missing-payload", snapshotId);
    store.seed(ctx);

    FetchTargetStatsRequest request =
        FetchTargetStatsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .addTables(
                ai.floedb.floecat.query.rpc.TableStatsRequest.newBuilder()
                    .setTableId(TABLE)
                    .addTargets(sketchNeed(1L, 1)))
            .build();
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    List<TargetStatsResult> results = flatten(chunks);
    assertEquals(1, results.size());
    assertEquals(StatsResultStatus.STATS_RESULT_HIT_PARTIAL, results.get(0).getStatus());
    ReturnedStat returnedStat = results.get(0).getReturnedStats(0);
    assertEquals(StatsResultStatus.STATS_RESULT_NOT_FOUND, returnedStat.getStatus());
    assertEquals("requested_sketch_missing", returnedStat.getReason());
    assertFalse(
        results.get(0).getStats().hasScalar(),
        "sketch-only misses must not return unrequested scalar payload");
    TargetStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(1L, end.getReturnedTargets());
    assertEquals(1L, end.getPartialTargets());
  }

  @Test
  void snapshotIdRestatingPinServesFromPinnedSnapshot() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 5, /* maxTargets= */ 10);

    long pinnedSnapshotId = 500L;
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(
            TABLE, pinnedSnapshotId, 1L, sampleStats(TABLE, pinnedSnapshotId, 1L), null));

    QueryContext ctx = queryContextWithPin("snap-restate", pinnedSnapshotId);
    store.seed(ctx);

    // A request snapshot_id equal to the pinned snapshot is a harmless restatement.
    FetchTargetStatsRequest request =
        FetchTargetStatsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .addTables(tableRequestWithSnapshot(TABLE, List.of(1L), pinnedSnapshotId))
            .build();
    List<TargetStatsResult> results =
        flatten(
            service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely());
    assertEquals(1, results.size());
    assertEquals(StatsResultStatus.STATS_RESULT_HIT_COMPLETE, results.get(0).getStatus());
    assertEquals(pinnedSnapshotId, results.get(0).getSnapshotId());
  }

  @Test
  void snapshotIdDivergingFromPinFailsAsConsistencyError() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 5, /* maxTargets= */ 10);

    long pinnedSnapshotId = 500L;
    long divergentSnapshotId = 490L;
    // Stats exist at the divergent snapshot, but the pin is authoritative — the request must NOT be
    // able to redirect reads there (this is what would let correctness constraints drift).
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(
            TABLE, divergentSnapshotId, 1L, sampleStats(TABLE, divergentSnapshotId, 1L), null));

    QueryContext ctx = queryContextWithPin("snap-diverge", pinnedSnapshotId);
    store.seed(ctx);

    FetchTargetStatsRequest request =
        FetchTargetStatsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .addTables(tableRequestWithSnapshot(TABLE, List.of(1L), divergentSnapshotId))
            .build();

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .streamTargets("corr", ctx, request)
                    .collect()
                    .asList()
                    .await()
                    .indefinitely());
    // A divergent request snapshot is a query-consistency error, not a generic failure: assert the
    // FAILED_PRECONDITION status that QUERY_TABLE_PIN_CONFLICT maps to, so a regression to
    // INVALID_ARGUMENT/INTERNAL (or a dropped conflict) is caught.
    assertEquals(io.grpc.Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
  }

  @Test
  void staleStats_viaRealOrchestrator_returnsHitStaleBeforeSyncCapture() {
    // Integration test: verifies that StatsOrchestrator.resolvePlannerBatch() correctly applies
    // stale-before-sync ordering and that resolveStale() is actually called (not the test stub).
    // Uses forTestingWithRealLookup() so the full production path is exercised.
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    TableRepository tableRepository = org.mockito.Mockito.mock(TableRepository.class);
    StatsOrchestrator orchestrator =
        new StatsOrchestrator(
            repository,
            org.mockito.Mockito.mock(ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.class),
            tableRepository,
            org.mockito.Mockito.mock(
                ai.floedb.floecat.service.repo.impl.ConnectorRepository.class));
    StatsProviderFactory factory = new StatsProviderFactory(orchestrator, tableRepository, store);

    PlannerStatsBundleService service =
        PlannerStatsBundleService.forTestingWithRealLookup(
            orchestrator,
            tableRepository,
            factory,
            /* maxTables= */ 5,
            /* maxTargets= */ 10,
            /* maxResultsPerChunk= */ 5);

    // Put stats at snapshot 480L; pin the context to snapshot 481L (miss on exact).
    // resolvePlannerBatch should find the stale record at 480L BEFORE attempting sync capture.
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(TABLE, 480L, 1L, sampleStats(TABLE, 480L, 1L), null));
    QueryContext ctx = queryContextWithPin("real-orchestrator-stale", 481L);
    store.seed(ctx);

    FetchTargetStatsRequest request = requestFor(ctx.getQueryId(), TABLE, List.of(1L));
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    List<TargetStatsResult> results = flatten(chunks);
    assertEquals(1, results.size(), "one column must produce a result");
    assertEquals(
        StatsResultStatus.STATS_RESULT_HIT_STALE,
        results.get(0).getStatus(),
        "stale stats at snapshot 480 must be returned with HIT_STALE status");
    assertEquals(480L, results.get(0).getSnapshotId(), "returned snapshot must be 480 (stale)");
    // The divergent pinned-snapshot stamp is the whole reason the field exists: served 480, pin
    // 481.
    assertEquals(
        481L,
        results.get(0).getPinnedSnapshotId(),
        "result must stamp the pinned snapshot (481) so the planner sees the served stats are stale");

    TargetStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(1L, end.getStaleTargets(), "one stale target must be counted in end chunk");
    assertEquals(0L, end.getNotFoundTargets(), "no NOT_FOUND — stale hit found before sync");
  }

  @Test
  void realPlannerLookupReadsTheStatsGenerationFrozenOnThePin() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createServiceWithRealLookup(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 5, /* maxTargets= */ 10);

    long snapshotId = 482L;
    // Pinned generation: col1 = 10.
    publishColumn(
        repository,
        snapshotId,
        1L,
        ScalarStats.newBuilder().setDisplayName("col1").setRowCount(10L).build());
    String pinnedGeneration = repository.activeStatsGeneration(TABLE, snapshotId).orElseThrow();

    // A newer generation for the same snapshot: col1 = 20.
    publishColumn(
        repository,
        snapshotId,
        1L,
        ScalarStats.newBuilder().setDisplayName("col1").setRowCount(20L).build());

    QueryContext ctx =
        queryContextWithStatsGenerationRef(
            "real-orchestrator-pinned-generation", snapshotId, pinnedGeneration);
    store.seed(ctx);

    FetchTargetStatsRequest request = requestFor(ctx.getQueryId(), TABLE, List.of(1L));
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    // The plan reads the generation frozen on the pin (10), not the newer live one (20) — stable
    // and
    // reproducible for a given pin.
    List<TargetStatsResult> results = flatten(chunks);
    assertEquals(1, results.size(), "one column must produce a result");
    assertEquals(StatsResultStatus.STATS_RESULT_HIT_COMPLETE, results.get(0).getStatus());
    assertEquals(10L, results.get(0).getStats().getScalar().getRowCount());
    TargetStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(1L, end.getReturnedTargets());
    assertEquals(0L, end.getNotFoundTargets());
  }

  @Test
  void realPlannerLookupFillsFromNewestWhenPinnedLacksTarget() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createServiceWithRealLookup(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 5, /* maxTargets= */ 10);

    long snapshotId = 482L;
    // Pinned generation carries only col2 — it never had col1.
    publishColumn(
        repository,
        snapshotId,
        2L,
        ScalarStats.newBuilder().setDisplayName("col2").setRowCount(99L).build());
    String pinnedGeneration = repository.activeStatsGeneration(TABLE, snapshotId).orElseThrow();

    // The newest generation carries col1 = 20.
    publishColumn(
        repository,
        snapshotId,
        1L,
        ScalarStats.newBuilder().setDisplayName("col1").setRowCount(20L).build());

    QueryContext ctx =
        queryContextWithStatsGenerationRef(
            "real-orchestrator-newest-fill", snapshotId, pinnedGeneration);
    store.seed(ctx);

    FetchTargetStatsRequest request = requestFor(ctx.getQueryId(), TABLE, List.of(1L));
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    // The pinned generation lacks col1, so the newest generation fills it instead of NOT_FOUND.
    List<TargetStatsResult> results = flatten(chunks);
    assertEquals(1, results.size(), "one column must produce a result");
    assertEquals(StatsResultStatus.STATS_RESULT_HIT_COMPLETE, results.get(0).getStatus());
    assertEquals(20L, results.get(0).getStats().getScalar().getRowCount());
    TargetStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(1L, end.getReturnedTargets());
    assertEquals(0L, end.getNotFoundTargets());
  }

  @Test
  void realPlannerLookupFillsFromNewestWhenPinnedRecordLacksRequestedSketch() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createServiceWithRealLookup(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 5, /* maxTargets= */ 10);

    long snapshotId = 483L;
    // Pinned generation carries col1 scalar-only: it EXISTS, but cannot serve a sketch need.
    publishColumn(
        repository,
        snapshotId,
        1L,
        ScalarStats.newBuilder().setDisplayName("col1").setRowCount(10L).build());
    String pinnedGeneration = repository.activeStatsGeneration(TABLE, snapshotId).orElseThrow();

    // The newest generation of the SAME snapshot was enriched with the theta sketch.
    SketchPayload sketch =
        SketchPayload.newBuilder()
            .setRole(SketchRole.SKETCH_ROLE_NDV)
            .setSketchType(THETA_SKETCH_TYPE)
            .setData(ByteString.copyFromUtf8("sketch-bytes"))
            .setCompleteness(StatsCompleteness.SC_COMPLETE)
            .build();
    publishColumn(
        repository,
        snapshotId,
        1L,
        ScalarStats.newBuilder()
            .setDisplayName("col1")
            .setRowCount(10L)
            .setNdv(Ndv.newBuilder().setExact(10L).addSketches(sketch))
            .addSketches(sketch)
            .build());

    QueryContext ctx =
        queryContextWithStatsGenerationRef(
            "real-orchestrator-completeness-fill", snapshotId, pinnedGeneration);
    store.seed(ctx);

    FetchTargetStatsRequest request =
        FetchTargetStatsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .addTables(
                ai.floedb.floecat.query.rpc.TableStatsRequest.newBuilder()
                    .setTableId(TABLE)
                    .addTargets(sketchNeed(1L, 1)))
            .build();
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    // The pinned record exists but is scalar-only: "exists" alone must not be a hit for a sketch
    // need. Resolution falls to the newest generation and serves the sketch — complete, not
    // downgraded.
    List<TargetStatsResult> results = flatten(chunks);
    assertEquals(1, results.size(), "one column must produce a result");
    assertEquals(StatsResultStatus.STATS_RESULT_HIT_COMPLETE, results.get(0).getStatus());
    assertEquals(1, results.get(0).getStats().getScalar().getSketchesCount());
    TargetStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(1L, end.getReturnedTargets());
    assertEquals(0L, end.getNotFoundTargets());
    assertEquals(0L, end.getPartialTargets());
  }

  @Test
  void staleStatsAreReturnedByDefault() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 10, /* maxTargets= */ 10);
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(TABLE, 460L, 1L, sampleStats(TABLE, 460L, 1L), null));
    QueryContext ctx = queryContextWithPin("query-stale-default", 461L);
    store.seed(ctx);

    FetchTargetStatsRequest request = requestFor(ctx.getQueryId(), TABLE, List.of(1L));
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    List<TargetStatsResult> results = flatten(chunks);
    assertEquals(1, results.size());
    assertEquals(StatsResultStatus.STATS_RESULT_HIT_STALE, results.get(0).getStatus());
    assertEquals(460L, results.get(0).getSnapshotId());
    assertEquals(461L, results.get(0).getPinnedSnapshotId(), "served 460 is stale against pin 461");
    TargetStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(1L, end.getReturnedTargets());
    assertEquals(1L, end.getStaleTargets());
    assertEquals(0L, end.getNotFoundTargets());
  }

  @Test
  void staleStatsCanBeDisabled() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 10, /* maxTargets= */ 10);
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(TABLE, 470L, 1L, sampleStats(TABLE, 470L, 1L), null));
    QueryContext ctx = queryContextWithPin("query-stale-disabled", 471L);
    store.seed(ctx);

    FetchTargetStatsRequest request =
        FetchTargetStatsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .setOptions(StatsServingOptions.newBuilder().setStaleOk(false))
            .addTables(tableRequest(TABLE, List.of(1L)))
            .build();
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    List<TargetStatsResult> results = flatten(chunks);
    assertEquals(1, results.size());
    assertEquals(StatsResultStatus.STATS_RESULT_NOT_FOUND, results.get(0).getStatus());
    TargetStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(0L, end.getReturnedTargets());
    assertEquals(0L, end.getStaleTargets());
    assertEquals(1L, end.getNotFoundTargets());
  }

  /** Publishes one column record as a full replacement (a new live generation) of the snapshot. */
  private static void publishColumn(
      StatsRepository repository, long snapshotId, long columnId, ScalarStats scalar) {
    repository.replaceAllStatsForSnapshot(
        TABLE,
        snapshotId,
        List.of(TargetStatsRecords.columnRecord(TABLE, snapshotId, columnId, scalar, null)));
  }

  private static TargetStatsNeed sketchNeed(long columnId, int priority) {
    return TargetStatsNeed.newBuilder()
        .setTarget(
            StatsTarget.newBuilder()
                .setColumn(ColumnStatsTarget.newBuilder().setColumnId(columnId)))
        .setPriority(priority)
        .addRequestedStats(thetaRequest(priority))
        .build();
  }

  private static TargetStatsNeed fullNeed(long columnId, int priority) {
    return TargetStatsNeed.newBuilder()
        .setTarget(
            StatsTarget.newBuilder()
                .setColumn(ColumnStatsTarget.newBuilder().setColumnId(columnId)))
        .setPriority(priority)
        .addRequestedStats(tupleRequest(priority))
        .addRequestedStats(thetaRequest(priority + 1))
        .build();
  }

  private static RequestedStat thetaRequest(int priority) {
    return RequestedStat.newBuilder()
        .setRole(StatRole.STAT_ROLE_NDV)
        .setSketchType(THETA_SKETCH_TYPE)
        .setPriority(priority)
        .build();
  }

  private static RequestedStat tupleRequest(int priority) {
    return RequestedStat.newBuilder()
        .setRole(StatRole.STAT_ROLE_TUPLE_NDV)
        .setSketchType(TUPLE_SKETCH_TYPE)
        .setPriority(priority)
        .build();
  }

  @Test
  void multiTableRequestOneTableMissingPin() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 5, /* maxTargets= */ 10);

    long snapshotId = 450L;
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(
            TABLE, snapshotId, 1L, sampleStats(TABLE, snapshotId, 1L), null));

    // TABLE has a pin; TABLE_TWO does not
    QueryContext ctx = queryContextWithPin("multi-pin", snapshotId);
    store.seed(ctx);

    FetchTargetStatsRequest request =
        FetchTargetStatsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .addTables(tableRequest(TABLE, List.of(1L)))
            .addTables(tableRequest(TABLE_TWO, List.of(2L)))
            .build();
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    List<TargetStatsResult> results = flatten(chunks);
    assertEquals(2, results.size(), "both columns must produce a result");

    long hitCount =
        results.stream()
            .filter(r -> r.getStatus() == StatsResultStatus.STATS_RESULT_HIT_COMPLETE)
            .count();
    long errorCount =
        results.stream().filter(r -> r.getStatus() == StatsResultStatus.STATS_RESULT_ERROR).count();
    assertEquals(1L, hitCount, "TABLE with pin must return HIT_COMPLETE");
    assertEquals(1L, errorCount, "TABLE_TWO without pin must return ERROR");

    TargetStatsResult errorResult =
        results.stream()
            .filter(r -> r.getStatus() == StatsResultStatus.STATS_RESULT_ERROR)
            .findFirst()
            .orElseThrow();
    assertEquals("planner_stats.pin.missing", errorResult.getFailure().getCode());

    TargetStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(1L, end.getReturnedTargets());
    assertEquals(1L, end.getErrorTargets());
  }

  @Test
  void requestedTargetsCountIncludesOmittedByBudget() {
    // With maxTargets=2 and 5 targets requested, requestedTargets in end chunk must be 5.
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 1, /* maxTargets= */ 2);
    QueryContext ctx = queryContextWithPin("cap-count", 460L);
    store.seed(ctx);

    FetchTargetStatsRequest request =
        FetchTargetStatsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .addTables(tableRequest(TABLE, List.of(1L, 2L, 3L, 4L, 5L)))
            .build();
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    TargetStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(5L, end.getRequestedTargets(), "requestedTargets must include omitted-by-budget");
    assertEquals(3L, end.getOmittedByBudget(), "3 of 5 targets must be omitted by budget");
  }

  @Test
  void servingOptionsWithoutStaleOkDefaultsToStaleEnabled() {
    // Sending options with only maxResponseBytes set (stale_ok field absent) must default to
    // staleOk=true.  This tests the three-way logic in ServingPolicy.from().
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 5, /* maxTargets= */ 10);
    QueryContext ctx = queryContextWithPin("stale-default", 470L);
    store.seed(ctx);

    // No stale_ok field set explicitly — must default to true (accept stale)
    FetchTargetStatsRequest request =
        FetchTargetStatsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .setOptions(StatsServingOptions.newBuilder().setMaxResponseBytes(1024 * 1024))
            .addTables(tableRequest(TABLE, List.of(99L)))
            .build();
    // Should not throw — serves NOT_FOUND gracefully (stale_ok=true, no stale available)
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();
    assertFalse(chunks.isEmpty(), "request with options-but-no-stale_ok must succeed");
    List<TargetStatsResult> results = flatten(chunks);
    assertEquals(1, results.size());
    // staleOk=true with no stale stats → NOT_FOUND (not an error)
    assertEquals(StatsResultStatus.STATS_RESULT_NOT_FOUND, results.get(0).getStatus());
  }
}
