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

import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.catalog.rpc.ConstraintType;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.BundleResultStatus;
import ai.floedb.floecat.query.rpc.FetchTargetStatsRequest;
import ai.floedb.floecat.query.rpc.TableConstraintsResult;
import ai.floedb.floecat.query.rpc.TargetStatsBundleChunk;
import ai.floedb.floecat.scanner.spi.ConstraintProvider;
import ai.floedb.floecat.service.query.catalog.testsupport.UserObjectBundleTestSupport;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import ai.floedb.floecat.stats.identity.TargetStatsRecords;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class PlannerStatsBundleServiceCombinedConstraintsTest
    extends PlannerStatsBundleServiceTestSupport {

  @Test
  void includeConstraintsEmitsFoundOncePerTable() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    QueryContext ctx =
        queryContextWithPins(
            "query-constraints-found", List.of(pin(TABLE, 500L), pin(TABLE_TWO, 501L)));
    store.seed(ctx);
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(TABLE, 500L, 1L, sampleStats(TABLE, 500L, 1L), null));
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(
            TABLE_TWO, 501L, 2L, sampleStats(TABLE_TWO, 501L, 2L), null));

    ConstraintProvider provider =
        new ConstraintProvider() {
          @Override
          public Optional<ConstraintSetView> constraints(
              ResourceId relationId, OptionalLong snapshotId) {
            if (snapshotId.isEmpty()) {
              return Optional.empty();
            }
            long sid = snapshotId.getAsLong();
            if (relationId.equals(TABLE) && sid == 500L) {
              return Optional.of(
                  newConstraintSet(
                      TABLE,
                      List.of(constraint("pk_users", ConstraintType.CT_PRIMARY_KEY, List.of(1L)))));
            }
            if (relationId.equals(TABLE_TWO) && sid == 501L) {
              return Optional.of(
                  newConstraintSet(
                      TABLE_TWO,
                      List.of(
                          constraint("pk_orders", ConstraintType.CT_PRIMARY_KEY, List.of(2L)))));
            }
            return Optional.empty();
          }
        };

    PlannerStatsBundleService service =
        createService(
            repository,
            store,
            provider,
            /* chunkSize= */ 10,
            /* maxTables= */ 10,
            /* maxTargets= */ 20);

    FetchTargetStatsRequest request =
        FetchTargetStatsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .setIncludeConstraints(true)
            .addTables(tableRequest(TABLE, List.of(1L)))
            .addTables(tableRequest(TABLE_TWO, List.of(2L)))
            .build();
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    List<TableConstraintsResult> constraints = flattenConstraints(chunks);
    assertEquals(2, constraints.size());
    assertEquals(
        2,
        constraints.stream()
            .filter(c -> c.getStatus() == BundleResultStatus.BUNDLE_RESULT_STATUS_FOUND)
            .count());
    assertEquals(
        1L,
        constraints.stream()
            .filter(
                c ->
                    c.getConstraintsCount() == 1
                        && c.getConstraints(0).getName().equals("pk_users"))
            .count(),
        "constraints should be emitted once per table");
    assertEquals(
        1L,
        constraints.stream()
            .filter(
                c ->
                    c.getConstraintsCount() == 1
                        && c.getConstraints(0).getName().equals("pk_orders"))
            .count(),
        "constraints should be emitted once per table");
  }

  @Test
  void includeConstraintsChunkedSingleTableEmitsConstraintsOnlyOnceAcrossBatches() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    QueryContext ctx = queryContextWithPin("query-constraints-chunked-once", 555L);
    store.seed(ctx);
    for (long columnId = 1; columnId <= 5; columnId++) {
      repository.putTargetStats(
          TargetStatsRecords.columnRecord(
              TABLE, 555L, columnId, sampleStats(TABLE, 555L, columnId), null));
    }

    ConstraintDefinition pk =
        constraint("pk_users_chunked", ConstraintType.CT_PRIMARY_KEY, List.of(1L));
    ConstraintProvider provider =
        new ConstraintProvider() {
          @Override
          public Optional<ConstraintSetView> constraints(
              ResourceId relationId, OptionalLong snapshotId) {
            if (relationId.equals(TABLE)
                && snapshotId.isPresent()
                && snapshotId.getAsLong() == 555L) {
              return Optional.of(newConstraintSet(TABLE, List.of(pk)));
            }
            return Optional.empty();
          }
        };

    PlannerStatsBundleService service =
        createService(
            repository,
            store,
            provider,
            /* chunkSize= */ 2,
            /* maxTables= */ 10,
            /* maxTargets= */ 20);

    FetchTargetStatsRequest request =
        FetchTargetStatsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .setIncludeConstraints(true)
            .addTables(tableRequest(TABLE, List.of(1L, 2L, 3L, 4L, 5L)))
            .build();
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    List<TargetStatsBundleChunk> batches = new ArrayList<>();
    for (TargetStatsBundleChunk chunk : chunks) {
      if (chunk.hasBatch()) {
        batches.add(chunk);
      }
    }
    assertEquals(3, batches.size(), "chunk size should force multiple batches");
    assertEquals(1, batches.stream().filter(c -> c.getBatch().getConstraintsCount() == 1).count());
    assertEquals(2, batches.stream().filter(c -> c.getBatch().getConstraintsCount() == 0).count());

    List<TableConstraintsResult> constraints = flattenConstraints(chunks);
    assertEquals(1, constraints.size());
    assertEquals(BundleResultStatus.BUNDLE_RESULT_STATUS_FOUND, constraints.get(0).getStatus());
    assertEquals("pk_users_chunked", constraints.get(0).getConstraints(0).getName());
  }

  @Test
  void includeConstraintsPassesThroughWhenNoPruningNeeded() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    QueryContext ctx = queryContextWithPin("query-constraints-pass-through", 560L);
    store.seed(ctx);
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(TABLE, 560L, 1L, sampleStats(TABLE, 560L, 1L), null));
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(TABLE, 560L, 2L, sampleStats(TABLE, 560L, 2L), null));

    ConstraintDefinition expectedPk =
        constraint("pk_users_passthrough", ConstraintType.CT_PRIMARY_KEY, List.of(1L));
    ConstraintDefinition expectedUnique =
        constraint("uq_users_passthrough", ConstraintType.CT_UNIQUE, List.of(2L));
    List<ConstraintDefinition> expected = List.of(expectedPk, expectedUnique);
    ConstraintProvider provider =
        new ConstraintProvider() {
          @Override
          public Optional<ConstraintSetView> constraints(
              ResourceId relationId, OptionalLong snapshotId) {
            if (relationId.equals(TABLE)
                && snapshotId.isPresent()
                && snapshotId.getAsLong() == 560L) {
              return Optional.of(newConstraintSet(TABLE, expected));
            }
            return Optional.empty();
          }
        };

    PlannerStatsBundleService service =
        createService(
            repository,
            store,
            provider,
            /* chunkSize= */ 5,
            /* maxTables= */ 10,
            /* maxTargets= */ 10);

    FetchTargetStatsRequest request =
        FetchTargetStatsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .setIncludeConstraints(true)
            .addTables(tableRequest(TABLE, List.of(1L, 2L)))
            .build();
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    TableConstraintsResult result = flattenConstraints(chunks).get(0);
    assertEquals(BundleResultStatus.BUNDLE_RESULT_STATUS_FOUND, result.getStatus());
    assertEquals(expected.size(), result.getConstraintsCount());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(
          expected.get(i).toByteString(),
          result.getConstraints(i).toByteString(),
          "constraint should pass through without pruning changes");
    }
  }

  @Test
  void includeConstraintsMissingPinYieldsErrorResult() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository,
            store,
            ConstraintProvider.NONE,
            /* chunkSize= */ 5,
            /* maxTables= */ 10,
            /* maxTargets= */ 10);
    QueryContext ctx = queryContextWithoutPin("query-constraints-no-pin");
    store.seed(ctx);

    FetchTargetStatsRequest request =
        FetchTargetStatsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .setIncludeConstraints(true)
            .addTables(tableRequest(TABLE, List.of(1L)))
            .build();
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    TableConstraintsResult result = flattenConstraints(chunks).get(0);
    assertEquals(BundleResultStatus.BUNDLE_RESULT_STATUS_ERROR, result.getStatus());
    assertEquals("planner_stats.pin.missing", result.getFailure().getCode());
  }

  @Test
  void includeConstraintsMissingSnapshotBundleYieldsNotFound() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    QueryContext ctx = queryContextWithPin("query-constraints-not-found", 600L);
    store.seed(ctx);
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(TABLE, 600L, 1L, sampleStats(TABLE, 600L, 1L), null));
    PlannerStatsBundleService service =
        createService(
            repository,
            store,
            ConstraintProvider.NONE,
            /* chunkSize= */ 5,
            /* maxTables= */ 10,
            /* maxTargets= */ 10);

    FetchTargetStatsRequest request =
        FetchTargetStatsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .setIncludeConstraints(true)
            .addTables(tableRequest(TABLE, List.of(1L)))
            .build();
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    TableConstraintsResult result = flattenConstraints(chunks).get(0);
    assertEquals(BundleResultStatus.BUNDLE_RESULT_STATUS_NOT_FOUND, result.getStatus());
    assertEquals("planner_stats.constraints.missing", result.getFailure().getCode());
    assertEquals("provider_missing", result.getFailure().getDetailsOrDefault("reason", ""));
  }

  @Test
  void includeConstraintsPrunesForeignKeysAndCheckExpressions() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    QueryContext ctx = queryContextWithPin("query-constraints-prune", 700L);
    store.seed(ctx);
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(TABLE, 700L, 1L, sampleStats(TABLE, 700L, 1L), null));

    ConstraintDefinition hiddenCheck =
        ConstraintDefinition.newBuilder()
            .setName("check_hidden")
            .setType(ConstraintType.CT_CHECK)
            .setCheckExpression("secret_col > 0")
            .addColumns(columnRef(99L, "secret_col", 1))
            .build();
    ConstraintDefinition hiddenFk =
        ConstraintDefinition.newBuilder()
            .setName("fk_hidden_table")
            .setType(ConstraintType.CT_FOREIGN_KEY)
            .addColumns(columnRef(1L, "id", 1))
            .setReferencedTableId(TABLE_TWO)
            .addReferencedColumns(columnRef(2L, "order_id", 1))
            .build();
    ConstraintProvider provider =
        new ConstraintProvider() {
          @Override
          public Optional<ConstraintSetView> constraints(
              ResourceId relationId, OptionalLong snapshotId) {
            return Optional.of(newConstraintSet(TABLE, List.of(hiddenCheck, hiddenFk)));
          }
        };

    PlannerStatsBundleService service =
        createService(
            repository,
            store,
            provider,
            /* chunkSize= */ 5,
            /* maxTables= */ 10,
            /* maxTargets= */ 10);

    FetchTargetStatsRequest request =
        FetchTargetStatsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .setIncludeConstraints(true)
            .addTables(tableRequest(TABLE, List.of(1L)))
            .build();
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    TableConstraintsResult result = flattenConstraints(chunks).get(0);
    assertEquals(BundleResultStatus.BUNDLE_RESULT_STATUS_FOUND, result.getStatus());
    assertEquals(1, result.getConstraintsCount());
    ConstraintDefinition check = result.getConstraints(0);
    assertEquals("check_hidden", check.getName());
    assertEquals("", check.getCheckExpression(), "check expression should be masked");
  }

  @Test
  void includeConstraintsKeepsPkUniqueNotNullEvenWhenColumnsNotProjected() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    QueryContext ctx = queryContextWithPin("query-constraints-keep-relation-scoped", 705L);
    store.seed(ctx);
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(TABLE, 705L, 1L, sampleStats(TABLE, 705L, 1L), null));

    ConstraintDefinition pkHiddenProjection =
        constraint("pk_not_projected", ConstraintType.CT_PRIMARY_KEY, List.of(2L));
    ConstraintProvider provider =
        new ConstraintProvider() {
          @Override
          public Optional<ConstraintSetView> constraints(
              ResourceId relationId, OptionalLong snapshotId) {
            if (relationId.equals(TABLE)
                && snapshotId.isPresent()
                && snapshotId.getAsLong() == 705L) {
              return Optional.of(newConstraintSet(TABLE, List.of(pkHiddenProjection)));
            }
            return Optional.empty();
          }
        };

    PlannerStatsBundleService service =
        createService(
            repository,
            store,
            provider,
            /* chunkSize= */ 5,
            /* maxTables= */ 10,
            /* maxTargets= */ 10);

    FetchTargetStatsRequest request =
        FetchTargetStatsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .setIncludeConstraints(true)
            .addTables(tableRequest(TABLE, List.of(1L)))
            .build();
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    TableConstraintsResult result = flattenConstraints(chunks).get(0);
    assertEquals(BundleResultStatus.BUNDLE_RESULT_STATUS_FOUND, result.getStatus());
    assertEquals(1, result.getConstraintsCount());
    assertEquals("pk_not_projected", result.getConstraints(0).getName());
    assertEquals(1, result.getConstraints(0).getColumnsCount());
    assertEquals(2L, result.getConstraints(0).getColumns(0).getColumnId());
  }

  @Test
  void includeConstraintsPrunedEmptyYieldsNotFoundWithReason() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    QueryContext ctx =
        queryContextWithPins(
            "query-constraints-pruned-empty", List.of(pin(TABLE, 710L), pin(TABLE_TWO, 711L)));
    store.seed(ctx);
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(TABLE, 710L, 1L, sampleStats(TABLE, 710L, 1L), null));

    ConstraintDefinition hiddenFk =
        ConstraintDefinition.newBuilder()
            .setName("fk_hidden_only")
            .setType(ConstraintType.CT_FOREIGN_KEY)
            .addColumns(columnRef(1L, "id", 1))
            .setReferencedTableId(TABLE_TWO)
            .addReferencedColumns(columnRef(2L, "order_id", 1))
            .build();
    ConstraintProvider provider =
        new ConstraintProvider() {
          @Override
          public Optional<ConstraintSetView> constraints(
              ResourceId relationId, OptionalLong snapshotId) {
            if (relationId.equals(TABLE)
                && snapshotId.isPresent()
                && snapshotId.getAsLong() == 710L) {
              return Optional.of(newConstraintSet(TABLE, List.of(hiddenFk)));
            }
            return Optional.empty();
          }
        };

    PlannerStatsBundleService service =
        createService(
            repository,
            store,
            provider,
            /* chunkSize= */ 5,
            /* maxTables= */ 10,
            /* maxTargets= */ 10);

    FetchTargetStatsRequest request =
        FetchTargetStatsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .setIncludeConstraints(true)
            .addTables(tableRequest(TABLE, List.of(1L)))
            .build();
    List<TargetStatsBundleChunk> chunks =
        service.streamTargets("corr", ctx, request).collect().asList().await().indefinitely();

    TableConstraintsResult result = flattenConstraints(chunks).get(0);
    assertEquals(BundleResultStatus.BUNDLE_RESULT_STATUS_NOT_FOUND, result.getStatus());
    assertEquals("planner_stats.constraints.missing", result.getFailure().getCode());
    assertEquals("pruned_empty", result.getFailure().getDetailsOrDefault("reason", ""));
  }
}
