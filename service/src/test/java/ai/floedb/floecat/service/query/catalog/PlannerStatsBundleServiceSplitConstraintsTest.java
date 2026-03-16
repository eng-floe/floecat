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
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.catalog.rpc.ConstraintType;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.query.rpc.BundleResultStatus;
import ai.floedb.floecat.query.rpc.FetchTableConstraintsRequest;
import ai.floedb.floecat.query.rpc.TableConstraintsBundleChunk;
import ai.floedb.floecat.query.rpc.TableConstraintsBundleEnd;
import ai.floedb.floecat.query.rpc.TableConstraintsResult;
import ai.floedb.floecat.scanner.spi.ConstraintProvider;
import ai.floedb.floecat.service.query.catalog.testsupport.UserObjectBundleTestSupport;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class PlannerStatsBundleServiceSplitConstraintsTest extends PlannerStatsBundleServiceTestSupport {

  @Test
  void streamConstraintsRejectsEmptyTableIds() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 10, /* maxColumns= */ 10);
    QueryContext ctx = queryContextWithPin("query-constraints-empty-ids", 495L);
    store.seed(ctx);

    FetchTableConstraintsRequest request =
        FetchTableConstraintsRequest.newBuilder().setQueryId(ctx.getQueryId()).build();

    StatusRuntimeException failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .streamConstraints("corr", ctx, request)
                    .collect()
                    .asList()
                    .await()
                    .indefinitely());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), failure.getStatus().getCode());
  }

  @Test
  void streamConstraintsRejectsTooManyTableIds() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 1, /* maxColumns= */ 10);
    QueryContext ctx = queryContextWithPin("query-constraints-too-many-ids", 496L);
    store.seed(ctx);

    FetchTableConstraintsRequest request =
        FetchTableConstraintsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .addTableIds(TABLE)
            .addTableIds(TABLE_TWO)
            .build();

    StatusRuntimeException failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .streamConstraints("corr", ctx, request)
                    .collect()
                    .asList()
                    .await()
                    .indefinitely());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), failure.getStatus().getCode());
  }

  @Test
  void streamConstraintsRejectsBlankTableId() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    PlannerStatsBundleService service =
        createService(
            repository, store, /* chunkSize= */ 5, /* maxTables= */ 10, /* maxColumns= */ 10);
    QueryContext ctx = queryContextWithPin("query-constraints-blank-id", 497L);
    store.seed(ctx);

    ResourceId blankId =
        ResourceId.newBuilder()
            .setAccountId(TABLE.getAccountId())
            .setKind(ResourceKind.RK_TABLE)
            .setId("   ")
            .build();
    FetchTableConstraintsRequest request =
        FetchTableConstraintsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .addTableIds(blankId)
            .build();

    StatusRuntimeException failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .streamConstraints("corr", ctx, request)
                    .collect()
                    .asList()
                    .await()
                    .indefinitely());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), failure.getStatus().getCode());
  }

  @Test
  void streamConstraintsReturnsPerTableResults() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    QueryContext ctx =
        queryContextWithPins(
            "query-constraints-only", List.of(pin(TABLE, 800L), pin(TABLE_TWO, 801L)));
    store.seed(ctx);

    ConstraintDefinition pkUsers =
        constraint("pk_users_only", ConstraintType.CT_PRIMARY_KEY, List.of(1L));
    ConstraintDefinition pkOrders =
        constraint("pk_orders_only", ConstraintType.CT_PRIMARY_KEY, List.of(2L));
    ConstraintProvider provider =
        new ConstraintProvider() {
          @Override
          public Optional<ConstraintSetView> constraints(
              ResourceId relationId, OptionalLong snapshotId) {
            if (snapshotId.isEmpty()) {
              return Optional.empty();
            }
            if (relationId.equals(TABLE) && snapshotId.getAsLong() == 800L) {
              return Optional.of(newConstraintSet(TABLE, List.of(pkUsers)));
            }
            if (relationId.equals(TABLE_TWO) && snapshotId.getAsLong() == 801L) {
              return Optional.of(newConstraintSet(TABLE_TWO, List.of(pkOrders)));
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
            /* maxColumns= */ 20);
    FetchTableConstraintsRequest request =
        FetchTableConstraintsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .addTableIds(TABLE)
            .addTableIds(TABLE_TWO)
            .build();

    List<TableConstraintsBundleChunk> chunks =
        service.streamConstraints("corr", ctx, request).collect().asList().await().indefinitely();
    List<TableConstraintsResult> results = flattenConstraintChunks(chunks);
    assertEquals(2, results.size());
    assertEquals(BundleResultStatus.BUNDLE_RESULT_STATUS_FOUND, results.get(0).getStatus());
    assertEquals(BundleResultStatus.BUNDLE_RESULT_STATUS_FOUND, results.get(1).getStatus());

    TableConstraintsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(2L, end.getRequestedTables());
    assertEquals(2L, end.getReturnedTables());
    assertEquals(0L, end.getNotFoundTables());
    assertEquals(0L, end.getErrorTables());
  }

  @Test
  void streamConstraintsDedupesDuplicateRequestedTableIds() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    QueryContext ctx = queryContextWithPin("query-constraints-only-dedupe", 830L);
    store.seed(ctx);

    ConstraintDefinition pkUsers =
        constraint("pk_users_dedupe", ConstraintType.CT_PRIMARY_KEY, List.of(1L));
    ConstraintProvider provider =
        new ConstraintProvider() {
          @Override
          public Optional<ConstraintSetView> constraints(
              ResourceId relationId, OptionalLong snapshotId) {
            if (relationId.equals(TABLE)
                && snapshotId.isPresent()
                && snapshotId.getAsLong() == 830L) {
              return Optional.of(newConstraintSet(TABLE, List.of(pkUsers)));
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
            /* maxColumns= */ 20);
    FetchTableConstraintsRequest request =
        FetchTableConstraintsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .addTableIds(TABLE)
            .addTableIds(TABLE)
            .build();

    List<TableConstraintsBundleChunk> chunks =
        service.streamConstraints("corr", ctx, request).collect().asList().await().indefinitely();
    List<TableConstraintsResult> results = flattenConstraintChunks(chunks);
    assertEquals(1, results.size(), "duplicate table_ids should be emitted once");
    assertEquals(BundleResultStatus.BUNDLE_RESULT_STATUS_FOUND, results.get(0).getStatus());
    assertEquals("pk_users_dedupe", results.get(0).getConstraints(0).getName());

    TableConstraintsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(1L, end.getRequestedTables(), "deduped request count should be reflected in end");
    assertEquals(1L, end.getReturnedTables());
    assertEquals(0L, end.getNotFoundTables());
    assertEquals(0L, end.getErrorTables());
  }

  @Test
  void streamConstraintsChunkingRespectsMaxResultsPerChunk() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    QueryContext ctx =
        queryContextWithPins(
            "query-constraints-only-chunking", List.of(pin(TABLE, 840L), pin(TABLE_TWO, 841L)));
    store.seed(ctx);

    ConstraintDefinition pkUsers =
        constraint("pk_users_chunk", ConstraintType.CT_PRIMARY_KEY, List.of(1L));
    ConstraintDefinition pkOrders =
        constraint("pk_orders_chunk", ConstraintType.CT_PRIMARY_KEY, List.of(2L));
    ConstraintProvider provider =
        new ConstraintProvider() {
          @Override
          public Optional<ConstraintSetView> constraints(
              ResourceId relationId, OptionalLong snapshotId) {
            if (snapshotId.isEmpty()) {
              return Optional.empty();
            }
            if (relationId.equals(TABLE) && snapshotId.getAsLong() == 840L) {
              return Optional.of(newConstraintSet(TABLE, List.of(pkUsers)));
            }
            if (relationId.equals(TABLE_TWO) && snapshotId.getAsLong() == 841L) {
              return Optional.of(newConstraintSet(TABLE_TWO, List.of(pkOrders)));
            }
            return Optional.empty();
          }
        };

    PlannerStatsBundleService service =
        createService(
            repository,
            store,
            provider,
            /* chunkSize= */ 1,
            /* maxTables= */ 10,
            /* maxColumns= */ 20);
    FetchTableConstraintsRequest request =
        FetchTableConstraintsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .addTableIds(TABLE)
            .addTableIds(TABLE_TWO)
            .build();

    List<TableConstraintsBundleChunk> chunks =
        service.streamConstraints("corr", ctx, request).collect().asList().await().indefinitely();
    List<TableConstraintsBundleChunk> batches = new ArrayList<>();
    for (TableConstraintsBundleChunk chunk : chunks) {
      if (chunk.hasBatch()) {
        batches.add(chunk);
      }
    }
    assertEquals(2, batches.size(), "chunkSize=1 should emit one table result per batch");
    assertEquals(1, batches.get(0).getBatch().getConstraintsCount());
    assertEquals(1, batches.get(1).getBatch().getConstraintsCount());

    TableConstraintsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(2L, end.getRequestedTables());
    assertEquals(2L, end.getReturnedTables());
    assertEquals(0L, end.getNotFoundTables());
    assertEquals(0L, end.getErrorTables());
  }

  @Test
  void streamConstraintsKeepsPkUniqueNotNullWithoutColumnProjection() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    QueryContext ctx = queryContextWithPin("query-constraints-only-keep-relation-scoped", 850L);
    store.seed(ctx);

    ConstraintDefinition pkHiddenProjection =
        constraint("pk_split_not_projected", ConstraintType.CT_PRIMARY_KEY, List.of(99L));
    ConstraintProvider provider =
        new ConstraintProvider() {
          @Override
          public Optional<ConstraintSetView> constraints(
              ResourceId relationId, OptionalLong snapshotId) {
            if (relationId.equals(TABLE)
                && snapshotId.isPresent()
                && snapshotId.getAsLong() == 850L) {
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
            /* chunkSize= */ 10,
            /* maxTables= */ 10,
            /* maxColumns= */ 20);
    FetchTableConstraintsRequest request =
        FetchTableConstraintsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .addTableIds(TABLE)
            .build();

    List<TableConstraintsBundleChunk> chunks =
        service.streamConstraints("corr", ctx, request).collect().asList().await().indefinitely();
    List<TableConstraintsResult> results = flattenConstraintChunks(chunks);
    assertEquals(1, results.size());
    assertEquals(BundleResultStatus.BUNDLE_RESULT_STATUS_FOUND, results.get(0).getStatus());
    assertEquals(1, results.get(0).getConstraintsCount());
    assertEquals("pk_split_not_projected", results.get(0).getConstraints(0).getName());
    assertEquals(1, results.get(0).getConstraints(0).getColumnsCount());
    assertEquals(99L, results.get(0).getConstraints(0).getColumns(0).getColumnId());
  }

  @Test
  void streamConstraintsProviderEmptyYieldsNotFoundWithReason() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    QueryContext ctx = queryContextWithPin("query-constraints-only-provider-empty", 860L);
    store.seed(ctx);

    ConstraintProvider provider =
        new ConstraintProvider() {
          @Override
          public Optional<ConstraintSetView> constraints(
              ResourceId relationId, OptionalLong snapshotId) {
            if (relationId.equals(TABLE)
                && snapshotId.isPresent()
                && snapshotId.getAsLong() == 860L) {
              return Optional.of(newConstraintSet(TABLE, List.of()));
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
            /* maxColumns= */ 20);
    FetchTableConstraintsRequest request =
        FetchTableConstraintsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .addTableIds(TABLE)
            .build();

    List<TableConstraintsBundleChunk> chunks =
        service.streamConstraints("corr", ctx, request).collect().asList().await().indefinitely();
    List<TableConstraintsResult> results = flattenConstraintChunks(chunks);
    assertEquals(1, results.size());
    assertEquals(BundleResultStatus.BUNDLE_RESULT_STATUS_NOT_FOUND, results.get(0).getStatus());
    assertEquals("planner_stats.constraints.missing", results.get(0).getFailure().getCode());
    assertEquals("provider_empty", results.get(0).getFailure().getDetailsOrDefault("reason", ""));
  }

  @Test
  void streamConstraintsPrunesForeignKeysOutsideRequestedTables() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    QueryContext ctx =
        queryContextWithPins(
            "query-constraints-only-prune", List.of(pin(TABLE, 820L), pin(TABLE_TWO, 821L)));
    store.seed(ctx);

    ConstraintDefinition hiddenFk =
        ConstraintDefinition.newBuilder()
            .setName("fk_hidden_in_split")
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
                && snapshotId.getAsLong() == 820L) {
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
            /* chunkSize= */ 10,
            /* maxTables= */ 10,
            /* maxColumns= */ 20);
    FetchTableConstraintsRequest request =
        FetchTableConstraintsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .addTableIds(TABLE)
            .build();

    List<TableConstraintsBundleChunk> chunks =
        service.streamConstraints("corr", ctx, request).collect().asList().await().indefinitely();
    List<TableConstraintsResult> results = flattenConstraintChunks(chunks);
    assertEquals(1, results.size());
    assertEquals(BundleResultStatus.BUNDLE_RESULT_STATUS_NOT_FOUND, results.get(0).getStatus());
    assertEquals("pruned_empty", results.get(0).getFailure().getDetailsOrDefault("reason", ""));
  }

  @Test
  void streamConstraintsMissingPinYieldsError() {
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    StatsRepository repository = createRepository();
    QueryContext ctx = queryContextWithoutPin("query-constraints-only-no-pin");
    store.seed(ctx);

    PlannerStatsBundleService service =
        createService(
            repository,
            store,
            ConstraintProvider.NONE,
            /* chunkSize= */ 10,
            /* maxTables= */ 10,
            /* maxColumns= */ 20);
    FetchTableConstraintsRequest request =
        FetchTableConstraintsRequest.newBuilder()
            .setQueryId(ctx.getQueryId())
            .addTableIds(TABLE)
            .build();

    List<TableConstraintsBundleChunk> chunks =
        service.streamConstraints("corr", ctx, request).collect().asList().await().indefinitely();
    List<TableConstraintsResult> results = flattenConstraintChunks(chunks);
    assertEquals(1, results.size());
    assertEquals(BundleResultStatus.BUNDLE_RESULT_STATUS_ERROR, results.get(0).getStatus());
    assertEquals("planner_stats.pin.missing", results.get(0).getFailure().getCode());
  }
}
