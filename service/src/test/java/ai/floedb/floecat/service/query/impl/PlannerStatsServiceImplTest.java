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

package ai.floedb.floecat.service.query.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.query.rpc.ColumnStatsBundleChunk;
import ai.floedb.floecat.query.rpc.ColumnStatsBundleEnd;
import ai.floedb.floecat.query.rpc.ColumnStatsResult;
import ai.floedb.floecat.query.rpc.FetchColumnStatsRequest;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.query.rpc.SnapshotSet;
import ai.floedb.floecat.query.rpc.StatsStatus;
import ai.floedb.floecat.query.rpc.TableColumnStatsRequest;
import ai.floedb.floecat.service.query.catalog.PlannerStatsBundleService;
import ai.floedb.floecat.service.query.catalog.StatsProviderFactory;
import ai.floedb.floecat.service.query.catalog.testsupport.UserObjectBundleTestSupport;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class PlannerStatsServiceImplTest {

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
  void queryNotFoundIsReported() {
    StatsRepository repository = createRepository();
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    PlannerStatsBundleService bundles = createBundleService(repository, store);
    PlannerStatsServiceImpl service = createServiceImpl(bundles, store);

    FetchColumnStatsRequest request = requestFor("missing-query", TABLE, List.of(1L));
    PrincipalContext principal = principal("corr-missing", true);

    StatusRuntimeException failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                withPrincipal(
                    principal,
                    () ->
                        service.getColumnStats(request).collect().asList().await().indefinitely()));
    assertEquals(Status.NOT_FOUND.getCode(), failure.getStatus().getCode());
  }

  @Test
  void inactiveQueryIsRejected() {
    StatsRepository repository = createRepository();
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    PlannerStatsBundleService bundles = createBundleService(repository, store);
    PlannerStatsServiceImpl service = createServiceImpl(bundles, store);

    QueryContext ctx = queryContextWithPin("query-inactive", 200L, QueryContext.State.ENDED_ABORT);
    store.seed(ctx);

    FetchColumnStatsRequest request = requestFor(ctx.getQueryId(), TABLE, List.of(1L));
    PrincipalContext principal = principal("corr-inactive", true);

    StatusRuntimeException failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                withPrincipal(
                    principal,
                    () ->
                        service.getColumnStats(request).collect().asList().await().indefinitely()));
    assertEquals(Status.FAILED_PRECONDITION.getCode(), failure.getStatus().getCode());
  }

  @Test
  void missingPermissionIsDenied() {
    StatsRepository repository = createRepository();
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    PlannerStatsBundleService bundles = createBundleService(repository, store);
    PlannerStatsServiceImpl service = createServiceImpl(bundles, store);

    QueryContext ctx = queryContextWithPin("query-auth", 201L, QueryContext.State.ACTIVE);
    store.seed(ctx);

    FetchColumnStatsRequest request = requestFor(ctx.getQueryId(), TABLE, List.of(1L));
    PrincipalContext principal = principal("corr-auth", false);

    StatusRuntimeException failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                withPrincipal(
                    principal,
                    () ->
                        service.getColumnStats(request).collect().asList().await().indefinitely()));
    assertEquals(Status.PERMISSION_DENIED.getCode(), failure.getStatus().getCode());
  }

  @Test
  void happyPathStreamsStats() {
    StatsRepository repository = createRepository();
    UserObjectBundleTestSupport.TestQueryContextStore store =
        new UserObjectBundleTestSupport.TestQueryContextStore();
    PlannerStatsBundleService bundles = createBundleService(repository, store);
    PlannerStatsServiceImpl service = createServiceImpl(bundles, store);

    QueryContext ctx = queryContextWithPin("query-ok", 202L, QueryContext.State.ACTIVE);
    store.seed(ctx);
    repository.putColumnStats(TABLE, 202L, sampleStats(TABLE, 202L, 1L));

    FetchColumnStatsRequest request = requestFor(ctx.getQueryId(), TABLE, List.of(1L));
    List<ColumnStatsBundleChunk> chunks =
        withPrincipal(
            principal("corr-ok", true),
            () -> service.getColumnStats(request).collect().asList().await().indefinitely());

    List<ColumnStatsResult> results = flatten(chunks);
    assertEquals(1, results.size());
    assertEquals(StatsStatus.STATS_STATUS_FOUND, results.get(0).getStatus());

    ColumnStatsBundleEnd end = chunks.get(chunks.size() - 1).getEnd();
    assertEquals(1L, end.getReturnedColumns());
    assertEquals(0L, end.getNotFoundColumns());
  }

  private static PlannerStatsServiceImpl createServiceImpl(
      PlannerStatsBundleService bundles, UserObjectBundleTestSupport.TestQueryContextStore store) {
    PlannerStatsServiceImpl service = new PlannerStatsServiceImpl();
    service.principal = new PrincipalProvider();
    service.authz = new Authorizer();
    service.queryStore = store;
    service.bundles = bundles;
    return service;
  }

  private static PlannerStatsBundleService createBundleService(
      StatsRepository repository, UserObjectBundleTestSupport.TestQueryContextStore store) {
    StatsProviderFactory factory = new StatsProviderFactory(repository, store);
    return PlannerStatsBundleService.forTesting(
        factory, repository, /* maxTables= */ 10, /* maxColumns= */ 10, /* chunkSize= */ 5);
  }

  private static StatsRepository createRepository() {
    return new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
  }

  private static PrincipalContext principal(String correlationId, boolean withPermission) {
    PrincipalContext.Builder builder =
        PrincipalContext.newBuilder()
            .setAccountId(TABLE.getAccountId())
            .setSubject("tester")
            .setCorrelationId(correlationId);
    if (withPermission) {
      builder.addPermissions("catalog.read");
    }
    return builder.build();
  }

  private static <T> T withPrincipal(PrincipalContext principal, Supplier<T> action) {
    Context ctx = Context.current().withValue(PrincipalProvider.KEY, principal);
    Context previous = ctx.attach();
    try {
      return action.get();
    } finally {
      previous.detach(ctx);
    }
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

  private static QueryContext queryContextWithPin(
      String queryId, long snapshotId, QueryContext.State state) {
    SnapshotPin pin = SnapshotPin.newBuilder().setTableId(TABLE).setSnapshotId(snapshotId).build();
    SnapshotSet set = SnapshotSet.newBuilder().addPins(pin).build();
    PrincipalContext principal =
        PrincipalContext.newBuilder()
            .setAccountId(TABLE.getAccountId())
            .setSubject("tester")
            .setCorrelationId(queryId)
            .build();
    return QueryContext.builder()
        .queryId(queryId)
        .principal(principal)
        .snapshotSet(set.toByteArray())
        .createdAtMs(1)
        .expiresAtMs(1_000)
        .state(state)
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
}
