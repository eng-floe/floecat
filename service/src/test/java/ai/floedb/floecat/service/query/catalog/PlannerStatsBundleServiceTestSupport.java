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

import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.ConstraintColumnRef;
import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.catalog.rpc.ConstraintType;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.query.rpc.ColumnStatsBundleChunk;
import ai.floedb.floecat.query.rpc.ColumnStatsResult;
import ai.floedb.floecat.query.rpc.FetchColumnStatsRequest;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.query.rpc.SnapshotSet;
import ai.floedb.floecat.query.rpc.TableColumnStatsRequest;
import ai.floedb.floecat.query.rpc.TableConstraintsBundleChunk;
import ai.floedb.floecat.query.rpc.TableConstraintsResult;
import ai.floedb.floecat.scanner.spi.ConstraintProvider;
import ai.floedb.floecat.service.query.catalog.testsupport.UserObjectBundleTestSupport;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

abstract class PlannerStatsBundleServiceTestSupport {

  protected static final ResourceId CATALOG =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("catalog")
          .setKind(ResourceKind.RK_CATALOG)
          .build();

  protected static final ResourceId TABLE =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("users")
          .setKind(ResourceKind.RK_TABLE)
          .build();

  protected static final ResourceId TABLE_TWO =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("orders")
          .setKind(ResourceKind.RK_TABLE)
          .build();

  protected static PlannerStatsBundleService createService(
      StatsRepository repository,
      UserObjectBundleTestSupport.TestQueryContextStore store,
      int chunkSize,
      int maxTables,
      int maxColumns) {
    return createService(
        repository, store, ConstraintProvider.NONE, chunkSize, maxTables, maxColumns);
  }

  protected static PlannerStatsBundleService createService(
      StatsRepository repository,
      UserObjectBundleTestSupport.TestQueryContextStore store,
      ConstraintProvider constraintProvider,
      int chunkSize,
      int maxTables,
      int maxColumns) {
    StatsProviderFactory factory = new StatsProviderFactory(repository, store);
    return PlannerStatsBundleService.forTesting(
        factory, constraintProvider, repository, maxTables, maxColumns, chunkSize);
  }

  protected static StatsRepository createRepository() {
    return new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
  }

  protected static ColumnStats sampleStats(ResourceId tableId, long snapshotId, long columnId) {
    return ColumnStats.newBuilder()
        .setTableId(tableId)
        .setSnapshotId(snapshotId)
        .setColumnId(columnId)
        .setColumnName("col" + columnId)
        .setValueCount(columnId * 10)
        .setNullCount(columnId)
        .build();
  }

  protected static FetchColumnStatsRequest requestFor(
      String queryId, ResourceId tableId, List<Long> columnIds) {
    return FetchColumnStatsRequest.newBuilder()
        .setQueryId(queryId)
        .addTables(tableRequest(tableId, columnIds))
        .build();
  }

  protected static TableColumnStatsRequest tableRequest(ResourceId tableId, List<Long> columnIds) {
    return TableColumnStatsRequest.newBuilder()
        .setTableId(tableId)
        .addAllColumnIds(columnIds)
        .build();
  }

  protected static QueryContext queryContextWithPin(String queryId, long snapshotId) {
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

  protected static QueryContext queryContextWithoutPin(String queryId) {
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

  protected static QueryContext queryContextWithPins(String queryId, List<SnapshotPin> pins) {
    SnapshotSet set = SnapshotSet.newBuilder().addAllPins(pins).build();
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

  protected static SnapshotPin pin(ResourceId tableId, long snapshotId) {
    return SnapshotPin.newBuilder().setTableId(tableId).setSnapshotId(snapshotId).build();
  }

  protected static List<ColumnStatsResult> flatten(List<ColumnStatsBundleChunk> chunks) {
    List<ColumnStatsResult> results = new ArrayList<>();
    for (ColumnStatsBundleChunk chunk : chunks) {
      if (chunk.hasBatch()) {
        results.addAll(chunk.getBatch().getColumnsList());
      }
    }
    return results;
  }

  protected static List<TableConstraintsResult> flattenConstraints(
      List<ColumnStatsBundleChunk> chunks) {
    List<TableConstraintsResult> results = new ArrayList<>();
    for (ColumnStatsBundleChunk chunk : chunks) {
      if (chunk.hasBatch()) {
        results.addAll(chunk.getBatch().getConstraintsList());
      }
    }
    return results;
  }

  protected static List<TableConstraintsResult> flattenConstraintChunks(
      List<TableConstraintsBundleChunk> chunks) {
    List<TableConstraintsResult> results = new ArrayList<>();
    for (TableConstraintsBundleChunk chunk : chunks) {
      if (chunk.hasBatch()) {
        results.addAll(chunk.getBatch().getConstraintsList());
      }
    }
    return results;
  }

  protected static ConstraintDefinition constraint(
      String name, ConstraintType type, List<Long> columnIds) {
    ConstraintDefinition.Builder builder =
        ConstraintDefinition.newBuilder().setName(name).setType(type);
    int ordinal = 1;
    for (Long columnId : columnIds) {
      builder.addColumns(columnRef(columnId, "c" + columnId, ordinal++));
    }
    return builder.build();
  }

  protected static ConstraintColumnRef columnRef(long columnId, String name, int ordinal) {
    return ConstraintColumnRef.newBuilder()
        .setColumnId(columnId)
        .setColumnName(name)
        .setOrdinal(ordinal)
        .build();
  }

  protected static ConstraintProvider.ConstraintSetView newConstraintSet(
      ResourceId tableId, List<ConstraintDefinition> constraints) {
    return new ConstraintProvider.ConstraintSetView() {
      @Override
      public ResourceId relationId() {
        return tableId;
      }

      @Override
      public List<ConstraintDefinition> constraints() {
        return constraints;
      }
    };
  }

  protected static final class SmartScanOnlyStatsRepository extends StatsRepository {
    protected SmartScanOnlyStatsRepository(PointerStore pointerStore, BlobStore blobStore) {
      super(
          pointerStore,
          blobStore,
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

  protected static final class CappingStatsRepository extends StatsRepository {
    private final AtomicInteger batchCalls = new AtomicInteger();

    protected CappingStatsRepository(PointerStore pointerStore, BlobStore blobStore) {
      super(
          pointerStore,
          blobStore,
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

  protected static final class ThrowingStatsRepository extends StatsRepository {
    protected ThrowingStatsRepository(
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
