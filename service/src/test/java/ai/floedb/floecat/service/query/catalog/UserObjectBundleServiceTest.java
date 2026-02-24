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

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.rpc.TableStats;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.query.rpc.ColumnFailureCode;
import ai.floedb.floecat.query.rpc.ColumnInfo;
import ai.floedb.floecat.query.rpc.ColumnResult;
import ai.floedb.floecat.query.rpc.ColumnStatus;
import ai.floedb.floecat.query.rpc.EngineSpecific;
import ai.floedb.floecat.query.rpc.Origin;
import ai.floedb.floecat.query.rpc.RelationInfo;
import ai.floedb.floecat.query.rpc.RelationResolution;
import ai.floedb.floecat.query.rpc.ResolutionStatus;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.query.rpc.SnapshotSet;
import ai.floedb.floecat.query.rpc.TableReferenceCandidate;
import ai.floedb.floecat.query.rpc.UserObjectsBundleChunk;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.service.context.EngineContextProvider;
import ai.floedb.floecat.service.context.impl.InboundContextInterceptor;
import ai.floedb.floecat.service.query.catalog.testsupport.UserObjectBundleTestSupport;
import ai.floedb.floecat.service.query.catalog.testsupport.UserObjectBundleTestSupport.CancellingSubscriber;
import ai.floedb.floecat.service.query.catalog.testsupport.UserObjectBundleTestSupport.FakeCatalogOverlay;
import ai.floedb.floecat.service.query.catalog.testsupport.UserObjectBundleTestSupport.TestQueryContextStore;
import ai.floedb.floecat.service.query.catalog.testsupport.UserObjectBundleTestSupport.TestQueryInputResolver;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.query.resolver.QueryInputResolver;
import ai.floedb.floecat.service.query.resolver.QueryInputResolver.ResolutionResult;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import ai.floedb.floecat.systemcatalog.spi.decorator.ColumnDecoration;
import ai.floedb.floecat.systemcatalog.spi.decorator.DecorationException;
import ai.floedb.floecat.systemcatalog.spi.decorator.EngineMetadataDecorator;
import ai.floedb.floecat.systemcatalog.spi.decorator.EngineMetadataDecoratorProvider;
import com.google.protobuf.Timestamp;
import io.grpc.Context;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UserObjectBundleServiceTest {

  private static final ResourceId DEFAULT_CATALOG =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("catalog")
          .setKind(ResourceKind.RK_CATALOG)
          .build();

  private static final ResourceId TABLE_A =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("TABLE_A")
          .setKind(ResourceKind.RK_TABLE)
          .build();

  private static final ResourceId TABLE_B =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("TABLE_B")
          .setKind(ResourceKind.RK_TABLE)
          .build();

  private static final ResourceId TABLE_C =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("TABLE_C")
          .setKind(ResourceKind.RK_TABLE)
          .build();

  private static final ResourceId SYSTEM_TABLE =
      ResourceId.newBuilder()
          .setAccountId("sys")
          .setId("SYSTEM_TABLE")
          .setKind(ResourceKind.RK_TABLE)
          .build();

  private static final long TABLE_A_SNAPSHOT_ID = 123L;
  private static final SnapshotPin TABLE_A_PIN =
      SnapshotPin.newBuilder().setTableId(TABLE_A).setSnapshotId(TABLE_A_SNAPSHOT_ID).build();
  private static final SnapshotSet INITIAL_SNAPSHOT =
      SnapshotSet.newBuilder().addPins(TABLE_A_PIN).build();

  private final FakeCatalogOverlay overlay = new FakeCatalogOverlay();
  private final EngineMetadataDecoratorProvider decoratorProvider = ctx -> Optional.empty();
  private final EngineContextProvider engineContextProvider = new EngineContextProvider();
  private StatsRepository statsRepository;
  private StatsProviderFactory statsFactory;
  private TestQueryInputResolver resolver;
  private TestQueryContextStore queryStore;
  private UserObjectBundleService service;

  private final QueryContext ctx =
      QueryContext.builder()
          .queryId("q-1")
          .principal(
              PrincipalContext.newBuilder()
                  .setAccountId("acct")
                  .setSubject("tester")
                  .setCorrelationId("cid")
                  .build())
          .snapshotSet(INITIAL_SNAPSHOT.toByteArray())
          .createdAtMs(1)
          .expiresAtMs(1000)
          .state(QueryContext.State.ACTIVE)
          .version(1)
          .queryDefaultCatalogId(DEFAULT_CATALOG)
          .build();

  @BeforeEach
  void setUp() {
    resolver = new TestQueryInputResolver();
    queryStore = new TestQueryContextStore();
    overlay.clear();
    overlay.registerTable(
        TABLE_A,
        UserObjectBundleTestSupport.schemaFor("id_a"),
        NameRef.newBuilder().setCatalog("cat").setName("a").build());
    overlay.registerTable(
        TABLE_B,
        UserObjectBundleTestSupport.schemaFor("id_b"),
        NameRef.newBuilder().setCatalog("cat").setName("b").build());
    overlay.registerCatalog(DEFAULT_CATALOG, "cat");
    queryStore.seed(ctx);
    statsRepository = new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    statsFactory = new StatsProviderFactory(statsRepository, queryStore);
    service =
        new UserObjectBundleService(
            overlay,
            resolver,
            queryStore,
            statsFactory,
            decoratorProvider,
            engineContextProvider,
            false,
            "localhost",
            47470,
            false,
            "test");
  }

  @Test
  void streamsResolutionChunksInCandidateOrder() {
    TableReferenceCandidate a =
        TableReferenceCandidate.newBuilder()
            .addCandidates(QueryInput.newBuilder().setTableId(TABLE_A))
            .build();
    TableReferenceCandidate b =
        TableReferenceCandidate.newBuilder()
            .addCandidates(QueryInput.newBuilder().setTableId(TABLE_B))
            .build();

    List<UserObjectsBundleChunk> chunks =
        service.stream("cid", ctx, List.of(a, b)).collect().asList().await().indefinitely();

    assertThat(chunks).hasSize(3);
    assertThat(chunks.get(0).hasHeader()).isTrue();
    UserObjectsBundleChunk resolutions = chunks.get(1);
    assertThat(resolutions.hasResolutions()).isTrue();
    assertThat(resolutions.getResolutions().getItemsCount()).isEqualTo(2);
    RelationResolution first = resolutions.getResolutions().getItems(0);
    RelationResolution second = resolutions.getResolutions().getItems(1);
    assertThat(first.getInputIndex()).isEqualTo(0);
    assertThat(second.getInputIndex()).isEqualTo(1);
    assertThat(first.getStatus()).isEqualTo(ResolutionStatus.RESOLUTION_STATUS_FOUND);
    assertThat(second.getStatus()).isEqualTo(ResolutionStatus.RESOLUTION_STATUS_FOUND);
    assertThat(first.getRelation().getRelationId()).isEqualTo(TABLE_A);
    assertThat(second.getRelation().getRelationId()).isEqualTo(TABLE_B);

    UserObjectsBundleChunk end = chunks.get(2);
    assertThat(end.getEnd().getResolutionCount()).isEqualTo(2);
    assertThat(end.getEnd().getFoundCount()).isEqualTo(2);
    assertThat(end.getEnd().getNotFoundCount()).isZero();

    assertThat(queryStore.updateCount()).isEqualTo(1);
    assertThat(resolver.recordedInputs())
        .containsExactly(
            List.of(QueryInput.newBuilder().setTableId(TABLE_A).build()),
            List.of(QueryInput.newBuilder().setTableId(TABLE_B).build()));
  }

  @Test
  void relationIncludesStatsWhenPinned() {
    TableStats stats =
        TableStats.newBuilder()
            .setTableId(TABLE_A)
            .setSnapshotId(TABLE_A_SNAPSHOT_ID)
            .setRowCount(22)
            .build();
    statsRepository.putTableStats(TABLE_A, TABLE_A_SNAPSHOT_ID, stats);

    TableReferenceCandidate candidate =
        TableReferenceCandidate.newBuilder()
            .addCandidates(QueryInput.newBuilder().setTableId(TABLE_A))
            .build();

    List<UserObjectsBundleChunk> chunks =
        service.stream("cid", ctx, List.of(candidate)).collect().asList().await().indefinitely();

    RelationResolution resolution = chunks.get(1).getResolutions().getItems(0);
    RelationInfo relation = resolution.getRelation();
    assertThat(relation.hasStats()).isTrue();
    assertThat(relation.getStats().getRowCount()).isEqualTo(stats.getRowCount());
    assertThat(relation.getStats().getTotalSizeBytes()).isEqualTo(0L);
  }

  @Test
  void statsAvailableWhenPinAddedDuringBundle() {
    QueryContext noPinCtx =
        QueryContext.builder()
            .queryId("q-no-pin")
            .principal(ctx.getPrincipal())
            .snapshotSet(SnapshotSet.getDefaultInstance().toByteArray())
            .createdAtMs(ctx.getCreatedAtMs())
            .expiresAtMs(ctx.getExpiresAtMs())
            .state(QueryContext.State.ACTIVE)
            .version(1)
            .queryDefaultCatalogId(DEFAULT_CATALOG)
            .build();

    TestQueryInputResolver deterministicResolver = new TestQueryInputResolver(99);
    TestQueryContextStore localStore = new TestQueryContextStore();
    localStore.seed(noPinCtx);
    StatsRepository localStatsRepository =
        new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    StatsProviderFactory localStatsFactory =
        new StatsProviderFactory(localStatsRepository, localStore);
    UserObjectBundleService localService =
        new UserObjectBundleService(
            overlay,
            deterministicResolver,
            localStore,
            localStatsFactory,
            decoratorProvider,
            engineContextProvider,
            false,
            "localhost",
            47470,
            false,
            "test");

    long expectedSnapshotId = 99L;
    TableStats stats =
        TableStats.newBuilder()
            .setTableId(TABLE_A)
            .setSnapshotId(expectedSnapshotId)
            .setRowCount(42)
            .setTotalSizeBytes(99L)
            .build();
    localStatsRepository.putTableStats(TABLE_A, expectedSnapshotId, stats);

    TableReferenceCandidate candidate =
        TableReferenceCandidate.newBuilder()
            .addCandidates(QueryInput.newBuilder().setTableId(TABLE_A))
            .build();

    List<UserObjectsBundleChunk> chunks =
        localService.stream("cid", noPinCtx, List.of(candidate))
            .collect()
            .asList()
            .await()
            .indefinitely();

    RelationInfo relation = chunks.get(1).getResolutions().getItems(0).getRelation();
    assertThat(relation.hasStats()).isTrue();
    assertThat(relation.getStats().getRowCount()).isEqualTo(stats.getRowCount());
    assertThat(relation.getStats().getTotalSizeBytes()).isEqualTo(stats.getTotalSizeBytes());
    assertThat(localStore.updateCount()).isEqualTo(1);
  }

  @Test
  void chunkUpdateHappensOncePerChunk() {
    int chunkSize = 25;
    int totalCandidates = chunkSize + 1;
    List<TableReferenceCandidate> candidates = new ArrayList<>(totalCandidates);
    for (int i = 0; i < totalCandidates; i++) {
      candidates.add(
          TableReferenceCandidate.newBuilder()
              .addCandidates(QueryInput.newBuilder().setTableId(TABLE_A))
              .build());
    }

    QueryContext chunkCtx =
        QueryContext.builder()
            .queryId("q-chunk")
            .principal(ctx.getPrincipal())
            .snapshotSet(SnapshotSet.getDefaultInstance().toByteArray())
            .createdAtMs(ctx.getCreatedAtMs())
            .expiresAtMs(ctx.getExpiresAtMs())
            .state(QueryContext.State.ACTIVE)
            .version(1)
            .queryDefaultCatalogId(DEFAULT_CATALOG)
            .build();
    queryStore.seed(chunkCtx);
    List<UserObjectsBundleChunk> chunks =
        service.stream("cid", chunkCtx, candidates).collect().asList().await().indefinitely();

    assertThat(
            chunks.stream()
                .filter(UserObjectsBundleChunk::hasResolutions)
                .mapToInt(chunk -> chunk.getResolutions().getItemsCount())
                .sum())
        .isEqualTo(totalCandidates);
    assertThat(queryStore.updateCount()).isGreaterThan(0);
    assertThat(queryStore.updateCount()).isLessThan(totalCandidates);
  }

  @Test
  void commitSkippedWhenPinsEmpty() {
    QueryInputResolver emptyResolver =
        new QueryInputResolver() {
          @Override
          public ResolutionResult resolveInputs(
              String correlationId, List<QueryInput> inputs, Optional<Timestamp> asOfDefault) {
            return new ResolutionResult(
                List.of(inputs.get(0).getTableId()), SnapshotSet.getDefaultInstance(), null);
          }
        };
    service =
        new UserObjectBundleService(
            overlay,
            emptyResolver,
            queryStore,
            statsFactory,
            decoratorProvider,
            engineContextProvider,
            false,
            "localhost",
            47470,
            false,
            "test");

    TableReferenceCandidate candidate =
        TableReferenceCandidate.newBuilder()
            .addCandidates(QueryInput.newBuilder().setTableId(TABLE_A))
            .build();

    List<UserObjectsBundleChunk> chunks =
        service.stream("cid", ctx, List.of(candidate)).collect().asList().await().indefinitely();

    RelationResolution resolution = chunks.get(1).getResolutions().getItems(0);
    assertThat(resolution.getStatus()).isEqualTo(ResolutionStatus.RESOLUTION_STATUS_FOUND);
    assertThat(resolution.getRelation().hasStats()).isFalse();
    assertThat(queryStore.updateCount()).isEqualTo(0);
  }

  @Test
  void errorAndNotFoundOrderingUnchanged() {
    ResourceId missingTable =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("missing")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    TableReferenceCandidate missing =
        TableReferenceCandidate.newBuilder()
            .addCandidates(QueryInput.newBuilder().setTableId(missingTable))
            .build();
    TableReferenceCandidate found =
        TableReferenceCandidate.newBuilder()
            .addCandidates(QueryInput.newBuilder().setTableId(TABLE_A))
            .build();

    List<UserObjectsBundleChunk> chunks =
        service.stream("cid", ctx, List.of(missing, found))
            .collect()
            .asList()
            .await()
            .indefinitely();

    RelationResolution first = chunks.get(1).getResolutions().getItems(0);
    RelationResolution second = chunks.get(1).getResolutions().getItems(1);
    assertThat(first.getStatus()).isEqualTo(ResolutionStatus.RESOLUTION_STATUS_NOT_FOUND);
    assertThat(second.getStatus()).isEqualTo(ResolutionStatus.RESOLUTION_STATUS_FOUND);
    assertThat(second.getRelation().getRelationId()).isEqualTo(TABLE_A);
  }

  @Test
  void relationStillResolvesWhenStatsMissing() {
    TableReferenceCandidate candidate =
        TableReferenceCandidate.newBuilder()
            .addCandidates(QueryInput.newBuilder().setTableId(TABLE_A))
            .build();

    List<UserObjectsBundleChunk> chunks =
        service.stream("cid", ctx, List.of(candidate)).collect().asList().await().indefinitely();

    RelationResolution resolution = chunks.get(1).getResolutions().getItems(0);
    assertThat(resolution.getStatus()).isEqualTo(ResolutionStatus.RESOLUTION_STATUS_FOUND);
    RelationInfo relation = resolution.getRelation();

    assertThat(relation.getRelationId()).isEqualTo(TABLE_A);
    assertThat(relation.hasStats()).isFalse();
  }

  @Test
  void relationIncludesTotalSizeWhenProvided() {
    long totalSize = 5_000L;
    TableStats stats =
        TableStats.newBuilder()
            .setTableId(TABLE_A)
            .setSnapshotId(TABLE_A_SNAPSHOT_ID)
            .setRowCount(22)
            .setTotalSizeBytes(totalSize)
            .build();
    statsRepository.putTableStats(TABLE_A, TABLE_A_SNAPSHOT_ID, stats);

    TableReferenceCandidate candidate =
        TableReferenceCandidate.newBuilder()
            .addCandidates(QueryInput.newBuilder().setTableId(TABLE_A))
            .build();

    List<UserObjectsBundleChunk> chunks =
        service.stream("cid", ctx, List.of(candidate)).collect().asList().await().indefinitely();

    RelationResolution resolution = chunks.get(1).getResolutions().getItems(0);
    RelationInfo relation = resolution.getRelation();

    assertThat(relation.hasStats()).isTrue();
    assertThat(relation.getStats().getTotalSizeBytes()).isEqualTo(totalSize);
  }

  @Test
  void systemTableStatsAreSkippedWhenUnpinned() {
    overlay.registerTable(
        SYSTEM_TABLE,
        UserObjectBundleTestSupport.schemaFor("sys_id"),
        NameRef.newBuilder().setCatalog("sys").setName("system_table").build(),
        GraphNodeOrigin.SYSTEM);

    TableStats stats =
        TableStats.newBuilder().setTableId(SYSTEM_TABLE).setSnapshotId(999L).setRowCount(5).build();
    statsRepository.putTableStats(SYSTEM_TABLE, 999L, stats);

    TableReferenceCandidate systemCandidate =
        TableReferenceCandidate.newBuilder()
            .addCandidates(QueryInput.newBuilder().setTableId(SYSTEM_TABLE))
            .build();

    List<UserObjectsBundleChunk> chunks =
        service.stream("cid", ctx, List.of(systemCandidate))
            .collect()
            .asList()
            .await()
            .indefinitely();

    RelationResolution resolution = chunks.get(1).getResolutions().getItems(0);
    RelationInfo relation = resolution.getRelation();

    assertThat(relation.getRelationId()).isEqualTo(SYSTEM_TABLE);
    assertThat(relation.getOrigin()).isEqualTo(Origin.ORIGIN_BUILTIN);
    assertThat(relation.hasStats()).isFalse();
  }

  @Test
  void storageSystemTableFlightEndpointUsesConfigByEndpointKey() {
    ResourceId systemStorageA =
        ResourceId.newBuilder()
            .setAccountId("sys")
            .setId("SYSTEM_STORAGE_A")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    ResourceId systemStorageB =
        ResourceId.newBuilder()
            .setAccountId("sys")
            .setId("SYSTEM_STORAGE_B")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    String keyA = "bundle-test-a";
    String keyB = "bundle-test-b";
    String prefixA = "floedb.system-flight.endpoints." + keyA + ".";
    String prefixB = "floedb.system-flight.endpoints." + keyB + ".";
    List<String> propertyKeys =
        List.of(
            prefixA + "host",
            prefixA + "port",
            prefixA + "tls",
            prefixB + "host",
            prefixB + "port",
            prefixB + "tls");
    Map<String, String> previousValues = rememberProperties(propertyKeys);

    System.setProperty(prefixA + "host", "endpoint-a");
    System.setProperty(prefixA + "port", "4111");
    System.setProperty(prefixA + "tls", "true");
    System.setProperty(prefixB + "host", "endpoint-b");
    System.setProperty(prefixB + "port", "4222");
    System.setProperty(prefixB + "tls", "false");

    try {
      overlay.registerRelation(
          systemStorageA,
          storageSystemTableNode(systemStorageA, "sys://a", keyA),
          UserObjectBundleTestSupport.schemaFor("col_a"),
          NameRef.newBuilder().setCatalog("sys").setName("storage_a").build());
      overlay.registerRelation(
          systemStorageB,
          storageSystemTableNode(systemStorageB, "sys://b", keyB),
          UserObjectBundleTestSupport.schemaFor("col_b"),
          NameRef.newBuilder().setCatalog("sys").setName("storage_b").build());

      TableReferenceCandidate candidateA =
          TableReferenceCandidate.newBuilder()
              .addCandidates(QueryInput.newBuilder().setTableId(systemStorageA))
              .build();
      TableReferenceCandidate candidateB =
          TableReferenceCandidate.newBuilder()
              .addCandidates(QueryInput.newBuilder().setTableId(systemStorageB))
              .build();

      List<UserObjectsBundleChunk> chunks =
          service.stream("cid", ctx, List.of(candidateA, candidateB))
              .collect()
              .asList()
              .await()
              .indefinitely();

      RelationInfo relationA =
          chunks.get(1).getResolutions().getItemsList().stream()
              .map(RelationResolution::getRelation)
              .filter(r -> r.getRelationId().equals(systemStorageA))
              .findFirst()
              .orElseThrow();
      RelationInfo relationB =
          chunks.get(1).getResolutions().getItemsList().stream()
              .map(RelationResolution::getRelation)
              .filter(r -> r.getRelationId().equals(systemStorageB))
              .findFirst()
              .orElseThrow();

      assertThat(relationA.hasFlightEndpoint()).isTrue();
      assertThat(relationA.getFlightEndpoint().getHost()).isEqualTo("endpoint-a");
      assertThat(relationA.getFlightEndpoint().getPort()).isEqualTo(4111);
      assertThat(relationA.getFlightEndpoint().getTls()).isTrue();

      assertThat(relationB.hasFlightEndpoint()).isTrue();
      assertThat(relationB.getFlightEndpoint().getHost()).isEqualTo("endpoint-b");
      assertThat(relationB.getFlightEndpoint().getPort()).isEqualTo(4222);
      assertThat(relationB.getFlightEndpoint().getTls()).isFalse();
    } finally {
      restoreProperties(previousValues);
    }
  }

  @Test
  void cancellationStopsAfterFirstResolutionChunk() {
    TableReferenceCandidate first =
        TableReferenceCandidate.newBuilder()
            .addCandidates(QueryInput.newBuilder().setTableId(TABLE_A))
            .build();
    TableReferenceCandidate second =
        TableReferenceCandidate.newBuilder()
            .addCandidates(QueryInput.newBuilder().setTableId(TABLE_B))
            .build();

    CancellingSubscriber subscriber = new CancellingSubscriber();
    service.stream("cid", ctx, List.of(first, second)).subscribe().withSubscriber(subscriber);
    subscriber.await();

    assertThat(subscriber.items()).hasSize(2);
    assertThat(subscriber.items().get(0).hasHeader()).isTrue();
    assertThat(subscriber.items().get(1).hasResolutions()).isTrue();
    assertThat(subscriber.items().stream().noneMatch(UserObjectsBundleChunk::hasEnd)).isTrue();
    UserObjectsBundleChunk resolutionChunk = subscriber.items().get(1);
    assertThat(resolutionChunk.hasResolutions()).isTrue();
    assertThat(queryStore.updateCount()).isEqualTo(1);
  }

  @Test
  void mergesPinsOncePerChunk() throws Exception {
    TableReferenceCandidate a =
        TableReferenceCandidate.newBuilder()
            .addCandidates(QueryInput.newBuilder().setTableId(TABLE_A))
            .build();
    TableReferenceCandidate b =
        TableReferenceCandidate.newBuilder()
            .addCandidates(QueryInput.newBuilder().setTableId(TABLE_B))
            .build();

    service.stream("cid", ctx, List.of(a, b)).collect().asList().await().indefinitely();

    assertThat(queryStore.updateCount()).isEqualTo(1);
    QueryContext updated = queryStore.get(ctx.getQueryId()).orElseThrow();
    SnapshotSet pins = SnapshotSet.parseFrom(updated.getSnapshotSet());
    assertThat(pins.getPinsCount()).isEqualTo(2);
  }

  @Test
  void resolvesSecondCandidateWhenFirstNameMissing() {
    TableReferenceCandidate candidate =
        TableReferenceCandidate.newBuilder()
            .addCandidates(
                QueryInput.newBuilder()
                    .setName(NameRef.newBuilder().setCatalog("cat").setName("missing")))
            .addCandidates(QueryInput.newBuilder().setTableId(TABLE_B))
            .build();

    List<UserObjectsBundleChunk> chunks =
        service.stream("cid", ctx, List.of(candidate)).collect().asList().await().indefinitely();

    RelationResolution resolution = chunks.get(1).getResolutions().getItems(0);
    assertThat(resolution.getStatus()).isEqualTo(ResolutionStatus.RESOLUTION_STATUS_FOUND);
    assertThat(resolution.getRelation().getRelationId()).isEqualTo(TABLE_B);
    assertThat(chunks.get(2).getEnd().getFoundCount()).isEqualTo(1);
  }

  @Test
  void appliesDefaultCatalogWhenNameMissingCatalog() {
    TableReferenceCandidate candidate =
        TableReferenceCandidate.newBuilder()
            .addCandidates(
                QueryInput.newBuilder().setName(NameRef.newBuilder().setName("a").build()))
            .build();

    List<UserObjectsBundleChunk> chunks =
        service.stream("cid", ctx, List.of(candidate)).collect().asList().await().indefinitely();

    RelationResolution resolution = chunks.get(1).getResolutions().getItems(0);
    assertThat(resolution.getStatus()).isEqualTo(ResolutionStatus.RESOLUTION_STATUS_FOUND);
    assertThat(resolution.getRelation().getRelationId()).isEqualTo(TABLE_A);
    assertThat(chunks.get(2).getEnd().getFoundCount()).isEqualTo(1);
  }

  @Test
  void graphNodeMissingEmitsErrorWithoutDroppingStream() {
    overlay.hideNode(TABLE_A);
    TableReferenceCandidate candidate =
        TableReferenceCandidate.newBuilder()
            .addCandidates(
                QueryInput.newBuilder()
                    .setName(NameRef.newBuilder().setCatalog("cat").setName("a").build()))
            .build();

    List<UserObjectsBundleChunk> chunks =
        service.stream("cid", ctx, List.of(candidate)).collect().asList().await().indefinitely();

    RelationResolution resolution = chunks.get(1).getResolutions().getItems(0);
    assertThat(resolution.getStatus()).isEqualTo(ResolutionStatus.RESOLUTION_STATUS_ERROR);
    assertThat(resolution.getFailure().getCode()).isEqualTo("catalog_bundle.graph.missing_node");
    assertThat(chunks.get(2).getEnd().getFoundCount()).isZero();
    assertThat(chunks.get(2).getEnd().getResolutionCount()).isEqualTo(1);
  }

  @Test
  void decoratorSkippedWhenHeadersMissing() {
    AtomicInteger columnDecorations = new AtomicInteger();
    EngineMetadataDecoratorProvider provider =
        ctx -> Optional.of(new CountingDecorator(columnDecorations, true, false));
    UserObjectBundleService decoratedService =
        new UserObjectBundleService(
            overlay,
            resolver,
            queryStore,
            statsFactory,
            provider,
            engineContextProvider,
            true,
            "localhost",
            47470,
            false,
            "test");

    TableReferenceCandidate candidate =
        TableReferenceCandidate.newBuilder()
            .addCandidates(QueryInput.newBuilder().setTableId(TABLE_A))
            .build();

    decoratedService.stream("cid", ctx, List.of(candidate))
        .collect()
        .asList()
        .await()
        .indefinitely();

    assertThat(columnDecorations.get()).isZero();
  }

  @Test
  void decoratorInvokedWhenHeadersPresent() {
    AtomicInteger columnDecorations = new AtomicInteger();
    EngineMetadataDecoratorProvider provider =
        ctx -> Optional.of(new CountingDecorator(columnDecorations, true, false));
    UserObjectBundleService decoratedService =
        new UserObjectBundleService(
            overlay,
            resolver,
            queryStore,
            statsFactory,
            provider,
            engineContextProvider,
            true,
            "localhost",
            47470,
            false,
            "test");

    TableReferenceCandidate candidate =
        TableReferenceCandidate.newBuilder()
            .addCandidates(QueryInput.newBuilder().setTableId(TABLE_B))
            .build();

    EngineContext engineContext = EngineContext.of("pg", "16.0");
    Context context =
        Context.current().withValue(InboundContextInterceptor.ENGINE_CONTEXT_KEY, engineContext);
    Context previous = context.attach();
    List<UserObjectsBundleChunk> chunks;
    try {
      chunks =
          decoratedService.stream("cid", ctx, List.of(candidate))
              .collect()
              .asList()
              .await()
              .indefinitely();
    } finally {
      context.detach(previous);
    }

    assertThat(columnDecorations.get()).isGreaterThan(0);
    RelationInfo relation = chunks.get(1).getResolutions().getItems(0).getRelation();
    assertThat(relation.getColumnsList())
        .allMatch(c -> c.getStatus() == ColumnStatus.COLUMN_STATUS_READY && c.hasColumn());
  }

  @Test
  void decoratorMissingPayloadMarksColumnsFailed() {
    AtomicInteger columnDecorations = new AtomicInteger();
    EngineMetadataDecoratorProvider provider =
        ctx -> Optional.of(new CountingDecorator(columnDecorations, false, false));
    UserObjectBundleService decoratedService =
        new UserObjectBundleService(
            overlay,
            resolver,
            queryStore,
            statsFactory,
            provider,
            engineContextProvider,
            true,
            "localhost",
            47470,
            false,
            "test");

    TableReferenceCandidate candidate =
        TableReferenceCandidate.newBuilder()
            .addCandidates(QueryInput.newBuilder().setTableId(TABLE_B))
            .build();

    EngineContext engineContext = EngineContext.of("pg", "16.0");
    Context context =
        Context.current().withValue(InboundContextInterceptor.ENGINE_CONTEXT_KEY, engineContext);
    Context previous = context.attach();
    List<UserObjectsBundleChunk> chunks;
    try {
      chunks =
          decoratedService.stream("cid", ctx, List.of(candidate))
              .collect()
              .asList()
              .await()
              .indefinitely();
    } finally {
      context.detach(previous);
    }

    assertThat(columnDecorations.get()).isGreaterThan(0);
    RelationInfo relation = chunks.get(1).getResolutions().getItems(0).getRelation();
    assertThat(relation.getColumnsList())
        .allMatch(
            c ->
                c.getStatus() == ColumnStatus.COLUMN_STATUS_FAILED
                    && c.hasFailure()
                    && c.getFailure()
                        .getCode()
                        .equals(
                            ColumnFailureCode.COLUMN_FAILURE_CODE_ENGINE_PAYLOAD_REQUIRED_MISSING));
  }

  @Test
  void decoratorExceptionPropagatesAsPerColumnFailure() {
    AtomicInteger columnDecorations = new AtomicInteger();
    EngineMetadataDecoratorProvider provider =
        ctx -> Optional.of(new CountingDecorator(columnDecorations, false, true));
    UserObjectBundleService decoratedService =
        new UserObjectBundleService(
            overlay,
            resolver,
            queryStore,
            statsFactory,
            provider,
            engineContextProvider,
            true,
            "localhost",
            47470,
            false,
            "test");

    TableReferenceCandidate candidate =
        TableReferenceCandidate.newBuilder()
            .addCandidates(QueryInput.newBuilder().setTableId(TABLE_B))
            .build();

    EngineContext engineContext = EngineContext.of("pg", "16.0");
    Context context =
        Context.current().withValue(InboundContextInterceptor.ENGINE_CONTEXT_KEY, engineContext);
    Context previous = context.attach();
    List<UserObjectsBundleChunk> chunks;
    try {
      chunks =
          decoratedService.stream("cid", ctx, List.of(candidate))
              .collect()
              .asList()
              .await()
              .indefinitely();
    } finally {
      context.detach(previous);
    }

    assertThat(columnDecorations.get()).isGreaterThan(0);
    RelationInfo relation = chunks.get(1).getResolutions().getItems(0).getRelation();
    assertThat(relation.getColumnsList())
        .allMatch(
            c ->
                c.getStatus() == ColumnStatus.COLUMN_STATUS_FAILED
                    && c.hasFailure()
                    && c.getFailure()
                        .getCode()
                        .equals(ColumnFailureCode.COLUMN_FAILURE_CODE_TYPE_NOT_SUPPORTED)
                    && c.getFailure()
                        .getMessage()
                        .equals(
                            "This column type is not supported by the engine metadata decorator."));
    assertThat(relation.getColumns(0).getFailure().getMessage())
        .doesNotContain("test unsupported type");
  }

  @Test
  void decoratorNumericExtensionCodePropagatesAsUnknownEnumValue() {
    AtomicInteger columnDecorations = new AtomicInteger();
    EngineMetadataDecoratorProvider provider =
        ctx ->
            Optional.of(
                new EngineMetadataDecorator() {
                  @Override
                  public void decorateColumn(EngineContext ec, ColumnDecoration columnDecoration) {
                    columnDecorations.incrementAndGet();
                    throw new DecorationException(1201, "engine extension failure");
                  }
                });
    UserObjectBundleService decoratedService =
        new UserObjectBundleService(
            overlay,
            resolver,
            queryStore,
            statsFactory,
            provider,
            engineContextProvider,
            true,
            "localhost",
            47470,
            false,
            "test");

    TableReferenceCandidate candidate =
        TableReferenceCandidate.newBuilder()
            .addCandidates(QueryInput.newBuilder().setTableId(TABLE_B))
            .build();

    EngineContext engineContext = EngineContext.of("pg", "16.0");
    Context context =
        Context.current().withValue(InboundContextInterceptor.ENGINE_CONTEXT_KEY, engineContext);
    Context previous = context.attach();
    List<UserObjectsBundleChunk> chunks;
    try {
      chunks =
          decoratedService.stream("cid", ctx, List.of(candidate))
              .collect()
              .asList()
              .await()
              .indefinitely();
    } finally {
      context.detach(previous);
    }

    assertThat(columnDecorations.get()).isGreaterThan(0);
    RelationInfo relation = chunks.get(1).getResolutions().getItems(0).getRelation();
    assertThat(relation.getColumnsList())
        .allMatch(
            c ->
                c.getStatus() == ColumnStatus.COLUMN_STATUS_FAILED
                    && c.hasFailure()
                    && c.getFailure().getCode()
                        == ColumnFailureCode.COLUMN_FAILURE_CODE_ENGINE_EXTENSION
                    && c.getFailure().getExtensionCodeValue() == 1201
                    && c.getFailure().getMessage().equals("engine extension failure"));
  }

  @Test
  void decoratorProviderEmptyMarksColumnsDecoratorUnavailable() {
    EngineMetadataDecoratorProvider provider = ignored -> Optional.empty();
    UserObjectBundleService decoratedService =
        new UserObjectBundleService(
            overlay,
            resolver,
            queryStore,
            statsFactory,
            provider,
            engineContextProvider,
            true,
            "localhost",
            47470,
            false,
            "test");

    TableReferenceCandidate candidate =
        TableReferenceCandidate.newBuilder()
            .addCandidates(QueryInput.newBuilder().setTableId(TABLE_B))
            .build();

    EngineContext engineContext = EngineContext.of("pg", "16.0");
    Context context =
        Context.current().withValue(InboundContextInterceptor.ENGINE_CONTEXT_KEY, engineContext);
    Context previous = context.attach();
    List<UserObjectsBundleChunk> chunks;
    try {
      chunks =
          decoratedService.stream("cid", ctx, List.of(candidate))
              .collect()
              .asList()
              .await()
              .indefinitely();
    } finally {
      context.detach(previous);
    }

    RelationInfo relation = chunks.get(1).getResolutions().getItems(0).getRelation();
    assertThat(relation.getColumnsList())
        .allMatch(
            c ->
                c.getStatus() == ColumnStatus.COLUMN_STATUS_FAILED
                    && c.hasFailure()
                    && c.getFailure().getCode()
                        == ColumnFailureCode.COLUMN_FAILURE_CODE_DECORATOR_UNAVAILABLE);
  }

  @Test
  @SuppressWarnings("unchecked")
  void schemaMismatchPathMarksAllColumnsFailed() throws Exception {
    Method decorateColumns =
        UserObjectBundleService.class.getDeclaredMethod(
            "decorateColumns",
            List.class,
            List.class,
            ai.floedb.floecat.systemcatalog.spi.decorator.RelationDecoration.class,
            Optional.class,
            EngineContext.class,
            boolean.class,
            ResourceId.class);
    decorateColumns.setAccessible(true);

    List<ColumnInfo> columns =
        List.of(
            ColumnInfo.newBuilder().setId(11).setName("c1").setOrdinal(1).build(),
            ColumnInfo.newBuilder().setId(12).setName("c2").setOrdinal(2).build());
    List<SchemaColumn> pruned =
        List.of(SchemaColumn.newBuilder().setId(11).setName("c1").setOrdinal(1).build());
    ResourceId relationId = TABLE_A;

    List<ColumnResult> results =
        (List<ColumnResult>)
            decorateColumns.invoke(
                service,
                columns,
                pruned,
                null,
                Optional.empty(),
                EngineContext.of("pg", "16.0"),
                true,
                relationId);

    assertThat(results).hasSize(2);
    assertThat(results)
        .allMatch(
            c ->
                c.getStatus() == ColumnStatus.COLUMN_STATUS_FAILED
                    && c.hasFailure()
                    && c.getFailure().getCode()
                        == ColumnFailureCode.COLUMN_FAILURE_CODE_SCHEMA_MISMATCH
                    && c.getFailure().getDetailsMap().get("relation_id").equals(relationId.getId())
                    && c.getFailure().getMessage().contains("Column/schema mismatch"));
  }

  @Test
  void mixedReadyFailedColumnsCommitOnlyReadyColumnIds() {
    List<SchemaColumn> schema =
        List.of(
            SchemaColumn.newBuilder()
                .setId(101)
                .setName("c_ready")
                .setLogicalType("INT32")
                .setNullable(true)
                .setOrdinal(1)
                .build(),
            SchemaColumn.newBuilder()
                .setId(102)
                .setName("c_failed")
                .setLogicalType("INT32")
                .setNullable(true)
                .setOrdinal(2)
                .build());
    overlay.registerTable(
        TABLE_C, schema, NameRef.newBuilder().setCatalog("cat").setName("c").build());

    AtomicReference<Boolean> commitRelationHints = new AtomicReference<>();
    AtomicReference<Boolean> commitColumnHints = new AtomicReference<>();
    AtomicReference<Set<Long>> committedColumnIds = new AtomicReference<>();
    EngineMetadataDecoratorProvider provider =
        ignored ->
            Optional.of(
                new EngineMetadataDecorator() {
                  @Override
                  public void decorateColumn(EngineContext ec, ColumnDecoration columnDecoration) {
                    if ("c_ready".equals(columnDecoration.builder().getName())) {
                      columnDecoration
                          .builder()
                          .addEngineSpecific(
                              EngineSpecific.newBuilder()
                                  .setEngineKind(ec.normalizedKind())
                                  .setPayloadType("test.column")
                                  .setPayload(com.google.protobuf.ByteString.copyFromUtf8("ok"))
                                  .build());
                    }
                  }

                  @Override
                  public void completeRelation(
                      EngineContext ec,
                      ai.floedb.floecat.systemcatalog.spi.decorator.RelationDecoration rel,
                      boolean commitRelation,
                      boolean commitColumns,
                      Set<Long> columnIds) {
                    commitRelationHints.set(commitRelation);
                    commitColumnHints.set(commitColumns);
                    committedColumnIds.set(columnIds);
                  }
                });
    UserObjectBundleService decoratedService =
        new UserObjectBundleService(
            overlay,
            resolver,
            queryStore,
            statsFactory,
            provider,
            engineContextProvider,
            true,
            "localhost",
            47470,
            false,
            "test");

    TableReferenceCandidate candidate =
        TableReferenceCandidate.newBuilder()
            .addCandidates(QueryInput.newBuilder().setTableId(TABLE_C))
            .build();

    EngineContext engineContext = EngineContext.of("pg", "16.0");
    Context context =
        Context.current().withValue(InboundContextInterceptor.ENGINE_CONTEXT_KEY, engineContext);
    Context previous = context.attach();
    List<UserObjectsBundleChunk> chunks;
    try {
      chunks =
          decoratedService.stream("cid", ctx, List.of(candidate))
              .collect()
              .asList()
              .await()
              .indefinitely();
    } finally {
      context.detach(previous);
    }

    RelationInfo relation = chunks.get(1).getResolutions().getItems(0).getRelation();
    assertThat(relation.getColumnsList())
        .anyMatch(c -> c.getStatus() == ColumnStatus.COLUMN_STATUS_READY && c.getColumnId() == 101);
    assertThat(relation.getColumnsList())
        .anyMatch(
            c ->
                c.getStatus() == ColumnStatus.COLUMN_STATUS_FAILED
                    && c.getColumnId() == 102
                    && c.getFailure().getCode()
                        == ColumnFailureCode.COLUMN_FAILURE_CODE_ENGINE_PAYLOAD_REQUIRED_MISSING);
    assertThat(commitRelationHints.get()).isTrue();
    assertThat(commitColumnHints.get()).isTrue();
    assertThat(committedColumnIds.get()).containsExactly(101L);
  }

  @Test
  void relationDecorationFailureSkipsHintCommitForRelationAndColumns() {
    AtomicReference<Boolean> commitRelationHints = new AtomicReference<>();
    AtomicReference<Boolean> commitColumnHints = new AtomicReference<>();
    AtomicReference<Set<Long>> committedColumnIds = new AtomicReference<>();
    AtomicInteger columnDecorations = new AtomicInteger();
    EngineMetadataDecoratorProvider provider =
        ctx ->
            Optional.of(
                new EngineMetadataDecorator() {
                  @Override
                  public void decorateRelation(
                      EngineContext ec,
                      ai.floedb.floecat.systemcatalog.spi.decorator.RelationDecoration relation) {
                    throw new RuntimeException("relation decoration failed");
                  }

                  @Override
                  public void decorateColumn(EngineContext ec, ColumnDecoration columnDecoration) {
                    columnDecorations.incrementAndGet();
                    columnDecoration
                        .builder()
                        .addEngineSpecific(
                            EngineSpecific.newBuilder()
                                .setEngineKind(ec.normalizedKind())
                                .setPayloadType("test.column")
                                .setPayload(com.google.protobuf.ByteString.copyFromUtf8("ok"))
                                .build());
                  }

                  @Override
                  public void completeRelation(
                      EngineContext ec,
                      ai.floedb.floecat.systemcatalog.spi.decorator.RelationDecoration rel,
                      boolean commitRelation,
                      boolean commitColumns,
                      Set<Long> columnIds) {
                    commitRelationHints.set(commitRelation);
                    commitColumnHints.set(commitColumns);
                    committedColumnIds.set(columnIds);
                  }
                });
    UserObjectBundleService decoratedService =
        new UserObjectBundleService(
            overlay,
            resolver,
            queryStore,
            statsFactory,
            provider,
            engineContextProvider,
            true,
            "localhost",
            47470,
            false,
            "test");

    TableReferenceCandidate candidate =
        TableReferenceCandidate.newBuilder()
            .addCandidates(QueryInput.newBuilder().setTableId(TABLE_B))
            .build();

    EngineContext engineContext = EngineContext.of("pg", "16.0");
    Context context =
        Context.current().withValue(InboundContextInterceptor.ENGINE_CONTEXT_KEY, engineContext);
    Context previous = context.attach();
    List<UserObjectsBundleChunk> chunks;
    try {
      chunks =
          decoratedService.stream("cid", ctx, List.of(candidate))
              .collect()
              .asList()
              .await()
              .indefinitely();
    } finally {
      context.detach(previous);
    }

    assertThat(columnDecorations.get()).isGreaterThan(0);
    RelationInfo relation = chunks.get(1).getResolutions().getItems(0).getRelation();
    assertThat(relation.getColumnsList())
        .allMatch(c -> c.getStatus() == ColumnStatus.COLUMN_STATUS_READY && c.hasColumn());
    assertThat(commitRelationHints.get()).isFalse();
    assertThat(commitColumnHints.get()).isFalse();
    assertThat(committedColumnIds.get()).isEmpty();
  }

  @Test
  void nonDecorationExceptionDoesNotExposeExceptionClassInFailureDetails() {
    EngineMetadataDecoratorProvider provider =
        ctx ->
            Optional.of(
                new EngineMetadataDecorator() {
                  @Override
                  public void decorateColumn(EngineContext ec, ColumnDecoration columnDecoration) {
                    throw new RuntimeException("internal backend error");
                  }
                });
    UserObjectBundleService decoratedService =
        new UserObjectBundleService(
            overlay,
            resolver,
            queryStore,
            statsFactory,
            provider,
            engineContextProvider,
            true,
            "localhost",
            47470,
            false,
            "test");

    TableReferenceCandidate candidate =
        TableReferenceCandidate.newBuilder()
            .addCandidates(QueryInput.newBuilder().setTableId(TABLE_B))
            .build();

    EngineContext engineContext = EngineContext.of("pg", "16.0");
    Context context =
        Context.current().withValue(InboundContextInterceptor.ENGINE_CONTEXT_KEY, engineContext);
    Context previous = context.attach();
    List<UserObjectsBundleChunk> chunks;
    try {
      chunks =
          decoratedService.stream("cid", ctx, List.of(candidate))
              .collect()
              .asList()
              .await()
              .indefinitely();
    } finally {
      context.detach(previous);
    }

    RelationInfo relation = chunks.get(1).getResolutions().getItems(0).getRelation();
    assertThat(relation.getColumnsList())
        .allMatch(
            c ->
                c.getStatus() == ColumnStatus.COLUMN_STATUS_FAILED
                    && c.hasFailure()
                    && !c.getFailure().getDetailsMap().containsKey("exception"));
  }

  private static final class CountingDecorator implements EngineMetadataDecorator {

    private final AtomicInteger columnDecorations;
    private final boolean emitPayload;
    private final boolean throwError;

    private CountingDecorator(
        AtomicInteger columnDecorations, boolean emitPayload, boolean throwError) {
      this.columnDecorations = columnDecorations;
      this.emitPayload = emitPayload;
      this.throwError = throwError;
    }

    @Override
    public void decorateColumn(EngineContext ctx, ColumnDecoration columnDecoration) {
      columnDecorations.incrementAndGet();
      if (throwError) {
        throw new DecorationException(
            ColumnFailureCode.COLUMN_FAILURE_CODE_TYPE_NOT_SUPPORTED, "test unsupported type");
      }
      if (emitPayload) {
        columnDecoration
            .builder()
            .addEngineSpecific(
                EngineSpecific.newBuilder()
                    .setEngineKind(ctx.normalizedKind())
                    .setPayloadType("test.column")
                    .setPayload(com.google.protobuf.ByteString.copyFromUtf8("ok"))
                    .build());
      }
    }
  }

  private static SystemTableNode.StorageSystemTableNode storageSystemTableNode(
      ResourceId id, String storagePath, String endpointKey) {
    return new SystemTableNode.StorageSystemTableNode(
        id,
        1L,
        Instant.EPOCH,
        "",
        id.getId(),
        ResourceId.getDefaultInstance(),
        List.of(),
        Map.of(),
        Map.of(),
        storagePath,
        endpointKey,
        null);
  }

  private static Map<String, String> rememberProperties(List<String> keys) {
    java.util.HashMap<String, String> values = new java.util.HashMap<>();
    for (String key : keys) {
      values.put(key, System.getProperty(key));
    }
    return values;
  }

  private static void restoreProperties(Map<String, String> values) {
    values.forEach(
        (key, value) -> {
          if (value == null) {
            System.clearProperty(key);
          } else {
            System.setProperty(key, value);
          }
        });
  }
}
