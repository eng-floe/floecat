package ai.floedb.floecat.service.query.catalog;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.query.rpc.CatalogBundleChunk;
import ai.floedb.floecat.query.rpc.RelationResolution;
import ai.floedb.floecat.query.rpc.ResolutionStatus;
import ai.floedb.floecat.query.rpc.SnapshotSet;
import ai.floedb.floecat.query.rpc.TableReferenceCandidate;
import ai.floedb.floecat.service.query.catalog.testsupport.CatalogBundleTestSupport;
import ai.floedb.floecat.service.query.catalog.testsupport.CatalogBundleTestSupport.CancellingSubscriber;
import ai.floedb.floecat.service.query.catalog.testsupport.CatalogBundleTestSupport.FakeCatalogOverlay;
import ai.floedb.floecat.service.query.catalog.testsupport.CatalogBundleTestSupport.TestQueryContextStore;
import ai.floedb.floecat.service.query.catalog.testsupport.CatalogBundleTestSupport.TestQueryInputResolver;
import ai.floedb.floecat.service.query.impl.QueryContext;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CatalogBundleServiceTest {

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

  private final FakeCatalogOverlay overlay = new FakeCatalogOverlay();
  private TestQueryInputResolver resolver;
  private TestQueryContextStore queryStore;
  private CatalogBundleService service;

  private final QueryContext ctx =
      QueryContext.builder()
          .queryId("q-1")
          .principal(
              PrincipalContext.newBuilder()
                  .setAccountId("acct")
                  .setSubject("tester")
                  .setCorrelationId("cid")
                  .build())
          .snapshotSet(new byte[0])
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
        CatalogBundleTestSupport.schemaFor("id_a"),
        NameRef.newBuilder().setCatalog("cat").setName("a").build());
    overlay.registerTable(
        TABLE_B,
        CatalogBundleTestSupport.schemaFor("id_b"),
        NameRef.newBuilder().setCatalog("cat").setName("b").build());
    overlay.registerCatalog(DEFAULT_CATALOG, "cat");
    queryStore.seed(ctx);
    service = new CatalogBundleService(overlay, resolver, queryStore);
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

    List<CatalogBundleChunk> chunks =
        service.stream("cid", ctx, List.of(a, b)).collect().asList().await().indefinitely();

    assertThat(chunks).hasSize(3);
    assertThat(chunks.get(0).hasHeader()).isTrue();
    CatalogBundleChunk resolutions = chunks.get(1);
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

    CatalogBundleChunk end = chunks.get(2);
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
    assertThat(subscriber.items().stream().noneMatch(CatalogBundleChunk::hasEnd)).isTrue();
    CatalogBundleChunk resolutionChunk = subscriber.items().get(1);
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

    List<CatalogBundleChunk> chunks =
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

    List<CatalogBundleChunk> chunks =
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

    List<CatalogBundleChunk> chunks =
        service.stream("cid", ctx, List.of(candidate)).collect().asList().await().indefinitely();

    RelationResolution resolution = chunks.get(1).getResolutions().getItems(0);
    assertThat(resolution.getStatus()).isEqualTo(ResolutionStatus.RESOLUTION_STATUS_ERROR);
    assertThat(resolution.getFailure().getCode()).isEqualTo("catalog_bundle.graph.missing_node");
    assertThat(chunks.get(2).getEnd().getFoundCount()).isZero();
    assertThat(chunks.get(2).getEnd().getResolutionCount()).isEqualTo(1);
  }
}
