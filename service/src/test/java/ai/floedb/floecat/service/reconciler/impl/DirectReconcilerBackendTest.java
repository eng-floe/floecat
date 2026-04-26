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

package ai.floedb.floecat.service.reconciler.impl;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.EngineExpressionStatsTarget;
import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.IndexArtifactRecord;
import ai.floedb.floecat.catalog.rpc.IndexArtifactState;
import ai.floedb.floecat.catalog.rpc.IndexFileTarget;
import ai.floedb.floecat.catalog.rpc.IndexTarget;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.StatsTargetKind;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.catalog.rpc.ViewSpec;
import ai.floedb.floecat.catalog.rpc.ViewSqlDefinition;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorState;
import ai.floedb.floecat.connector.spi.ConnectorFormat;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.reconciler.impl.FileGroupIndexArtifactStager;
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend.TableSpecDescriptor;
import ai.floedb.floecat.service.metagraph.snapshot.SnapshotHelper;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.IndexArtifactRepository;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import ai.floedb.floecat.service.testsupport.FakeCatalogRepository;
import ai.floedb.floecat.service.testsupport.FakeNamespaceRepository;
import ai.floedb.floecat.service.testsupport.FakeTableRepository;
import ai.floedb.floecat.service.testsupport.FakeViewRepository;
import ai.floedb.floecat.service.testsupport.SnapshotTestSupport;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.identity.TargetStatsRecords;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DirectReconcilerBackendTest {
  private static final String ACCOUNT = "acct";
  private static final String CATALOG_NAME = "cat";
  private static final long SNAPSHOT_ID = 42L;

  private DirectReconcilerBackend backend;
  private FakeCatalogRepository catalogRepo;
  private FakeNamespaceRepository namespaceRepo;
  private FakeTableRepository tableRepo;
  private FakeViewRepository viewRepo;
  private SnapshotTestSupport.FakeSnapshotRepository snapshotRepo;
  private StatsRepository statsRepository;
  private IndexArtifactRepository indexArtifactRepository;
  private SnapshotHelper snapshotHelper;
  private ConnectorRepository connectorRepo;
  private InMemoryBlobStore indexBlobStore;

  private ReconcileContext ctx;
  private ResourceId catalogId;
  private ResourceId namespaceId;
  private ResourceId tableId;
  private ResourceId connectorId;

  @BeforeEach
  void setUp() {
    catalogRepo = new FakeCatalogRepository();
    namespaceRepo = new FakeNamespaceRepository();
    tableRepo = new FakeTableRepository();
    viewRepo = new FakeViewRepository();
    snapshotRepo = new SnapshotTestSupport.FakeSnapshotRepository();
    statsRepository = new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    indexBlobStore = new InMemoryBlobStore();
    indexArtifactRepository =
        new IndexArtifactRepository(new InMemoryPointerStore(), indexBlobStore);
    snapshotHelper = new SnapshotHelper(snapshotRepo);
    connectorRepo = new ConnectorRepository(new InMemoryPointerStore(), new InMemoryBlobStore());

    backend = new DirectReconcilerBackend();
    backend.catalogRepo = catalogRepo;
    backend.namespaceRepo = namespaceRepo;
    backend.tableRepo = tableRepo;
    backend.viewRepo = viewRepo;
    backend.snapshotRepo = snapshotRepo;
    backend.statsStore = statsRepository;
    backend.indexArtifactRepo = indexArtifactRepository;
    backend.blobStore = indexBlobStore;
    backend.snapshotHelper = snapshotHelper;
    backend.connectorRepo = connectorRepo;

    catalogId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT)
            .setId("cat-1")
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    Catalog catalog =
        Catalog.newBuilder().setDisplayName(CATALOG_NAME).setResourceId(catalogId).build();
    catalogRepo.put(catalog, MutationMeta.newBuilder().setPointerVersion(1).setEtag("v1").build());

    PrincipalContext principal =
        PrincipalContext.newBuilder().setAccountId(ACCOUNT).setCorrelationId("ctx").build();
    ctx =
        new ReconcileContext(
            "ctx", principal, "backend-test", Instant.now(), Optional.<String>empty());

    connectorId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT)
            .setId("conn-1")
            .setKind(ResourceKind.RK_CONNECTOR)
            .build();
    Connector connector =
        Connector.newBuilder()
            .setResourceId(connectorId)
            .setDisplayName("conn")
            .setState(ConnectorState.CS_ACTIVE)
            .build();
    connectorRepo.create(connector);

    namespaceId =
        backend.ensureNamespace(
            ctx,
            catalogId,
            NameRef.newBuilder()
                .setCatalog(CATALOG_NAME)
                .addPath("parent")
                .setName("child")
                .build());

    Namespace namespace =
        Namespace.newBuilder()
            .setResourceId(namespaceId)
            .setCatalogId(catalogId)
            .setDisplayName("child")
            .addParents("parent")
            .build();
    namespaceRepo.put(
        namespace, MutationMeta.newBuilder().setPointerVersion(1).setEtag("v1").build());

    tableId = createReferenceTable();
  }

  @Test
  void ensureNamespaceCreatesPathAndResolvesFq() {
    assertThat(namespaceRepo.getById(namespaceId)).isPresent();
    assertThat(backend.resolveNamespaceFq(ctx, namespaceId)).isEqualTo("parent.child");
  }

  @Test
  void ensureTablePersistsSchemaAndUpstream() {
    Table table = tableRepo.getById(tableId).get();
    assertThat(table.getDisplayName()).isEqualTo("orders");
    assertThat(table.getSchemaJson()).isEqualTo("{}");
    assertThat(table.getUpstream().getConnectorId().getId()).isEqualTo(connectorId.getId());
  }

  @Test
  void ensureViewUpdatesExistingDefinitionWhenItDrifts() {
    ResourceId viewId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT)
            .setId("view-1")
            .setKind(ResourceKind.RK_VIEW)
            .build();
    View existing =
        View.newBuilder()
            .setResourceId(viewId)
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName("orders_view")
            .addSqlDefinitions(
                ViewSqlDefinition.newBuilder().setSql("SELECT 1").setDialect("ansi").build())
            .addCreationSearchPath("parent")
            .putProperties("comment", "before")
            .build();
    viewRepo.put(existing, MutationMeta.newBuilder().setPointerVersion(3).setEtag("v3").build());

    ViewSpec spec =
        ViewSpec.newBuilder()
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName("orders_view")
            .addSqlDefinitions(
                ViewSqlDefinition.newBuilder()
                    .setSql("SELECT order_id FROM orders")
                    .setDialect("spark")
                    .build())
            .addCreationSearchPath("child")
            .addOutputColumns(
                SchemaColumn.newBuilder()
                    .setName("order_id")
                    .setLogicalType("INT")
                    .setNullable(false)
                    .build())
            .putProperties("comment", "after")
            .build();

    var changed = backend.ensureView(ctx, spec, "ns.orders_view");

    assertThat(changed.viewId()).isEqualTo(viewId);
    assertThat(changed.changed()).isTrue();
    View updated = viewRepo.getById(viewId).orElseThrow();
    assertThat(updated.getSqlDefinitions(0).getSql()).isEqualTo("SELECT order_id FROM orders");
    assertThat(updated.getSqlDefinitions(0).getDialect()).isEqualTo("spark");
    assertThat(updated.getCreationSearchPathList()).containsExactly("child");
    assertThat(updated.getOutputColumnsCount()).isEqualTo(1);
    assertThat(updated.getPropertiesOrThrow("comment")).isEqualTo("after");
  }

  @Test
  void lookupCatalogNameAndConnectorReturnEntries() {
    assertThat(backend.lookupCatalogName(ctx, catalogId)).isEqualTo(CATALOG_NAME);
    assertThat(backend.lookupConnector(ctx, connectorId).getResourceId().getId())
        .isEqualTo(connectorId.getId());
  }

  @Test
  void lookupDestinationViewMetadataPreservesSourceConnectorAccountId() {
    ResourceId viewId =
        ResourceId.newBuilder()
            .setAccountId(ACCOUNT)
            .setId("view-source-1")
            .setKind(ResourceKind.RK_VIEW)
            .build();
    View view =
        View.newBuilder()
            .setResourceId(viewId)
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName("orders_view")
            .putProperties(ReconcilerBackend.SOURCE_NAMESPACE_PROPERTY, "src_cat.src_ns")
            .putProperties(ReconcilerBackend.SOURCE_NAME_PROPERTY, "orders_view")
            .putProperties(ReconcilerBackend.SOURCE_CONNECTOR_ID_PROPERTY, connectorId.getId())
            .build();
    viewRepo.put(view, MutationMeta.newBuilder().setPointerVersion(1).setEtag("v1").build());

    var metadata = backend.lookupDestinationViewMetadata(ctx, viewId).orElseThrow();

    assertThat(metadata.sourceConnectorId()).isEqualTo(connectorId);
  }

  @Test
  void snapshotPinsReadFromRepositoryAndCanBeIngested() {
    Snapshot snapshot =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(SNAPSHOT_ID)
            .setSchemaJson("{}")
            .setUpstreamCreatedAt(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()))
            .build();
    backend.ingestSnapshot(ctx, tableId, snapshot);
    SnapshotPin pin =
        backend.snapshotPinFor(
            ctx,
            tableId,
            SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT).build(),
            Optional.empty());
    assertThat(pin.getSnapshotId()).isEqualTo(SNAPSHOT_ID);
    assertThat(snapshotRepo.getById(tableId, SNAPSHOT_ID)).isPresent();
  }

  @Test
  void statsPuttersStoreAndTrackTablesColumnsAndFiles() {
    TableValueStats tableStats = TableValueStats.newBuilder().build();
    backend.putTargetStats(
        ctx, List.of(TargetStatsRecords.tableRecord(tableId, SNAPSHOT_ID, tableStats, null)));
    assertThat(
            statsRepository.getTargetStats(tableId, SNAPSHOT_ID, StatsTargetIdentity.tableTarget()))
        .isPresent();
    assertThat(
            backend.statsAlreadyCapturedForTargetKind(
                ctx, tableId, SNAPSHOT_ID, StatsTargetKind.STK_UNSPECIFIED))
        .isTrue();
    assertThat(
            backend.statsAlreadyCapturedForTargetKind(
                ctx, tableId, SNAPSHOT_ID, StatsTargetKind.STK_TABLE))
        .isTrue();
    assertThat(
            backend.statsAlreadyCapturedForTargetKind(
                ctx, tableId, SNAPSHOT_ID, StatsTargetKind.STK_COLUMN))
        .isFalse();

    ScalarStats columnStats = ScalarStats.newBuilder().setDisplayName("col").build();
    backend.putTargetStats(
        ctx, List.of(TargetStatsRecords.columnRecord(tableId, SNAPSHOT_ID, 1L, columnStats, null)));
    assertThat(
            statsRepository.getTargetStats(
                tableId, SNAPSHOT_ID, StatsTargetIdentity.columnTarget(1L)))
        .isPresent();
    assertThat(
            backend.statsAlreadyCapturedForTargetKind(
                ctx, tableId, SNAPSHOT_ID, StatsTargetKind.STK_COLUMN))
        .isTrue();

    FileTargetStats fileStats = FileTargetStats.newBuilder().setFilePath("/data/file").build();
    backend.putTargetStats(
        ctx, List.of(TargetStatsRecords.fileRecord(tableId, SNAPSHOT_ID, fileStats)));
    assertThat(
            statsRepository.getTargetStats(
                tableId, SNAPSHOT_ID, StatsTargetIdentity.fileTarget("/data/file")))
        .isPresent();
    assertThat(
            backend.statsAlreadyCapturedForTargetKind(
                ctx, tableId, SNAPSHOT_ID, StatsTargetKind.STK_FILE))
        .isTrue();

    ScalarStats expressionStats = ScalarStats.newBuilder().setDisplayName("expr").build();
    backend.putTargetStats(
        ctx,
        List.of(
            TargetStatsRecords.expressionRecord(
                tableId,
                SNAPSHOT_ID,
                EngineExpressionStatsTarget.newBuilder()
                    .setEngineKind("duckdb")
                    .setEngineExpressionKey(ByteString.copyFromUtf8("sum_col"))
                    .build(),
                expressionStats)));
    assertThat(
            backend.statsAlreadyCapturedForTargetKind(
                ctx, tableId, SNAPSHOT_ID, StatsTargetKind.STK_EXPRESSION))
        .isTrue();
  }

  @Test
  void indexArtifactsCanBeStoredAndReadBack() {
    IndexArtifactRecord record =
        IndexArtifactRecord.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(SNAPSHOT_ID)
            .setTarget(
                IndexTarget.newBuilder()
                    .setFile(IndexFileTarget.newBuilder().setFilePath("/data/file.parquet").build())
                    .build())
            .setArtifactUri("/data/file.parquet.floe-index.parquet")
            .setArtifactFormat("parquet")
            .setArtifactFormatVersion(1)
            .setState(IndexArtifactState.IAS_PENDING)
            .build();

    backend.putIndexArtifacts(
        ctx,
        List.of(new ReconcilerBackend.StagedIndexArtifact(record, new byte[] {1}, "test/type")));

    assertThat(
            indexArtifactRepository.getIndexArtifact(
                tableId,
                SNAPSHOT_ID,
                IndexTarget.newBuilder()
                    .setFile(IndexFileTarget.newBuilder().setFilePath("/data/file.parquet").build())
                    .build()))
        .isPresent()
        .get()
        .extracting(IndexArtifactRecord::getArtifactUri)
        .isEqualTo("/data/file.parquet.floe-index.parquet");
  }

  @Test
  void materializePlannedFileGroupIndexArtifactsStagesRealParquetBlob() throws Exception {
    FileTargetStats fileStats =
        FileTargetStats.newBuilder()
            .setFilePath("/data/file.parquet")
            .setFileFormat("parquet")
            .setRowCount(33L)
            .setSizeBytes(2048L)
            .setSequenceNumber(9L)
            .build();
    TargetStatsRecord record = TargetStatsRecords.fileRecord(tableId, SNAPSHOT_ID, fileStats);
    var pageEntry =
        new ai.floedb.floecat.connector.spi.FloecatConnector.ParquetPageIndexEntry(
            "/data/file.parquet",
            "id",
            0,
            0,
            0L,
            33,
            33,
            256L,
            512,
            128L,
            128,
            true,
            "INT64",
            "UNCOMPRESSED",
            (short) 1,
            (short) 0,
            null,
            null,
            null,
            null,
            null,
            10L,
            42L,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null);

    List<ReconcilerBackend.StagedIndexArtifact> artifacts =
        FileGroupIndexArtifactStager.stage(
            tableId,
            SNAPSHOT_ID,
            List.of("/data/file.parquet"),
            List.of(record),
            List.of(pageEntry));

    assertThat(artifacts).hasSize(1);
    ReconcilerBackend.StagedIndexArtifact artifact = artifacts.getFirst();
    IndexArtifactRecord artifactRecord = artifact.record();
    assertThat(artifactRecord.getState()).isEqualTo(IndexArtifactState.IAS_READY);
    assertThat(artifactRecord.getArtifactFormat()).isEqualTo("parquet");
    assertThat(artifactRecord.getArtifactUri()).contains("/index-sidecars/");
    assertThat(artifactRecord.getContentEtag()).isNotBlank();
    assertThat(artifactRecord.getContentSha256B64()).isNotBlank();
    assertThat(artifactRecord.getCoverage().getRowsIndexed()).isEqualTo(33L);
    assertThat(artifactRecord.getCoverage().getLiveRowsIndexed()).isEqualTo(33L);
    assertThat(
            indexArtifactRepository.getIndexArtifact(
                tableId,
                SNAPSHOT_ID,
                IndexTarget.newBuilder()
                    .setFile(IndexFileTarget.newBuilder().setFilePath("/data/file.parquet").build())
                    .build()))
        .isEmpty();
    byte[] stored = artifact.content();
    assertThat(stored).isNotNull();
    assertThat(stored).isNotEmpty();
    assertThat(new String(stored, 0, 4, java.nio.charset.StandardCharsets.US_ASCII))
        .isEqualTo("PAR1");
    try (ParquetFileReader reader = ParquetFileReader.open(new ByteArrayInputFile(stored))) {
      assertThat(reader.getFooter().getBlocks()).hasSize(1);
      assertThat(reader.getFooter().getBlocks().getFirst().getRowCount()).isEqualTo(1L);
      assertThat(reader.getFooter().getFileMetaData().getKeyValueMetaData())
          .containsEntry("sidecar.data_file_path", "/data/file.parquet");
      assertThat(reader.getFooter().getBlocks().getFirst().getColumns()).hasSize(34);
    }
    backend.putIndexArtifacts(ctx, artifacts);
    assertThat(indexBlobStore.get(artifactRecord.getArtifactUri())).isEqualTo(stored);
    assertThat(
            indexArtifactRepository.getIndexArtifact(
                tableId,
                SNAPSHOT_ID,
                IndexTarget.newBuilder()
                    .setFile(IndexFileTarget.newBuilder().setFilePath("/data/file.parquet").build())
                    .build()))
        .isPresent()
        .get()
        .extracting(IndexArtifactRecord::getArtifactUri)
        .isEqualTo(artifactRecord.getArtifactUri());
  }

  @Test
  void statsCapturedForColumnSelectorsRequiresFullSelectorCoverage() {
    ScalarStats col1 = ScalarStats.newBuilder().setDisplayName("i").build();
    ScalarStats col2 = ScalarStats.newBuilder().setDisplayName("j").build();
    backend.putTargetStats(
        ctx,
        List.of(
            TargetStatsRecords.columnRecord(tableId, SNAPSHOT_ID, 1L, col1, null),
            TargetStatsRecords.columnRecord(tableId, SNAPSHOT_ID, 2L, col2, null)));

    assertThat(
            backend.statsCapturedForColumnSelectors(ctx, tableId, SNAPSHOT_ID, Set.of("#1", "j")))
        .isTrue();
    assertThat(
            backend.statsCapturedForColumnSelectors(ctx, tableId, SNAPSHOT_ID, Set.of("#1", "#3")))
        .isFalse();
    assertThat(backend.statsCapturedForColumnSelectors(ctx, tableId, SNAPSHOT_ID, Set.of("i", "z")))
        .isFalse();
  }

  @Test
  void statsCapturedForColumnSelectorsRejectsMalformedIdSelectors() {
    ScalarStats col1 = ScalarStats.newBuilder().setDisplayName("i").build();
    backend.putTargetStats(
        ctx, List.of(TargetStatsRecords.columnRecord(tableId, SNAPSHOT_ID, 1L, col1, null)));

    assertThat(backend.statsCapturedForColumnSelectors(ctx, tableId, SNAPSHOT_ID, Set.of("#bad")))
        .isFalse();
  }

  @Test
  void statsCapturedForColumnSelectorsFallsBackToAnyColumnWhenSelectorsAreEmpty() {
    assertThat(backend.statsCapturedForColumnSelectors(ctx, tableId, SNAPSHOT_ID, Set.of()))
        .isFalse();

    ScalarStats col1 = ScalarStats.newBuilder().setDisplayName("i").build();
    backend.putTargetStats(
        ctx, List.of(TargetStatsRecords.columnRecord(tableId, SNAPSHOT_ID, 1L, col1, null)));

    assertThat(backend.statsCapturedForColumnSelectors(ctx, tableId, SNAPSHOT_ID, Set.of()))
        .isTrue();
  }

  @Test
  void statsAlreadyCapturedRequiresFileStatsWhenTableReportsDataFiles() {
    TableValueStats tableStats = TableValueStats.newBuilder().setDataFileCount(2L).build();
    backend.putTargetStats(
        ctx, List.of(TargetStatsRecords.tableRecord(tableId, SNAPSHOT_ID, tableStats, null)));

    assertThat(
            backend.statsAlreadyCapturedForTargetKind(
                ctx, tableId, SNAPSHOT_ID, StatsTargetKind.STK_TABLE))
        .isFalse();

    FileTargetStats fileStats = FileTargetStats.newBuilder().setFilePath("/data/file-1").build();
    backend.putTargetStats(
        ctx, List.of(TargetStatsRecords.fileRecord(tableId, SNAPSHOT_ID, fileStats)));

    assertThat(
            backend.statsAlreadyCapturedForTargetKind(
                ctx, tableId, SNAPSHOT_ID, StatsTargetKind.STK_TABLE))
        .isTrue();
  }

  @Test
  void ensureTableDoesNotMutateExistingCoreState() {
    NameRef tableRef = referenceNameRef();
    Table before = tableRepo.getById(tableId).orElseThrow();
    TableSpecDescriptor updatedDescriptor =
        new TableSpecDescriptor(
            "parent",
            "orders",
            "{\"fields\":[{\"name\":\"new\"}]}",
            Map.of("foo", "bar", "baz", "qux"),
            List.of("col"),
            ColumnIdAlgorithm.CID_FIELD_ID,
            ConnectorFormat.CF_ICEBERG,
            connectorId,
            "uri",
            "source.ns",
            "new-table");

    backend.ensureTable(ctx, namespaceId, tableRef, updatedDescriptor);

    Table updated = tableRepo.getById(tableId).orElseThrow();
    assertThat(updated).isEqualTo(before);
  }

  @Test
  void ingestSnapshotIsIdempotent() {
    Snapshot snapshot =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(SNAPSHOT_ID)
            .setSchemaJson("{}")
            .setUpstreamCreatedAt(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()))
            .build();
    backend.ingestSnapshot(ctx, tableId, snapshot);
    backend.ingestSnapshot(ctx, tableId, snapshot);

    Optional<Snapshot> stored = snapshotRepo.getById(tableId, SNAPSHOT_ID);
    assertThat(stored).isPresent();
    assertThat(stored.get().getSchemaJson()).isEqualTo(snapshot.getSchemaJson());
  }

  @Test
  void ingestSnapshotStoresProvidedSnapshotId() {
    Snapshot snapshot =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(5L)
            .setSchemaJson("{}")
            .setUpstreamCreatedAt(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()))
            .build();

    backend.ingestSnapshot(ctx, tableId, snapshot);

    assertThat(snapshotRepo.getById(tableId, 5L)).isPresent();
  }

  @Test
  void lookupTableMatchesNormalizedNameRef() {
    ResourceId tableId = createReferenceTable();
    assertThat(backend.lookupTable(ctx, referenceNameRef())).contains(tableId);

    NameRef noisyRef =
        NameRef.newBuilder()
            .setCatalog(" " + CATALOG_NAME + " ")
            .addPath(" parent ")
            .addPath(" child ")
            .setName(" orders ")
            .build();
    assertThat(backend.lookupTable(ctx, noisyRef)).contains(tableId);
  }

  private ResourceId createReferenceTable() {
    return backend.ensureTable(ctx, namespaceId, referenceNameRef(), sampleDescriptor());
  }

  private NameRef referenceNameRef() {
    return NameRef.newBuilder()
        .setCatalog(CATALOG_NAME)
        .addPath("parent")
        .addPath("child")
        .setName("orders")
        .build();
  }

  private TableSpecDescriptor sampleDescriptor() {
    return new TableSpecDescriptor(
        "parent",
        "orders",
        "{}",
        Map.of("foo", "bar"),
        List.of("col"),
        ColumnIdAlgorithm.CID_FIELD_ID,
        ConnectorFormat.CF_ICEBERG,
        connectorId,
        "uri",
        "source.ns",
        "source_table");
  }

  private static final class ByteArrayInputFile implements InputFile {
    private final byte[] bytes;

    private ByteArrayInputFile(byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public long getLength() {
      return bytes.length;
    }

    @Override
    public SeekableInputStream newStream() {
      return new ByteArraySeekableInputStream(bytes);
    }
  }

  private static final class ByteArraySeekableInputStream extends SeekableInputStream {
    private final byte[] bytes;
    private int position = 0;

    private ByteArraySeekableInputStream(byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public long getPos() {
      return position;
    }

    @Override
    public void seek(long newPos) {
      if (newPos < 0 || newPos > bytes.length) {
        throw new IllegalArgumentException("invalid seek position: " + newPos);
      }
      position = (int) newPos;
    }

    @Override
    public int read() {
      if (position >= bytes.length) {
        return -1;
      }
      return bytes[position++] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) {
      if (position >= bytes.length) {
        return -1;
      }
      int toRead = Math.min(len, bytes.length - position);
      System.arraycopy(bytes, position, b, off, toRead);
      position += toRead;
      return toRead;
    }

    @Override
    public int read(ByteBuffer dst) {
      if (position >= bytes.length) {
        return -1;
      }
      int toRead = Math.min(dst.remaining(), bytes.length - position);
      dst.put(bytes, position, toRead);
      position += toRead;
      return toRead;
    }

    @Override
    public void readFully(byte[] bytes) throws IOException {
      readFully(bytes, 0, bytes.length);
    }

    @Override
    public void readFully(byte[] bytes, int start, int len) throws IOException {
      int total = 0;
      while (total < len) {
        int n = read(bytes, start + total, len - total);
        if (n < 0) {
          throw new IOException("Unexpected EOF");
        }
        total += n;
      }
    }

    @Override
    public void readFully(ByteBuffer dst) throws IOException {
      while (dst.hasRemaining()) {
        int n = read(dst);
        if (n < 0) {
          throw new IOException("Unexpected EOF");
        }
      }
    }

    @Override
    public void close() {}
  }
}
