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
import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableStats;
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
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend.TableSpecDescriptor;
import ai.floedb.floecat.service.metagraph.snapshot.SnapshotHelper;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import ai.floedb.floecat.service.testsupport.FakeCatalogRepository;
import ai.floedb.floecat.service.testsupport.FakeNamespaceRepository;
import ai.floedb.floecat.service.testsupport.FakeTableRepository;
import ai.floedb.floecat.service.testsupport.SnapshotTestSupport;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import com.google.protobuf.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
  private SnapshotTestSupport.FakeSnapshotRepository snapshotRepo;
  private StatsRepository statsRepository;
  private SnapshotHelper snapshotHelper;
  private ConnectorRepository connectorRepo;

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
    snapshotRepo = new SnapshotTestSupport.FakeSnapshotRepository();
    statsRepository =
        StatsRepository.forTesting(new InMemoryPointerStore(), new InMemoryBlobStore(), 64, 200, 5);
    snapshotHelper = new SnapshotHelper(snapshotRepo);
    connectorRepo = new ConnectorRepository(new InMemoryPointerStore(), new InMemoryBlobStore());

    backend = new DirectReconcilerBackend();
    backend.catalogRepo = catalogRepo;
    backend.namespaceRepo = namespaceRepo;
    backend.tableRepo = tableRepo;
    backend.snapshotRepo = snapshotRepo;
    backend.statsRepository = statsRepository;
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
  void lookupCatalogNameAndConnectorReturnEntries() {
    assertThat(backend.lookupCatalogName(ctx, catalogId)).isEqualTo(CATALOG_NAME);
    assertThat(backend.lookupConnector(ctx, connectorId).getResourceId().getId())
        .isEqualTo(connectorId.getId());
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
    TableStats tableStats =
        TableStats.newBuilder().setTableId(tableId).setSnapshotId(SNAPSHOT_ID).build();
    backend.putTableStats(ctx, tableId, tableStats);
    assertThat(statsRepository.getTableStats(tableId, SNAPSHOT_ID)).isPresent();
    assertThat(backend.statsAlreadyCaptured(ctx, tableId, SNAPSHOT_ID)).isTrue();

    ColumnStats columnStats =
        ColumnStats.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(SNAPSHOT_ID)
            .setColumnId(1)
            .setColumnName("col")
            .build();
    backend.putColumnStats(ctx, List.of(columnStats));
    assertThat(statsRepository.getColumnStats(tableId, SNAPSHOT_ID, 1)).isPresent();

    FileColumnStats fileStats =
        FileColumnStats.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(SNAPSHOT_ID)
            .setFilePath("/data/file")
            .build();
    backend.putFileColumnStats(ctx, List.of(fileStats));
    assertThat(statsRepository.getFileColumnStats(tableId, SNAPSHOT_ID, "/data/file")).isPresent();
  }

  @Test
  void ensureTableUpdatesSchemaUpstreamAndProperties() {
    NameRef tableRef = referenceNameRef();
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

    Table updated = tableRepo.getById(tableId).get();
    assertThat(updated.getSchemaJson()).isEqualTo(updatedDescriptor.schemaJson());
    assertThat(updated.getPropertiesMap()).containsEntry("baz", "qux");
    assertThat(updated.getUpstream().getTableDisplayName()).isEqualTo("new-table");
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
}
