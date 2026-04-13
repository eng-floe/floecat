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

package ai.floedb.floecat.reconciler.impl;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.catalog.rpc.ConstraintType;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotConstraints;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.rpc.ConnectorState;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.rpc.NamespacePath;
import ai.floedb.floecat.connector.rpc.SourceSelector;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import ai.floedb.floecat.stats.spi.StatsCaptureControlPlane;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import jakarta.enterprise.inject.Instance;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ReconcilerServiceTest extends AbstractReconcilerServiceTestBase {

  @Test
  void reconcileReturnsResultWhenConnectorLookupFails() {
    service.backend = new ThrowingBackend(new RuntimeException("boom"));
    var result = service.reconcile(principal, connectorId, true, null);
    assertThat(result.errors).isEqualTo(1);
    assertThat(result.error).isInstanceOf(ReconcileFailureException.class);
    assertThat(result.error.getMessage()).contains("getConnector failed:");
  }

  @Test
  void reconcileRejectsInactiveConnector() {
    Connector connector =
        Connector.newBuilder()
            .setResourceId(connectorId)
            .setState(ConnectorState.CS_PAUSED)
            .build();
    service.backend = new ReturningBackend(connector);
    var result = service.reconcile(principal, connectorId, false, null);
    assertThat(result.errors).isEqualTo(1);
    assertThat(result.error).isInstanceOf(IllegalStateException.class);
    assertThat(result.error.getMessage()).contains("Connector not ACTIVE");
  }

  @Test
  void reconcileWritesSnapshotConstraintsViaStatsPathWhenProvidedByConnector() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("table-1")
            .setKind(ResourceKind.RK_TABLE)
            .build();

    class CapturingBackend extends DefaultBackend {
      Set<Long> capturedTargetSnapshotIds = Set.of();
      Set<Long> capturedKnownSnapshotIds = Set.of();
      long putConstraintsSnapshotId = -1L;
      SnapshotConstraints putConstraints = null;

      @Override
      public Connector lookupConnector(ReconcileContext ctx, ResourceId ignoredConnectorId) {
        return activeConnector();
      }

      @Override
      public String lookupCatalogName(ReconcileContext ctx, ResourceId catalogId) {
        return "dest_cat";
      }

      @Override
      public String resolveNamespaceFq(ReconcileContext ctx, ResourceId namespaceId) {
        return "dest_ns";
      }

      @Override
      public ResourceId ensureTable(
          ReconcileContext ctx,
          ResourceId namespaceId,
          NameRef table,
          TableSpecDescriptor descriptor) {
        return tableId;
      }

      @Override
      public ResourceId ensureNamespace(
          ReconcileContext ctx, ResourceId catalogId, NameRef namespace) {
        return ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("dest-ns")
            .build();
      }

      @Override
      public Optional<Snapshot> fetchSnapshot(
          ReconcileContext ctx, ResourceId ignoredTableId, long snapshotId) {
        return Optional.empty();
      }

      @Override
      public void ingestSnapshot(
          ReconcileContext ctx, ResourceId ignoredTableId, Snapshot snapshot) {}

      @Override
      public Set<Long> existingSnapshotIds(ReconcileContext ctx, ResourceId ignoredTableId) {
        return Set.of(42L);
      }

      @Override
      public boolean statsAlreadyCapturedForTargetKind(
          ReconcileContext ctx,
          ResourceId ignoredTableId,
          long snapshotId,
          ai.floedb.floecat.catalog.rpc.StatsTargetKind targetKind) {
        return true;
      }

      @Override
      public void putSnapshotConstraints(
          ReconcileContext ctx,
          ResourceId ignoredTableId,
          long snapshotId,
          SnapshotConstraints constraints) {
        this.putConstraintsSnapshotId = snapshotId;
        this.putConstraints = constraints;
      }

      @Override
      public void updateConnectorDestination(
          ReconcileContext ctx, ResourceId connectorId, DestinationTarget destination) {}
    }

    class ConstraintsConnector extends FakeConnector {
      ConstraintsConnector() {
        super(List.of());
      }

      @Override
      public List<String> listTables(String namespaceFq) {
        return List.of("tbl");
      }

      @Override
      public TableDescriptor describe(String namespaceFq, String tableName) {
        return new TableDescriptor(
            namespaceFq,
            tableName,
            "s3://bucket/path",
            "{\"type\":\"struct\",\"fields\":[]}",
            List.of(),
            ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm.CID_PATH_ORDINAL,
            Map.of());
      }

      @Override
      public List<SnapshotBundle> enumerateSnapshots(
          String namespaceFq,
          String tableName,
          ResourceId destinationTableId,
          SnapshotEnumerationOptions options) {
        return List.of(
            new SnapshotBundle(
                42L, 0L, Instant.now().toEpochMilli(), "", null, 0L, null, Map.of(), 0, Map.of()));
      }

      @Override
      public Optional<SnapshotConstraints> snapshotConstraints(
          String namespaceFq,
          String tableName,
          ResourceId destinationTableId,
          SnapshotBundle snapshotBundle) {
        return Optional.of(
            SnapshotConstraints.newBuilder()
                .addConstraints(
                    ConstraintDefinition.newBuilder()
                        .setName("pk_tbl")
                        .setType(ConstraintType.CT_PRIMARY_KEY)
                        .build())
                .build());
      }
    }

    CapturingBackend backend = new CapturingBackend();
    service.backend = backend;
    service.connectorOpener =
        cfg ->
            new ConstraintsConnector() {
              @Override
              public List<SnapshotBundle> enumerateSnapshots(
                  String namespaceFq,
                  String tableName,
                  ResourceId destinationTableId,
                  SnapshotEnumerationOptions options) {
                backend.capturedTargetSnapshotIds = options.targetSnapshotIds();
                backend.capturedKnownSnapshotIds = options.knownSnapshotIds();
                return super.enumerateSnapshots(
                    namespaceFq, tableName, destinationTableId, options);
              }
            };

    ReconcileScope scope = ReconcileScope.of(List.of(List.of("dest_ns")), "tbl", List.of());
    var result = service.reconcile(principal, connectorId, false, scope);

    assertThat(result.ok()).isTrue();
    assertThat(backend.capturedTargetSnapshotIds).isEmpty();
    assertThat(backend.capturedKnownSnapshotIds).containsExactly(42L);
    assertThat(backend.putConstraintsSnapshotId).isEqualTo(42L);
    assertThat(backend.putConstraints).isNotNull();
    assertThat(backend.putConstraints.getConstraintsCount()).isEqualTo(1);
    assertThat(backend.putConstraints.getConstraints(0).getName()).isEqualTo("pk_tbl");
  }

  @Test
  void reconcileFailsWhenConnectorSnapshotConstraintsThrows() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("table-connector-throw")
            .setKind(ResourceKind.RK_TABLE)
            .build();

    class Backend extends DefaultBackend {
      int putConstraintsCalls = 0;

      @Override
      public Connector lookupConnector(ReconcileContext ctx, ResourceId ignoredConnectorId) {
        return activeConnector();
      }

      @Override
      public String lookupCatalogName(ReconcileContext ctx, ResourceId catalogId) {
        return "dest_cat";
      }

      @Override
      public String resolveNamespaceFq(ReconcileContext ctx, ResourceId namespaceId) {
        return "dest_ns";
      }

      @Override
      public ResourceId ensureTable(
          ReconcileContext ctx,
          ResourceId namespaceId,
          NameRef table,
          TableSpecDescriptor descriptor) {
        return tableId;
      }

      @Override
      public Optional<Snapshot> fetchSnapshot(
          ReconcileContext ctx, ResourceId ignoredTableId, long snapshotId) {
        return Optional.empty();
      }

      @Override
      public void ingestSnapshot(
          ReconcileContext ctx, ResourceId ignoredTableId, Snapshot snapshot) {}

      @Override
      public boolean statsAlreadyCapturedForTargetKind(
          ReconcileContext ctx,
          ResourceId ignoredTableId,
          long snapshotId,
          ai.floedb.floecat.catalog.rpc.StatsTargetKind targetKind) {
        return false;
      }

      @Override
      public Optional<String> lookupTableDisplayName(ReconcileContext ctx, ResourceId tableId) {
        return Optional.of("tbl");
      }

      @Override
      public void putSnapshotConstraints(
          ReconcileContext ctx,
          ResourceId ignoredTableId,
          long snapshotId,
          SnapshotConstraints constraints) {
        putConstraintsCalls++;
      }

      @Override
      public void updateConnectorDestination(
          ReconcileContext ctx, ResourceId connectorId, DestinationTarget destination) {}
    }

    class ThrowingConnector extends FakeConnector {
      ThrowingConnector() {
        super(List.of());
      }

      @Override
      public List<String> listTables(String namespaceFq) {
        return List.of("tbl");
      }

      @Override
      public TableDescriptor describe(String namespaceFq, String tableName) {
        return new TableDescriptor(
            namespaceFq,
            tableName,
            "s3://bucket/path",
            "{\"type\":\"struct\",\"fields\":[]}",
            List.of(),
            ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm.CID_PATH_ORDINAL,
            Map.of());
      }

      @Override
      public List<SnapshotBundle> enumerateSnapshots(
          String namespaceFq,
          String tableName,
          ResourceId destinationTableId,
          SnapshotEnumerationOptions options) {
        return List.of(
            new SnapshotBundle(
                101L, 0L, Instant.now().toEpochMilli(), "", null, 0L, null, Map.of(), 0, Map.of()));
      }

      @Override
      public Optional<SnapshotConstraints> snapshotConstraints(
          String namespaceFq,
          String tableName,
          ResourceId destinationTableId,
          SnapshotBundle snapshotBundle) {
        throw new RuntimeException("connector-constraints-fail");
      }
    }

    Backend backend = new Backend();
    service.backend = backend;
    service.connectorOpener = cfg -> new ThrowingConnector();

    var result = service.reconcile(principal, connectorId, false, null);

    assertThat(result.ok()).isFalse();
    assertThat(result.errors).isEqualTo(1);
    assertThat(result.error).isNotNull();
    assertThat(result.error.getMessage()).contains("connector-constraints-fail");
    assertThat(backend.putConstraintsCalls).isZero();
  }

  @Test
  void reconcileFailsWhenBackendPutSnapshotConstraintsThrows() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("table-backend-throw")
            .setKind(ResourceKind.RK_TABLE)
            .build();

    class Backend extends DefaultBackend {
      @Override
      public Connector lookupConnector(ReconcileContext ctx, ResourceId ignoredConnectorId) {
        Connector base = activeConnector();
        return base.toBuilder()
            .setDestination(base.getDestination().toBuilder().setTableId(tableId))
            .build();
      }

      @Override
      public String lookupCatalogName(ReconcileContext ctx, ResourceId catalogId) {
        return "dest_cat";
      }

      @Override
      public String resolveNamespaceFq(ReconcileContext ctx, ResourceId namespaceId) {
        return "dest_ns";
      }

      @Override
      public ResourceId ensureTable(
          ReconcileContext ctx,
          ResourceId namespaceId,
          NameRef table,
          TableSpecDescriptor descriptor) {
        return tableId;
      }

      @Override
      public Optional<Snapshot> fetchSnapshot(
          ReconcileContext ctx, ResourceId ignoredTableId, long snapshotId) {
        return Optional.empty();
      }

      @Override
      public void ingestSnapshot(
          ReconcileContext ctx, ResourceId ignoredTableId, Snapshot snapshot) {}

      @Override
      public boolean statsAlreadyCapturedForTargetKind(
          ReconcileContext ctx,
          ResourceId ignoredTableId,
          long snapshotId,
          ai.floedb.floecat.catalog.rpc.StatsTargetKind targetKind) {
        return false;
      }

      @Override
      public void putSnapshotConstraints(
          ReconcileContext ctx,
          ResourceId ignoredTableId,
          long snapshotId,
          SnapshotConstraints constraints) {
        throw new RuntimeException("backend-constraints-fail");
      }

      @Override
      public void updateConnectorDestination(
          ReconcileContext ctx, ResourceId connectorId, DestinationTarget destination) {}
    }

    class ConstraintsConnector extends FakeConnector {
      ConstraintsConnector() {
        super(List.of());
      }

      @Override
      public List<String> listTables(String namespaceFq) {
        return List.of("tbl");
      }

      @Override
      public TableDescriptor describe(String namespaceFq, String tableName) {
        return new TableDescriptor(
            namespaceFq,
            tableName,
            "s3://bucket/path",
            "{\"type\":\"struct\",\"fields\":[]}",
            List.of(),
            ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm.CID_PATH_ORDINAL,
            Map.of());
      }

      @Override
      public List<SnapshotBundle> enumerateSnapshots(
          String namespaceFq,
          String tableName,
          ResourceId destinationTableId,
          SnapshotEnumerationOptions options) {
        return List.of(
            new SnapshotBundle(
                102L, 0L, Instant.now().toEpochMilli(), "", null, 0L, null, Map.of(), 0, Map.of()));
      }

      @Override
      public Optional<SnapshotConstraints> snapshotConstraints(
          String namespaceFq,
          String tableName,
          ResourceId destinationTableId,
          SnapshotBundle snapshotBundle) {
        return Optional.of(
            SnapshotConstraints.newBuilder()
                .addConstraints(
                    ConstraintDefinition.newBuilder()
                        .setName("pk_backend")
                        .setType(ConstraintType.CT_PRIMARY_KEY)
                        .build())
                .build());
      }
    }

    service.backend = new Backend();
    service.connectorOpener = cfg -> new ConstraintsConnector();

    var result = service.reconcile(principal, connectorId, false, null);

    assertThat(result.ok()).isFalse();
    assertThat(result.errors).isEqualTo(1);
    assertThat(result.error).isNotNull();
    assertThat(result.error.getMessage()).contains("backend-constraints-fail");
  }

  @Test
  void reconcileSucceedsWhenConnectorReturnsEmptySnapshotConstraints() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("table-no-constraints")
            .setKind(ResourceKind.RK_TABLE)
            .build();

    class Backend extends DefaultBackend {
      int putConstraintsCalls = 0;

      @Override
      public Connector lookupConnector(ReconcileContext ctx, ResourceId ignoredConnectorId) {
        return activeConnector();
      }

      @Override
      public String lookupCatalogName(ReconcileContext ctx, ResourceId catalogId) {
        return "dest_cat";
      }

      @Override
      public String resolveNamespaceFq(ReconcileContext ctx, ResourceId namespaceId) {
        return "dest_ns";
      }

      @Override
      public ResourceId ensureTable(
          ReconcileContext ctx,
          ResourceId namespaceId,
          NameRef table,
          TableSpecDescriptor descriptor) {
        return tableId;
      }

      @Override
      public Optional<Snapshot> fetchSnapshot(
          ReconcileContext ctx, ResourceId ignoredTableId, long snapshotId) {
        return Optional.empty();
      }

      @Override
      public void ingestSnapshot(
          ReconcileContext ctx, ResourceId ignoredTableId, Snapshot snapshot) {}

      @Override
      public boolean statsAlreadyCapturedForTargetKind(
          ReconcileContext ctx,
          ResourceId ignoredTableId,
          long snapshotId,
          ai.floedb.floecat.catalog.rpc.StatsTargetKind targetKind) {
        return false;
      }

      @Override
      public void putSnapshotConstraints(
          ReconcileContext ctx,
          ResourceId ignoredTableId,
          long snapshotId,
          SnapshotConstraints constraints) {
        putConstraintsCalls++;
      }

      @Override
      public void updateConnectorDestination(
          ReconcileContext ctx, ResourceId connectorId, DestinationTarget destination) {}
    }

    class EmptyConstraintsConnector extends FakeConnector {
      EmptyConstraintsConnector() {
        super(List.of());
      }

      @Override
      public List<String> listTables(String namespaceFq) {
        return List.of("tbl");
      }

      @Override
      public TableDescriptor describe(String namespaceFq, String tableName) {
        return new TableDescriptor(
            namespaceFq,
            tableName,
            "s3://bucket/path",
            "{\"type\":\"struct\",\"fields\":[]}",
            List.of(),
            ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm.CID_PATH_ORDINAL,
            Map.of());
      }

      @Override
      public List<SnapshotBundle> enumerateSnapshots(
          String namespaceFq,
          String tableName,
          ResourceId destinationTableId,
          SnapshotEnumerationOptions options) {
        return List.of(
            new SnapshotBundle(
                103L, 0L, Instant.now().toEpochMilli(), "", null, 0L, null, Map.of(), 0, Map.of()));
      }

      @Override
      public Optional<SnapshotConstraints> snapshotConstraints(
          String namespaceFq,
          String tableName,
          ResourceId destinationTableId,
          SnapshotBundle snapshotBundle) {
        return Optional.empty();
      }
    }

    Backend backend = new Backend();
    service.backend = backend;
    service.connectorOpener = cfg -> new EmptyConstraintsConnector();

    var result = service.reconcile(principal, connectorId, false, null);

    assertThat(result.ok()).isTrue();
    assertThat(result.errors).isZero();
    assertThat(backend.putConstraintsCalls).isZero();
  }

  @Test
  void reconcileFailsWhenStatsAlreadyCapturedAndConstraintWriteFails() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("table-stats-captured-constraints-fail")
            .setKind(ResourceKind.RK_TABLE)
            .build();

    class Backend extends DefaultBackend {
      int putTargetStatsCalls = 0;

      @Override
      public Connector lookupConnector(ReconcileContext ctx, ResourceId ignoredConnectorId) {
        return activeConnector();
      }

      @Override
      public String lookupCatalogName(ReconcileContext ctx, ResourceId catalogId) {
        return "dest_cat";
      }

      @Override
      public String resolveNamespaceFq(ReconcileContext ctx, ResourceId namespaceId) {
        return "dest_ns";
      }

      @Override
      public ResourceId ensureTable(
          ReconcileContext ctx,
          ResourceId namespaceId,
          NameRef table,
          TableSpecDescriptor descriptor) {
        return tableId;
      }

      @Override
      public Optional<Snapshot> fetchSnapshot(
          ReconcileContext ctx, ResourceId ignoredTableId, long snapshotId) {
        return Optional.empty();
      }

      @Override
      public void ingestSnapshot(
          ReconcileContext ctx, ResourceId ignoredTableId, Snapshot snapshot) {}

      @Override
      public boolean statsAlreadyCapturedForTargetKind(
          ReconcileContext ctx,
          ResourceId ignoredTableId,
          long snapshotId,
          ai.floedb.floecat.catalog.rpc.StatsTargetKind targetKind) {
        return true;
      }

      @Override
      public void putTargetStats(ReconcileContext ctx, List<TargetStatsRecord> stats) {
        putTargetStatsCalls++;
      }

      @Override
      public void putSnapshotConstraints(
          ReconcileContext ctx,
          ResourceId ignoredTableId,
          long snapshotId,
          SnapshotConstraints constraints) {
        throw new RuntimeException("stats-captured-constraints-fail");
      }

      @Override
      public void updateConnectorDestination(
          ReconcileContext ctx, ResourceId connectorId, DestinationTarget destination) {}
    }

    class ConstraintsConnector extends FakeConnector {
      ConstraintsConnector() {
        super(List.of());
      }

      @Override
      public List<String> listTables(String namespaceFq) {
        return List.of("tbl");
      }

      @Override
      public TableDescriptor describe(String namespaceFq, String tableName) {
        return new TableDescriptor(
            namespaceFq,
            tableName,
            "s3://bucket/path",
            "{\"type\":\"struct\",\"fields\":[]}",
            List.of(),
            ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm.CID_PATH_ORDINAL,
            Map.of());
      }

      @Override
      public List<SnapshotBundle> enumerateSnapshots(
          String namespaceFq,
          String tableName,
          ResourceId destinationTableId,
          SnapshotEnumerationOptions options) {
        return List.of(
            new SnapshotBundle(
                104L, 0L, Instant.now().toEpochMilli(), "", null, 0L, null, Map.of(), 0, Map.of()));
      }

      @Override
      public Optional<SnapshotConstraints> snapshotConstraints(
          String namespaceFq,
          String tableName,
          ResourceId destinationTableId,
          SnapshotBundle snapshotBundle) {
        return Optional.of(
            SnapshotConstraints.newBuilder()
                .addConstraints(
                    ConstraintDefinition.newBuilder()
                        .setName("pk_stats_captured")
                        .setType(ConstraintType.CT_PRIMARY_KEY)
                        .build())
                .build());
      }
    }

    Backend backend = new Backend();
    service.backend = backend;
    service.connectorOpener = cfg -> new ConstraintsConnector();

    var result = service.reconcile(principal, connectorId, false, null);

    assertThat(result.ok()).isFalse();
    assertThat(result.errors).isEqualTo(1);
    assertThat(result.error).isNotNull();
    assertThat(result.error.getMessage()).contains("stats-captured-constraints-fail");
    assertThat(backend.putTargetStatsCalls).isZero();
  }

  @Test
  void statsOnlyRoutesThroughCaptureEnginesForAllDiscoveredSnapshots() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("table-stats-only-engine")
            .setKind(ResourceKind.RK_TABLE)
            .build();

    class Backend extends DefaultBackend {
      int putTargetStatsCalls = 0;

      @Override
      public Connector lookupConnector(ReconcileContext ctx, ResourceId ignoredConnectorId) {
        ResourceId destCatalogId =
            ResourceId.newBuilder().setAccountId("acct").setId("cat-1").build();
        ResourceId destNamespaceId =
            ResourceId.newBuilder().setAccountId("acct").setId("ns-1").build();
        return Connector.newBuilder()
            .setResourceId(connectorId)
            .setState(ConnectorState.CS_ACTIVE)
            .setKind(ConnectorKind.CK_ICEBERG)
            .setSource(
                SourceSelector.newBuilder()
                    .setNamespace(
                        NamespacePath.newBuilder().addSegments("src_cat").addSegments("src_ns")))
            .setDestination(
                DestinationTarget.newBuilder()
                    .setCatalogId(destCatalogId)
                    .setNamespaceId(destNamespaceId)
                    .setTableId(tableId))
            .build();
      }

      @Override
      public String lookupCatalogName(ReconcileContext ctx, ResourceId catalogId) {
        return "dest_cat";
      }

      @Override
      public String resolveNamespaceFq(ReconcileContext ctx, ResourceId namespaceId) {
        return "dest_ns";
      }

      @Override
      public Set<Long> existingSnapshotIds(ReconcileContext ctx, ResourceId ignoredTableId) {
        return Set.of();
      }

      @Override
      public boolean statsAlreadyCapturedForTargetKind(
          ReconcileContext ctx,
          ResourceId ignoredTableId,
          long snapshotId,
          ai.floedb.floecat.catalog.rpc.StatsTargetKind targetKind) {
        return false;
      }

      @Override
      public Optional<String> lookupTableDisplayName(ReconcileContext ctx, ResourceId tableId) {
        return Optional.of("tbl");
      }

      @Override
      public void putTargetStats(ReconcileContext ctx, List<TargetStatsRecord> stats) {
        putTargetStatsCalls++;
      }

      @Override
      public void updateConnectorDestination(
          ReconcileContext ctx, ResourceId connectorId, DestinationTarget destination) {}
    }

    class SnapshotOnlyConnector extends FakeConnector {
      SnapshotOnlyConnector() {
        super(List.of());
      }

      @Override
      public List<String> listTables(String namespaceFq) {
        return List.of("tbl");
      }

      @Override
      public TableDescriptor describe(String namespaceFq, String tableName) {
        return new TableDescriptor(
            namespaceFq,
            tableName,
            "s3://bucket/path",
            "{\"type\":\"struct\",\"fields\":[]}",
            List.of(),
            ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm.CID_PATH_ORDINAL,
            Map.of());
      }

      @Override
      public List<SnapshotBundle> enumerateSnapshots(
          String namespaceFq,
          String tableName,
          ResourceId destinationTableId,
          SnapshotEnumerationOptions options) {
        return List.of(
            new SnapshotBundle(
                201L, 0L, Instant.now().toEpochMilli(), "", null, 0L, null, Map.of(), 0, Map.of()),
            new SnapshotBundle(
                202L,
                201L,
                Instant.now().toEpochMilli(),
                "",
                null,
                0L,
                null,
                Map.of(),
                0,
                Map.of()));
      }
    }

    AtomicInteger captureCalls = new AtomicInteger();
    AtomicReference<StatsCaptureRequest> capturedRequest = new AtomicReference<>();
    StatsCaptureControlPlane controlPlane = capturedControlPlane(captureCalls, capturedRequest);

    @SuppressWarnings("unchecked")
    Instance<StatsCaptureControlPlane> instance = Mockito.mock(Instance.class);
    Mockito.when(instance.isUnsatisfied()).thenReturn(false);
    Mockito.when(instance.get()).thenReturn(controlPlane);

    Backend backend = new Backend();
    service.backend = backend;
    service.statsCaptureControlPlane = instance;
    service.connectorOpener = cfg -> new SnapshotOnlyConnector();

    var result =
        service.reconcile(
            principal,
            connectorId,
            false,
            ReconcileScope.of(List.of(List.of("dest_ns")), "tbl", List.of("c7", "#9")),
            ReconcilerService.CaptureMode.STATS_ONLY);

    assertThat(result.ok()).isTrue();
    assertThat(captureCalls.get()).isEqualTo(2);
    assertThat(result.statsProcessed).isEqualTo(2L);
    assertThat(capturedRequest.get()).isNotNull();
    assertThat(capturedRequest.get().columnSelectors()).containsExactlyInAnyOrder("c7", "#9");
    assertThat(backend.putTargetStatsCalls).isZero();
  }

  @Test
  void statsOnlyConnectorAssistedPathFailsWhenNoSnapshotsAreCapturable() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("table-stats-only-engine")
            .setKind(ResourceKind.RK_TABLE)
            .build();

    class Backend extends DefaultBackend {
      @Override
      public Connector lookupConnector(ReconcileContext ctx, ResourceId ignoredConnectorId) {
        ResourceId destCatalogId =
            ResourceId.newBuilder().setAccountId("acct").setId("cat-1").build();
        ResourceId destNamespaceId =
            ResourceId.newBuilder().setAccountId("acct").setId("ns-1").build();
        return Connector.newBuilder()
            .setResourceId(connectorId)
            .setState(ConnectorState.CS_ACTIVE)
            .setKind(ConnectorKind.CK_ICEBERG)
            .setSource(
                SourceSelector.newBuilder()
                    .setNamespace(
                        NamespacePath.newBuilder().addSegments("src_cat").addSegments("src_ns")))
            .setDestination(
                DestinationTarget.newBuilder()
                    .setCatalogId(destCatalogId)
                    .setNamespaceId(destNamespaceId)
                    .setTableId(tableId))
            .build();
      }

      @Override
      public String lookupCatalogName(ReconcileContext ctx, ResourceId catalogId) {
        return "dest_cat";
      }

      @Override
      public String resolveNamespaceFq(ReconcileContext ctx, ResourceId namespaceId) {
        return "dest_ns";
      }

      @Override
      public Set<Long> existingSnapshotIds(ReconcileContext ctx, ResourceId ignoredTableId) {
        return Set.of();
      }

      @Override
      public boolean statsAlreadyCapturedForTargetKind(
          ReconcileContext ctx,
          ResourceId ignoredTableId,
          long snapshotId,
          ai.floedb.floecat.catalog.rpc.StatsTargetKind targetKind) {
        return false;
      }

      @Override
      public void updateConnectorDestination(
          ReconcileContext ctx, ResourceId connectorId, DestinationTarget destination) {}
    }

    class SnapshotOnlyConnector extends FakeConnector {
      SnapshotOnlyConnector() {
        super(List.of());
      }

      @Override
      public List<String> listTables(String namespaceFq) {
        return List.of("tbl");
      }

      @Override
      public TableDescriptor describe(String namespaceFq, String tableName) {
        return new TableDescriptor(
            namespaceFq,
            tableName,
            "s3://bucket/path",
            "{\"type\":\"struct\",\"fields\":[]}",
            List.of(),
            ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm.CID_PATH_ORDINAL,
            Map.of());
      }

      @Override
      public List<SnapshotBundle> enumerateSnapshots(
          String namespaceFq,
          String tableName,
          ResourceId destinationTableId,
          SnapshotEnumerationOptions options) {
        return List.of(
            new SnapshotBundle(
                201L, 0L, Instant.now().toEpochMilli(), "", null, 0L, null, Map.of(), 0, Map.of()),
            new SnapshotBundle(
                202L,
                201L,
                Instant.now().toEpochMilli(),
                "",
                null,
                0L,
                null,
                Map.of(),
                0,
                Map.of()));
      }
    }

    @SuppressWarnings("unchecked")
    Instance<StatsCaptureControlPlane> instance = Mockito.mock(Instance.class);
    Mockito.when(instance.isUnsatisfied()).thenReturn(true);

    service.backend = new Backend();
    service.statsCaptureControlPlane = instance;
    service.connectorOpener = cfg -> new SnapshotOnlyConnector();

    var result =
        service.reconcile(
            principal,
            connectorId,
            false,
            ReconcileScope.of(List.of(List.of("dest_ns")), "tbl", List.of("c7", "#9")),
            ReconcilerService.CaptureMode.STATS_ONLY);

    assertThat(result.ok()).isTrue();
    assertThat(result.snapshotsProcessed).isEqualTo(2L);
    assertThat(result.statsProcessed).isZero();
    assertThat(result.errors).isZero();
  }
}
