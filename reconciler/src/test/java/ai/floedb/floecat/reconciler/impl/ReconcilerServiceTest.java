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

import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.catalog.rpc.ConstraintType;
import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotConstraints;
import ai.floedb.floecat.catalog.rpc.TableStats;
import ai.floedb.floecat.catalog.rpc.ViewSpec;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.connector.common.resolver.LogicalSchemaMapper;
import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.rpc.ConnectorState;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.rpc.NamespacePath;
import ai.floedb.floecat.connector.rpc.SourceSelector;
import ai.floedb.floecat.connector.spi.ConnectorFormat;
import ai.floedb.floecat.connector.spi.ConnectorNotReadyException;
import ai.floedb.floecat.connector.spi.CredentialResolver;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import ai.floedb.floecat.reconciler.spi.ReconcileExecutor;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongPredicate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ReconcilerServiceTest {
  private ReconcilerService service;
  private PrincipalContext principal;
  private ResourceId connectorId;

  @BeforeEach
  void setUp() {
    service = new ReconcilerService();
    service.schemaMapper = new LogicalSchemaMapper();
    service.credentialResolver = new InMemoryCredentialResolver();
    principal =
        PrincipalContext.newBuilder().setAccountId("acct").setCorrelationId("corr-id").build();
    connectorId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("connector-1")
            .setKind(ResourceKind.RK_CONNECTOR)
            .build();
  }

  @Test
  void reconcileReturnsResultWhenConnectorLookupFails() {
    service.backend = new ThrowingBackend(new RuntimeException("boom"));
    var result = service.reconcile(principal, connectorId, true, null);
    assertThat(result.errors).isEqualTo(1);
    assertThat(result.error).isInstanceOf(ReconcileFailureException.class);
    assertThat(((ReconcileFailureException) result.error).failureKind())
        .isEqualTo(ReconcileExecutor.ExecutionResult.FailureKind.CONNECTOR_MISSING);
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
      public boolean statsAlreadyCaptured(
          ReconcileContext ctx, ResourceId ignoredTableId, long snapshotId) {
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
      public List<SnapshotBundle> enumerateSnapshotsWithStats(
          String namespaceFq,
          String tableName,
          ResourceId destinationTableId,
          Set<String> includeColumns,
          SnapshotEnumerationOptions options) {
        return List.of(
            new SnapshotBundle(
                42L,
                0L,
                Instant.now().toEpochMilli(),
                null,
                List.of(),
                List.of(),
                null,
                null,
                0L,
                null,
                Map.of(),
                0,
                Map.of()));
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
              public List<SnapshotBundle> enumerateSnapshotsWithStats(
                  String namespaceFq,
                  String tableName,
                  ResourceId destinationTableId,
                  Set<String> includeColumns,
                  SnapshotEnumerationOptions options) {
                backend.capturedKnownSnapshotIds = options.knownSnapshotIds();
                return super.enumerateSnapshotsWithStats(
                    namespaceFq, tableName, destinationTableId, includeColumns, options);
              }
            };

    ReconcileScope scope = ReconcileScope.of(List.of(List.of("dest_ns")), "tbl", List.of());
    var result = service.reconcile(principal, connectorId, false, scope);

    assertThat(result.ok()).isTrue();
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
      public boolean statsAlreadyCaptured(
          ReconcileContext ctx, ResourceId ignoredTableId, long snapshotId) {
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
      public List<SnapshotBundle> enumerateSnapshotsWithStats(
          String namespaceFq,
          String tableName,
          ResourceId destinationTableId,
          Set<String> includeColumns,
          SnapshotEnumerationOptions options) {
        return List.of(
            new SnapshotBundle(
                101L,
                0L,
                Instant.now().toEpochMilli(),
                null,
                List.of(),
                List.of(),
                null,
                null,
                0L,
                null,
                Map.of(),
                0,
                Map.of()));
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
      public boolean statsAlreadyCaptured(
          ReconcileContext ctx, ResourceId ignoredTableId, long snapshotId) {
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
      public List<SnapshotBundle> enumerateSnapshotsWithStats(
          String namespaceFq,
          String tableName,
          ResourceId destinationTableId,
          Set<String> includeColumns,
          SnapshotEnumerationOptions options) {
        return List.of(
            new SnapshotBundle(
                102L,
                0L,
                Instant.now().toEpochMilli(),
                null,
                List.of(),
                List.of(),
                null,
                null,
                0L,
                null,
                Map.of(),
                0,
                Map.of()));
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
      public boolean statsAlreadyCaptured(
          ReconcileContext ctx, ResourceId ignoredTableId, long snapshotId) {
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
      public List<SnapshotBundle> enumerateSnapshotsWithStats(
          String namespaceFq,
          String tableName,
          ResourceId destinationTableId,
          Set<String> includeColumns,
          SnapshotEnumerationOptions options) {
        return List.of(
            new SnapshotBundle(
                103L,
                0L,
                Instant.now().toEpochMilli(),
                null,
                List.of(),
                List.of(),
                null,
                null,
                0L,
                null,
                Map.of(),
                0,
                Map.of()));
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
      int putTableStatsCalls = 0;
      int putColumnStatsCalls = 0;
      int putFileColumnStatsCalls = 0;

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
      public boolean statsAlreadyCaptured(
          ReconcileContext ctx, ResourceId ignoredTableId, long snapshotId) {
        return true;
      }

      @Override
      public void putTableStats(ReconcileContext ctx, ResourceId ignoredTableId, TableStats stats) {
        putTableStatsCalls++;
      }

      @Override
      public void putColumnStats(ReconcileContext ctx, List<ColumnStats> stats) {
        putColumnStatsCalls++;
      }

      @Override
      public void putFileColumnStats(ReconcileContext ctx, List<FileColumnStats> stats) {
        putFileColumnStatsCalls++;
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
      public List<SnapshotBundle> enumerateSnapshotsWithStats(
          String namespaceFq,
          String tableName,
          ResourceId destinationTableId,
          Set<String> includeColumns,
          SnapshotEnumerationOptions options) {
        return List.of(
            new SnapshotBundle(
                104L,
                0L,
                Instant.now().toEpochMilli(),
                TableStats.newBuilder().setSnapshotId(104L).build(),
                List.of(),
                List.of(),
                null,
                null,
                0L,
                null,
                Map.of(),
                0,
                Map.of()));
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
    assertThat(backend.putTableStatsCalls).isZero();
    assertThat(backend.putColumnStatsCalls).isZero();
    assertThat(backend.putFileColumnStatsCalls).isZero();
  }

  @Test
  void reconcileStatsOnlyFailsWhenDestinationTableIsNotVisibleYet() {
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
                    .setNamespaceId(destNamespaceId))
            .build();
      }

      @Override
      public String lookupCatalogName(ReconcileContext ctx, ResourceId catalogId) {
        return "examples";
      }

      @Override
      public String resolveNamespaceFq(ReconcileContext ctx, ResourceId namespaceId) {
        return "iceberg";
      }

      @Override
      public Optional<ResourceId> lookupTable(ReconcileContext ctx, NameRef table) {
        return Optional.empty();
      }

      @Override
      public void updateConnectorDestination(
          ReconcileContext ctx, ResourceId connectorId, DestinationTarget destination) {}
    }

    class StatsOnlyConnector extends FakeConnector {
      StatsOnlyConnector() {
        super(List.of());
      }

      @Override
      public ConnectorFormat format() {
        return ConnectorFormat.CF_ICEBERG;
      }

      @Override
      public List<String> listTables(String namespaceFq) {
        return List.of("duckdb_mutation_smoke");
      }

      @Override
      public TableDescriptor describe(String namespaceFq, String tableName) {
        return new TableDescriptor(
            "iceberg",
            "duckdb_mutation_smoke",
            "s3://floecat/iceberg/duckdb_mutation_smoke",
            "{\"type\":\"struct\",\"fields\":[]}",
            List.of(),
            ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm.CID_FIELD_ID,
            Map.of());
      }
    }

    service.backend = new Backend();
    service.connectorOpener = cfg -> new StatsOnlyConnector();

    var result =
        service.reconcile(
            principal, connectorId, false, null, ReconcilerService.CaptureMode.STATS_ONLY);

    assertThat(result.ok()).isFalse();
    assertThat(result.errors).isEqualTo(1);
    assertThat(result.snapshotsProcessed).isZero();
    assertThat(result.statsProcessed).isZero();
    assertThat(result.error).isNotNull();
    assertThat(result.error.getMessage())
        .contains("Destination table examples.iceberg.duckdb_mutation_smoke is not visible yet");
  }

  @Test
  void reconcileIcebergRestStatsCaptureRetriesWhenCurrentSnapshotEnumeratesEmpty() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("table-1")
            .setKind(ResourceKind.RK_TABLE)
            .build();

    class Backend extends DefaultBackend {
      @Override
      public Connector lookupConnector(ReconcileContext ctx, ResourceId ignoredConnectorId) {
        return activeIcebergConnector(tableId);
      }

      @Override
      public String lookupCatalogName(ReconcileContext ctx, ResourceId catalogId) {
        return "examples";
      }

      @Override
      public String resolveNamespaceFq(ReconcileContext ctx, ResourceId namespaceId) {
        return "iceberg";
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
      public Set<Long> existingSnapshotIds(ReconcileContext ctx, ResourceId ignoredTableId) {
        return Set.of();
      }

      @Override
      public void updateConnectorDestination(
          ReconcileContext ctx, ResourceId connectorId, DestinationTarget destination) {}
    }

    class EmptyIcebergRestConnector extends FakeConnector {
      EmptyIcebergRestConnector() {
        super(List.of());
      }

      @Override
      public ConnectorFormat format() {
        return ConnectorFormat.CF_ICEBERG;
      }

      @Override
      public List<String> listTables(String namespaceFq) {
        return List.of("duckdb_mutation_smoke");
      }

      @Override
      public TableDescriptor describe(String namespaceFq, String tableName) {
        return new TableDescriptor(
            "iceberg",
            "duckdb_mutation_smoke",
            "s3://floecat/iceberg/duckdb_mutation_smoke",
            "{\"type\":\"struct\",\"fields\":[]}",
            List.of(),
            ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm.CID_FIELD_ID,
            Map.of());
      }

      @Override
      public List<SnapshotBundle> enumerateSnapshotsWithStats(
          String namespaceFq,
          String tableName,
          ResourceId destinationTableId,
          Set<String> includeColumns,
          SnapshotEnumerationOptions options) {
        throw new ConnectorNotReadyException(
            "Current snapshot for iceberg.duckdb_mutation_smoke is not fully observable yet");
      }
    }

    service.backend = new Backend();
    service.connectorOpener = cfg -> new EmptyIcebergRestConnector();

    var result =
        service.reconcile(
            principal, connectorId, false, null, ReconcilerService.CaptureMode.METADATA_AND_STATS);

    assertThat(result.ok()).isFalse();
    assertThat(result.errors).isEqualTo(1);
    assertThat(result.snapshotsProcessed).isZero();
    assertThat(result.statsProcessed).isZero();
    assertThat(result.error).isNotNull();
    assertThat(result.error.getMessage())
        .contains("Current snapshot for iceberg.duckdb_mutation_smoke is not fully observable yet");
  }

  private static final class ThrowingBackend extends DefaultBackend {
    private final RuntimeException failure;

    private ThrowingBackend(RuntimeException failure) {
      this.failure = failure;
    }

    @Override
    public Connector lookupConnector(ReconcileContext ctx, ResourceId connectorId) {
      throw failure;
    }
  }

  private static final class ReturningBackend extends DefaultBackend {
    private final Connector connector;

    private ReturningBackend(Connector connector) {
      this.connector = connector;
    }

    @Override
    public Connector lookupConnector(ReconcileContext ctx, ResourceId connectorId) {
      return connector;
    }
  }

  private abstract static class DefaultBackend implements ReconcilerBackend {
    @Override
    public ResourceId ensureNamespace(
        ReconcileContext ctx, ResourceId catalogId, NameRef namespace) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ResourceId ensureTable(
        ReconcileContext ctx,
        ResourceId namespaceId,
        NameRef table,
        TableSpecDescriptor descriptor) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ResourceId> lookupTable(ReconcileContext ctx, NameRef table) {
      throw new UnsupportedOperationException();
    }

    @Override
    public SnapshotPin snapshotPinFor(
        ReconcileContext ctx, ResourceId tableId, SnapshotRef ref, Optional<Timestamp> asOf) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Snapshot> fetchSnapshot(
        ReconcileContext ctx, ResourceId tableId, long snapshotId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> existingSnapshotIds(ReconcileContext ctx, ResourceId tableId) {
      return Set.of();
    }

    @Override
    public void ingestSnapshot(ReconcileContext ctx, ResourceId tableId, Snapshot snapshot) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean statsAlreadyCaptured(ReconcileContext ctx, ResourceId tableId, long snapshotId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void putTableStats(ReconcileContext ctx, ResourceId tableId, TableStats stats) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void putColumnStats(ReconcileContext ctx, List<ColumnStats> stats) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void putFileColumnStats(ReconcileContext ctx, List<FileColumnStats> stats) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String lookupCatalogName(ReconcileContext ctx, ResourceId catalogId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String resolveNamespaceFq(ReconcileContext ctx, ResourceId namespaceId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Connector lookupConnector(ReconcileContext ctx, ResourceId connectorId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateConnectorDestination(
        ReconcileContext ctx, ResourceId connectorId, DestinationTarget destination) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ResourceId ensureView(ReconcileContext ctx, ViewSpec spec, String idempotencyKey) {
      throw new UnsupportedOperationException();
    }
  }

  private static final class InMemoryCredentialResolver implements CredentialResolver {
    private final Map<String, AuthCredentials> store = new HashMap<>();

    @Override
    public Optional<AuthCredentials> resolve(String accountId, String credentialId) {
      return Optional.ofNullable(store.get(key(accountId, credentialId)));
    }

    @Override
    public void store(String accountId, String credentialId, AuthCredentials credentials) {
      store.put(key(accountId, credentialId), credentials);
    }

    @Override
    public void delete(String accountId, String credentialId) {
      store.remove(key(accountId, credentialId));
    }

    private String key(String accountId, String credentialId) {
      return accountId + ":" + credentialId;
    }
  }

  @Test
  void buildSnapshotRetainsExistingDataWhenBundleOmitsFields() {
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("tbl").build();
    Snapshot existing =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(123L)
            .setSchemaJson("existing-schema")
            .setManifestList("existing-manifest")
            .putSummary("existing-key", "existing-val")
            .putFormatMetadata("meta-key", ByteString.copyFromUtf8("old"))
            .build();

    FloecatConnector.SnapshotBundle bundle =
        new FloecatConnector.SnapshotBundle(
            existing.getSnapshotId(),
            existing.getParentSnapshotId(),
            Instant.now().toEpochMilli(),
            null,
            List.of(),
            List.of(),
            null,
            null,
            0,
            null,
            Map.of("new-key", "new-val"),
            0,
            Map.of(
                "meta-key", ByteString.copyFromUtf8("new"),
                "extra", ByteString.copyFromUtf8("value")));

    ReconcileContext ctx =
        new ReconcileContext("ctx", principal, "svc-test", Instant.now(), Optional.<String>empty());
    Snapshot result = service.buildSnapshot(ctx, tableId, bundle, existing).orElseThrow();

    assertThat(result.getManifestList()).isEqualTo(existing.getManifestList());
    assertThat(result.getSchemaJson()).isEqualTo(existing.getSchemaJson());
    assertThat(result.getSummaryMap()).containsEntry("existing-key", "existing-val");
    assertThat(result.getSummaryMap()).containsEntry("new-key", "new-val");
    assertThat(result.getFormatMetadataMap())
        .containsEntry("meta-key", ByteString.copyFromUtf8("new"));
    assertThat(result.getFormatMetadataMap())
        .containsEntry("extra", ByteString.copyFromUtf8("value"));
  }

  @Test
  void effectiveSelectorsPreferScopeColumnsOverConnectorSourceColumns() throws Exception {
    ReconcileScope scope = ReconcileScope.of(List.of(List.of("ns")), "tbl", List.of("barf", "#2"));
    SourceSelector source = SourceSelector.newBuilder().addColumns("i").addColumns("#1").build();

    Set<String> selectors = invokeEffectiveSelectors(scope, source);

    assertThat(selectors).containsExactlyInAnyOrder("barf", "#2");
  }

  @Test
  void effectiveSelectorsFallbackToConnectorSourceColumnsWhenScopeHasNoColumns() throws Exception {
    ReconcileScope scope = ReconcileScope.of(List.of(List.of("ns")), "tbl", List.of());
    SourceSelector source = SourceSelector.newBuilder().addColumns("i").addColumns("#1").build();

    Set<String> selectors = invokeEffectiveSelectors(scope, source);

    assertThat(selectors).containsExactlyInAnyOrder("i", "#1");
  }

  @Test
  void filterBundlesForModeSkipsAlreadyIngestedSnapshotsForIncremental() throws Exception {
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("tbl").build();
    service.backend =
        new DefaultBackend() {
          @Override
          public Set<Long> existingSnapshotIds(ReconcileContext ctx, ResourceId ignoredTableId) {
            return Set.of(10L, 12L);
          }
        };
    ReconcileContext ctx =
        new ReconcileContext("ctx", principal, "svc-test", Instant.now(), Optional.<String>empty());
    List<FloecatConnector.SnapshotBundle> bundles =
        List.of(
            new FloecatConnector.SnapshotBundle(
                10L, 0L, 1L, null, List.of(), List.of(), null, null, 0L, null, Map.of(), 0,
                Map.of()),
            new FloecatConnector.SnapshotBundle(
                11L, 10L, 2L, null, List.of(), List.of(), null, null, 0L, null, Map.of(), 0,
                Map.of()),
            new FloecatConnector.SnapshotBundle(
                12L, 11L, 3L, null, List.of(), List.of(), null, null, 0L, null, Map.of(), 0,
                Map.of()));

    @SuppressWarnings("unchecked")
    List<FloecatConnector.SnapshotBundle> filtered =
        (List<FloecatConnector.SnapshotBundle>)
            invokeFilterBundlesForMode(bundles, false, true, false, Set.of(10L, 12L));

    assertThat(filtered)
        .extracting(FloecatConnector.SnapshotBundle::snapshotId)
        .containsExactly(11L);
  }

  @Test
  void filterBundlesForModeKeepsAllSnapshotsForFullRescan() throws Exception {
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("tbl").build();
    service.backend =
        new DefaultBackend() {
          @Override
          public Set<Long> existingSnapshotIds(ReconcileContext ctx, ResourceId ignoredTableId) {
            return Set.of(10L, 12L);
          }
        };
    ReconcileContext ctx =
        new ReconcileContext("ctx", principal, "svc-test", Instant.now(), Optional.<String>empty());
    List<FloecatConnector.SnapshotBundle> bundles =
        List.of(
            new FloecatConnector.SnapshotBundle(
                10L, 0L, 1L, null, List.of(), List.of(), null, null, 0L, null, Map.of(), 0,
                Map.of()),
            new FloecatConnector.SnapshotBundle(
                11L, 10L, 2L, null, List.of(), List.of(), null, null, 0L, null, Map.of(), 0,
                Map.of()));

    @SuppressWarnings("unchecked")
    List<FloecatConnector.SnapshotBundle> filtered =
        (List<FloecatConnector.SnapshotBundle>)
            invokeFilterBundlesForMode(bundles, true, true, false, Set.of(10L, 12L));

    assertThat(filtered)
        .extracting(FloecatConnector.SnapshotBundle::snapshotId)
        .containsExactly(10L, 11L);
  }

  @Test
  void filterBundlesForModeSkipsKnownSnapshotsWhenStatsAreIncluded() throws Exception {
    List<FloecatConnector.SnapshotBundle> bundles =
        List.of(
            new FloecatConnector.SnapshotBundle(
                10L, 0L, 1L, null, List.of(), List.of(), null, null, 0L, null, Map.of(), 0,
                Map.of()),
            new FloecatConnector.SnapshotBundle(
                11L, 10L, 2L, null, List.of(), List.of(), null, null, 0L, null, Map.of(), 0,
                Map.of()));

    @SuppressWarnings("unchecked")
    List<FloecatConnector.SnapshotBundle> filtered =
        (List<FloecatConnector.SnapshotBundle>)
            invokeFilterBundlesForMode(bundles, false, true, true, Set.of(10L));

    assertThat(filtered)
        .extracting(FloecatConnector.SnapshotBundle::snapshotId)
        .containsExactly(10L, 11L);
  }

  @Test
  void knownSnapshotIdsForEnumerationKeepsKnownSnapshotsForMetadataOnlyRuns() throws Exception {
    @SuppressWarnings("unchecked")
    Set<Long> metadataOnly =
        (Set<Long>)
            invokeKnownSnapshotIdsForEnumeration(false, false, Set.of(10L, 11L), id -> false);
    @SuppressWarnings("unchecked")
    Set<Long> fullRescan =
        (Set<Long>)
            invokeKnownSnapshotIdsForEnumeration(true, false, Set.of(10L, 11L), id -> false);

    assertThat(metadataOnly).containsExactlyInAnyOrder(10L, 11L);
    assertThat(fullRescan).isEmpty();
  }

  @Test
  void knownSnapshotIdsForEnumerationKeepsKnownSnapshotsWithoutStatsEnumerable() throws Exception {
    @SuppressWarnings("unchecked")
    Set<Long> statsOnly =
        (Set<Long>)
            invokeKnownSnapshotIdsForEnumeration(false, true, Set.of(10L, 11L), id -> false);

    assertThat(statsOnly).isEmpty();
  }

  @Test
  void knownSnapshotIdsForEnumerationPrunesOnlyKnownSnapshotsWithCapturedStats() throws Exception {
    @SuppressWarnings("unchecked")
    Set<Long> statsOnly =
        (Set<Long>)
            invokeKnownSnapshotIdsForEnumeration(false, true, Set.of(10L, 11L), id -> id == 10L);

    assertThat(statsOnly).containsExactly(10L);
  }

  @Test
  void tableChangedIsFalseWhenNoBundlesRemain() throws Exception {
    assertThat(invokeTableChanged(List.of())).isFalse();
  }

  @Test
  void tableChangedIsTrueWhenBundlesRemain() throws Exception {
    assertThat(
            invokeTableChanged(
                List.of(
                    new FloecatConnector.SnapshotBundle(
                        11L, 10L, 2L, null, List.of(), List.of(), null, null, 0L, null, Map.of(), 0,
                        Map.of()))))
        .isTrue();
  }

  @SuppressWarnings("unchecked")
  private static Set<String> invokeEffectiveSelectors(ReconcileScope scope, SourceSelector source)
      throws Exception {
    Method method =
        ReconcilerService.class.getDeclaredMethod(
            "effectiveSelectors", ReconcileScope.class, SourceSelector.class);
    method.setAccessible(true);
    return (Set<String>) method.invoke(null, scope, source);
  }

  private Object invokeFilterBundlesForMode(
      List<FloecatConnector.SnapshotBundle> bundles,
      boolean fullRescan,
      boolean includeCoreMetadata,
      boolean includeStats,
      Set<Long> existingSnapshotIds)
      throws Exception {
    Method method =
        ReconcilerService.class.getDeclaredMethod(
            "filterBundlesForMode",
            List.class,
            boolean.class,
            boolean.class,
            boolean.class,
            Set.class,
            ReconcilerService.ProgressListener.class);
    method.setAccessible(true);
    return method.invoke(
        service,
        bundles,
        fullRescan,
        includeCoreMetadata,
        includeStats,
        existingSnapshotIds,
        (ReconcilerService.ProgressListener) (s, c, e, sp, stp, m) -> {});
  }

  private static Object invokeKnownSnapshotIdsForEnumeration(
      boolean fullRescan,
      boolean includeStats,
      Set<Long> existingSnapshotIds,
      LongPredicate statsAlreadyCaptured)
      throws Exception {
    Method method =
        ReconcilerService.class.getDeclaredMethod(
            "knownSnapshotIdsForEnumeration",
            boolean.class,
            boolean.class,
            Set.class,
            LongPredicate.class);
    method.setAccessible(true);
    return method.invoke(null, fullRescan, includeStats, existingSnapshotIds, statsAlreadyCaptured);
  }

  private static boolean invokeTableChanged(List<FloecatConnector.SnapshotBundle> bundles)
      throws Exception {
    Method method = ReconcilerService.class.getDeclaredMethod("tableChanged", List.class);
    method.setAccessible(true);
    return (boolean) method.invoke(null, bundles);
  }

  // -------------------------------------------------------------------------
  // View pass tests
  // -------------------------------------------------------------------------

  @Test
  void viewPassSyncsViewsViaEnsureView() {
    FloecatConnector.ViewDescriptor viewDesc =
        new FloecatConnector.ViewDescriptor(
            "src_cat.src_ns",
            "revenue_view",
            "SELECT amount FROM sales",
            "spark",
            List.of("src_ns"), // schema-only; connector strips catalog prefix (fix 3)
            "{\"type\":\"struct\",\"fields\":"
                + "[{\"name\":\"amount\",\"type\":\"double\",\"nullable\":true}]}");

    var capturingBackend = new ViewCapturingBackend(activeConnector());
    service.backend = capturingBackend;
    service.connectorOpener = cfg -> new FakeConnector(List.of(viewDesc));

    var result = service.reconcile(principal, connectorId, true, null);

    assertThat(result.ok()).isTrue();
    assertThat(result.scanned).isEqualTo(1);
    assertThat(result.changed).isEqualTo(1);
    assertThat(capturingBackend.capturedViews).hasSize(1);
    ViewSpec spec = capturingBackend.capturedViews.get(0);
    assertThat(spec.getDisplayName()).isEqualTo("revenue_view");
    assertThat(spec.getSql()).isEqualTo("SELECT amount FROM sales");
    assertThat(spec.getDialect()).isEqualTo("spark");
    assertThat(spec.getOutputColumnsCount()).isEqualTo(1);
    assertThat(spec.getOutputColumns(0).getName()).isEqualTo("amount");
    assertThat(spec.getOutputColumns(0).getLogicalType()).isEqualTo("DOUBLE");
    assertThat(spec.getCreationSearchPathList()).containsExactly("src_ns");
    assertThat(capturingBackend.capturedIdempotencyKeys).containsExactly("dest_ns.revenue_view");
  }

  @Test
  void viewPassCountsErrorWhenListViewDescriptorsThrows() {
    var capturingBackend = new ViewCapturingBackend(activeConnector());
    service.backend = capturingBackend;
    service.connectorOpener =
        cfg ->
            new FakeConnector(List.of()) {
              @Override
              public List<FloecatConnector.ViewDescriptor> listViewDescriptors(String ns) {
                throw new RuntimeException("UC unavailable");
              }
            };

    var result = service.reconcile(principal, connectorId, true, null);

    assertThat(result.errors).isGreaterThanOrEqualTo(1);
    assertThat(capturingBackend.capturedViews).isEmpty();
    assertThat(result.error).isNotNull();
    assertThat(result.error.getMessage()).contains("listViewDescriptors");
  }

  @Test
  void viewPassSkipsViewWithBlankSql() {
    FloecatConnector.ViewDescriptor noSql =
        new FloecatConnector.ViewDescriptor(
            "src_cat.src_ns", "empty_view", "", "spark", List.of(), "");

    var capturingBackend = new ViewCapturingBackend(activeConnector());
    service.backend = capturingBackend;
    service.connectorOpener = cfg -> new FakeConnector(List.of(noSql));

    var result = service.reconcile(principal, connectorId, true, null);

    assertThat(result.ok()).isTrue();
    // Blank SQL views are filtered before scanned++ — they should not count as scanned.
    assertThat(result.scanned).isEqualTo(0);
    assertThat(result.changed).isEqualTo(0);
    assertThat(capturingBackend.capturedViews).isEmpty();
  }

  @Test
  void viewPassSkipsViewWithNoOutputColumns() {
    // schemaJson present but resolves to 0 leaf columns (empty struct)
    FloecatConnector.ViewDescriptor noSchema =
        new FloecatConnector.ViewDescriptor(
            "src_cat.src_ns",
            "no_cols_view",
            "SELECT 1",
            "spark",
            List.of(),
            "{\"type\":\"struct\",\"fields\":[]}");

    var capturingBackend = new ViewCapturingBackend(activeConnector());
    service.backend = capturingBackend;
    service.connectorOpener = cfg -> new FakeConnector(List.of(noSchema));

    var result = service.reconcile(principal, connectorId, true, null);

    assertThat(result.ok()).isTrue();
    // Empty-column views are filtered before scanned++ — they should not count as scanned.
    assertThat(result.scanned).isEqualTo(0);
    assertThat(result.changed).isEqualTo(0);
    assertThat(capturingBackend.capturedViews).isEmpty();
  }

  @Test
  void viewPassCountsErrorWhenEnsureViewThrows() {
    FloecatConnector.ViewDescriptor view1 =
        new FloecatConnector.ViewDescriptor(
            "src_cat.src_ns",
            "bad_view",
            "SELECT a FROM t",
            "spark",
            List.of("src_cat", "src_ns"),
            "{\"type\":\"struct\",\"fields\":"
                + "[{\"name\":\"a\",\"type\":\"int\",\"nullable\":true}]}");
    FloecatConnector.ViewDescriptor view2 =
        new FloecatConnector.ViewDescriptor(
            "src_cat.src_ns",
            "good_view",
            "SELECT b FROM t",
            "spark",
            List.of("src_cat", "src_ns"),
            "{\"type\":\"struct\",\"fields\":"
                + "[{\"name\":\"b\",\"type\":\"double\",\"nullable\":true}]}");

    // ensureView throws for the first view, succeeds for the second.
    var capturingBackend =
        new ViewCapturingBackend(activeConnector()) {
          @Override
          public ResourceId ensureView(ReconcileContext ctx, ViewSpec spec, String idempotencyKey) {
            if ("dest_ns.bad_view".equals(idempotencyKey)) {
              throw new RuntimeException("backend error for " + spec.getDisplayName());
            }
            return super.ensureView(ctx, spec, idempotencyKey);
          }
        };
    service.backend = capturingBackend;
    service.connectorOpener = cfg -> new FakeConnector(List.of(view1, view2));

    var result = service.reconcile(principal, connectorId, true, null);

    // One error from the failing view; one change from the succeeding view.
    assertThat(result.errors).isEqualTo(1);
    assertThat(result.changed).isEqualTo(1);
    assertThat(result.error).isNotNull();
    assertThat(result.error.getMessage()).contains("bad_view");
    // The good view must still have been synced despite the earlier failure.
    assertThat(capturingBackend.capturedViews).hasSize(1);
    assertThat(capturingBackend.capturedViews.get(0).getDisplayName()).isEqualTo("good_view");
  }

  @Test
  void viewPassDoesNotCountAlreadyExistingView() {
    FloecatConnector.ViewDescriptor viewDesc =
        new FloecatConnector.ViewDescriptor(
            "src_cat.src_ns",
            "existing_view",
            "SELECT x FROM t",
            "spark",
            List.of("src_cat", "src_ns"),
            "{\"type\":\"struct\",\"fields\":"
                + "[{\"name\":\"x\",\"type\":\"int\",\"nullable\":false}]}");

    // ensureView returns getDefaultInstance() — signals ALREADY_EXISTS.
    var capturingBackend =
        new ViewCapturingBackend(activeConnector()) {
          @Override
          public ResourceId ensureView(ReconcileContext ctx, ViewSpec spec, String idempotencyKey) {
            capturedViews.add(spec);
            capturedIdempotencyKeys.add(idempotencyKey);
            return ResourceId.getDefaultInstance(); // empty id → already exists
          }
        };
    service.backend = capturingBackend;
    service.connectorOpener = cfg -> new FakeConnector(List.of(viewDesc));

    var result = service.reconcile(principal, connectorId, true, null);

    assertThat(result.ok()).isTrue();
    assertThat(result.scanned).isEqualTo(1);
    // View already existed → should NOT be counted as a change.
    assertThat(result.changed).isEqualTo(0);
    assertThat(capturingBackend.capturedViews).hasSize(1);
  }

  @Test
  void viewsOnlyNamespaceWithTableFilterSyncsViews() {
    // Verifies fix for issue 1: the "No tables matched scope" guard must not prevent the view pass
    // from running. A table-filtered reconcile on a views-only namespace should record the miss as
    // an error AND still sync all views.
    FloecatConnector.ViewDescriptor viewDesc =
        new FloecatConnector.ViewDescriptor(
            "src_cat.src_ns",
            "revenue_view",
            "SELECT amount FROM sales",
            "spark",
            List.of("src_ns"),
            "{\"type\":\"struct\",\"fields\":"
                + "[{\"name\":\"amount\",\"type\":\"double\",\"nullable\":true}]}");

    var capturingBackend = new ViewCapturingBackend(activeConnector());
    service.backend = capturingBackend;
    // FakeConnector has no tables (views-only namespace).
    service.connectorOpener = cfg -> new FakeConnector(List.of(viewDesc));

    // Scope carries a table filter even though the namespace has no tables.
    ReconcileScope scope = ReconcileScope.of(List.of(List.of("dest_ns")), "some_table", List.of());

    var result = service.reconcile(principal, connectorId, true, scope);

    // Table filter didn't match → one error recorded …
    assertThat(result.errors).isEqualTo(1);
    assertThat(result.error).isNotNull();
    assertThat(result.error.getMessage()).contains("No tables matched scope");
    // … but the view pass still ran and synced the view.
    assertThat(capturingBackend.capturedViews).hasSize(1);
    assertThat(capturingBackend.capturedViews.get(0).getDisplayName()).isEqualTo("revenue_view");
  }

  /** A minimal {@link Connector} proto configured as CS_ACTIVE with source + destination. */
  private Connector activeConnector() {
    ResourceId destCatalogId = ResourceId.newBuilder().setAccountId("acct").setId("cat-1").build();
    ResourceId destNamespaceId = ResourceId.newBuilder().setAccountId("acct").setId("ns-1").build();
    return Connector.newBuilder()
        .setResourceId(connectorId)
        .setState(ConnectorState.CS_ACTIVE)
        .setKind(ConnectorKind.CK_DELTA)
        .setSource(
            SourceSelector.newBuilder()
                .setNamespace(
                    NamespacePath.newBuilder().addSegments("src_cat").addSegments("src_ns")))
        .setDestination(
            DestinationTarget.newBuilder()
                .setCatalogId(destCatalogId)
                .setNamespaceId(destNamespaceId))
        .build();
  }

  private Connector activeIcebergConnector(ResourceId tableId) {
    ResourceId destCatalogId = ResourceId.newBuilder().setAccountId("acct").setId("cat-1").build();
    ResourceId destNamespaceId = ResourceId.newBuilder().setAccountId("acct").setId("ns-1").build();
    return Connector.newBuilder()
        .setResourceId(connectorId)
        .setState(ConnectorState.CS_ACTIVE)
        .setKind(ConnectorKind.CK_ICEBERG)
        .setUri("http://iceberg-rest:9200")
        .putProperties("iceberg.source", "rest")
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

  /**
   * Backend that tracks {@link #ensureView} calls and provides enough plumbing for the view pass to
   * run end-to-end (no table snapshots needed since FakeConnector returns empty tables list).
   */
  private static class ViewCapturingBackend extends DefaultBackend {
    private final Connector connector;
    final List<ViewSpec> capturedViews = new ArrayList<>();
    final List<String> capturedIdempotencyKeys = new ArrayList<>();

    ViewCapturingBackend(Connector connector) {
      this.connector = connector;
    }

    @Override
    public Connector lookupConnector(ReconcileContext ctx, ResourceId connectorId) {
      return connector;
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
    public ResourceId ensureView(ReconcileContext ctx, ViewSpec spec, String idempotencyKey) {
      capturedViews.add(spec);
      capturedIdempotencyKeys.add(idempotencyKey);
      return ResourceId.newBuilder().setId("view-1").build();
    }

    @Override
    public void updateConnectorDestination(
        ReconcileContext ctx, ResourceId connectorId, DestinationTarget destination) {
      // no-op — destination already has namespaceId set
    }
  }

  /**
   * A no-op {@link FloecatConnector} that returns empty tables and a configurable list of view
   * descriptors. Connectors are auto-closed; {@link #close()} is a no-op.
   */
  private static class FakeConnector implements FloecatConnector {
    private final List<FloecatConnector.ViewDescriptor> viewDescriptors;

    FakeConnector(List<FloecatConnector.ViewDescriptor> viewDescriptors) {
      this.viewDescriptors = viewDescriptors;
    }

    @Override
    public String id() {
      return "fake";
    }

    @Override
    public ConnectorFormat format() {
      return ConnectorFormat.CF_DELTA;
    }

    @Override
    public List<String> listNamespaces() {
      return List.of();
    }

    @Override
    public List<String> listTables(String namespaceFq) {
      return List.of();
    }

    @Override
    public TableDescriptor describe(String namespaceFq, String tableName) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<SnapshotBundle> enumerateSnapshotsWithStats(
        String namespaceFq,
        String tableName,
        ResourceId destinationTableId,
        Set<String> includeColumns) {
      return List.of();
    }

    @Override
    public List<FloecatConnector.ViewDescriptor> listViewDescriptors(String namespaceFq) {
      return viewDescriptors;
    }

    @Override
    public void close() {}
  }
}
