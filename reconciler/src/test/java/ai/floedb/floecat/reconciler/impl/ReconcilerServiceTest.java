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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.catalog.rpc.ConstraintType;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotConstraints;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
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
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend.DestinationTableMetadata;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend.DestinationViewMetadata;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.identity.StatsTargetScopeCodec;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchItemResult;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchResult;
import ai.floedb.floecat.stats.spi.StatsCaptureControlPlane;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureResult;
import ai.floedb.floecat.stats.spi.StatsTriggerOutcome;
import jakarta.enterprise.inject.Instance;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class ReconcilerServiceTest extends AbstractReconcilerServiceTestBase {
  private static final String DEST_CATALOG = "cat-1";
  private static final List<String> DEST_NAMESPACE_PATH = List.of("dest_ns");

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
  void tableIdScopedExecutionRequiresConcreteTaskBeforeDiscovery() {
    int[] ensureNamespaceCalls = {0};
    service.backend =
        new DefaultBackend() {
          @Override
          public Connector lookupConnector(ReconcileContext ctx, ResourceId connectorId) {
            return activeConnector();
          }

          @Override
          public ResourceId ensureNamespace(
              ReconcileContext ctx, ResourceId catalogId, NameRef namespace) {
            ensureNamespaceCalls[0]++;
            return super.ensureNamespace(ctx, catalogId, namespace);
          }
        };

    var result =
        service.reconcile(
            principal,
            connectorId,
            false,
            ReconcileScope.of(List.of(), "table-1"),
            ReconcileTableTask.empty(),
            ReconcilerService.CaptureMode.METADATA_ONLY,
            null,
            () -> false,
            (tablesScanned,
                tablesChanged,
                viewsScanned,
                viewsChanged,
                errors,
                snapshotsProcessed,
                statsProcessed,
                message) -> {});

    assertThat(result.ok()).isFalse();
    assertThat(result.error.getMessage()).contains("Concrete table task is required");
    assertThat(ensureNamespaceCalls[0]).isZero();
  }

  @Test
  void tableIdScopedExecutionRejectsMismatchedTaskIdBeforeDiscovery() {
    service.backend = new ReturningBackend(activeConnector());

    var result =
        service.reconcile(
            principal,
            connectorId,
            false,
            ReconcileScope.of(List.of(), "table-scope"),
            ReconcileTableTask.of("src_cat.src_ns", "tbl", "table-task", "tbl"),
            ReconcilerService.CaptureMode.METADATA_ONLY,
            null,
            () -> false,
            (tablesScanned,
                tablesChanged,
                viewsScanned,
                viewsChanged,
                errors,
                snapshotsProcessed,
                statsProcessed,
                message) -> {});

    assertThat(result.ok()).isFalse();
    assertThat(result.error.getMessage()).contains("does not match requested scope");
  }

  @Test
  void tableFilterPlanningRejectsMetadataWithoutSourceIdentity() {
    service.backend =
        new DefaultBackend() {
          @Override
          public Connector lookupConnector(ReconcileContext ctx, ResourceId connectorId) {
            return activeConnector();
          }

          @Override
          public Optional<DestinationTableMetadata> lookupDestinationTableMetadata(
              ReconcileContext ctx, ResourceId tableId) {
            return Optional.of(
                new DestinationTableMetadata(
                    ResourceId.newBuilder()
                        .setAccountId("acct")
                        .setKind(ResourceKind.RK_CATALOG)
                        .setId("cat-1")
                        .build(),
                    ResourceId.newBuilder()
                        .setAccountId("acct")
                        .setKind(ResourceKind.RK_NAMESPACE)
                        .setId("ns-1")
                        .build(),
                    "tbl"));
          }
        };
    service.connectorOpener = cfg -> new FakeConnector(List.of());

    assertThatThrownBy(
            () ->
                service.planTableTasks(
                    principal, connectorId, ReconcileScope.of(List.of(), "table-1"), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("missing persisted source identity");
  }

  @Test
  void tableFilterPlanningRequiresExactPersistedSourceConnectorIdentity() {
    ResourceId legacyConnectorRef =
        ResourceId.newBuilder()
            .setKind(ResourceKind.RK_CONNECTOR)
            .setId(connectorId.getId())
            .build();
    service.backend =
        new DefaultBackend() {
          @Override
          public Connector lookupConnector(ReconcileContext ctx, ResourceId connectorId) {
            return activeConnector();
          }

          @Override
          public Optional<DestinationTableMetadata> lookupDestinationTableMetadata(
              ReconcileContext ctx, ResourceId tableId) {
            return Optional.of(
                new DestinationTableMetadata(
                    ResourceId.newBuilder()
                        .setAccountId("acct")
                        .setKind(ResourceKind.RK_CATALOG)
                        .setId("cat-1")
                        .build(),
                    ResourceId.newBuilder()
                        .setAccountId("acct")
                        .setKind(ResourceKind.RK_NAMESPACE)
                        .setId("ns-1")
                        .build(),
                    "tbl",
                    "src_cat.src_ns",
                    "tbl",
                    legacyConnectorRef));
          }
        };
    service.connectorOpener = cfg -> new FakeConnector(List.of());

    assertThatThrownBy(
            () ->
                service.planTableTasks(
                    principal, connectorId, ReconcileScope.of(List.of(), "table-1"), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not match requested connector");
  }

  @Test
  void tableFilterPlanningRejectsMetadataWithoutCatalogIdentity() {
    service.backend =
        new DefaultBackend() {
          @Override
          public Connector lookupConnector(ReconcileContext ctx, ResourceId connectorId) {
            return activeConnector();
          }

          @Override
          public Optional<DestinationTableMetadata> lookupDestinationTableMetadata(
              ReconcileContext ctx, ResourceId tableId) {
            return Optional.of(
                new DestinationTableMetadata(
                    null,
                    ResourceId.newBuilder()
                        .setAccountId("acct")
                        .setKind(ResourceKind.RK_NAMESPACE)
                        .setId("ns-1")
                        .build(),
                    "tbl",
                    "src_cat.src_ns",
                    "tbl",
                    connectorId));
          }
        };
    service.connectorOpener = cfg -> new FakeConnector(List.of());

    assertThatThrownBy(
            () ->
                service.planTableTasks(
                    principal, connectorId, ReconcileScope.of(List.of(), "table-1"), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Destination table catalog cannot be resolved from id: table-1");
  }

  @Test
  void tableFilterDefaultPathPlansConcreteTaskBeforeStrictExecution() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("table-1")
            .build();
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("ns-existing")
            .build();
    ResourceId catalogId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_CATALOG)
            .setId("cat-1")
            .build();
    int[] ensureNamespaceCalls = {0};
    int[] updateConnectorCalls = {0};

    service.backend =
        new DefaultBackend() {
          @Override
          public Connector lookupConnector(ReconcileContext ctx, ResourceId connectorId) {
            return activeConnector();
          }

          @Override
          public Optional<DestinationTableMetadata> lookupDestinationTableMetadata(
              ReconcileContext ctx, ResourceId ignoredTableId) {
            return Optional.of(
                new DestinationTableMetadata(
                    catalogId, namespaceId, "tbl", "src_cat.src_ns", "tbl", connectorId));
          }

          @Override
          public String resolveNamespaceFq(ReconcileContext ctx, ResourceId ignoredNamespaceId) {
            return "dest_ns";
          }

          @Override
          public ResourceId ensureNamespace(
              ReconcileContext ctx, ResourceId catalogId, NameRef namespace) {
            ensureNamespaceCalls[0]++;
            throw new AssertionError("table-filter default path must not ensure namespace");
          }

          @Override
          public void updateConnectorDestination(
              ReconcileContext ctx, ResourceId connectorId, DestinationTarget destination) {
            updateConnectorCalls[0]++;
          }

          @Override
          public Set<Long> existingSnapshotIds(ReconcileContext ctx, ResourceId ignoredTableId) {
            return Set.of();
          }
        };
    service.connectorOpener =
        cfg ->
            new FakeConnector(List.of()) {
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
            };

    var result =
        service.reconcile(
            principal,
            connectorId,
            false,
            ReconcileScope.of(List.of(), tableId.getId()),
            ReconcilerService.CaptureMode.METADATA_ONLY);

    assertThat(result.ok()).isTrue();
    assertThat(ensureNamespaceCalls[0]).isZero();
    assertThat(updateConnectorCalls[0]).isZero();
  }

  @Test
  void strictSingleTableExecutionUsesIdBackedNamespaceAndDoesNotMutateConnectorDestination() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("table-1")
            .build();
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("ns-existing")
            .build();
    int[] ensureNamespaceCalls = {0};
    int[] updateConnectorCalls = {0};
    int[] listTablesCalls = {0};
    int[] updateTableCalls = {0};

    Connector connector =
        Connector.newBuilder()
            .setResourceId(connectorId)
            .setState(ConnectorState.CS_ACTIVE)
            .setKind(ConnectorKind.CK_DELTA)
            .setDestination(
                DestinationTarget.newBuilder()
                    .setCatalogId(ResourceId.newBuilder().setAccountId("acct").setId("cat-1")))
            .build();

    service.backend =
        new DefaultBackend() {
          @Override
          public Connector lookupConnector(ReconcileContext ctx, ResourceId connectorId) {
            return connector;
          }

          @Override
          public Optional<DestinationTableMetadata> lookupDestinationTableMetadata(
              ReconcileContext ctx, ResourceId ignoredTableId) {
            return Optional.of(
                new DestinationTableMetadata(
                    ResourceId.newBuilder()
                        .setAccountId("acct")
                        .setKind(ResourceKind.RK_CATALOG)
                        .setId("cat-1")
                        .build(),
                    namespaceId,
                    "tbl",
                    "src_cat.src_ns",
                    "tbl",
                    connectorId));
          }

          @Override
          public String resolveNamespaceFq(ReconcileContext ctx, ResourceId ignoredNamespaceId) {
            return "dest_ns";
          }

          @Override
          public ResourceId ensureNamespace(
              ReconcileContext ctx, ResourceId catalogId, NameRef namespace) {
            ensureNamespaceCalls[0]++;
            throw new AssertionError("strict table execution must not ensure namespace");
          }

          @Override
          public boolean updateTableById(
              ReconcileContext ctx,
              ResourceId ignoredTableId,
              ResourceId ignoredNamespaceId,
              NameRef table,
              ReconcilerBackend.TableSpecDescriptor descriptor) {
            updateTableCalls[0]++;
            assertThat(ignoredTableId).isEqualTo(tableId);
            assertThat(table.getName()).isEqualTo("tbl");
            assertThat(descriptor.sourceNamespace()).isEqualTo("src_cat.src_ns");
            assertThat(descriptor.sourceTable()).isEqualTo("tbl");
            return true;
          }

          @Override
          public void updateConnectorDestination(
              ReconcileContext ctx, ResourceId connectorId, DestinationTarget destination) {
            updateConnectorCalls[0]++;
          }

          @Override
          public Set<Long> existingSnapshotIds(ReconcileContext ctx, ResourceId ignoredTableId) {
            return Set.of();
          }
        };
    service.connectorOpener =
        cfg ->
            new FakeConnector(List.of()) {
              @Override
              public List<String> listTables(String namespaceFq) {
                listTablesCalls[0]++;
                return List.of("unrelated");
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
            };

    var result =
        service.reconcile(
            principal,
            connectorId,
            false,
            ReconcileScope.of(List.of(), tableId.getId()),
            ReconcileTableTask.of("src_cat.src_ns", "tbl", tableId.getId(), "ignored"),
            ReconcilerService.CaptureMode.METADATA_ONLY,
            null,
            () -> false,
            (tablesScanned,
                tablesChanged,
                viewsScanned,
                viewsChanged,
                errors,
                snapshotsProcessed,
                statsProcessed,
                message) -> {});

    assertThat(result.ok()).isTrue();
    assertThat(result.tablesChanged).isEqualTo(1);
    assertThat(ensureNamespaceCalls[0]).isZero();
    assertThat(updateTableCalls[0]).isEqualTo(1);
    assertThat(updateConnectorCalls[0]).isZero();
    assertThat(listTablesCalls[0]).isZero();
  }

  @Test
  void strictSingleTableFailurePreservesScannedProgress() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("table-1")
            .build();
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("ns-existing")
            .build();

    service.backend =
        new DefaultBackend() {
          @Override
          public Connector lookupConnector(ReconcileContext ctx, ResourceId connectorId) {
            return activeConnector();
          }

          @Override
          public Optional<DestinationTableMetadata> lookupDestinationTableMetadata(
              ReconcileContext ctx, ResourceId ignoredTableId) {
            return Optional.of(
                new DestinationTableMetadata(
                    ResourceId.newBuilder()
                        .setAccountId("acct")
                        .setKind(ResourceKind.RK_CATALOG)
                        .setId("cat-1")
                        .build(),
                    namespaceId,
                    "tbl",
                    "src_cat.src_ns",
                    "tbl",
                    connectorId));
          }

          @Override
          public String resolveNamespaceFq(ReconcileContext ctx, ResourceId ignoredNamespaceId) {
            return "dest_ns";
          }

          @Override
          public Set<Long> existingSnapshotIds(ReconcileContext ctx, ResourceId ignoredTableId) {
            return Set.of();
          }
        };
    service.connectorOpener =
        cfg ->
            new FakeConnector(List.of()) {
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
                throw new RuntimeException("snapshot enumeration failed");
              }
            };

    var result =
        service.reconcile(
            principal,
            connectorId,
            false,
            ReconcileScope.of(List.of(), tableId.getId()),
            ReconcileTableTask.of("src_cat.src_ns", "tbl", tableId.getId(), "ignored"),
            ReconcilerService.CaptureMode.METADATA_ONLY,
            null,
            () -> false,
            (tablesScanned,
                tablesChanged,
                viewsScanned,
                viewsChanged,
                errors,
                snapshotsProcessed,
                statsProcessed,
                message) -> {});

    assertThat(result.ok()).isFalse();
    assertThat(result.tablesScanned).isEqualTo(1);
    assertThat(result.errors).isEqualTo(1);
    assertThat(result.error.getMessage()).contains("snapshot enumeration failed");
  }

  @Test
  void strictSingleViewExecutionUsesIdBackedNamespaceAndDoesNotEnsureNamespace() {
    var viewDesc =
        new ai.floedb.floecat.connector.spi.FloecatConnector.ViewDescriptor(
            "src_cat.src_ns",
            "revenue_view",
            "SELECT amount FROM sales",
            "spark",
            List.of("src_ns"),
            "{\"type\":\"struct\",\"fields\":[{\"name\":\"amount\",\"type\":\"double\",\"nullable\":true}]}");
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("ns-existing")
            .build();
    ResourceId catalogId = ResourceId.newBuilder().setAccountId("acct").setId("cat-1").build();
    int[] ensureNamespaceCalls = {0};

    service.backend =
        new ViewCapturingBackend(activeConnector()) {
          @Override
          public Optional<DestinationViewMetadata> lookupDestinationViewMetadata(
              ReconcileContext ctx, ResourceId viewId) {
            return Optional.of(
                new DestinationViewMetadata(
                    catalogId,
                    namespaceId,
                    "revenue_view",
                    "src_cat.src_ns",
                    "revenue_view",
                    connectorId));
          }

          @Override
          public ResourceId ensureNamespace(
              ReconcileContext ctx, ResourceId catalogId, NameRef namespace) {
            ensureNamespaceCalls[0]++;
            throw new AssertionError("strict view execution must not ensure namespace");
          }
        };
    service.connectorOpener = cfg -> new FakeConnector(List.of(viewDesc));

    var result =
        service.reconcileView(
            principal,
            connectorId,
            ReconcileScope.ofView(List.of(), "view-1"),
            ReconcileViewTask.of("src_cat.src_ns", "revenue_view", "", "view-1"),
            null,
            () -> false,
            (tablesScanned,
                tablesChanged,
                viewsScanned,
                viewsChanged,
                errors,
                snapshotsProcessed,
                statsProcessed,
                message) -> {});

    assertThat(result.ok()).isTrue();
    assertThat(ensureNamespaceCalls[0]).isZero();
  }

  @Test
  void strictSingleViewExecutionDoesNotEnumerateViewsWhenDescribeMisses() {
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("ns-existing")
            .build();
    ResourceId catalogId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_CATALOG)
            .setId("cat-existing")
            .build();
    int[] listViewDescriptorsCalls = {0};

    service.backend =
        new ViewCapturingBackend(activeConnector()) {
          @Override
          public Optional<DestinationViewMetadata> lookupDestinationViewMetadata(
              ReconcileContext ctx, ResourceId viewId) {
            return Optional.of(
                new DestinationViewMetadata(
                    catalogId,
                    namespaceId,
                    "revenue_view",
                    "src_cat.src_ns",
                    "revenue_view",
                    connectorId));
          }
        };
    service.connectorOpener =
        cfg ->
            new FakeConnector(List.of()) {
              @Override
              public Optional<FloecatConnector.ViewDescriptor> describeView(
                  String namespaceFq, String name) {
                return Optional.empty();
              }

              @Override
              public List<FloecatConnector.ViewDescriptor> listViewDescriptors(String namespaceFq) {
                listViewDescriptorsCalls[0]++;
                throw new AssertionError("strict view reconcile must not enumerate views");
              }
            };

    var result =
        service.reconcileView(
            principal,
            connectorId,
            ReconcileScope.ofView(List.of(), "view-1"),
            ReconcileViewTask.of("src_cat.src_ns", "revenue_view", namespaceId.getId(), "view-1"),
            null,
            () -> false,
            (tablesScanned,
                tablesChanged,
                viewsScanned,
                viewsChanged,
                errors,
                snapshotsProcessed,
                statsProcessed,
                message) -> {});

    assertThat(result.ok()).isFalse();
    assertThat(result.error.getMessage()).contains("View not found");
    assertThat(listViewDescriptorsCalls[0]).isZero();
  }

  @Test
  void viewFilterDefaultPathPlansFromDestinationViewIdAndDoesNotUseDiscoveryMutation() {
    var viewDesc =
        new ai.floedb.floecat.connector.spi.FloecatConnector.ViewDescriptor(
            "src_cat.src_ns",
            "revenue_view",
            "SELECT amount FROM sales",
            "spark",
            List.of("src_ns"),
            "{\"type\":\"struct\",\"fields\":[{\"name\":\"amount\",\"type\":\"double\",\"nullable\":true}]}");
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("ns-existing")
            .build();
    ResourceId catalogId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_CATALOG)
            .setId("cat-existing")
            .build();
    int[] ensureNamespaceCalls = {0};
    int[] lookupNamespaceCalls = {0};
    int[] updateConnectorCalls = {0};

    service.backend =
        new ViewCapturingBackend(activeConnector()) {
          @Override
          public Optional<DestinationViewMetadata> lookupDestinationViewMetadata(
              ReconcileContext ctx, ResourceId viewId) {
            return Optional.of(
                new DestinationViewMetadata(
                    catalogId,
                    namespaceId,
                    "revenue_view",
                    "src_cat.src_ns",
                    "revenue_view",
                    connectorId));
          }

          @Override
          public ResourceId ensureNamespace(
              ReconcileContext ctx, ResourceId catalogId, NameRef namespace) {
            ensureNamespaceCalls[0]++;
            throw new AssertionError("view-filter default path must not ensure namespace");
          }

          @Override
          public Optional<ResourceId> lookupNamespace(ReconcileContext ctx, NameRef namespace) {
            lookupNamespaceCalls[0]++;
            throw new AssertionError("view-filter default path must not lookup namespace by name");
          }

          @Override
          public void updateConnectorDestination(
              ReconcileContext ctx, ResourceId connectorId, DestinationTarget destination) {
            updateConnectorCalls[0]++;
          }
        };
    service.connectorOpener = cfg -> new FakeConnector(List.of(viewDesc));

    var result =
        service.reconcile(
            principal,
            connectorId,
            false,
            ReconcileScope.ofView(List.of(), "view-1"),
            ReconcilerService.CaptureMode.METADATA_ONLY);

    assertThat(result.ok()).isTrue();
    assertThat(result.scanned).isEqualTo(1);
    assertThat(ensureNamespaceCalls[0]).isZero();
    assertThat(lookupNamespaceCalls[0]).isZero();
    assertThat(updateConnectorCalls[0]).isZero();
  }

  @Test
  void viewFilterPlanningRejectsMetadataWithoutSourceIdentity() {
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("ns-existing")
            .build();
    ResourceId catalogId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_CATALOG)
            .setId("cat-existing")
            .build();

    service.backend =
        new ViewCapturingBackend(activeConnector()) {
          @Override
          public Optional<DestinationViewMetadata> lookupDestinationViewMetadata(
              ReconcileContext ctx, ResourceId viewId) {
            return Optional.of(new DestinationViewMetadata(catalogId, namespaceId, "revenue_view"));
          }
        };
    service.connectorOpener = cfg -> new FakeConnector(List.of());

    assertThatThrownBy(
            () ->
                service.planViewTasks(
                    principal, connectorId, ReconcileScope.ofView(List.of(), "view-1"), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("missing persisted source identity");
  }

  @Test
  void tablePlannerEmitsSingleTaskForPinnedDestinationTableId() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("table-pinned")
            .build();
    Connector pinnedConnector =
        activeConnector().toBuilder()
            .setSource(activeConnector().getSource().toBuilder().setTable("tbl"))
            .setDestination(activeConnector().getDestination().toBuilder().setTableId(tableId))
            .build();
    service.backend =
        new DefaultBackend() {
          @Override
          public Connector lookupConnector(ReconcileContext ctx, ResourceId connectorId) {
            return pinnedConnector;
          }

          @Override
          public String resolveNamespaceFq(ReconcileContext ctx, ResourceId namespaceId) {
            return "dest_ns";
          }
        };
    service.connectorOpener =
        cfg ->
            new FakeConnector(List.of()) {
              @Override
              public List<FloecatConnector.PlannedTableTask> planTableTasks(
                  FloecatConnector.TablePlanningRequest request) {
                return List.of(
                    new FloecatConnector.PlannedTableTask("src_cat.src_ns", "tbl", "tbl_display"));
              }
            };

    List<ReconcileTableTask> tasks =
        service.planTableTasks(principal, connectorId, ReconcileScope.empty(), null);

    assertThat(tasks)
        .containsExactly(
            ReconcileTableTask.of("src_cat.src_ns", "tbl", "table-pinned", "tbl_display"));
  }

  @Test
  void tablePlannerRejectsPinnedDestinationTableIdWithoutPinnedSourceTable() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("table-pinned")
            .build();
    Connector stalePinnedConnector =
        activeConnector().toBuilder()
            .setDestination(activeConnector().getDestination().toBuilder().setTableId(tableId))
            .build();
    service.backend =
        new DefaultBackend() {
          @Override
          public Connector lookupConnector(ReconcileContext ctx, ResourceId connectorId) {
            return stalePinnedConnector;
          }

          @Override
          public String resolveNamespaceFq(ReconcileContext ctx, ResourceId namespaceId) {
            return "dest_ns";
          }
        };
    service.connectorOpener =
        cfg ->
            new FakeConnector(List.of()) {
              @Override
              public List<FloecatConnector.PlannedTableTask> planTableTasks(
                  FloecatConnector.TablePlanningRequest request) {
                throw new AssertionError(
                    "stale pinned destination table id should be rejected before planning");
              }
            };

    assertThatThrownBy(
            () -> service.planTableTasks(principal, connectorId, ReconcileScope.empty(), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Pinned destination table id requires connector.source.table");
  }

  @Test
  void tablePlannerRejectsMultipleSourceTablesForPinnedDestinationTableId() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("table-pinned")
            .build();
    Connector pinnedConnector =
        activeConnector().toBuilder()
            .setSource(activeConnector().getSource().toBuilder().setTable("tbl"))
            .setDestination(activeConnector().getDestination().toBuilder().setTableId(tableId))
            .build();
    service.backend =
        new DefaultBackend() {
          @Override
          public Connector lookupConnector(ReconcileContext ctx, ResourceId connectorId) {
            return pinnedConnector;
          }

          @Override
          public String resolveNamespaceFq(ReconcileContext ctx, ResourceId namespaceId) {
            return "dest_ns";
          }
        };
    service.connectorOpener =
        cfg ->
            new FakeConnector(List.of()) {
              @Override
              public List<FloecatConnector.PlannedTableTask> planTableTasks(
                  FloecatConnector.TablePlanningRequest request) {
                return List.of(
                    new FloecatConnector.PlannedTableTask("src_cat.src_ns", "tbl_a", "tbl_a"),
                    new FloecatConnector.PlannedTableTask("src_cat.src_ns", "tbl_b", "tbl_b"));
              }
            };

    assertThatThrownBy(
            () -> service.planTableTasks(principal, connectorId, ReconcileScope.empty(), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Pinned destination table id requires exactly one planned source table, but found 2");
  }

  @Test
  void tablePlannerEmitsDiscoveryTasksWithoutConcreteDestinationId() {
    int[] lookupTableCalls = {0};
    ResourceId existingTableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("table-existing")
            .build();
    service.backend =
        new DefaultBackend() {
          @Override
          public Connector lookupConnector(ReconcileContext ctx, ResourceId connectorId) {
            return activeConnector();
          }

          @Override
          public String resolveNamespaceFq(ReconcileContext ctx, ResourceId namespaceId) {
            return "dest_ns";
          }

          @Override
          public String lookupCatalogName(ReconcileContext ctx, ResourceId catalogId) {
            return "dest_cat";
          }

          @Override
          public Optional<ResourceId> lookupTable(ReconcileContext ctx, NameRef table) {
            lookupTableCalls[0]++;
            assertThat(table.getName()).isEqualTo("tbl_display");
            return Optional.of(existingTableId);
          }
        };
    service.connectorOpener =
        cfg ->
            new FakeConnector(List.of()) {
              @Override
              public List<FloecatConnector.PlannedTableTask> planTableTasks(
                  FloecatConnector.TablePlanningRequest request) {
                return List.of(
                    new FloecatConnector.PlannedTableTask("src_cat.src_ns", "tbl", "tbl_display"));
              }
            };

    List<ReconcileTableTask> tasks =
        service.planTableTasks(principal, connectorId, ReconcileScope.empty(), null);

    assertThat(tasks)
        .containsExactly(
            ReconcileTableTask.discovery(
                "src_cat.src_ns", "tbl", "ns-1", "table-existing", "tbl_display"));
    assertThat(lookupTableCalls[0]).isEqualTo(1);
  }

  @Test
  void tablePlannerEmitsDiscoveryTasksForNewDestinationNameWithoutBlankId() {
    service.backend =
        new DefaultBackend() {
          @Override
          public Connector lookupConnector(ReconcileContext ctx, ResourceId connectorId) {
            return activeConnector();
          }

          @Override
          public String resolveNamespaceFq(ReconcileContext ctx, ResourceId namespaceId) {
            return "dest_ns";
          }

          @Override
          public String lookupCatalogName(ReconcileContext ctx, ResourceId catalogId) {
            return "dest_cat";
          }

          @Override
          public Optional<ResourceId> lookupTable(ReconcileContext ctx, NameRef table) {
            assertThat(table.getName()).isEqualTo("new_tbl");
            return Optional.empty();
          }
        };
    service.connectorOpener =
        cfg ->
            new FakeConnector(List.of()) {
              @Override
              public List<FloecatConnector.PlannedTableTask> planTableTasks(
                  FloecatConnector.TablePlanningRequest request) {
                return List.of(
                    new FloecatConnector.PlannedTableTask("src_cat.src_ns", "new_tbl", "new_tbl"));
              }
            };

    List<ReconcileTableTask> tasks =
        service.planTableTasks(principal, connectorId, ReconcileScope.empty(), null);

    assertThat(tasks)
        .containsExactly(
            ReconcileTableTask.discovery("src_cat.src_ns", "new_tbl", "ns-1", "new_tbl"));
    assertThat(tasks.getFirst().destinationTableId()).isNull();
  }

  @Test
  void viewPlannerEmitsDiscoveryTasksWithoutDestinationViewId() {
    var viewDesc =
        new ai.floedb.floecat.connector.spi.FloecatConnector.ViewDescriptor(
            "src_cat.src_ns",
            "revenue_view",
            "SELECT amount FROM sales",
            "spark",
            List.of("src_ns"),
            "{\"type\":\"struct\",\"fields\":[{\"name\":\"amount\",\"type\":\"double\",\"nullable\":true}]}");
    ResourceId existingViewId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_VIEW)
            .setId("view-existing")
            .build();
    service.backend =
        new DefaultBackend() {
          @Override
          public Connector lookupConnector(ReconcileContext ctx, ResourceId connectorId) {
            return activeConnector();
          }

          @Override
          public String resolveNamespaceFq(ReconcileContext ctx, ResourceId namespaceId) {
            return "dest_ns";
          }

          @Override
          public String lookupCatalogName(ReconcileContext ctx, ResourceId catalogId) {
            return "dest_cat";
          }

          @Override
          public Optional<ResourceId> lookupView(ReconcileContext ctx, NameRef view) {
            assertThat(view.getName()).isEqualTo("revenue_view");
            return Optional.of(existingViewId);
          }
        };
    service.connectorOpener = cfg -> new FakeConnector(List.of(viewDesc));

    List<ReconcileViewTask> tasks =
        service.planViewTasks(principal, connectorId, ReconcileScope.empty(), null);

    assertThat(tasks)
        .containsExactly(
            ReconcileViewTask.discovery(
                "src_cat.src_ns", "revenue_view", "ns-1", "view-existing", "revenue_view"));
  }

  @Test
  void viewPlannerEmitsDiscoveryTasksForNewDestinationNameWithoutBlankId() {
    var viewDesc =
        new ai.floedb.floecat.connector.spi.FloecatConnector.ViewDescriptor(
            "src_cat.src_ns",
            "new_view",
            "SELECT amount FROM sales",
            "spark",
            List.of("src_ns"),
            "{\"type\":\"struct\",\"fields\":[{\"name\":\"amount\",\"type\":\"double\",\"nullable\":true}]}");
    service.backend =
        new DefaultBackend() {
          @Override
          public Connector lookupConnector(ReconcileContext ctx, ResourceId connectorId) {
            return activeConnector();
          }

          @Override
          public String resolveNamespaceFq(ReconcileContext ctx, ResourceId namespaceId) {
            return "dest_ns";
          }

          @Override
          public String lookupCatalogName(ReconcileContext ctx, ResourceId catalogId) {
            return "dest_cat";
          }

          @Override
          public Optional<ResourceId> lookupView(ReconcileContext ctx, NameRef view) {
            assertThat(view.getName()).isEqualTo("new_view");
            return Optional.empty();
          }
        };
    service.connectorOpener = cfg -> new FakeConnector(List.of(viewDesc));

    List<ReconcileViewTask> tasks =
        service.planViewTasks(principal, connectorId, ReconcileScope.empty(), null);

    assertThat(tasks)
        .containsExactly(
            ReconcileViewTask.discovery("src_cat.src_ns", "new_view", "ns-1", "new_view"));
    assertThat(tasks.getFirst().destinationViewId()).isNull();
  }

  @Test
  void discoveryTableExecutionCreatesTableOnNameMiss() {
    int[] ensureTableCalls = {0};
    service.backend =
        new DefaultBackend() {
          @Override
          public Connector lookupConnector(ReconcileContext ctx, ResourceId connectorId) {
            return activeConnector();
          }

          @Override
          public String resolveNamespaceFq(ReconcileContext ctx, ResourceId namespaceId) {
            return "dest_ns";
          }

          @Override
          public String lookupCatalogName(ReconcileContext ctx, ResourceId catalogId) {
            return "dest_cat";
          }

          @Override
          public ResourceId ensureTable(
              ReconcileContext ctx,
              ResourceId namespaceId,
              NameRef table,
              ReconcilerBackend.TableSpecDescriptor descriptor) {
            ensureTableCalls[0]++;
            assertThat(namespaceId.getId()).isEqualTo("ns-1");
            assertThat(table.getName()).isEqualTo("tbl_display");
            assertThat(descriptor.sourceNamespace()).isEqualTo("src_cat.src_ns");
            assertThat(descriptor.sourceTable()).isEqualTo("tbl");
            return ResourceId.newBuilder()
                .setAccountId("acct")
                .setKind(ResourceKind.RK_TABLE)
                .setId("table-created")
                .build();
          }

          @Override
          public Set<Long> existingSnapshotIds(ReconcileContext ctx, ResourceId tableId) {
            return Set.of();
          }
        };
    service.connectorOpener = cfg -> tableDescriptorConnector();

    var result =
        service.reconcile(
            principal,
            connectorId,
            false,
            ReconcileScope.empty(),
            ReconcileTableTask.discovery("src_cat.src_ns", "tbl", "ns-1", "tbl_display"),
            ReconcilerService.CaptureMode.METADATA_ONLY,
            null,
            () -> false,
            (tablesScanned,
                tablesChanged,
                viewsScanned,
                viewsChanged,
                errors,
                snapshotsProcessed,
                statsProcessed,
                message) -> {});

    assertThat(result.ok()).isTrue();
    assertThat(result.tablesScanned).isEqualTo(1);
    assertThat(ensureTableCalls[0]).isEqualTo(1);
  }

  @Test
  void discoveryTableExecutionUpdatesExistingTableByName() {
    ResourceId existingTableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("table-existing")
            .build();
    int[] updateTableCalls = {0};
    service.backend =
        new DefaultBackend() {
          @Override
          public Connector lookupConnector(ReconcileContext ctx, ResourceId connectorId) {
            return activeConnector();
          }

          @Override
          public String resolveNamespaceFq(ReconcileContext ctx, ResourceId namespaceId) {
            return "dest_ns";
          }

          @Override
          public String lookupCatalogName(ReconcileContext ctx, ResourceId catalogId) {
            return "dest_cat";
          }

          @Override
          public Optional<ResourceId> lookupTable(ReconcileContext ctx, NameRef table) {
            assertThat(table.getName()).isEqualTo("tbl_display");
            return Optional.of(existingTableId);
          }

          @Override
          public boolean updateTableById(
              ReconcileContext ctx,
              ResourceId tableId,
              ResourceId namespaceId,
              NameRef table,
              ReconcilerBackend.TableSpecDescriptor descriptor) {
            updateTableCalls[0]++;
            assertThat(tableId).isEqualTo(existingTableId);
            assertThat(namespaceId.getId()).isEqualTo("ns-1");
            assertThat(table.getName()).isEqualTo("tbl_display");
            assertThat(descriptor.sourceNamespace()).isEqualTo("src_cat.src_ns");
            assertThat(descriptor.sourceTable()).isEqualTo("tbl");
            return true;
          }

          @Override
          public ResourceId ensureTable(
              ReconcileContext ctx,
              ResourceId namespaceId,
              NameRef table,
              ReconcilerBackend.TableSpecDescriptor descriptor) {
            throw new AssertionError("Existing discovery table must update by id, not create");
          }

          @Override
          public Set<Long> existingSnapshotIds(ReconcileContext ctx, ResourceId tableId) {
            return Set.of();
          }
        };
    service.connectorOpener = cfg -> tableDescriptorConnector();

    var result =
        service.reconcile(
            principal,
            connectorId,
            false,
            ReconcileScope.empty(),
            ReconcileTableTask.discovery("src_cat.src_ns", "tbl", "ns-1", "tbl_display"),
            ReconcilerService.CaptureMode.METADATA_ONLY,
            null,
            () -> false,
            (tablesScanned,
                tablesChanged,
                viewsScanned,
                viewsChanged,
                errors,
                snapshotsProcessed,
                statsProcessed,
                message) -> {});

    assertThat(result.ok()).isTrue();
    assertThat(result.tablesScanned).isEqualTo(1);
    assertThat(result.tablesChanged).isEqualTo(1);
    assertThat(updateTableCalls[0]).isEqualTo(1);
  }

  @Test
  void discoveryTableExecutionStatsOnlyDoesNotCreateTableOnNameMiss() {
    int[] ensureTableCalls = {0};
    service.backend =
        new DefaultBackend() {
          @Override
          public Connector lookupConnector(ReconcileContext ctx, ResourceId connectorId) {
            return activeConnector();
          }

          @Override
          public String resolveNamespaceFq(ReconcileContext ctx, ResourceId namespaceId) {
            return "dest_ns";
          }

          @Override
          public String lookupCatalogName(ReconcileContext ctx, ResourceId catalogId) {
            return "dest_cat";
          }

          @Override
          public Optional<ResourceId> lookupTable(ReconcileContext ctx, NameRef table) {
            return Optional.empty();
          }

          @Override
          public ResourceId ensureTable(
              ReconcileContext ctx,
              ResourceId namespaceId,
              NameRef table,
              ReconcilerBackend.TableSpecDescriptor descriptor) {
            ensureTableCalls[0]++;
            throw new AssertionError("STATS_ONLY discovery must not create destination tables");
          }
        };
    service.connectorOpener = cfg -> tableDescriptorConnector();

    var result =
        service.reconcile(
            principal,
            connectorId,
            false,
            ReconcileScope.empty(),
            ReconcileTableTask.discovery("src_cat.src_ns", "tbl", "ns-1", "tbl_display"),
            ReconcilerService.CaptureMode.STATS_ONLY,
            null,
            () -> false,
            (tablesScanned,
                tablesChanged,
                viewsScanned,
                viewsChanged,
                errors,
                snapshotsProcessed,
                statsProcessed,
                message) -> {});

    assertThat(result.ok()).isTrue();
    assertThat(result.tablesScanned).isZero();
    assertThat(ensureTableCalls[0]).isZero();
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
      public boolean putSnapshotConstraints(
          ReconcileContext ctx,
          ResourceId ignoredTableId,
          long snapshotId,
          SnapshotConstraints constraints) {
        this.putConstraintsSnapshotId = snapshotId;
        this.putConstraints = constraints;
        return true;
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
      public List<FloecatConnector.PlannedTableTask> planTableTasks(
          FloecatConnector.TablePlanningRequest request) {
        return List.of(new FloecatConnector.PlannedTableTask("src_cat.src_ns", "tbl", "tbl"));
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

    ReconcileScope scope = ReconcileScope.of(List.of(), tableId.getId());
    var result =
        reconcileTableTask(tableId, true, scope, ReconcilerService.CaptureMode.METADATA_AND_STATS);

    assertThat(result.ok()).isTrue();
    assertThat(result.degraded()).isTrue();
    assertThat(result.degradedReasons)
        .anyMatch(reason -> reason.contains("stats_followup_unavailable"));
    assertThat(backend.capturedTargetSnapshotIds).isEmpty();
    assertThat(backend.capturedKnownSnapshotIds).isEmpty();
    assertThat(backend.putConstraintsSnapshotId).isEqualTo(42L);
    assertThat(backend.putConstraints).isNotNull();
    assertThat(backend.putConstraints.getConstraintsCount()).isEqualTo(1);
    assertThat(backend.putConstraints.getConstraints(0).getName()).isEqualTo("pk_tbl");
  }

  @Test
  void metadataAndStatsFollowUpEnqueueDefaultsToTableStatsWhenUnscoped() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("table-follow-up-columns")
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
      public ResourceId ensureNamespace(
          ReconcileContext ctx, ResourceId catalogId, NameRef namespace) {
        return ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("dest-ns")
            .build();
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
      public Optional<ResourceId> lookupTable(ReconcileContext ctx, NameRef table) {
        return Optional.of(tableId);
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

    class SingleSnapshotConnector extends FakeConnector {
      SingleSnapshotConnector() {
        super(List.of());
      }

      @Override
      public List<String> listTables(String namespaceFq) {
        return List.of("tbl");
      }

      @Override
      public List<FloecatConnector.PlannedTableTask> planTableTasks(
          FloecatConnector.TablePlanningRequest request) {
        return List.of(new FloecatConnector.PlannedTableTask("src_cat.src_ns", "tbl", "tbl"));
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
    }

    service.backend = new Backend();
    service.connectorOpener = cfg -> new SingleSnapshotConnector();

    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    Mockito.when(
            jobStore.enqueue(
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyBoolean(),
                Mockito.any(),
                Mockito.any()))
        .thenReturn("job-1");
    @SuppressWarnings("unchecked")
    Instance<ReconcileJobStore> jobStoreInstance = Mockito.mock(Instance.class);
    Mockito.when(jobStoreInstance.isUnsatisfied()).thenReturn(false);
    Mockito.when(jobStoreInstance.get()).thenReturn(jobStore);
    service.reconcileJobStore = jobStoreInstance;

    ReconcileScope scope = ReconcileScope.of(List.of(), tableId.getId());
    var result =
        reconcileTableTask(tableId, true, scope, ReconcilerService.CaptureMode.METADATA_AND_STATS);

    assertThat(result.ok()).isTrue();
    assertThat(result.degraded()).isFalse();

    ArgumentCaptor<ReconcileScope> scopeCaptor = ArgumentCaptor.forClass(ReconcileScope.class);
    Mockito.verify(jobStore)
        .enqueue(
            Mockito.eq("acct"),
            Mockito.eq("connector-1"),
            Mockito.eq(false),
            Mockito.eq(ReconcilerService.CaptureMode.STATS_ONLY),
            scopeCaptor.capture());

    ReconcileScope queuedScope = scopeCaptor.getValue();
    assertThat(queuedScope.destinationStatsRequests())
        .containsExactly(
            scopedStatsRequest(
                tableId.getId(),
                42L,
                StatsTargetScopeCodec.encode(StatsTargetIdentity.tableTarget()),
                List.of()));
  }

  @Test
  void metadataAndStatsFollowUpEnqueuePreservesExactScopedRequests() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("table-follow-up-exact-scope")
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
      public ResourceId ensureNamespace(
          ReconcileContext ctx, ResourceId catalogId, NameRef namespace) {
        return ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("dest-ns")
            .build();
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
      public Optional<ResourceId> lookupTable(ReconcileContext ctx, NameRef table) {
        return Optional.of(tableId);
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

    class TwoSnapshotConnector extends FakeConnector {
      TwoSnapshotConnector() {
        super(List.of());
      }

      @Override
      public List<String> listTables(String namespaceFq) {
        return List.of("tbl");
      }

      @Override
      public List<FloecatConnector.PlannedTableTask> planTableTasks(
          FloecatConnector.TablePlanningRequest request) {
        return List.of(new FloecatConnector.PlannedTableTask("src_cat.src_ns", "tbl", "tbl"));
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
                202L, 0L, Instant.now().toEpochMilli(), "", null, 0L, null, Map.of(), 0, Map.of()));
      }
    }

    service.backend = new Backend();
    service.connectorOpener = cfg -> new TwoSnapshotConnector();

    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    Mockito.when(
            jobStore.enqueue(
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyBoolean(),
                Mockito.any(),
                Mockito.any()))
        .thenReturn("job-1");
    @SuppressWarnings("unchecked")
    Instance<ReconcileJobStore> jobStoreInstance = Mockito.mock(Instance.class);
    Mockito.when(jobStoreInstance.isUnsatisfied()).thenReturn(false);
    Mockito.when(jobStoreInstance.get()).thenReturn(jobStore);
    service.reconcileJobStore = jobStoreInstance;

    ReconcileScope scope =
        ReconcileScope.of(
            List.of(),
            tableId.getId(),
            List.of(
                scopedStatsRequest(
                    tableId.getId(),
                    201L,
                    StatsTargetScopeCodec.encode(StatsTargetIdentity.tableTarget()),
                    List.of("#9")),
                scopedStatsRequest(
                    tableId.getId(),
                    202L,
                    StatsTargetScopeCodec.encode(StatsTargetIdentity.columnTarget(9L)),
                    List.of())));
    var result =
        reconcileTableTask(tableId, true, scope, ReconcilerService.CaptureMode.METADATA_AND_STATS);

    assertThat(result.ok()).isTrue();
    assertThat(result.degraded()).isFalse();

    ArgumentCaptor<ReconcileScope> scopeCaptor = ArgumentCaptor.forClass(ReconcileScope.class);
    Mockito.verify(jobStore)
        .enqueue(
            Mockito.eq("acct"),
            Mockito.eq("connector-1"),
            Mockito.eq(false),
            Mockito.eq(ReconcilerService.CaptureMode.STATS_ONLY),
            scopeCaptor.capture());

    ReconcileScope queuedScope = scopeCaptor.getValue();
    assertThat(queuedScope.destinationStatsRequests())
        .containsExactly(
            scopedStatsRequest(
                tableId.getId(),
                201L,
                StatsTargetScopeCodec.encode(StatsTargetIdentity.tableTarget()),
                List.of("#9")),
            scopedStatsRequest(
                tableId.getId(),
                202L,
                StatsTargetScopeCodec.encode(StatsTargetIdentity.columnTarget(9L)),
                List.of()));
  }

  @Test
  void metadataAndStatsFiltersConnectorSnapshotsOutsideExplicitScope() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("table-follow-up-scope-filter")
            .setKind(ResourceKind.RK_TABLE)
            .build();

    class Backend extends DefaultBackend {
      final List<Long> ingestedSnapshotIds = new ArrayList<>();

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
      public ResourceId ensureNamespace(
          ReconcileContext ctx, ResourceId catalogId, NameRef namespace) {
        return ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("dest-ns")
            .build();
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
      public Optional<ResourceId> lookupTable(ReconcileContext ctx, NameRef table) {
        return Optional.of(tableId);
      }

      @Override
      public Optional<Snapshot> fetchSnapshot(
          ReconcileContext ctx, ResourceId ignoredTableId, long snapshotId) {
        return Optional.empty();
      }

      @Override
      public void ingestSnapshot(
          ReconcileContext ctx, ResourceId ignoredTableId, Snapshot snapshot) {
        ingestedSnapshotIds.add(snapshot.getSnapshotId());
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

    class OverReturningConnector extends FakeConnector {
      OverReturningConnector() {
        super(List.of());
      }

      @Override
      public List<String> listTables(String namespaceFq) {
        return List.of("tbl");
      }

      @Override
      public List<FloecatConnector.PlannedTableTask> planTableTasks(
          FloecatConnector.TablePlanningRequest request) {
        return List.of(new FloecatConnector.PlannedTableTask("src_cat.src_ns", "tbl", "tbl"));
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
                202L, 0L, Instant.now().toEpochMilli(), "", null, 0L, null, Map.of(), 0, Map.of()));
      }
    }

    Backend backend = new Backend();
    service.backend = backend;
    service.connectorOpener = cfg -> new OverReturningConnector();

    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    Mockito.when(
            jobStore.enqueue(
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyBoolean(),
                Mockito.any(),
                Mockito.any()))
        .thenReturn("job-1");
    @SuppressWarnings("unchecked")
    Instance<ReconcileJobStore> jobStoreInstance = Mockito.mock(Instance.class);
    Mockito.when(jobStoreInstance.isUnsatisfied()).thenReturn(false);
    Mockito.when(jobStoreInstance.get()).thenReturn(jobStore);
    service.reconcileJobStore = jobStoreInstance;

    ReconcileScope scope =
        ReconcileScope.of(
            List.of(),
            tableId.getId(),
            List.of(
                scopedStatsRequest(
                    tableId.getId(),
                    201L,
                    StatsTargetScopeCodec.encode(StatsTargetIdentity.tableTarget()),
                    List.of("#9"))));
    var result =
        reconcileTableTask(tableId, true, scope, ReconcilerService.CaptureMode.METADATA_AND_STATS);

    assertThat(result.ok()).isTrue();
    assertThat(backend.ingestedSnapshotIds).containsExactly(201L);

    ArgumentCaptor<ReconcileScope> scopeCaptor = ArgumentCaptor.forClass(ReconcileScope.class);
    Mockito.verify(jobStore)
        .enqueue(
            Mockito.eq("acct"),
            Mockito.eq("connector-1"),
            Mockito.eq(false),
            Mockito.eq(ReconcilerService.CaptureMode.STATS_ONLY),
            scopeCaptor.capture());

    ReconcileScope queuedScope = scopeCaptor.getValue();
    assertThat(queuedScope.destinationStatsRequests())
        .containsExactly(
            scopedStatsRequest(
                tableId.getId(),
                201L,
                StatsTargetScopeCodec.encode(StatsTargetIdentity.tableTarget()),
                List.of("#9")));
  }

  @Test
  void metadataAndStatsReconcilesMetadataForUnrequestedTablesButScopesStatsFollowUp() {
    ResourceId requestedTableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("table-requested")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    ResourceId unrequestedTableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("table-unrequested")
            .setKind(ResourceKind.RK_TABLE)
            .build();

    class Backend extends DefaultBackend {
      final List<String> ensuredTables = new ArrayList<>();
      final List<String> ingestedTables = new ArrayList<>();

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
      public ResourceId ensureNamespace(
          ReconcileContext ctx, ResourceId catalogId, NameRef namespace) {
        return ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("dest-ns")
            .build();
      }

      @Override
      public ResourceId ensureTable(
          ReconcileContext ctx,
          ResourceId namespaceId,
          NameRef table,
          TableSpecDescriptor descriptor) {
        ensuredTables.add(table.getName());
        return table.getName().equals("tbl_requested") ? requestedTableId : unrequestedTableId;
      }

      @Override
      public Optional<Snapshot> fetchSnapshot(
          ReconcileContext ctx, ResourceId ignoredTableId, long snapshotId) {
        return Optional.empty();
      }

      @Override
      public void ingestSnapshot(ReconcileContext ctx, ResourceId tableId, Snapshot snapshot) {
        ingestedTables.add(tableId.equals(requestedTableId) ? "tbl_requested" : "tbl_unrequested");
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

    class TwoTableConnector extends FakeConnector {
      TwoTableConnector() {
        super(List.of());
      }

      @Override
      public List<String> listTables(String namespaceFq) {
        return List.of("tbl_requested", "tbl_unrequested");
      }

      @Override
      public TableDescriptor describe(String namespaceFq, String tableName) {
        return new TableDescriptor(
            namespaceFq,
            tableName,
            "s3://bucket/" + tableName,
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
                201L, 0L, Instant.now().toEpochMilli(), "", null, 0L, null, Map.of(), 0, Map.of()));
      }
    }

    Backend backend = new Backend();
    service.backend = backend;
    service.connectorOpener = cfg -> new TwoTableConnector();

    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    Mockito.when(
            jobStore.enqueue(
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyBoolean(),
                Mockito.any(),
                Mockito.any()))
        .thenReturn("job-1");
    @SuppressWarnings("unchecked")
    Instance<ReconcileJobStore> jobStoreInstance = Mockito.mock(Instance.class);
    Mockito.when(jobStoreInstance.isUnsatisfied()).thenReturn(false);
    Mockito.when(jobStoreInstance.get()).thenReturn(jobStore);
    service.reconcileJobStore = jobStoreInstance;

    ReconcileScope scope =
        ReconcileScope.of(
            List.of("ns-1"),
            null,
            List.of(
                scopedStatsRequest(
                    requestedTableId.getId(),
                    201L,
                    StatsTargetScopeCodec.encode(StatsTargetIdentity.tableTarget()),
                    List.of("#9"))));

    var result = service.reconcile(principal, connectorId, true, scope);

    assertThat(result.ok()).isTrue();
    assertThat(backend.ensuredTables).containsExactlyInAnyOrder("tbl_requested", "tbl_unrequested");
    assertThat(backend.ingestedTables)
        .containsExactlyInAnyOrder("tbl_requested", "tbl_unrequested");

    ArgumentCaptor<ReconcileScope> scopeCaptor = ArgumentCaptor.forClass(ReconcileScope.class);
    Mockito.verify(jobStore)
        .enqueue(
            Mockito.eq("acct"),
            Mockito.eq("connector-1"),
            Mockito.eq(false),
            Mockito.eq(ReconcilerService.CaptureMode.STATS_ONLY),
            scopeCaptor.capture());
    assertThat(scopeCaptor.getValue().destinationStatsRequests())
        .containsExactly(
            scopedStatsRequest(
                requestedTableId.getId(),
                201L,
                StatsTargetScopeCodec.encode(StatsTargetIdentity.tableTarget()),
                List.of("#9")));
  }

  @Test
  void planTableTasksRejectsInvalidScopedRequestsBeforePlanning() {
    service.backend = new ReturningBackend(activeConnector());
    service.connectorOpener = cfg -> new FakeConnector(List.of());

    ReconcileScope scope =
        ReconcileScope.of(
            List.of(),
            "tbl-id",
            List.of(
                scopedStatsRequest(
                    "",
                    201L,
                    StatsTargetScopeCodec.encode(StatsTargetIdentity.tableTarget()),
                    List.of())));

    assertThatThrownBy(() -> service.planTableTasks(principal, connectorId, scope, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("missing table id");
  }

  @Test
  void tablePlanningRejectsViewIdScope() {
    ReconcileScope scope = ReconcileScope.ofView(List.of(), "view-1");

    assertThatThrownBy(() -> service.planTableTasks(principal, connectorId, scope, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("table planning cannot be combined with destination view id scope");
  }

  @Test
  void viewPlanningRejectsTableIdScope() {
    ReconcileScope scope = ReconcileScope.of(List.of(), "table-1");

    assertThatThrownBy(() -> service.planViewTasks(principal, connectorId, scope, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("view planning cannot be combined with destination table id scope");
  }

  @Test
  void planViewTasksRejectsInvalidScopedStatsRequestSnapshotIdsBeforePlanning() {
    service.backend = new ReturningBackend(activeConnector());
    service.connectorOpener = cfg -> new FakeConnector(List.of());

    ReconcileScope scope =
        ReconcileScope.of(
            List.of("ns-1"),
            null,
            List.of(
                scopedStatsRequest(
                    "tbl",
                    -1L,
                    StatsTargetScopeCodec.encode(StatsTargetIdentity.tableTarget()),
                    List.of())));

    assertThatThrownBy(() -> service.planViewTasks(principal, connectorId, scope, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("invalid snapshot id");
  }

  @Test
  void incrementalMetadataAndStatsDoesNotCountUnchangedKnownSnapshotsAsTableChange() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("table-known-snapshot-no-change")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    long createdAtMs = Instant.parse("2026-04-22T12:00:00Z").toEpochMilli();

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
      public ResourceId ensureNamespace(
          ReconcileContext ctx, ResourceId catalogId, NameRef namespace) {
        return ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("dest-ns")
            .build();
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
      public Optional<ResourceId> lookupTable(ReconcileContext ctx, NameRef table) {
        return Optional.of(tableId);
      }

      @Override
      public Optional<Snapshot> fetchSnapshot(
          ReconcileContext ctx, ResourceId ignoredTableId, long snapshotId) {
        return Optional.of(
            Snapshot.newBuilder()
                .setTableId(tableId)
                .setSnapshotId(snapshotId)
                .setUpstreamCreatedAt(com.google.protobuf.util.Timestamps.fromMillis(createdAtMs))
                .setIngestedAt(com.google.protobuf.util.Timestamps.fromMillis(createdAtMs))
                .build());
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

    class KnownSnapshotConnector extends FakeConnector {
      KnownSnapshotConnector() {
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
            new SnapshotBundle(201L, 0L, createdAtMs, "", null, 0L, null, Map.of(), 0, Map.of()));
      }
    }

    service.backend = new Backend();
    service.connectorOpener = cfg -> new KnownSnapshotConnector();

    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    Mockito.when(
            jobStore.enqueue(
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyBoolean(),
                Mockito.any(),
                Mockito.any()))
        .thenReturn("job-1");
    @SuppressWarnings("unchecked")
    Instance<ReconcileJobStore> jobStoreInstance = Mockito.mock(Instance.class);
    Mockito.when(jobStoreInstance.isUnsatisfied()).thenReturn(false);
    Mockito.when(jobStoreInstance.get()).thenReturn(jobStore);
    service.reconcileJobStore = jobStoreInstance;

    var result = service.reconcile(principal, connectorId, false, null);

    assertThat(result.ok()).isTrue();
    assertThat(result.tablesChanged).isZero();
    assertThat(result.changed).isZero();
  }

  @Test
  void incrementalMetadataAndStatsDoesNotCountConstraintRewriteAsTableChange() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("table-known-snapshot-constraints-only")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    long createdAtMs = Instant.parse("2026-04-22T12:00:00Z").toEpochMilli();

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
      public ResourceId ensureNamespace(
          ReconcileContext ctx, ResourceId catalogId, NameRef namespace) {
        return ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("dest-ns")
            .build();
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
      public Optional<ResourceId> lookupTable(ReconcileContext ctx, NameRef table) {
        return Optional.of(tableId);
      }

      @Override
      public Optional<Snapshot> fetchSnapshot(
          ReconcileContext ctx, ResourceId ignoredTableId, long snapshotId) {
        return Optional.of(
            Snapshot.newBuilder()
                .setTableId(tableId)
                .setSnapshotId(snapshotId)
                .setUpstreamCreatedAt(com.google.protobuf.util.Timestamps.fromMillis(createdAtMs))
                .setIngestedAt(com.google.protobuf.util.Timestamps.fromMillis(createdAtMs))
                .build());
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
      public boolean putSnapshotConstraints(
          ReconcileContext ctx,
          ResourceId ignoredTableId,
          long snapshotId,
          SnapshotConstraints constraints) {
        putConstraintsCalls++;
        return false;
      }

      @Override
      public void updateConnectorDestination(
          ReconcileContext ctx, ResourceId connectorId, DestinationTarget destination) {}
    }

    class KnownSnapshotConnector extends FakeConnector {
      KnownSnapshotConnector() {
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
            new SnapshotBundle(201L, 0L, createdAtMs, "", null, 0L, null, Map.of(), 0, Map.of()));
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

    Backend backend = new Backend();
    service.backend = backend;
    service.connectorOpener = cfg -> new KnownSnapshotConnector();

    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    Mockito.when(
            jobStore.enqueue(
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyBoolean(),
                Mockito.any(),
                Mockito.any()))
        .thenReturn("job-1");
    @SuppressWarnings("unchecked")
    Instance<ReconcileJobStore> jobStoreInstance = Mockito.mock(Instance.class);
    Mockito.when(jobStoreInstance.isUnsatisfied()).thenReturn(false);
    Mockito.when(jobStoreInstance.get()).thenReturn(jobStore);
    service.reconcileJobStore = jobStoreInstance;

    var result = service.reconcile(principal, connectorId, false, null);

    assertThat(result.ok()).isTrue();
    assertThat(backend.putConstraintsCalls).isEqualTo(1);
    assertThat(result.tablesChanged).isZero();
    assertThat(result.changed).isZero();
  }

  @Test
  void statsOnlyDedupesDuplicateScopedRequestsBeforeBatchCapture() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("table-stats-only-deduped-requests")
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
                        NamespacePath.newBuilder().addSegments("src_cat").addSegments("src_ns"))
                    .setTable("tbl"))
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
      public Optional<String> lookupTableDisplayName(
          ReconcileContext ctx, ResourceId ignoredTableId) {
        return Optional.of("tbl");
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
                201L, 0L, Instant.now().toEpochMilli(), "", null, 0L, null, Map.of(), 0, Map.of()));
      }
    }

    StatsCaptureControlPlane controlPlane = Mockito.mock(StatsCaptureControlPlane.class);
    ArgumentCaptor<StatsCaptureBatchRequest> batchCaptor =
        ArgumentCaptor.forClass(StatsCaptureBatchRequest.class);
    Mockito.when(controlPlane.triggerBatch(batchCaptor.capture()))
        .thenAnswer(
            invocation -> {
              StatsCaptureBatchRequest batchRequest = invocation.getArgument(0);
              return StatsCaptureBatchResult.of(
                  batchRequest.requests().stream()
                      .map(ReconcilerServiceTest::capturedItem)
                      .toList());
            });

    @SuppressWarnings("unchecked")
    Instance<StatsCaptureControlPlane> instance = Mockito.mock(Instance.class);
    Mockito.when(instance.isUnsatisfied()).thenReturn(false);
    Mockito.when(instance.get()).thenReturn(controlPlane);

    service.backend = new Backend();
    service.statsCaptureControlPlane = instance;
    service.connectorOpener = cfg -> new SnapshotOnlyConnector();

    String targetSpec = StatsTargetScopeCodec.encode(StatsTargetIdentity.tableTarget());
    var result =
        reconcileTableTask(
            tableId,
            false,
            ReconcileScope.of(
                List.of(),
                tableId.getId(),
                List.of(
                    scopedStatsRequest(tableId.getId(), 201L, targetSpec, List.of(" #9 ", "c7")),
                    scopedStatsRequest(tableId.getId(), 201L, targetSpec, List.of("c7", "#9")))),
            ReconcilerService.CaptureMode.STATS_ONLY);

    assertThat(result.ok()).isTrue();
    Mockito.verify(controlPlane).triggerBatch(Mockito.any());
    assertThat(batchCaptor.getValue().requests()).hasSize(1);
    assertThat(batchCaptor.getValue().requests().getFirst().columnSelectors())
        .containsExactlyInAnyOrder("#9", "c7");
  }

  @Test
  void metadataFollowUpScopeDedupesDuplicateScopedRequests() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("table-follow-up-deduped-requests")
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
      public ResourceId ensureNamespace(
          ReconcileContext ctx, ResourceId catalogId, NameRef namespace) {
        return ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("dest-ns")
            .build();
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

    class OneSnapshotConnector extends FakeConnector {
      OneSnapshotConnector() {
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
                201L, 0L, Instant.now().toEpochMilli(), "", null, 0L, null, Map.of(), 0, Map.of()));
      }
    }

    service.backend = new Backend();
    service.connectorOpener = cfg -> new OneSnapshotConnector();

    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    Mockito.when(
            jobStore.enqueue(
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyBoolean(),
                Mockito.any(),
                Mockito.any()))
        .thenReturn("job-1");
    @SuppressWarnings("unchecked")
    Instance<ReconcileJobStore> jobStoreInstance = Mockito.mock(Instance.class);
    Mockito.when(jobStoreInstance.isUnsatisfied()).thenReturn(false);
    Mockito.when(jobStoreInstance.get()).thenReturn(jobStore);
    service.reconcileJobStore = jobStoreInstance;

    String targetSpec = StatsTargetScopeCodec.encode(StatsTargetIdentity.tableTarget());
    ReconcileScope scope =
        ReconcileScope.of(
            List.of(),
            tableId.getId(),
            List.of(
                scopedStatsRequest(tableId.getId(), 201L, targetSpec, List.of(" #9 ", "c7")),
                scopedStatsRequest(tableId.getId(), 201L, targetSpec, List.of("c7", "#9"))));

    var result =
        reconcileTableTask(tableId, true, scope, ReconcilerService.CaptureMode.METADATA_AND_STATS);

    assertThat(result.ok()).isTrue();

    ArgumentCaptor<ReconcileScope> scopeCaptor = ArgumentCaptor.forClass(ReconcileScope.class);
    Mockito.verify(jobStore)
        .enqueue(
            Mockito.eq("acct"),
            Mockito.eq("connector-1"),
            Mockito.eq(false),
            Mockito.eq(ReconcilerService.CaptureMode.STATS_ONLY),
            scopeCaptor.capture());

    assertThat(scopeCaptor.getValue().destinationStatsRequests())
        .containsExactly(
            scopedStatsRequest(tableId.getId(), 201L, targetSpec, List.of("#9", "c7")));
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
      public boolean putSnapshotConstraints(
          ReconcileContext ctx,
          ResourceId ignoredTableId,
          long snapshotId,
          SnapshotConstraints constraints) {
        putConstraintsCalls++;
        return true;
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
            .setSource(base.getSource().toBuilder().setTable("tbl"))
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
      public boolean putSnapshotConstraints(
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
      public boolean putSnapshotConstraints(
          ReconcileContext ctx,
          ResourceId ignoredTableId,
          long snapshotId,
          SnapshotConstraints constraints) {
        putConstraintsCalls++;
        return true;
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
      public boolean putSnapshotConstraints(
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
                        NamespacePath.newBuilder().addSegments("src_cat").addSegments("src_ns"))
                    .setTable("tbl"))
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

    StatsCaptureControlPlane controlPlane = Mockito.mock(StatsCaptureControlPlane.class);
    ArgumentCaptor<StatsCaptureBatchRequest> batchCaptor =
        ArgumentCaptor.forClass(StatsCaptureBatchRequest.class);
    Mockito.when(controlPlane.triggerBatch(batchCaptor.capture()))
        .thenAnswer(
            invocation -> {
              StatsCaptureBatchRequest batchRequest = invocation.getArgument(0);
              return StatsCaptureBatchResult.of(
                  batchRequest.requests().stream()
                      .map(ReconcilerServiceTest::capturedItem)
                      .toList());
            });

    @SuppressWarnings("unchecked")
    Instance<StatsCaptureControlPlane> instance = Mockito.mock(Instance.class);
    Mockito.when(instance.isUnsatisfied()).thenReturn(false);
    Mockito.when(instance.get()).thenReturn(controlPlane);

    Backend backend = new Backend();
    service.backend = backend;
    service.statsCaptureControlPlane = instance;
    service.connectorOpener = cfg -> new SnapshotOnlyConnector();

    var result =
        reconcileTableTask(
            tableId,
            false,
            ReconcileScope.of(
                List.of(),
                tableId.getId(),
                List.of(
                    scopedStatsRequest(
                        tableId.getId(),
                        201L,
                        StatsTargetScopeCodec.encode(StatsTargetIdentity.tableTarget()),
                        List.of("#9", "c7")),
                    scopedStatsRequest(
                        tableId.getId(),
                        201L,
                        StatsTargetScopeCodec.encode(StatsTargetIdentity.columnTarget(9L)),
                        List.of()),
                    scopedStatsRequest(
                        tableId.getId(),
                        202L,
                        StatsTargetScopeCodec.encode(StatsTargetIdentity.tableTarget()),
                        List.of("#9", "c7")),
                    scopedStatsRequest(
                        tableId.getId(),
                        202L,
                        StatsTargetScopeCodec.encode(StatsTargetIdentity.columnTarget(9L)),
                        List.of()))),
            ReconcilerService.CaptureMode.STATS_ONLY);

    assertThat(result.ok()).isTrue();
    Mockito.verify(controlPlane).triggerBatch(Mockito.any());
    assertThat(batchCaptor.getValue().requests()).hasSize(4);
    assertThat(batchCaptor.getValue().requests())
        .extracting(StatsCaptureRequest::target)
        .anyMatch(target -> target.hasColumn() && target.getColumn().getColumnId() == 9L);
    assertThat(batchCaptor.getValue().requests())
        .filteredOn(request -> request.target().hasTable())
        .extracting(StatsCaptureRequest::columnSelectors)
        .allSatisfy(selectors -> assertThat(selectors).containsExactlyInAnyOrder("c7", "#9"));
    assertThat(result.statsProcessed).isEqualTo(2L);
    assertThat(backend.putTargetStatsCalls).isZero();
  }

  @Test
  void statsOnlyDiscoveryResolvesDestinationTableByNameAndSkipsWhenMissing() {
    int[] lookupTableCalls = {0};
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
      public String resolveNamespaceFq(ReconcileContext ctx, ResourceId namespaceId) {
        return "dest_ns";
      }

      @Override
      public String lookupCatalogName(ReconcileContext ctx, ResourceId catalogId) {
        return "dest_cat";
      }

      @Override
      public Optional<ResourceId> lookupTable(ReconcileContext ctx, NameRef table) {
        lookupTableCalls[0]++;
        assertThat(table.getName()).isEqualTo("tbl");
        return Optional.empty();
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
      public List<FloecatConnector.PlannedTableTask> planTableTasks(
          FloecatConnector.TablePlanningRequest request) {
        return List.of(new FloecatConnector.PlannedTableTask("src_cat.src_ns", "tbl", "tbl"));
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
    }

    StatsCaptureControlPlane controlPlane = Mockito.mock(StatsCaptureControlPlane.class);
    @SuppressWarnings("unchecked")
    Instance<StatsCaptureControlPlane> instance = Mockito.mock(Instance.class);
    Mockito.when(instance.isUnsatisfied()).thenReturn(false);
    Mockito.when(instance.get()).thenReturn(controlPlane);

    service.backend = new Backend();
    service.statsCaptureControlPlane = instance;
    service.connectorOpener = cfg -> new SnapshotOnlyConnector();

    var result =
        service.reconcile(
            principal,
            connectorId,
            false,
            ReconcileScope.empty(),
            ReconcilerService.CaptureMode.STATS_ONLY);

    assertThat(result.ok()).isTrue();
    assertThat(result.scanned).isZero();
    assertThat(result.statsProcessed).isZero();
    assertThat(lookupTableCalls[0]).isEqualTo(2);
    Mockito.verifyNoInteractions(controlPlane);
  }

  @Test
  void statsOnlyDiscoveryMatchedByNameClearsScopedStatsRequest() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("table-stats-only-by-name")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    int[] lookupTableCalls = {0};
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
        return "dest_cat";
      }

      @Override
      public String resolveNamespaceFq(ReconcileContext ctx, ResourceId namespaceId) {
        return "dest_ns";
      }

      @Override
      public Optional<ResourceId> lookupTable(ReconcileContext ctx, NameRef table) {
        lookupTableCalls[0]++;
        assertThat(table.getName()).isEqualTo("tbl");
        return lookupTableCalls[0] == 1 ? Optional.empty() : Optional.of(tableId);
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
      public boolean statsCapturedForTargets(
          ReconcileContext ctx,
          ResourceId ignoredTableId,
          long snapshotId,
          Set<StatsTarget> targets) {
        return false;
      }

      @Override
      public boolean statsCapturedForColumnSelectors(
          ReconcileContext ctx, ResourceId ignoredTableId, long snapshotId, Set<String> selectors) {
        return false;
      }
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
      public List<FloecatConnector.PlannedTableTask> planTableTasks(
          FloecatConnector.TablePlanningRequest request) {
        return List.of(new FloecatConnector.PlannedTableTask("src_cat.src_ns", "tbl", "tbl"));
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
                201L, 0L, Instant.now().toEpochMilli(), "", null, 0L, null, Map.of(), 0, Map.of()));
      }
    }

    StatsCaptureControlPlane controlPlane =
        capturedControlPlane(new java.util.concurrent.atomic.AtomicInteger());
    @SuppressWarnings("unchecked")
    Instance<StatsCaptureControlPlane> instance = Mockito.mock(Instance.class);
    Mockito.when(instance.isUnsatisfied()).thenReturn(false);
    Mockito.when(instance.get()).thenReturn(controlPlane);

    service.backend = new Backend();
    service.statsCaptureControlPlane = instance;
    service.connectorOpener = cfg -> new SnapshotOnlyConnector();

    String targetSpec = StatsTargetScopeCodec.encode(StatsTargetIdentity.tableTarget());
    var result =
        service.reconcile(
            principal,
            connectorId,
            false,
            ReconcileScope.of(
                List.of(),
                null,
                List.of(scopedStatsRequest(tableId.getId(), 201L, targetSpec, List.of()))),
            ReconcilerService.CaptureMode.STATS_ONLY);

    assertThat(result.ok()).isTrue();
    assertThat(result.errors).isZero();
    assertThat(result.tablesScanned).isEqualTo(1L);
    assertThat(result.snapshotsProcessed).isEqualTo(1L);
    assertThat(result.statsProcessed).isEqualTo(1L);
    assertThat(lookupTableCalls[0]).isEqualTo(2);
  }

  @Test
  void statsOnlySkipsMalformedEncodedTargetsBeforeBatchSubmission() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("table-stats-only-malformed-target")
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
                        NamespacePath.newBuilder().addSegments("src_cat").addSegments("src_ns"))
                    .setTable("tbl"))
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

    StatsCaptureControlPlane controlPlane = Mockito.mock(StatsCaptureControlPlane.class);
    ArgumentCaptor<StatsCaptureBatchRequest> batchCaptor =
        ArgumentCaptor.forClass(StatsCaptureBatchRequest.class);
    Mockito.when(controlPlane.triggerBatch(batchCaptor.capture()))
        .thenAnswer(
            invocation -> {
              StatsCaptureBatchRequest batchRequest = invocation.getArgument(0);
              return StatsCaptureBatchResult.of(
                  batchRequest.requests().stream()
                      .map(ReconcilerServiceTest::capturedItem)
                      .toList());
            });

    @SuppressWarnings("unchecked")
    Instance<StatsCaptureControlPlane> instance = Mockito.mock(Instance.class);
    Mockito.when(instance.isUnsatisfied()).thenReturn(false);
    Mockito.when(instance.get()).thenReturn(controlPlane);

    service.backend = new Backend();
    service.statsCaptureControlPlane = instance;
    service.connectorOpener = cfg -> new SnapshotOnlyConnector();

    var result =
        reconcileTableTask(
            tableId,
            false,
            ReconcileScope.of(
                List.of(),
                tableId.getId(),
                List.of(
                    scopedStatsRequest(tableId.getId(), 201L, "malformed-target-spec", List.of()),
                    scopedStatsRequest(
                        tableId.getId(),
                        201L,
                        StatsTargetScopeCodec.encode(StatsTargetIdentity.tableTarget()),
                        List.of()))),
            ReconcilerService.CaptureMode.STATS_ONLY);

    assertThat(result.ok()).isFalse();
    Mockito.verifyNoInteractions(controlPlane);
  }

  @Test
  void statsOnlyKnownSnapshotDoesNotTreatMalformedScopedRequestAsComplete() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("table-stats-known-malformed-target")
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
                        NamespacePath.newBuilder().addSegments("src_cat").addSegments("src_ns"))
                    .setTable("tbl"))
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
        return Set.of(201L);
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
      public boolean statsCapturedForColumnSelectors(
          ReconcileContext ctx, ResourceId ignoredTableId, long snapshotId, Set<String> selectors) {
        return true;
      }

      @Override
      public boolean statsCapturedForTargets(
          ReconcileContext ctx,
          ResourceId ignoredTableId,
          long snapshotId,
          Set<StatsTarget> targets) {
        return true;
      }

      @Override
      public Optional<String> lookupTableDisplayName(
          ReconcileContext ctx, ResourceId ignoredTableId) {
        return Optional.of("tbl");
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
                201L, 0L, Instant.now().toEpochMilli(), "", null, 0L, null, Map.of(), 0, Map.of()));
      }
    }

    StatsCaptureControlPlane controlPlane = Mockito.mock(StatsCaptureControlPlane.class);
    ArgumentCaptor<StatsCaptureBatchRequest> batchCaptor =
        ArgumentCaptor.forClass(StatsCaptureBatchRequest.class);
    Mockito.when(controlPlane.triggerBatch(batchCaptor.capture()))
        .thenAnswer(
            invocation -> {
              StatsCaptureBatchRequest batchRequest = invocation.getArgument(0);
              return StatsCaptureBatchResult.of(
                  batchRequest.requests().stream()
                      .map(ReconcilerServiceTest::capturedItem)
                      .toList());
            });

    @SuppressWarnings("unchecked")
    Instance<StatsCaptureControlPlane> instance = Mockito.mock(Instance.class);
    Mockito.when(instance.isUnsatisfied()).thenReturn(false);
    Mockito.when(instance.get()).thenReturn(controlPlane);

    service.backend = new Backend();
    service.statsCaptureControlPlane = instance;
    service.connectorOpener = cfg -> new SnapshotOnlyConnector();

    var result =
        reconcileTableTask(
            tableId,
            false,
            ReconcileScope.of(
                List.of(),
                tableId.getId(),
                List.of(
                    scopedStatsRequest(tableId.getId(), 201L, "malformed-target-spec", List.of()),
                    scopedStatsRequest(
                        tableId.getId(),
                        201L,
                        StatsTargetScopeCodec.encode(StatsTargetIdentity.tableTarget()),
                        List.of()))),
            ReconcilerService.CaptureMode.STATS_ONLY);

    assertThat(result.ok()).isFalse();
    Mockito.verifyNoInteractions(controlPlane);
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
                        NamespacePath.newBuilder().addSegments("src_cat").addSegments("src_ns"))
                    .setTable("tbl"))
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
        reconcileTableTask(
            tableId,
            false,
            ReconcileScope.of(List.of(), tableId.getId()),
            ReconcilerService.CaptureMode.STATS_ONLY);

    assertThat(result.ok()).isTrue();
    assertThat(result.snapshotsProcessed).isEqualTo(2L);
    assertThat(result.statsProcessed).isZero();
    assertThat(result.errors).isZero();
  }

  @Test
  void statsOnlyConnectorAssistedPathRemainsSuccessfulWhenBatchIsDegraded() {
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
                        NamespacePath.newBuilder().addSegments("src_cat").addSegments("src_ns"))
                    .setTable("tbl"))
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

    StatsCaptureControlPlane controlPlane = Mockito.mock(StatsCaptureControlPlane.class);
    Mockito.when(controlPlane.triggerBatch(Mockito.any()))
        .thenAnswer(
            invocation -> {
              StatsCaptureBatchRequest batchRequest = invocation.getArgument(0);
              return StatsCaptureBatchResult.of(
                  batchRequest.requests().stream()
                      .map(req -> StatsCaptureBatchItemResult.degraded(req, "runtime"))
                      .toList());
            });

    @SuppressWarnings("unchecked")
    Instance<StatsCaptureControlPlane> instance = Mockito.mock(Instance.class);
    Mockito.when(instance.isUnsatisfied()).thenReturn(false);
    Mockito.when(instance.get()).thenReturn(controlPlane);

    service.backend = new Backend();
    service.statsCaptureControlPlane = instance;
    service.connectorOpener = cfg -> new SnapshotOnlyConnector();

    var result =
        reconcileTableTask(
            tableId,
            false,
            ReconcileScope.of(List.of(), tableId.getId()),
            ReconcilerService.CaptureMode.STATS_ONLY);

    assertThat(result.ok()).isTrue();
    assertThat(result.snapshotsProcessed).isEqualTo(2L);
    assertThat(result.statsProcessed).isZero();
    assertThat(result.errors).isZero();
  }

  @Test
  void statsOnlyKnownSnapshotWithMissingExplicitTargetIsNotSkipped() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("table-stats-only-explicit-target")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    StatsTarget explicitTarget = StatsTargetIdentity.columnTarget(9L);

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
                        NamespacePath.newBuilder().addSegments("src_cat").addSegments("src_ns"))
                    .setTable("tbl"))
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
        return Set.of(201L);
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
      public boolean statsCapturedForColumnSelectors(
          ReconcileContext ctx, ResourceId ignoredTableId, long snapshotId, Set<String> selectors) {
        return true;
      }

      @Override
      public boolean statsCapturedForTargets(
          ReconcileContext ctx,
          ResourceId ignoredTableId,
          long snapshotId,
          Set<StatsTarget> targets) {
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
                201L, 0L, Instant.now().toEpochMilli(), "", null, 0L, null, Map.of(), 0, Map.of()));
      }
    }

    StatsCaptureControlPlane controlPlane = Mockito.mock(StatsCaptureControlPlane.class);
    ArgumentCaptor<StatsCaptureBatchRequest> batchCaptor =
        ArgumentCaptor.forClass(StatsCaptureBatchRequest.class);
    Mockito.when(controlPlane.triggerBatch(batchCaptor.capture()))
        .thenAnswer(
            invocation -> {
              StatsCaptureBatchRequest batchRequest = invocation.getArgument(0);
              return StatsCaptureBatchResult.of(
                  batchRequest.requests().stream()
                      .map(ReconcilerServiceTest::capturedItem)
                      .toList());
            });

    @SuppressWarnings("unchecked")
    Instance<StatsCaptureControlPlane> instance = Mockito.mock(Instance.class);
    Mockito.when(instance.isUnsatisfied()).thenReturn(false);
    Mockito.when(instance.get()).thenReturn(controlPlane);

    service.backend = new Backend();
    service.statsCaptureControlPlane = instance;
    service.connectorOpener = cfg -> new SnapshotOnlyConnector();

    var result =
        reconcileTableTask(
            tableId,
            false,
            ReconcileScope.of(
                List.of(),
                tableId.getId(),
                List.of(
                    scopedStatsRequest(
                        tableId.getId(),
                        201L,
                        StatsTargetScopeCodec.encode(explicitTarget),
                        List.of()))),
            ReconcilerService.CaptureMode.STATS_ONLY);

    assertThat(result.ok()).isTrue();
    Mockito.verify(controlPlane).triggerBatch(Mockito.any());
    assertThat(batchCaptor.getValue().requests()).hasSize(1);
    assertThat(batchCaptor.getValue().requests().getFirst().snapshotId()).isEqualTo(201L);
    assertThat(batchCaptor.getValue().requests().getFirst().target()).isEqualTo(explicitTarget);
  }

  private static StatsCaptureBatchItemResult capturedItem(StatsCaptureRequest request) {
    StatsCaptureResult captureResult =
        StatsCaptureResult.forRecord(
            "test-engine",
            TargetStatsRecord.newBuilder()
                .setTableId(request.tableId())
                .setSnapshotId(request.snapshotId())
                .setTarget(request.target())
                .setTable(
                    ai.floedb.floecat.catalog.rpc.TableValueStats.newBuilder()
                        .setRowCount(1L)
                        .build())
                .build(),
            Map.of("outcome", StatsTriggerOutcome.CAPTURED.name()));
    return StatsCaptureBatchItemResult.captured(request, captureResult);
  }

  private static FloecatConnector tableDescriptorConnector() {
    return new FakeConnector(List.of()) {
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
    };
  }

  private ReconcilerService.Result reconcileTableTask(
      ResourceId tableId,
      boolean fullRescan,
      ReconcileScope scope,
      ReconcilerService.CaptureMode captureMode) {
    return service.reconcile(
        principal,
        connectorId,
        fullRescan,
        scope,
        ReconcileTableTask.of("src_cat.src_ns", "tbl", tableId.getId(), "tbl"),
        captureMode,
        null,
        () -> false,
        (tablesScanned,
            tablesChanged,
            viewsScanned,
            viewsChanged,
            errors,
            snapshotsProcessed,
            statsProcessed,
            message) -> {});
  }

  private static ReconcileScope.ScopedStatsRequest scopedStatsRequest(
      String tableId, long snapshotId, String targetSpec, List<String> columnSelectors) {
    return new ReconcileScope.ScopedStatsRequest(tableId, snapshotId, targetSpec, columnSelectors);
  }
}
