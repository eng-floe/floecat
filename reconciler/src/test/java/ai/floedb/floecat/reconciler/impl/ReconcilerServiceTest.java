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
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.rpc.ConnectorState;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend.DestinationTableMetadata;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend.DestinationViewMetadata;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.identity.StatsTargetScopeCodec;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ReconcilerServiceTest extends AbstractReconcilerServiceTestBase {
  private static final String DEST_CATALOG = "cat-1";
  private static final List<String> DEST_NAMESPACE_PATH = List.of("dest_ns");

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
            };

    assertThatThrownBy(
            () ->
                service.planTableTasks(
                    principal, connectorId, ReconcileScope.of(List.of(), "table-1"), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("missing persisted source identity");
  }

  @Test
  void tableFilterPlanningAcceptsLegacyPersistedSourceConnectorIdentityWithoutAccountOrKind() {
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
            };

    var tasks =
        service.planTableTasks(
            principal, connectorId, ReconcileScope.of(List.of(), "table-1"), null);

    assertThat(tasks)
        .containsExactly(ReconcileTableTask.of("src_cat.src_ns", "tbl", "table-1", "tbl"));
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
        reconcileTableTask(
            tableId,
            false,
            ReconcileScope.of(List.of(), tableId.getId()),
            ReconcilerService.CaptureMode.METADATA_ONLY);

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
        reconcileTableTask(
            tableId,
            false,
            ReconcileScope.of(List.of(), tableId.getId()),
            ReconcilerService.CaptureMode.METADATA_ONLY);

    assertThat(result.ok()).isFalse();
    assertThat(result.tablesScanned).isEqualTo(1);
    assertThat(result.errors).isEqualTo(1);
    assertThat(result.error.getMessage()).contains("snapshot enumeration failed");
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
        reconcileQueuedTableExecution(
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
        reconcileQueuedTableExecution(
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
            throw new AssertionError("CAPTURE_ONLY discovery must not create destination tables");
          }
        };
    service.connectorOpener = cfg -> tableDescriptorConnector();

    var result =
        reconcileQueuedTableExecution(
            principal,
            connectorId,
            false,
            ReconcileScope.of(
                List.of(),
                null,
                List.of(),
                ReconcileCapturePolicy.of(
                    List.of(),
                    Set.of(
                        ReconcileCapturePolicy.Output.TABLE_STATS,
                        ReconcileCapturePolicy.Output.FILE_STATS,
                        ReconcileCapturePolicy.Output.COLUMN_STATS,
                        ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX))),
            ReconcileTableTask.discovery("src_cat.src_ns", "tbl", "ns-1", "tbl_display"),
            ReconcilerService.CaptureMode.CAPTURE_ONLY,
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
      public Optional<DestinationTableMetadata> lookupDestinationTableMetadata(
          ReconcileContext ctx, ResourceId ignoredTableId) {
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
                connectorId));
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
                42L, 0L, Instant.now().toEpochMilli(), "", null, 0L, null, Map.of(), 0, null));
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

    ReconcileScope scope =
        ReconcileScope.of(
            List.of(),
            tableId.getId(),
            List.of(),
            ReconcileCapturePolicy.of(
                List.of(),
                Set.of(
                    ReconcileCapturePolicy.Output.TABLE_STATS,
                    ReconcileCapturePolicy.Output.FILE_STATS,
                    ReconcileCapturePolicy.Output.COLUMN_STATS,
                    ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX)));
    var result =
        reconcileTableTask(
            tableId, true, scope, ReconcilerService.CaptureMode.METADATA_AND_CAPTURE);

    assertThat(result.ok()).isTrue();
    assertThat(result.degraded()).isFalse();
    assertThat(backend.capturedTargetSnapshotIds).isEmpty();
    assertThat(backend.capturedKnownSnapshotIds).isEmpty();
    assertThat(backend.putConstraintsSnapshotId).isEqualTo(42L);
    assertThat(backend.putConstraints).isNotNull();
    assertThat(backend.putConstraints.getConstraintsCount()).isEqualTo(1);
    assertThat(backend.putConstraints.getConstraints(0).getName()).isEqualTo("pk_tbl");
  }

  @Test
  void metadataAndStatsDoesNotEnqueueFollowUpStatsJobWhenUnscoped() {
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
      public Optional<DestinationTableMetadata> lookupDestinationTableMetadata(
          ReconcileContext ctx, ResourceId ignoredTableId) {
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
                connectorId));
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
        return Set.of(201L);
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

    Backend backend = new Backend();

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
                42L, 0L, Instant.now().toEpochMilli(), "", null, 0L, null, Map.of(), 0, null));
      }
    }

    service.backend = new Backend();
    service.connectorOpener = cfg -> new SingleSnapshotConnector();

    ReconcileScope scope =
        ReconcileScope.of(
            List.of(),
            tableId.getId(),
            List.of(),
            ReconcileCapturePolicy.of(
                List.of(),
                Set.of(
                    ReconcileCapturePolicy.Output.TABLE_STATS,
                    ReconcileCapturePolicy.Output.FILE_STATS,
                    ReconcileCapturePolicy.Output.COLUMN_STATS,
                    ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX)));
    var result =
        reconcileTableTask(
            tableId, true, scope, ReconcilerService.CaptureMode.METADATA_AND_CAPTURE);

    assertThat(result.ok()).isTrue();
    assertThat(result.degraded()).isFalse();
  }

  @Test
  void metadataExecutionIgnoresCaptureCompletenessWhenCaptureRunsOutOfBand() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("table-file-stats-only")
            .setKind(ResourceKind.RK_TABLE)
            .build();

    class Backend extends DefaultBackend {
      Set<Long> capturedKnownSnapshotIds = Set.of();

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
        return Set.of(42L);
      }

      @Override
      public boolean statsAlreadyCapturedForTargetKind(
          ReconcileContext ctx,
          ResourceId ignoredTableId,
          long snapshotId,
          ai.floedb.floecat.catalog.rpc.StatsTargetKind targetKind) {
        return targetKind == ai.floedb.floecat.catalog.rpc.StatsTargetKind.STK_TABLE;
      }

      @Override
      public void updateConnectorDestination(
          ReconcileContext ctx, ResourceId connectorId, DestinationTarget destination) {}
    }

    Backend backend = new Backend();

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
        backend.capturedKnownSnapshotIds = options.knownSnapshotIds();
        return List.of(
            new SnapshotBundle(
                42L, 0L, Instant.now().toEpochMilli(), "", null, 0L, null, Map.of(), 0, null));
      }
    }

    service.backend = backend;
    service.connectorOpener = cfg -> new SingleSnapshotConnector();

    ReconcileScope scope =
        ReconcileScope.of(
            List.of(),
            tableId.getId(),
            List.of(),
            ReconcileCapturePolicy.of(List.of(), Set.of(ReconcileCapturePolicy.Output.FILE_STATS)));

    var result =
        reconcileTableTask(
            tableId, false, scope, ReconcilerService.CaptureMode.METADATA_AND_CAPTURE);

    assertThat(result.ok()).isTrue();
    assertThat(backend.capturedKnownSnapshotIds).containsExactly(42L);
  }

  @Test
  void snapshotPlanningTreatsSnapshotAsIncompleteWhenOnlyTableStatsExist() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("table-file-stats-plan-only")
            .setKind(ResourceKind.RK_TABLE)
            .build();

    class Backend extends DefaultBackend {
      Set<Long> capturedKnownSnapshotIds = Set.of();

      @Override
      public Connector lookupConnector(ReconcileContext ctx, ResourceId ignoredConnectorId) {
        return activeConnector();
      }

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
        return targetKind == ai.floedb.floecat.catalog.rpc.StatsTargetKind.STK_TABLE;
      }
    }

    Backend backend = new Backend();

    class SingleSnapshotConnector extends FakeConnector {
      SingleSnapshotConnector() {
        super(List.of());
      }

      @Override
      public List<SnapshotBundle> enumerateSnapshots(
          String namespaceFq,
          String tableName,
          ResourceId destinationTableId,
          SnapshotEnumerationOptions options) {
        backend.capturedKnownSnapshotIds = options.knownSnapshotIds();
        return List.of(
            new SnapshotBundle(
                42L, 0L, Instant.now().toEpochMilli(), "", null, 0L, null, Map.of(), 0, null));
      }
    }

    service.backend = backend;
    service.connectorOpener = cfg -> new SingleSnapshotConnector();

    ReconcileScope scope =
        ReconcileScope.of(
            List.of(),
            tableId.getId(),
            List.of(),
            ReconcileCapturePolicy.of(List.of(), Set.of(ReconcileCapturePolicy.Output.FILE_STATS)));

    List<ReconcileSnapshotTask> tasks =
        service.planSnapshotTasks(
            principal,
            connectorId,
            false,
            scope,
            ReconcileTableTask.of("src_cat.src_ns", "tbl", tableId.getId(), "tbl"),
            ReconcilerService.CaptureMode.METADATA_AND_CAPTURE,
            null);

    assertThat(backend.capturedKnownSnapshotIds).isEmpty();
    assertThat(tasks)
        .containsExactly(ReconcileSnapshotTask.of(tableId.getId(), 42L, "src_cat.src_ns", "tbl"));
  }

  @Test
  void metadataAndStatsDoesNotEnqueueFollowUpStatsJobForScopedRequests() {
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
        return Set.of(201L);
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
                201L, 0L, Instant.now().toEpochMilli(), "", null, 0L, null, Map.of(), 0, null),
            new SnapshotBundle(
                202L, 0L, Instant.now().toEpochMilli(), "", null, 0L, null, Map.of(), 0, null));
      }
    }

    service.backend = new Backend();
    service.connectorOpener = cfg -> new TwoSnapshotConnector();

    ReconcileScope scope =
        ReconcileScope.of(
            List.of(),
            tableId.getId(),
            List.of(
                scopedCaptureRequest(
                    tableId.getId(),
                    201L,
                    StatsTargetScopeCodec.encode(StatsTargetIdentity.tableTarget()),
                    List.of("#9")),
                scopedCaptureRequest(
                    tableId.getId(),
                    202L,
                    StatsTargetScopeCodec.encode(StatsTargetIdentity.columnTarget(9L)),
                    List.of())),
            ReconcileCapturePolicy.of(
                List.of(),
                Set.of(
                    ReconcileCapturePolicy.Output.TABLE_STATS,
                    ReconcileCapturePolicy.Output.COLUMN_STATS)));
    var result =
        reconcileTableTask(
            tableId, true, scope, ReconcilerService.CaptureMode.METADATA_AND_CAPTURE);

    assertThat(result.ok()).isTrue();
    assertThat(result.degraded()).isFalse();
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
                scopedCaptureRequest(
                    "",
                    201L,
                    StatsTargetScopeCodec.encode(StatsTargetIdentity.tableTarget()),
                    List.of())),
            ReconcileCapturePolicy.of(
                List.of(), Set.of(ReconcileCapturePolicy.Output.TABLE_STATS)));

    assertThatThrownBy(() -> service.planTableTasks(principal, connectorId, scope, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("missing table id");
  }

  @Test
  void planTableTasksRejectsScopedRequestsWithoutExplicitCaptureOutputs() {
    service.backend = new ReturningBackend(activeConnector());
    service.connectorOpener = cfg -> new FakeConnector(List.of());

    ReconcileScope scope =
        ReconcileScope.of(
            List.of(),
            "tbl-id",
            List.of(
                scopedCaptureRequest(
                    "tbl-id",
                    201L,
                    StatsTargetScopeCodec.encode(StatsTargetIdentity.columnTarget(9L)),
                    List.of())),
            ReconcileCapturePolicy.empty());

    assertThatThrownBy(() -> service.planTableTasks(principal, connectorId, scope, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("explicit capture_policy.outputs");
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
  void planViewTasksRejectsInvalidScopedCaptureRequestSnapshotIdsBeforePlanning() {
    service.backend = new ReturningBackend(activeConnector());
    service.connectorOpener = cfg -> new FakeConnector(List.of());

    ReconcileScope scope =
        ReconcileScope.of(
            List.of("ns-1"),
            null,
            List.of(
                scopedCaptureRequest(
                    "tbl",
                    -1L,
                    StatsTargetScopeCodec.encode(StatsTargetIdentity.tableTarget()),
                    List.of())),
            ReconcileCapturePolicy.of(
                List.of(), Set.of(ReconcileCapturePolicy.Output.TABLE_STATS)));

    assertThatThrownBy(() -> service.planViewTasks(principal, connectorId, scope, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("invalid snapshot id");
  }

  @Test
  void planViewTasksRejectsScopedRequestsWithoutExplicitCaptureOutputs() {
    service.backend = new ReturningBackend(activeConnector());
    service.connectorOpener = cfg -> new FakeConnector(List.of());

    ReconcileScope scope =
        ReconcileScope.of(
            List.of("ns-1"),
            null,
            List.of(
                scopedCaptureRequest(
                    "tbl",
                    55L,
                    StatsTargetScopeCodec.encode(StatsTargetIdentity.tableTarget()),
                    List.of())),
            ReconcileCapturePolicy.empty());

    assertThatThrownBy(() -> service.planViewTasks(principal, connectorId, scope, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("explicit capture_policy.outputs");
  }

  @Test
  void planViewTasksRejectsScopedFileCaptureTargetsBeforePlanning() {
    service.backend = new ReturningBackend(activeConnector());
    service.connectorOpener = cfg -> new FakeConnector(List.of());

    ReconcileScope scope =
        ReconcileScope.of(
            List.of("ns-1"),
            null,
            List.of(
                scopedCaptureRequest(
                    "tbl",
                    55L,
                    StatsTargetScopeCodec.encode(
                        StatsTargetIdentity.fileTarget("s3://bucket/data/file-1.parquet")),
                    List.of())),
            ReconcileCapturePolicy.of(
                List.of(), Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX)));

    assertThatThrownBy(() -> service.planViewTasks(principal, connectorId, scope, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("file targets are not supported");
  }

  @Test
  void planViewTasksRejectsScopedExpressionCaptureTargetsAsNotYetImplemented() {
    service.backend = new ReturningBackend(activeConnector());
    service.connectorOpener = cfg -> new FakeConnector(List.of());

    ReconcileScope scope =
        ReconcileScope.of(
            List.of("ns-1"),
            null,
            List.of(
                scopedCaptureRequest(
                    "tbl",
                    55L,
                    StatsTargetScopeCodec.encode(
                        StatsTargetIdentity.expressionTarget(
                            ai.floedb.floecat.catalog.rpc.EngineExpressionStatsTarget.newBuilder()
                                .setEngineKind("trino")
                                .setEngineExpressionKey(
                                    com.google.protobuf.ByteString.copyFromUtf8("expr-key"))
                                .build())),
                    List.of())),
            ReconcileCapturePolicy.of(
                List.of(), Set.of(ReconcileCapturePolicy.Output.COLUMN_STATS)));

    assertThatThrownBy(() -> service.planViewTasks(principal, connectorId, scope, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("recognized but not yet implemented in unified capture");
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
        return Set.of(201L);
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
            new SnapshotBundle(201L, 0L, createdAtMs, "", null, 0L, null, Map.of(), 0, null));
      }
    }

    service.backend = new Backend();
    service.connectorOpener = cfg -> new KnownSnapshotConnector();

    var result =
        reconcileTableTask(
            tableId,
            false,
            defaultCaptureScope(),
            ReconcilerService.CaptureMode.METADATA_AND_CAPTURE);

    assertThat(result.ok()).isTrue();
    assertThat(result.tablesChanged).isZero();
    assertThat(result.changed()).isZero();
  }

  @Test
  void restCaptureOnlyConnectorDoesNotMutateTableMetadataDuringReconcile() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("table-rest-capture-only")
            .setKind(ResourceKind.RK_TABLE)
            .build();

    class Backend extends DefaultBackend {
      int updateTableCalls = 0;

      @Override
      public Connector lookupConnector(ReconcileContext ctx, ResourceId ignoredConnectorId) {
        Connector base = activeConnector();
        return base.toBuilder()
            .setSource(base.getSource().toBuilder().setTable("tbl"))
            .setDestination(base.getDestination().toBuilder().setTableId(tableId))
            .putProperties(
                ReconcilerService.CONNECTOR_MODE_PROPERTY,
                ReconcilerService.CONNECTOR_MODE_CAPTURE_ONLY)
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
      public boolean updateTableById(
          ReconcileContext ctx,
          ResourceId ignoredTableId,
          ResourceId ignoredNamespaceId,
          NameRef ignoredTable,
          ReconcilerBackend.TableSpecDescriptor descriptor) {
        updateTableCalls++;
        return true;
      }

      @Override
      public Set<Long> existingSnapshotIds(ReconcileContext ctx, ResourceId ignoredTableId) {
        return Set.of();
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
      public boolean statsCapturedForColumnSelectors(
          ReconcileContext ctx, ResourceId ignoredTableId, long snapshotId, Set<String> selectors) {
        return false;
      }

      @Override
      public void putTargetStats(ReconcileContext ctx, List<TargetStatsRecord> stats) {}
    }

    Backend backend = new Backend();
    service.backend = backend;
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
                return List.of(
                    new SnapshotBundle(
                        301L,
                        0L,
                        Instant.now().toEpochMilli(),
                        "",
                        null,
                        0L,
                        null,
                        Map.of(),
                        0,
                        null));
              }
            };

    var result =
        reconcileTableTask(
            tableId,
            false,
            defaultCaptureScope(),
            ReconcilerService.CaptureMode.METADATA_AND_CAPTURE);

    assertThat(result.ok()).isTrue();
    assertThat(backend.updateTableCalls).isZero();
  }

  @Test
  void activeCaptureOnlyConnectorReconcileSkipsTableMutationAndSnapshotIngestion() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("table-active-capture-only")
            .setKind(ResourceKind.RK_TABLE)
            .build();

    class Backend extends DefaultBackend {
      int ensureTableCalls = 0;
      int updateTableCalls = 0;
      int ingestSnapshotCalls = 0;

      @Override
      public Connector lookupConnector(ReconcileContext ctx, ResourceId ignoredConnectorId) {
        Connector base = activeConnector();
        return base.toBuilder()
            .setSource(base.getSource().toBuilder().setTable("tbl"))
            .setDestination(base.getDestination().toBuilder().setTableId(tableId))
            .putProperties(
                ReconcilerService.CONNECTOR_MODE_PROPERTY,
                ReconcilerService.CONNECTOR_MODE_CAPTURE_ONLY)
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
      public Optional<DestinationTableMetadata> lookupDestinationTableMetadata(
          ReconcileContext ctx, ResourceId ignoredTableId) {
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
                connectorId));
      }

      @Override
      public ResourceId ensureTable(
          ReconcileContext ctx,
          ResourceId namespaceId,
          NameRef table,
          TableSpecDescriptor descriptor) {
        ensureTableCalls++;
        return tableId;
      }

      @Override
      public boolean updateTableById(
          ReconcileContext ctx,
          ResourceId ignoredTableId,
          ResourceId ignoredNamespaceId,
          NameRef ignoredTable,
          ReconcilerBackend.TableSpecDescriptor descriptor) {
        updateTableCalls++;
        return true;
      }

      @Override
      public Optional<Snapshot> fetchSnapshot(
          ReconcileContext ctx, ResourceId ignoredTableId, long snapshotId) {
        return Optional.empty();
      }

      @Override
      public Set<Long> existingSnapshotIds(ReconcileContext ctx, ResourceId ignoredTableId) {
        return Set.of();
      }

      @Override
      public void ingestSnapshot(
          ReconcileContext ctx, ResourceId ignoredTableId, Snapshot snapshot) {
        ingestSnapshotCalls++;
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
      public boolean statsCapturedForColumnSelectors(
          ReconcileContext ctx, ResourceId ignoredTableId, long snapshotId, Set<String> selectors) {
        return false;
      }

      @Override
      public void putTargetStats(ReconcileContext ctx, List<TargetStatsRecord> stats) {}

      @Override
      public void updateConnectorDestination(
          ReconcileContext ctx, ResourceId connectorId, DestinationTarget destination) {}
    }

    Backend backend = new Backend();
    service.backend = backend;
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
                return List.of(
                    new SnapshotBundle(
                        401L,
                        0L,
                        Instant.now().toEpochMilli(),
                        "",
                        null,
                        0L,
                        null,
                        Map.of(),
                        0,
                        null));
              }
            };

    var result =
        reconcileTableTask(
            tableId,
            false,
            defaultCaptureScope(),
            ReconcilerService.CaptureMode.METADATA_AND_CAPTURE);

    assertThat(result.ok()).isTrue();
    assertThat(backend.ensureTableCalls).isZero();
    assertThat(backend.updateTableCalls).isZero();
    assertThat(backend.ingestSnapshotCalls).isZero();
    assertThat(result.snapshotsProcessed).isZero();
  }

  @Test
  void captureOnlyConnectorDoesNotWriteSnapshotConstraints() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("table-capture-only-constraints")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    long createdAtMs = Instant.parse("2026-04-22T12:00:00Z").toEpochMilli();

    class Backend extends DefaultBackend {
      int putConstraintsCalls = 0;

      @Override
      public Connector lookupConnector(ReconcileContext ctx, ResourceId ignoredConnectorId) {
        Connector base = activeConnector();
        return base.toBuilder()
            .setSource(base.getSource().toBuilder().setTable("tbl"))
            .setDestination(base.getDestination().toBuilder().setTableId(tableId))
            .putProperties(
                ReconcilerService.CONNECTOR_MODE_PROPERTY,
                ReconcilerService.CONNECTOR_MODE_CAPTURE_ONLY)
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
      public Optional<DestinationTableMetadata> lookupDestinationTableMetadata(
          ReconcileContext ctx, ResourceId ignoredTableId) {
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
                connectorId));
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
        return Set.of(501L);
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
            new SnapshotBundle(501L, 0L, createdAtMs, "", null, 0L, null, Map.of(), 0, null));
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
                        .setName("pk_capture_only")
                        .setType(ConstraintType.CT_PRIMARY_KEY)
                        .build())
                .build());
      }
    }

    Backend backend = new Backend();
    service.backend = backend;
    service.connectorOpener = cfg -> new ConstraintsConnector();

    var result =
        reconcileTableTask(
            tableId,
            false,
            defaultCaptureScope(),
            ReconcilerService.CaptureMode.METADATA_AND_CAPTURE);

    assertThat(result.ok()).isTrue();
    assertThat(backend.putConstraintsCalls).isZero();
    assertThat(result.snapshotsProcessed).isZero();
  }

  @Test
  void reconcileDoesNotFetchSnapshotWhenSnapshotIdIsNotYetKnown() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("table-new-snapshot-no-fetch")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    long createdAtMs = Instant.parse("2026-04-22T12:00:00Z").toEpochMilli();

    class Backend extends DefaultBackend {
      int fetchSnapshotCalls = 0;
      int ingestSnapshotCalls = 0;

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
        fetchSnapshotCalls++;
        return Optional.empty();
      }

      @Override
      public void ingestSnapshot(
          ReconcileContext ctx, ResourceId ignoredTableId, Snapshot snapshot) {
        ingestSnapshotCalls++;
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

    class NewSnapshotConnector extends FakeConnector {
      NewSnapshotConnector() {
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
            new SnapshotBundle(201L, 0L, createdAtMs, "", null, 0L, null, Map.of(), 0, null));
      }
    }

    Backend backend = new Backend();
    service.backend = backend;
    service.connectorOpener = cfg -> new NewSnapshotConnector();

    var result =
        reconcileTableTask(
            tableId,
            false,
            defaultCaptureScope(),
            ReconcilerService.CaptureMode.METADATA_AND_CAPTURE);

    assertThat(result.ok()).isTrue();
    assertThat(backend.fetchSnapshotCalls).isZero();
    assertThat(backend.ingestSnapshotCalls).isEqualTo(1);
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
        return Set.of(201L);
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
            new SnapshotBundle(201L, 0L, createdAtMs, "", null, 0L, null, Map.of(), 0, null));
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

    var result =
        reconcileTableTask(
            tableId,
            false,
            defaultCaptureScope(),
            ReconcilerService.CaptureMode.METADATA_AND_CAPTURE);

    assertThat(result.ok()).isTrue();
    assertThat(backend.putConstraintsCalls).isEqualTo(1);
    assertThat(result.tablesChanged).isZero();
    assertThat(result.changed()).isZero();
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
      public Optional<DestinationTableMetadata> lookupDestinationTableMetadata(
          ReconcileContext ctx, ResourceId ignoredTableId) {
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
                connectorId));
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
                101L, 0L, Instant.now().toEpochMilli(), "", null, 0L, null, Map.of(), 0, null));
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

    var result =
        reconcileTableTask(
            tableId,
            false,
            defaultCaptureScope(),
            ReconcilerService.CaptureMode.METADATA_AND_CAPTURE);

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
                102L, 0L, Instant.now().toEpochMilli(), "", null, 0L, null, Map.of(), 0, null));
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

    var result =
        reconcileTableTask(
            tableId,
            false,
            defaultCaptureScope(),
            ReconcilerService.CaptureMode.METADATA_AND_CAPTURE);

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
                103L, 0L, Instant.now().toEpochMilli(), "", null, 0L, null, Map.of(), 0, null));
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

    var result =
        reconcileTableTask(
            tableId,
            false,
            defaultCaptureScope(),
            ReconcilerService.CaptureMode.METADATA_AND_CAPTURE);

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
                104L, 0L, Instant.now().toEpochMilli(), "", null, 0L, null, Map.of(), 0, null));
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

    var result =
        reconcileTableTask(
            tableId,
            false,
            defaultCaptureScope(),
            ReconcilerService.CaptureMode.METADATA_AND_CAPTURE);

    assertThat(result.ok()).isFalse();
    assertThat(result.errors).isEqualTo(1);
    assertThat(result.error).isNotNull();
    assertThat(result.error.getMessage()).contains("stats-captured-constraints-fail");
    assertThat(backend.putTargetStatsCalls).isZero();
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

  private TestResult reconcileTableTask(
      ResourceId tableId,
      boolean fullRescan,
      ReconcileScope scope,
      ReconcilerService.CaptureMode captureMode) {
    return reconcileQueuedTableExecution(
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

  private TestResult reconcileQueuedTableExecution(
      ai.floedb.floecat.common.rpc.PrincipalContext principal,
      ResourceId connectorId,
      boolean fullRescan,
      ReconcileScope scope,
      ReconcileTableTask tableTask,
      ReconcilerService.CaptureMode captureMode,
      String bearerToken,
      java.util.function.BooleanSupplier cancelRequested,
      ReconcileExecutor.ProgressListener progress) {
    var execution =
        queuedWorkerSupport()
            .executePlannedTable(
                principal,
                connectorId,
                fullRescan,
                scope,
                tableTask,
                captureMode,
                bearerToken,
                cancelRequested,
                progress)
            .result();
    return new TestResult(
        execution.cancelled,
        execution.tablesScanned,
        execution.tablesChanged,
        execution.viewsScanned,
        execution.viewsChanged,
        execution.errors,
        execution.snapshotsProcessed,
        execution.statsProcessed,
        execution.error);
  }

  private record TestResult(
      boolean cancelled,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed,
      Exception error) {
    private long changed() {
      return tablesChanged + viewsChanged;
    }

    private boolean degraded() {
      return false;
    }

    private boolean ok() {
      return !cancelled && error == null;
    }
  }

  private static ReconcileScope.ScopedCaptureRequest scopedCaptureRequest(
      String tableId, long snapshotId, String targetSpec, List<String> columnSelectors) {
    return new ReconcileScope.ScopedCaptureRequest(
        tableId, snapshotId, targetSpec, columnSelectors);
  }

  private static ReconcileScope defaultCaptureScope() {
    return ReconcileScope.of(
        List.of(),
        null,
        List.of(),
        ReconcileCapturePolicy.of(
            List.of(),
            Set.of(
                ReconcileCapturePolicy.Output.TABLE_STATS,
                ReconcileCapturePolicy.Output.FILE_STATS,
                ReconcileCapturePolicy.Output.COLUMN_STATS,
                ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX)));
  }
}
