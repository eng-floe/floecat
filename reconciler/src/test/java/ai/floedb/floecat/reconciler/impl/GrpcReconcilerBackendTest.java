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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.catalog.rpc.ConstraintType;
import ai.floedb.floecat.catalog.rpc.FileContent;
import ai.floedb.floecat.catalog.rpc.ForeignKeyActionRule;
import ai.floedb.floecat.catalog.rpc.ForeignKeyMatchOption;
import ai.floedb.floecat.catalog.rpc.GetNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.GetTableResponse;
import ai.floedb.floecat.catalog.rpc.GetViewResponse;
import ai.floedb.floecat.catalog.rpc.ListTargetStatsRequest;
import ai.floedb.floecat.catalog.rpc.LookupCatalogResponse;
import ai.floedb.floecat.catalog.rpc.PutTableConstraintsRequest;
import ai.floedb.floecat.catalog.rpc.ResolveViewResponse;
import ai.floedb.floecat.catalog.rpc.SnapshotConstraints;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.catalog.rpc.UpdateViewRequest;
import ai.floedb.floecat.catalog.rpc.UpdateViewResponse;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.catalog.rpc.ViewSpec;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.rpc.ConnectorsGrpc;
import ai.floedb.floecat.connector.rpc.GetConnectorResponse;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngine;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineCapabilities;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineRegistry;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineRequest;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineResult;
import ai.floedb.floecat.reconciler.spi.capture.PlannedFileGroupCaptureRequest;
import io.grpc.Status;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class GrpcReconcilerBackendTest {
  @Test
  void withBearerPrefixLeavesExistingBearer() {
    String token = GrpcReconcilerBackend.withBearerPrefix("Bearer abc123");
    assertThat(token).isEqualTo("Bearer abc123");
  }

  @Test
  void withBearerPrefixAddsPrefixWhenMissing() {
    String token = GrpcReconcilerBackend.withBearerPrefix("abc123");
    assertThat(token).isEqualTo("Bearer abc123");
  }

  @Test
  void snapshotConstraintsIdempotencyKeyIsStableForSamePayload() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("users")
            .build();
    SnapshotConstraints constraints =
        SnapshotConstraints.newBuilder()
            .addConstraints(
                ConstraintDefinition.newBuilder()
                    .setName("pk_users")
                    .setType(ConstraintType.CT_PRIMARY_KEY)
                    .build())
            .build();

    PutTableConstraintsRequest request1 =
        GrpcReconcilerBackend.buildPutTableConstraintsRequest(tableId, 42L, constraints);
    PutTableConstraintsRequest request2 =
        GrpcReconcilerBackend.buildPutTableConstraintsRequest(tableId, 42L, constraints);

    assertThat(request1.getIdempotency().getKey()).isEqualTo(request2.getIdempotency().getKey());
  }

  @Test
  void snapshotConstraintsIdempotencyKeyChangesWhenPayloadChanges() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("users")
            .build();
    SnapshotConstraints first =
        SnapshotConstraints.newBuilder()
            .addConstraints(
                ConstraintDefinition.newBuilder()
                    .setName("pk_users")
                    .setType(ConstraintType.CT_PRIMARY_KEY)
                    .build())
            .build();
    SnapshotConstraints second =
        SnapshotConstraints.newBuilder()
            .addConstraints(
                ConstraintDefinition.newBuilder()
                    .setName("pk_users_alt")
                    .setType(ConstraintType.CT_PRIMARY_KEY)
                    .build())
            .build();

    PutTableConstraintsRequest request1 =
        GrpcReconcilerBackend.buildPutTableConstraintsRequest(tableId, 42L, first);
    PutTableConstraintsRequest request2 =
        GrpcReconcilerBackend.buildPutTableConstraintsRequest(tableId, 42L, second);

    assertThat(request1.getIdempotency().getKey()).isNotEqualTo(request2.getIdempotency().getKey());
  }

  @Test
  void buildPutTableConstraintsRequestPopulatesAllFieldsAndStableIdempotencyKey() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("users")
            .build();
    SnapshotConstraints snapshotConstraints =
        SnapshotConstraints.newBuilder()
            .addConstraints(
                ConstraintDefinition.newBuilder()
                    .setName("pk_users")
                    .setType(ConstraintType.CT_PRIMARY_KEY)
                    .build())
            .build();

    PutTableConstraintsRequest request =
        GrpcReconcilerBackend.buildPutTableConstraintsRequest(tableId, 42L, snapshotConstraints);
    PutTableConstraintsRequest request2 =
        GrpcReconcilerBackend.buildPutTableConstraintsRequest(tableId, 42L, snapshotConstraints);

    assertThat(request.getTableId()).isEqualTo(tableId);
    assertThat(request.getSnapshotId()).isEqualTo(42L);
    assertThat(request.getConstraints()).isEqualTo(snapshotConstraints);
    assertThat(request.getIdempotency().getKey()).isNotBlank();
    assertThat(request2.getIdempotency().getKey()).isEqualTo(request.getIdempotency().getKey());
  }

  @Test
  void buildPutTableConstraintsRequestPreservesForeignKeyMetadataFields() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("orders")
            .build();
    ConstraintDefinition fk =
        ConstraintDefinition.newBuilder()
            .setName("fk_orders_customers")
            .setType(ConstraintType.CT_FOREIGN_KEY)
            .setReferencedTable(NameRef.newBuilder().addPath("sales").setName("customers").build())
            .setReferencedConstraintName("pk_customers")
            .setMatchOption(ForeignKeyMatchOption.FK_MATCH_OPTION_FULL)
            .setUpdateRule(ForeignKeyActionRule.FK_ACTION_RULE_CASCADE)
            .setDeleteRule(ForeignKeyActionRule.FK_ACTION_RULE_RESTRICT)
            .build();
    SnapshotConstraints snapshotConstraints =
        SnapshotConstraints.newBuilder().addConstraints(fk).build();

    PutTableConstraintsRequest request =
        GrpcReconcilerBackend.buildPutTableConstraintsRequest(tableId, 77L, snapshotConstraints);
    ConstraintDefinition emitted = request.getConstraints().getConstraints(0);

    assertThat(emitted.getReferencedConstraintName()).isEqualTo("pk_customers");
    assertThat(emitted.getMatchOption()).isEqualTo(ForeignKeyMatchOption.FK_MATCH_OPTION_FULL);
    assertThat(emitted.getUpdateRule()).isEqualTo(ForeignKeyActionRule.FK_ACTION_RULE_CASCADE);
    assertThat(emitted.getDeleteRule()).isEqualTo(ForeignKeyActionRule.FK_ACTION_RULE_RESTRICT);
  }

  @Test
  void buildPutTableConstraintsRequestLeavesForeignKeyRulesUnspecifiedWhenNotProvided() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("orders")
            .build();
    ConstraintDefinition fk =
        ConstraintDefinition.newBuilder()
            .setName("fk_orders_customers_default")
            .setType(ConstraintType.CT_FOREIGN_KEY)
            .setReferencedTable(NameRef.newBuilder().addPath("sales").setName("customers").build())
            .build();
    SnapshotConstraints snapshotConstraints =
        SnapshotConstraints.newBuilder().addConstraints(fk).build();

    PutTableConstraintsRequest request =
        GrpcReconcilerBackend.buildPutTableConstraintsRequest(tableId, 78L, snapshotConstraints);
    ConstraintDefinition emitted = request.getConstraints().getConstraints(0);

    assertThat(emitted.getMatchOption())
        .isEqualTo(ForeignKeyMatchOption.FK_MATCH_OPTION_UNSPECIFIED);
    assertThat(emitted.getUpdateRule()).isEqualTo(ForeignKeyActionRule.FK_ACTION_RULE_UNSPECIFIED);
    assertThat(emitted.getDeleteRule()).isEqualTo(ForeignKeyActionRule.FK_ACTION_RULE_UNSPECIFIED);
  }

  @Test
  void buildStatsAlreadyCapturedRequestIncludesTableId() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("orders")
            .build();

    ListTargetStatsRequest request =
        GrpcReconcilerBackend.buildStatsAlreadyCapturedRequest(tableId, 99L);

    assertThat(request.getTableId()).isEqualTo(tableId);
    assertThat(request.getSnapshot().getSnapshotId()).isEqualTo(99L);
    assertThat(request.getPage().getPageSize()).isEqualTo(1);
  }

  @Test
  void statsCapturedForColumnSelectorsRejectsMalformedIdSelectorsWithoutGrpcCall() {
    GrpcReconcilerBackend backend =
        new GrpcReconcilerBackend(
            Optional.<String>empty(), Optional.<String>empty(), Optional.<Duration>empty());
    backend.statistics = mock(TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub.class);
    ReconcileContext ctx =
        new ReconcileContext(
            "corr",
            PrincipalContext.newBuilder().setAccountId("acct").setCorrelationId("corr").build(),
            "svc",
            Instant.now(),
            Optional.empty());
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("orders")
            .build();

    boolean captured =
        backend.statsCapturedForColumnSelectors(ctx, tableId, 42L, Set.of("#bad", "amount"));

    assertThat(captured).isFalse();
    verifyNoInteractions(backend.statistics);
  }

  @Test
  void fetchSnapshotFilePlanUsesSourceConnectorFromUpstreamMetadata() throws Exception {
    GrpcReconcilerBackend backend =
        new GrpcReconcilerBackend(
            Optional.<String>empty(), Optional.<String>empty(), Optional.<Duration>empty());
    backend.table =
        mock(ai.floedb.floecat.catalog.rpc.TableServiceGrpc.TableServiceBlockingStub.class);
    backend.connector = mock(ConnectorsGrpc.ConnectorsBlockingStub.class);
    when(backend.table.withInterceptors(any())).thenReturn(backend.table);
    when(backend.connector.withInterceptors(any())).thenReturn(backend.connector);

    ResourceId connectorId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_CONNECTOR)
            .setId("conn-1")
            .build();
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("tbl-1")
            .build();
    Table table =
        Table.newBuilder()
            .setResourceId(tableId)
            .setUpstream(
                UpstreamRef.newBuilder()
                    .setConnectorId(connectorId)
                    .addNamespacePath("main")
                    .addNamespacePath("sales")
                    .setTableDisplayName("orders")
                    .build())
            .build();
    when(backend.table.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(table).build());

    Connector connector =
        Connector.newBuilder().setResourceId(connectorId).setKind(ConnectorKind.CK_DELTA).build();
    when(backend.connector.getConnector(any()))
        .thenReturn(GetConnectorResponse.newBuilder().setConnector(connector).build());

    FloecatConnector source = mock(FloecatConnector.class);
    FloecatConnector.SnapshotFilePlan plan =
        new FloecatConnector.SnapshotFilePlan(
            List.of(
                new FloecatConnector.SnapshotFileEntry(
                    "s3://bucket/path/file.parquet",
                    "parquet",
                    10L,
                    0L,
                    FileContent.FC_DATA,
                    "",
                    0,
                    List.of(),
                    null)),
            List.of());
    when(source.planSnapshotFiles(anyString(), anyString(), any(), anyLong()))
        .thenReturn(Optional.of(plan));
    backend.connectorOpener = cfg -> source;

    ReconcileContext ctx = reconcileContext();

    Optional<FloecatConnector.SnapshotFilePlan> result =
        backend.fetchSnapshotFilePlan(ctx, tableId, 44L);

    assertThat(result).contains(plan);
    verify(source).planSnapshotFiles("main.sales", "orders", tableId, 44L);
  }

  @Test
  void capturePlannedFileGroupStatsUsesSourceConnectorFromUpstreamMetadata() throws Exception {
    GrpcReconcilerBackend backend =
        new GrpcReconcilerBackend(
            Optional.<String>empty(), Optional.<String>empty(), Optional.<Duration>empty());
    backend.table =
        mock(ai.floedb.floecat.catalog.rpc.TableServiceGrpc.TableServiceBlockingStub.class);
    backend.connector = mock(ConnectorsGrpc.ConnectorsBlockingStub.class);
    when(backend.table.withInterceptors(any())).thenReturn(backend.table);
    when(backend.connector.withInterceptors(any())).thenReturn(backend.connector);

    ResourceId connectorId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_CONNECTOR)
            .setId("conn-1")
            .build();
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("tbl-1")
            .build();
    Table table =
        Table.newBuilder()
            .setResourceId(tableId)
            .setUpstream(
                UpstreamRef.newBuilder()
                    .setConnectorId(connectorId)
                    .addNamespacePath("main")
                    .addNamespacePath("sales")
                    .setTableDisplayName("orders")
                    .build())
            .build();
    when(backend.table.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(table).build());

    Connector connector =
        Connector.newBuilder().setResourceId(connectorId).setKind(ConnectorKind.CK_DELTA).build();
    when(backend.connector.getConnector(any()))
        .thenReturn(GetConnectorResponse.newBuilder().setConnector(connector).build());

    List<TargetStatsRecord> stats = List.of(TargetStatsRecord.getDefaultInstance());
    FloecatConnector source = mock(FloecatConnector.class);
    backend.connectorOpener = cfg -> source;
    CaptureEngine captureEngine =
        new CaptureEngine() {
          @Override
          public String id() {
            return "test";
          }

          @Override
          public int priority() {
            return 0;
          }

          @Override
          public CaptureEngineCapabilities capabilities() {
            return CaptureEngineCapabilities.of(Set.of(), false);
          }

          @Override
          public boolean supports(CaptureEngineRequest request) {
            return true;
          }

          @Override
          public Optional<CaptureEngineResult> capture(CaptureEngineRequest request) {
            return Optional.of(CaptureEngineResult.of(stats, List.of(), List.of()));
          }
        };
    backend.captureEngineRegistry = new CaptureEngineRegistry(List.of(captureEngine));

    CaptureEngineResult result =
        backend.capturePlannedFileGroup(
            reconcileContext(),
            PlannedFileGroupCaptureRequest.of(
                "plan-1",
                "group-1",
                tableId,
                44L,
                List.of("s3://bucket/path/file.parquet"),
                Set.of(),
                Set.of(),
                Set.of(ai.floedb.floecat.connector.spi.FloecatConnector.StatsTargetKind.FILE),
                false));

    assertThat(result.statsRecords()).isEqualTo(stats);
    verify(source).close();
  }

  @Test
  void ensureViewUpdatesExistingViewAfterAlreadyExistsConflict() {
    GrpcReconcilerBackend backend =
        new GrpcReconcilerBackend(
            Optional.<String>empty(), Optional.<String>empty(), Optional.<Duration>empty());
    backend.view =
        mock(ai.floedb.floecat.catalog.rpc.ViewServiceGrpc.ViewServiceBlockingStub.class);
    backend.directory =
        mock(ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc.DirectoryServiceBlockingStub.class);
    backend.namespace =
        mock(ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc.NamespaceServiceBlockingStub.class);
    when(backend.view.withInterceptors(any())).thenReturn(backend.view);
    when(backend.directory.withInterceptors(any())).thenReturn(backend.directory);
    when(backend.namespace.withInterceptors(any())).thenReturn(backend.namespace);

    ReconcileContext ctx =
        new ReconcileContext(
            "corr",
            PrincipalContext.newBuilder().setAccountId("acct").setCorrelationId("corr").build(),
            "svc",
            Instant.now(),
            Optional.empty());
    ResourceId catalogId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_CATALOG)
            .setId("cat")
            .build();
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("ns")
            .build();
    ResourceId viewId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_VIEW)
            .setId("view")
            .build();

    ViewSpec spec =
        ViewSpec.newBuilder()
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName("orders_view")
            .putProperties("comment", "after")
            .addSqlDefinitions(
                ai.floedb.floecat.catalog.rpc.ViewSqlDefinition.newBuilder()
                    .setSql("SELECT order_id FROM orders")
                    .setDialect("spark")
                    .build())
            .addCreationSearchPath("analytics")
            .addOutputColumns(
                ai.floedb.floecat.query.rpc.SchemaColumn.newBuilder()
                    .setName("order_id")
                    .setLogicalType("INT")
                    .setNullable(false)
                    .build())
            .build();

    View existing =
        View.newBuilder()
            .setResourceId(viewId)
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName("orders_view")
            .putProperties("comment", "before")
            .addSqlDefinitions(
                ai.floedb.floecat.catalog.rpc.ViewSqlDefinition.newBuilder()
                    .setSql("SELECT 1")
                    .setDialect("ansi")
                    .build())
            .build();
    View updated =
        existing.toBuilder()
            .clearProperties()
            .putAllProperties(spec.getPropertiesMap())
            .clearSqlDefinitions()
            .addAllSqlDefinitions(spec.getSqlDefinitionsList())
            .clearCreationSearchPath()
            .addAllCreationSearchPath(spec.getCreationSearchPathList())
            .clearOutputColumns()
            .addAllOutputColumns(spec.getOutputColumnsList())
            .build();

    when(backend.view.createView(any())).thenThrow(Status.ALREADY_EXISTS.asRuntimeException());
    when(backend.directory.lookupCatalog(any()))
        .thenReturn(LookupCatalogResponse.newBuilder().setDisplayName("dest_cat").build());
    when(backend.namespace.getNamespace(any()))
        .thenReturn(
            GetNamespaceResponse.newBuilder()
                .setNamespace(
                    ai.floedb.floecat.catalog.rpc.Namespace.newBuilder()
                        .setCatalogId(catalogId)
                        .setDisplayName("analytics")
                        .build())
                .build());
    when(backend.directory.resolveView(any()))
        .thenReturn(ResolveViewResponse.newBuilder().setResourceId(viewId).build());
    when(backend.view.getView(any()))
        .thenReturn(GetViewResponse.newBuilder().setView(existing).build());
    when(backend.view.updateView(any()))
        .thenReturn(UpdateViewResponse.newBuilder().setView(updated).build());

    var result = backend.ensureView(ctx, spec, "analytics.orders_view");

    assertThat(result.viewId()).isEqualTo(viewId);
    assertThat(result.changed()).isTrue();
    var updateCaptor = org.mockito.ArgumentCaptor.forClass(UpdateViewRequest.class);
    verify(backend.view).updateView(updateCaptor.capture());
    assertThat(updateCaptor.getValue().getUpdateMask().getPathsList())
        .containsExactlyInAnyOrder(
            "properties",
            "sql_definitions",
            "base_relations",
            "creation_search_path",
            "output_columns");
  }

  private static ReconcileContext reconcileContext() {
    return new ReconcileContext(
        "corr",
        PrincipalContext.newBuilder().setAccountId("acct").setCorrelationId("corr").build(),
        "svc",
        Instant.now(),
        Optional.empty());
  }
}
