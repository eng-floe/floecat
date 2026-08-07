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
package ai.floedb.floecat.service.catalog.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.catalog.rpc.UpdateViewRequest;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.catalog.rpc.ViewSpec;
import ai.floedb.floecat.catalog.rpc.ViewSqlDefinition;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.Precondition;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineHintKey;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.spi.TopologyGraph;
import ai.floedb.floecat.service.catalog.hint.EngineHintSchemaCleaner;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.service.testsupport.TestPrincipals;
import ai.floedb.floecat.systemcatalog.util.TestCatalogOverlay;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A rename that releases a stranded name and then loses the canonical CAS.
 *
 * <p>Two different things make the retry's {@code update} report failure, and only one of them is
 * about the name. A live relation holding it is a real collision. A lost CAS is an ordinary version
 * race with a concurrent writer — the name was released a moment earlier — and the path that never
 * reclaims answers that with a retryable conflict, which {@code runWithRetry} resolves on the next
 * attempt. Collapsing both into one boolean answered the race with a terminal ALREADY_EXISTS, so
 * the same storage state got two different answers depending on whether a stranded row was in the
 * way.
 */
class RenameReclaimRetryTest {

  private static final ResourceId CATALOG =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setKind(ResourceKind.RK_CATALOG)
          .setId("cat_reclaim")
          .build();
  private static final ResourceId NAMESPACE =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setKind(ResourceKind.RK_NAMESPACE)
          .setId("ns_reclaim")
          .build();
  private static final ResourceId TABLE =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setKind(ResourceKind.RK_TABLE)
          .setId("tbl_reclaim")
          .build();
  private static final ResourceId VIEW =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setKind(ResourceKind.RK_VIEW)
          .setId("view_reclaim")
          .build();

  private TableServiceImpl tableSvc;
  private ViewServiceImpl viewSvc;
  private TableRepository tableRepo;
  private ViewRepository viewRepo;
  private RecursiveResourceDropper dropper;
  private TestCatalogOverlay overlay;

  @BeforeEach
  void setUp() {
    overlay = new TestCatalogOverlay();
    overlay.addNode(
        new NamespaceNode(
            NAMESPACE,
            "blob://test/v1",
            CATALOG,
            List.of(),
            "public",
            GraphNodeOrigin.USER,
            Map.of(),
            Map.of()));

    tableRepo = mock(TableRepository.class);
    viewRepo = mock(ViewRepository.class);
    dropper = mock(RecursiveResourceDropper.class);

    tableSvc = new TableServiceImpl();
    tableSvc.tableRepo = tableRepo;
    tableSvc.principal = mock(PrincipalProvider.class);
    tableSvc.authz = mock(Authorizer.class);
    tableSvc.overlay = overlay;
    tableSvc.hintCleaner = mock(EngineHintSchemaCleaner.class);
    tableSvc.topology = mock(TopologyGraph.class);
    tableSvc.metadataGraph = mock(UserGraph.class);
    tableSvc.recursiveDropper = dropper;
    TestPrincipals.stubPrincipal(tableSvc.principal, tableSvc.authz);
    when(tableSvc.hintCleaner.shouldClearHints(any())).thenReturn(false);

    viewSvc = new ViewServiceImpl();
    viewSvc.viewRepo = viewRepo;
    viewSvc.principal = mock(PrincipalProvider.class);
    viewSvc.authz = mock(Authorizer.class);
    viewSvc.overlay = overlay;
    viewSvc.hintCleaner = mock(EngineHintSchemaCleaner.class);
    viewSvc.topology = mock(TopologyGraph.class);
    viewSvc.metadataGraph = mock(UserGraph.class);
    viewSvc.recursiveDropper = dropper;
    TestPrincipals.stubPrincipal(viewSvc.principal, viewSvc.authz);

    // The name was genuinely stranded, and the reclaim released it.
    when(dropper.relationNameHeld(any(), anyString())).thenReturn(false);
    when(dropper.reclaimStrandedRelationNames(any(), any())).thenReturn(1);
  }

  private Table table() {
    return Table.newBuilder()
        .setResourceId(TABLE)
        .setCatalogId(CATALOG)
        .setNamespaceId(NAMESPACE)
        .setDisplayName("orders")
        .setSchemaJson("{}")
        .build();
  }

  private View view() {
    return View.newBuilder()
        .setResourceId(VIEW)
        .setCatalogId(CATALOG)
        .setNamespaceId(NAMESPACE)
        .setDisplayName("orders_v")
        .addSqlDefinitions(ViewSqlDefinition.newBuilder().setSql("select 1").setDialect("sql"))
        .build();
  }

  private UpdateTableRequest renameTable() {
    return UpdateTableRequest.newBuilder()
        .setTableId(TABLE)
        .setSpec(TableSpec.newBuilder().setDisplayName("renamed").build())
        .setUpdateMask(FieldMask.newBuilder().addPaths("display_name").build())
        .build();
  }

  private UpdateViewRequest renameView() {
    return UpdateViewRequest.newBuilder()
        .setViewId(VIEW)
        .setSpec(ViewSpec.newBuilder().setDisplayName("renamed_v").build())
        .setUpdateMask(FieldMask.newBuilder().addPaths("display_name").build())
        .build();
  }

  /**
   * The reclaim succeeds, the retry loses the CAS, and the RPC-level retry then commits — the same
   * convergence {@code updateTable_withoutPrecondition_retriesConcurrentMutation} asserts for a
   * plain lost CAS. Before the fix this answered TABLE_ALREADY_EXISTS and never retried.
   */
  @Test
  void aTableRenameThatLosesTheCasAfterReclaimingRetriesRatherThanReportingAlreadyExists() {
    overlay.addNode(userTableNode());
    when(tableRepo.getById(TABLE)).thenReturn(Optional.of(table()));
    when(tableRepo.metaFor(TABLE))
        .thenReturn(
            MutationMeta.newBuilder().setPointerVersion(7L).build(),
            MutationMeta.newBuilder().setPointerVersion(8L).build());
    when(tableRepo.metaForSafe(TABLE))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(9L).build());
    // First attempt: the shared claim collides. The reclaim releases it, and the retry then loses
    // the canonical CAS to a concurrent writer. Second RPC attempt: clean commit.
    when(tableRepo.update(any(Table.class), anyLong(), any()))
        .thenThrow(new BaseResourceRepository.NameConflictException("claim held"))
        .thenReturn(false)
        .thenReturn(true);

    tableSvc.updateTable(renameTable()).await().indefinitely();

    // Three calls: the collision, the post-reclaim attempt that lost, and the retry that committed.
    verify(tableRepo, times(3)).update(any(Table.class), anyLong(), any());
  }

  /**
   * With a precondition the caller owns the conflict, so it surfaces rather than being retried —
   * but as a precondition failure about versions, never as a name collision.
   */
  @Test
  void aTableRenameThatLosesTheCasAfterReclaimingReportsAVersionConflictNotANameOne() {
    overlay.addNode(userTableNode());
    when(tableRepo.getById(TABLE)).thenReturn(Optional.of(table()));
    when(tableRepo.metaFor(TABLE))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(7L).setEtag("e7").build());
    when(tableRepo.metaForSafe(TABLE))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(9L).build());
    when(tableRepo.update(any(Table.class), anyLong(), any()))
        .thenThrow(new BaseResourceRepository.NameConflictException("claim held"))
        .thenReturn(false);

    var withPrecondition =
        renameTable().toBuilder()
            .setPrecondition(
                Precondition.newBuilder().setExpectedVersion(7L).setExpectedEtag("e7").build())
            .build();

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () -> tableSvc.updateTable(withPrecondition).await().indefinitely());

    assertEquals(Status.Code.FAILED_PRECONDITION, ex.getStatus().getCode());
    assertNotEquals(
        Status.Code.ALREADY_EXISTS,
        ex.getStatus().getCode(),
        "a version race is not a name collision");
  }

  /** The view side carries the same reclaim-retry, so it gets the same assertion. */
  @Test
  void aViewRenameThatLosesTheCasAfterReclaimingReportsAVersionConflictNotANameOne() {
    overlay.addNode(userViewNode());
    when(viewRepo.getById(VIEW)).thenReturn(Optional.of(view()));
    when(viewRepo.metaFor(VIEW))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(4L).setEtag("e4").build());
    when(viewRepo.metaForSafe(VIEW))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(6L).build());
    when(viewRepo.update(any(View.class), anyLong(), any()))
        .thenThrow(new BaseResourceRepository.NameConflictException("claim held"))
        .thenReturn(false);

    var withPrecondition =
        renameView().toBuilder()
            .setPrecondition(
                Precondition.newBuilder().setExpectedVersion(4L).setExpectedEtag("e4").build())
            .build();

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () -> viewSvc.updateView(withPrecondition).await().indefinitely());

    assertEquals(Status.Code.FAILED_PRECONDITION, ex.getStatus().getCode());
    assertNotEquals(
        Status.Code.ALREADY_EXISTS,
        ex.getStatus().getCode(),
        "a version race is not a name collision");
  }

  private UserTableNode userTableNode() {
    return new UserTableNode(
        TABLE,
        "blob://test/tbl/v1",
        CATALOG,
        NAMESPACE,
        "orders",
        TableFormat.TF_ICEBERG,
        ColumnIdAlgorithm.CID_FIELD_ID,
        "{}",
        Map.of(),
        List.of(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        List.of(),
        Map.of(),
        Map.<Long, Map<EngineHintKey, EngineHint>>of());
  }

  private ViewNode userViewNode() {
    return new ViewNode(
        VIEW,
        "blob://test/view/v1",
        CATALOG,
        NAMESPACE,
        "orders_v",
        "select 1",
        "sql",
        List.<SchemaColumn>of(),
        List.of(),
        List.of(),
        GraphNodeOrigin.USER,
        Map.of(),
        Optional.empty(),
        Map.of(),
        Map.<EngineHintKey, EngineHint>of());
  }
}
