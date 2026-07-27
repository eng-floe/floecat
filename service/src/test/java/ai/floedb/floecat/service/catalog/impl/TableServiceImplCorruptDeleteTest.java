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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.DeleteTableRequest;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.Precondition;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.spi.TopologyGraph;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.service.testsupport.TestNodes;
import ai.floedb.floecat.service.testsupport.TestPrincipals;
import ai.floedb.floecat.systemcatalog.util.TestCatalogOverlay;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * DeleteTable when the table's blob is missing or unparseable.
 *
 * <p>That path cannot read the table, so it deletes the canonical pointer directly, pinned to the
 * version a pointer-only read observed, and then purges the state the table owned: snapshots,
 * stats, the table root, and the root-resync pointer. Every one of those purges is irreversible, so
 * they may run only once the pinned delete has actually committed — a lost CAS means the pointer
 * moved and the table is still there.
 */
class TableServiceImplCorruptDeleteTest {

  private static final ResourceId TABLE_ID =
      ResourceId.newBuilder()
          .setAccountId(TestPrincipals.ACCOUNT_ID)
          .setKind(ResourceKind.RK_TABLE)
          .setId("tbl-corrupt")
          .build();
  private static final long SAFE_POINTER_VERSION = 4L;

  private TableServiceImpl svc;
  private TableRepository tableRepo;
  private RecursiveResourceDropper recursiveDropper;

  @BeforeEach
  void setUp() {
    svc = new TableServiceImpl();
    tableRepo = mock(TableRepository.class);
    recursiveDropper = mock(RecursiveResourceDropper.class);

    var principal = mock(PrincipalProvider.class);
    var authz = mock(Authorizer.class);
    TestPrincipals.stubPrincipal(principal, authz);

    svc.tableRepo = tableRepo;
    svc.principal = principal;
    svc.authz = authz;
    // An ordinary user table, so the surface write policy permits the delete and a caller-supplied
    // precondition still resolves the table rather than reporting NOT_FOUND.
    var overlay = new TestCatalogOverlay();
    overlay.addNode(TestNodes.tableNode(TABLE_ID, "{}"));
    svc.overlay = overlay;
    svc.topology = mock(TopologyGraph.class);
    svc.metadataGraph = mock(UserGraph.class);
    svc.recursiveDropper = recursiveDropper;

    // The blob is gone, so neither the table nor its meta can be read.
    when(tableRepo.getById(eq(TABLE_ID)))
        .thenThrow(new BaseResourceRepository.CorruptionException("dangling pointer", null));
    when(tableRepo.metaFor(eq(TABLE_ID)))
        .thenThrow(new BaseResourceRepository.CorruptionException("dangling pointer", null));
    when(tableRepo.metaForSafe(eq(TABLE_ID)))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(SAFE_POINTER_VERSION).build());
  }

  @Test
  void lostCasWithNoPreconditionDoesNotPurgeTheStateOfATableThatStillExists() {
    // A concurrent writer advanced the table's pointer, so the pinned delete loses.
    when(tableRepo.deleteWithPrecondition(eq(TABLE_ID), eq(SAFE_POINTER_VERSION)))
        .thenReturn(false);

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                svc.deleteTable(DeleteTableRequest.newBuilder().setTableId(TABLE_ID).build())
                    .await()
                    .indefinitely());

    // Retryable, not a silent success: the caller must not read this as "the table is gone".
    assertEquals(Status.Code.ABORTED, ex.getStatus().getCode());
    verify(recursiveDropper, never()).cleanupDeletedTable(any(), any());
  }

  @Test
  void lostCasWithAPreconditionStillReportsTheVersionMismatch() {
    when(tableRepo.deleteWithPrecondition(eq(TABLE_ID), anyLong())).thenReturn(false);

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                svc.deleteTable(
                        DeleteTableRequest.newBuilder()
                            .setTableId(TABLE_ID)
                            .setPrecondition(
                                Precondition.newBuilder()
                                    .setExpectedVersion(SAFE_POINTER_VERSION)
                                    .build())
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.FAILED_PRECONDITION, ex.getStatus().getCode());
    verify(recursiveDropper, never()).cleanupDeletedTable(any(), any());
  }

  @Test
  void committedDeleteStillPurgesTheStateTheTableOwned() {
    when(tableRepo.deleteWithPrecondition(eq(TABLE_ID), eq(SAFE_POINTER_VERSION))).thenReturn(true);

    var response =
        svc.deleteTable(DeleteTableRequest.newBuilder().setTableId(TABLE_ID).build())
            .await()
            .indefinitely();

    assertEquals(SAFE_POINTER_VERSION, response.getMeta().getPointerVersion());
    // The table blob was unreadable, so the enclosing namespace is unknown and no marker is bumped.
    verify(recursiveDropper).cleanupDeletedTable(eq(TABLE_ID), eq(null));
  }
}
