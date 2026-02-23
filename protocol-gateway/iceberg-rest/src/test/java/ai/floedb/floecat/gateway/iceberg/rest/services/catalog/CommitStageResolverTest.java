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

package ai.floedb.floecat.gateway.iceberg.rest.services.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergErrorResponse;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.StageCommitProcessor.StageCommitResult;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.StageMaterializationService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.TableCommitService;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CommitStageResolverTest {
  private final CommitStageResolver resolver = new CommitStageResolver();
  private final TableLifecycleService tableLifecycleService = mock(TableLifecycleService.class);
  private final StageMaterializationService stageMaterializationService =
      mock(StageMaterializationService.class);

  @BeforeEach
  void setUp() {
    resolver.tableLifecycleService = tableLifecycleService;
    resolver.stageMaterializationService = stageMaterializationService;
  }

  @Test
  void resolveReturnsTableIdWhenNoMaterializationOccurs() {
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    when(tableLifecycleService.resolveTableId("cat", List.of("db"), "orders")).thenReturn(tableId);
    when(stageMaterializationService.materializeExplicitStage(
            "pref", "cat", List.of("db"), "orders", null, null))
        .thenReturn(null);

    var resolution = resolver.resolve(command(null, null));

    assertEquals(tableId, resolution.tableId());
    assertNull(resolution.stageCommitResult());
    assertNull(resolution.materializedStageId());
    assertNull(resolution.error());
    assertNull(resolution.stagedTable());
  }

  @Test
  void resolvePrefersExplicitStageMaterializationResult() {
    ResourceId originalTableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    ResourceId stagedTableId = ResourceId.newBuilder().setId("cat:db:orders-staged").build();
    Table stagedTable = Table.newBuilder().setResourceId(stagedTableId).build();
    StageCommitResult stageResult = new StageCommitResult(stagedTable, null);
    StageMaterializationService.StageMaterializationResult materialized =
        new StageMaterializationService.StageMaterializationResult("stage-42", stageResult);

    when(tableLifecycleService.resolveTableId("cat", List.of("db"), "orders"))
        .thenReturn(originalTableId);
    when(stageMaterializationService.materializeExplicitStage(
            "pref", "cat", List.of("db"), "orders", "stage-42", "txn-1"))
        .thenReturn(materialized);

    var resolution = resolver.resolve(command("stage-42", "txn-1"));

    assertEquals(stagedTableId, resolution.tableId());
    assertSame(stageResult, resolution.stageCommitResult());
    assertEquals("stage-42", resolution.materializedStageId());
    assertSame(stagedTable, resolution.stagedTable());
  }

  @Test
  void resolveReturnsFailureResponseWhenStageMaterializationThrowsStageCommitException() {
    StatusRuntimeException notFound = Status.NOT_FOUND.asRuntimeException();
    when(tableLifecycleService.resolveTableId("cat", List.of("db"), "orders")).thenThrow(notFound);
    when(stageMaterializationService.materializeIfTableMissing(
            any(), any(), any(), any(), any(), any(), any()))
        .thenThrow(StageCommitException.validation("bad stage"));

    var resolution = resolver.resolve(command(null, null));

    assertNotNull(resolution.error());
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resolution.error().getStatus());
    IcebergErrorResponse entity = (IcebergErrorResponse) resolution.error().getEntity();
    assertEquals("bad stage", entity.error().message());
    assertEquals("ValidationException", entity.error().type());
  }

  @Test
  void resolveMapsNotFoundToNoSuchTableWhenNoMaterializationAvailable() {
    when(tableLifecycleService.resolveTableId("cat", List.of("db"), "orders"))
        .thenThrow(Status.NOT_FOUND.asRuntimeException());
    when(stageMaterializationService.materializeIfTableMissing(
            any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(null);

    var resolution = resolver.resolve(command(null, null));

    assertNotNull(resolution.error());
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resolution.error().getStatus());
    IcebergErrorResponse entity = (IcebergErrorResponse) resolution.error().getEntity();
    assertEquals("NoSuchTableException", entity.error().type());
    assertEquals("Table db.orders not found", entity.error().message());
  }

  @Test
  void resolveRethrowsNonNotFoundResolutionExceptions() {
    StatusRuntimeException unavailable = Status.UNAVAILABLE.asRuntimeException();
    when(tableLifecycleService.resolveTableId("cat", List.of("db"), "orders"))
        .thenThrow(unavailable);
    when(stageMaterializationService.materializeIfTableMissing(
            any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(null);

    StatusRuntimeException thrown =
        assertThrows(StatusRuntimeException.class, () -> resolver.resolve(command(null, null)));
    assertSame(unavailable, thrown);
  }

  @Test
  void resolveUsesMaterializedResultWhenTableMissing() {
    ResourceId stagedTableId = ResourceId.newBuilder().setId("cat:db:orders-staged").build();
    Table stagedTable = Table.newBuilder().setResourceId(stagedTableId).build();
    StageCommitResult stageResult = new StageCommitResult(stagedTable, null);
    StageMaterializationService.StageMaterializationResult materialized =
        new StageMaterializationService.StageMaterializationResult("stage-9", stageResult);
    when(tableLifecycleService.resolveTableId("cat", List.of("db"), "orders"))
        .thenThrow(Status.NOT_FOUND.asRuntimeException());
    when(stageMaterializationService.materializeIfTableMissing(
            any(), eq("pref"), eq("cat"), eq(List.of("db")), eq("orders"), any(), any()))
        .thenReturn(materialized);

    var resolution = resolver.resolve(command(null, null));

    assertEquals(stagedTableId, resolution.tableId());
    assertSame(stageResult, resolution.stageCommitResult());
    assertEquals("stage-9", resolution.materializedStageId());
    verify(stageMaterializationService)
        .materializeIfTableMissing(any(), eq("pref"), eq("cat"), eq(List.of("db")), eq("orders"), any(), any());
  }

  @Test
  void stageResolutionHasErrorReflectsErrorPresence() {
    Response error = Response.status(Response.Status.CONFLICT).entity("boom").build();
    CommitStageResolver.StageResolution failure =
        new CommitStageResolver.StageResolution(null, null, null, error);
    CommitStageResolver.StageResolution success =
        new CommitStageResolver.StageResolution(
            ResourceId.newBuilder().setId("t").build(), null, null, null);

    assertEquals(true, failure.hasError());
    assertEquals(false, success.hasError());
  }

  private TableCommitService.CommitCommand command(String stageId, String transactionId) {
    return new TableCommitService.CommitCommand(
        "pref",
        "db",
        List.of("db"),
        "orders",
        "cat",
        ResourceId.newBuilder().setId("cat").build(),
        ResourceId.newBuilder().setId("cat:db").build(),
        "idem",
        stageId,
        transactionId,
        new TableRequests.Commit(List.of(), List.of()),
        null);
  }
}
