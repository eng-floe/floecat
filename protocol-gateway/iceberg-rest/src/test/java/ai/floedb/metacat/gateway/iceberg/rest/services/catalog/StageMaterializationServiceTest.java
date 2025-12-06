package ai.floedb.metacat.gateway.iceberg.rest.services.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableSpec;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.gateway.iceberg.rest.api.dto.LoadTableResultDto;
import ai.floedb.metacat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.metacat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.metacat.gateway.iceberg.rest.services.catalog.StageCommitProcessor.StageCommitResult;
import ai.floedb.metacat.gateway.iceberg.rest.services.staging.StageState;
import ai.floedb.metacat.gateway.iceberg.rest.services.staging.StagedTableEntry;
import ai.floedb.metacat.gateway.iceberg.rest.services.staging.StagedTableKey;
import ai.floedb.metacat.gateway.iceberg.rest.services.staging.StagedTableService;
import ai.floedb.metacat.gateway.iceberg.rest.services.tenant.TenantContext;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StageMaterializationServiceTest {
  private final StageMaterializationService service = new StageMaterializationService();
  private final TenantContext tenantContext = mock(TenantContext.class);
  private final StagedTableService stagedTableService = mock(StagedTableService.class);
  private final StageCommitProcessor stageCommitProcessor = mock(StageCommitProcessor.class);

  @BeforeEach
  void setUp() {
    service.tenantContext = tenantContext;
    service.stagedTableService = stagedTableService;
    service.stageCommitProcessor = stageCommitProcessor;
  }

  @Test
  void materializeIfTableMissingSkipsNonNotFoundErrors() throws Exception {
    StatusRuntimeException failure = Status.PERMISSION_DENIED.asRuntimeException();

    StageMaterializationService.StageMaterializationResult result =
        service.materializeIfTableMissing(
            failure,
            "pref",
            "cat",
            List.of("db"),
            "orders",
            new TableRequests.Commit(null, null, null, null, null, null, null),
            null);

    assertNull(result);
    verify(stageCommitProcessor, never()).commitStage(any(), any(), any(), any(), any(), any());
  }

  @Test
  void materializeIfTableMissingUsesExplicitStageId() throws Exception {
    when(tenantContext.getTenantId()).thenReturn("tenant-1");
    Table table =
        Table.newBuilder().setResourceId(ResourceId.newBuilder().setId("cat:db:orders")).build();
    StageCommitResult stageResult =
        new StageCommitResult(
            table, new LoadTableResultDto(null, null, Map.of(), List.<StorageCredentialDto>of()));
    when(stageCommitProcessor.commitStage(any(), any(), any(), any(), any(), any()))
        .thenReturn(stageResult);

    StageMaterializationService.StageMaterializationResult result =
        service.materializeIfTableMissing(
            Status.NOT_FOUND.asRuntimeException(),
            "pref",
            "cat",
            List.of("db"),
            "orders",
            new TableRequests.Commit(null, null, null, null, "stage-1", null, null),
            "header-stage");

    assertNotNull(result);
    assertEquals("stage-1", result.stageId());
    verify(stageCommitProcessor)
        .commitStage("pref", "cat", "tenant-1", List.of("db"), "orders", "stage-1");
  }

  @Test
  void materializeIfTableMissingRequiresStageId() {
    when(tenantContext.getTenantId()).thenReturn("tenant-required");
    when(stagedTableService.findSingleStage("tenant-required", "cat", List.of("db"), "orders"))
        .thenReturn(Optional.empty());

    StageCommitException ex =
        assertThrows(
            StageCommitException.class,
            () ->
                service.materializeIfTableMissing(
                    Status.NOT_FOUND.asRuntimeException(),
                    "pref",
                    "cat",
                    List.of("db"),
                    "orders",
                    new TableRequests.Commit(null, null, null, null, null, null, null),
                    null));

    assertEquals(
        "stage-id is required when committing a staged create for db.orders", ex.getMessage());
    verify(stageCommitProcessor, never()).commitStage(any(), any(), any(), any(), any(), any());
  }

  @Test
  void materializeIfTableMissingPropagatesStageErrors() throws Exception {
    when(tenantContext.getTenantId()).thenReturn("tenant");
    when(stageCommitProcessor.commitStage(any(), any(), any(), any(), any(), any()))
        .thenThrow(StageCommitException.validation("bad stage"));

    try {
      service.materializeIfTableMissing(
          Status.NOT_FOUND.asRuntimeException(),
          "pref",
          "cat",
          List.of("db"),
          "orders",
          new TableRequests.Commit(null, null, null, null, "stage-1", null, null),
          null);
    } catch (StageCommitException expected) {
      assertEquals("bad stage", expected.getMessage());
    }
  }

  @Test
  void materializeIfTableMissingFallsBackWhenSingleStageExists() throws Exception {
    when(tenantContext.getTenantId()).thenReturn("tenant-2");
    Table table = Table.newBuilder().build();
    StageCommitResult stageResult =
        new StageCommitResult(
            table, new LoadTableResultDto(null, null, Map.of(), List.<StorageCredentialDto>of()));
    when(stageCommitProcessor.commitStage(any(), any(), any(), any(), any(), any()))
        .thenReturn(stageResult);
    StagedTableEntry entry =
        new StagedTableEntry(
            new StagedTableKey("tenant-2", "cat", List.of("db"), "orders", "only-stage"),
            ResourceId.newBuilder().setId("cat").build(),
            ResourceId.newBuilder().setId("cat:db").build(),
            new TableRequests.Create("orders", null, null, null, Map.of(), null, null, false),
            TableSpec.getDefaultInstance(),
            List.of(),
            StageState.STAGED,
            null,
            null,
            null);
    when(stagedTableService.findSingleStage("tenant-2", "cat", List.of("db"), "orders"))
        .thenReturn(Optional.of(entry));

    StageMaterializationService.StageMaterializationResult result =
        service.materializeIfTableMissing(
            Status.NOT_FOUND.asRuntimeException(),
            "pref",
            "cat",
            List.of("db"),
            "orders",
            new TableRequests.Commit(null, null, null, null, null, null, null),
            null);

    assertNotNull(result);
    assertEquals("only-stage", result.stageId());
    verify(stageCommitProcessor)
        .commitStage("pref", "cat", "tenant-2", List.of("db"), "orders", "only-stage");
  }

  @Test
  void materializeExplicitStageSkipsWhenNoStageIdAndNoFallback() throws Exception {
    StageMaterializationService.StageMaterializationResult result =
        service.materializeExplicitStage(
            "pref",
            "cat",
            List.of("db"),
            "orders",
            new TableRequests.Commit(null, null, null, null, null, null, null),
            null);

    assertNull(result);
    verify(stageCommitProcessor, never()).commitStage(any(), any(), any(), any(), any(), any());
  }

  @Test
  void materializeExplicitStageUsesProvidedStageId() throws Exception {
    when(tenantContext.getTenantId()).thenReturn("tenant-explicit");
    Table table =
        Table.newBuilder().setResourceId(ResourceId.newBuilder().setId("cat:db:orders")).build();
    StageCommitResult stageResult =
        new StageCommitResult(
            table, new LoadTableResultDto(null, null, Map.of(), List.<StorageCredentialDto>of()));
    when(stageCommitProcessor.commitStage(any(), any(), any(), any(), any(), any()))
        .thenReturn(stageResult);

    StageMaterializationService.StageMaterializationResult result =
        service.materializeExplicitStage(
            "pref",
            "cat",
            List.of("db"),
            "orders",
            new TableRequests.Commit(null, null, null, null, "explicit-stage", null, null),
            null);

    assertNotNull(result);
    assertEquals("explicit-stage", result.stageId());
    verify(stageCommitProcessor)
        .commitStage("pref", "cat", "tenant-explicit", List.of("db"), "orders", "explicit-stage");
  }

  @Test
  void materializeExplicitStageUsesSingleStageFallback() throws Exception {
    when(tenantContext.getTenantId()).thenReturn("tenant-fallback");
    StagedTableEntry entry =
        new StagedTableEntry(
            new StagedTableKey("tenant-fallback", "cat", List.of("db"), "orders", "only-stage"),
            ResourceId.newBuilder().setId("cat").build(),
            ResourceId.newBuilder().setId("cat:db").build(),
            new TableRequests.Create("orders", null, null, null, Map.of(), null, null, false),
            TableSpec.getDefaultInstance(),
            List.of(),
            StageState.STAGED,
            null,
            null,
            null);
    when(stagedTableService.findSingleStage("tenant-fallback", "cat", List.of("db"), "orders"))
        .thenReturn(Optional.of(entry));
    Table table = Table.newBuilder().build();
    StageCommitResult stageResult =
        new StageCommitResult(
            table, new LoadTableResultDto(null, null, Map.of(), List.<StorageCredentialDto>of()));
    when(stageCommitProcessor.commitStage(any(), any(), any(), any(), any(), any()))
        .thenReturn(stageResult);

    StageMaterializationService.StageMaterializationResult result =
        service.materializeExplicitStage(
            "pref",
            "cat",
            List.of("db"),
            "orders",
            new TableRequests.Commit(null, null, null, null, null, null, null),
            null);

    assertNotNull(result);
    assertEquals("only-stage", result.stageId());
    verify(stageCommitProcessor)
        .commitStage("pref", "cat", "tenant-fallback", List.of("db"), "orders", "only-stage");
  }

  @Test
  void materializeTransactionStageUsesProvidedStageId() throws Exception {
    when(tenantContext.getTenantId()).thenReturn("tenant-3");
    Table table =
        Table.newBuilder().setResourceId(ResourceId.newBuilder().setId("cat:db:orders")).build();
    StageCommitResult stageResult =
        new StageCommitResult(
            table, new LoadTableResultDto(null, null, Map.of(), List.<StorageCredentialDto>of()));
    when(stageCommitProcessor.commitStage(any(), any(), any(), any(), any(), any()))
        .thenReturn(stageResult);

    StageMaterializationService.StageMaterializationResult result =
        service.materializeTransactionStage("pref", "cat", List.of("db"), "orders", "txn-stage");

    assertNotNull(result);
    assertEquals("txn-stage", result.stageId());
    verify(stageCommitProcessor)
        .commitStage("pref", "cat", "tenant-3", List.of("db"), "orders", "txn-stage");
  }

  @Test
  void materializeTransactionStageFallsBackToSingleStage() throws Exception {
    when(tenantContext.getTenantId()).thenReturn("tenant-4");
    StagedTableEntry entry =
        new StagedTableEntry(
            new StagedTableKey("tenant-4", "cat", List.of("db"), "orders", "txn-only-stage"),
            ResourceId.newBuilder().setId("cat").build(),
            ResourceId.newBuilder().setId("cat:db").build(),
            new TableRequests.Create("orders", null, null, null, Map.of(), null, null, false),
            TableSpec.getDefaultInstance(),
            List.of(),
            StageState.STAGED,
            null,
            null,
            null);
    when(stagedTableService.findSingleStage("tenant-4", "cat", List.of("db"), "orders"))
        .thenReturn(Optional.of(entry));
    Table table = Table.newBuilder().build();
    StageCommitResult stageResult =
        new StageCommitResult(
            table, new LoadTableResultDto(null, null, Map.of(), List.<StorageCredentialDto>of()));
    when(stageCommitProcessor.commitStage(any(), any(), any(), any(), any(), any()))
        .thenReturn(stageResult);

    StageMaterializationService.StageMaterializationResult result =
        service.materializeTransactionStage("pref", "cat", List.of("db"), "orders", null);

    assertNotNull(result);
    assertEquals("txn-only-stage", result.stageId());
    verify(stageCommitProcessor)
        .commitStage("pref", "cat", "tenant-4", List.of("db"), "orders", "txn-only-stage");
  }
}
