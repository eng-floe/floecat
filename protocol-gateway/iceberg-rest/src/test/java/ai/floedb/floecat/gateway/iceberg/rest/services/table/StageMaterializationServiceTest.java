package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.LoadTableResultDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.common.TableMetadataBuilder;
import ai.floedb.floecat.gateway.iceberg.rest.common.TrinoFixtureTestSupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.account.AccountContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.StageCommitException;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StagedTableService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.StageCommitProcessor.StageCommitResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StageMaterializationServiceTest {
  private static final TrinoFixtureTestSupport.Fixture FIXTURE =
      TrinoFixtureTestSupport.simpleFixture();
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final TableMetadataView FIXTURE_METADATA_VIEW =
      TableMetadataBuilder.fromCatalog(
          FIXTURE.table().getDisplayName(),
          FIXTURE.table(),
          new LinkedHashMap<>(FIXTURE.table().getPropertiesMap()),
          FIXTURE.metadata(),
          FIXTURE.snapshots());
  private static final String METADATA_LOCATION = FIXTURE.metadataLocation();
  private static final String TABLE_LOCATION = FIXTURE.table().getPropertiesMap().get("location");

  static {
    if (TABLE_LOCATION == null || TABLE_LOCATION.isBlank()) {
      throw new IllegalStateException("fixture location is required");
    }
  }

  private final StageMaterializationService service = new StageMaterializationService();
  private final AccountContext accountContext = mock(AccountContext.class);
  private final StageCommitProcessor stageCommitProcessor = mock(StageCommitProcessor.class);
  private final StagedTableService stagedTableService = mock(StagedTableService.class);

  @BeforeEach
  void setUp() {
    service.accountContext = accountContext;
    service.stageCommitProcessor = stageCommitProcessor;
    service.stagedTableService = stagedTableService;
    when(stagedTableService.findSingleStage(any(), any(), any(), any()))
        .thenReturn(Optional.empty());
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
            new TableRequests.Commit(null, null, null, null, null, List.of(), List.of()),
            null);

    assertNull(result);
    verify(stageCommitProcessor, never()).commitStage(any(), any(), any(), any(), any(), any());
  }

  @Test
  void materializeIfTableMissingUsesExplicitStageId() throws Exception {
    when(accountContext.getAccountId()).thenReturn("account-1");
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
            new TableRequests.Commit(null, null, null, null, "stage-1", List.of(), List.of()),
            "header-stage");

    assertNotNull(result);
    assertEquals("stage-1", result.stageId());
    verify(stageCommitProcessor)
        .commitStage("pref", "cat", "account-1", List.of("db"), "orders", "stage-1");
  }

  @Test
  void materializeIfTableMissingSkipsWhenNoStageId() {
    when(accountContext.getAccountId()).thenReturn("account-required");

    StageMaterializationService.StageMaterializationResult result =
        service.materializeIfTableMissing(
            Status.NOT_FOUND.asRuntimeException(),
            "pref",
            "cat",
            List.of("db"),
            "orders",
            new TableRequests.Commit(null, null, null, null, null, List.of(), List.of()),
            null);

    assertNull(result);
    verify(stageCommitProcessor, never()).commitStage(any(), any(), any(), any(), any(), any());
  }

  @Test
  void materializeIfTableMissingPropagatesStageErrors() throws Exception {
    when(accountContext.getAccountId()).thenReturn("account");
    when(stageCommitProcessor.commitStage(any(), any(), any(), any(), any(), any()))
        .thenThrow(StageCommitException.validation("bad stage"));

    try {
      service.materializeIfTableMissing(
          Status.NOT_FOUND.asRuntimeException(),
          "pref",
          "cat",
          List.of("db"),
          "orders",
          new TableRequests.Commit(null, null, null, null, "stage-1", List.of(), List.of()),
          null);
    } catch (StageCommitException expected) {
      assertEquals("bad stage", expected.getMessage());
    }
  }

  @Test
  void materializeExplicitStageSkipsWhenNoStageIdAndNoFallback() throws Exception {
    StageMaterializationService.StageMaterializationResult result =
        service.materializeExplicitStage(
            "pref",
            "cat",
            List.of("db"),
            "orders",
            new TableRequests.Commit(null, null, null, null, null, List.of(), List.of()),
            null);

    assertNull(result);
    verify(stageCommitProcessor, never()).commitStage(any(), any(), any(), any(), any(), any());
  }

  @Test
  void materializeExplicitStageUsesProvidedStageId() throws Exception {
    when(accountContext.getAccountId()).thenReturn("account-explicit");
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
            new TableRequests.Commit(
                null, null, null, null, "explicit-stage", List.of(), List.of()),
            null);

    assertNotNull(result);
    assertEquals("explicit-stage", result.stageId());
    verify(stageCommitProcessor)
        .commitStage("pref", "cat", "account-explicit", List.of("db"), "orders", "explicit-stage");
  }

  @Test
  void materializeTransactionStageUsesProvidedStageId() throws Exception {
    when(accountContext.getAccountId()).thenReturn("account-3");
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
        .commitStage("pref", "cat", "account-3", List.of("db"), "orders", "txn-stage");
  }

  @Test
  void materializeTransactionStageRequiresStageId() {
    when(accountContext.getAccountId()).thenReturn("account-4");
    StageCommitException ex =
        assertThrows(
            StageCommitException.class,
            () ->
                service.materializeTransactionStage("pref", "cat", List.of("db"), "orders", null));

    assertTrue(ex.getMessage().contains("stage-id is required"));
    verify(stageCommitProcessor, never()).commitStage(any(), any(), any(), any(), any(), any());
  }

  private static Map<String, String> fixtureProperties() {
    return Map.of(
        "metadata-location", METADATA_LOCATION,
        "format-version", Integer.toString(FIXTURE.metadata().getFormatVersion()),
        "last-updated-ms", Long.toString(FIXTURE.metadata().getLastUpdatedMs()));
  }

  private static JsonNode fixtureSchemaNode() {
    Map<String, Object> schema =
        selectById(
            FIXTURE_METADATA_VIEW.schemas(),
            "schema-id",
            FIXTURE_METADATA_VIEW.currentSchemaId(),
            "fixture schema");
    Integer schemaId = FIXTURE_METADATA_VIEW.currentSchemaId();
    if (!schema.containsKey("schema-id")) {
      if (schemaId == null) {
        throw new IllegalStateException("fixture schema requires schema-id");
      }
      schema.put("schema-id", schemaId);
    }
    Integer lastColumnId = FIXTURE_METADATA_VIEW.lastColumnId();
    if (!schema.containsKey("last-column-id")) {
      if (lastColumnId == null) {
        throw new IllegalStateException("fixture schema requires last-column-id");
      }
      schema.put("last-column-id", lastColumnId);
    }
    return JSON.valueToTree(schema);
  }

  private static JsonNode fixturePartitionSpec() {
    Map<String, Object> spec =
        selectById(
            FIXTURE_METADATA_VIEW.partitionSpecs(),
            "spec-id",
            FIXTURE_METADATA_VIEW.defaultSpecId(),
            "fixture partition spec");
    return JSON.valueToTree(spec);
  }

  private static JsonNode fixtureWriteOrder() {
    Map<String, Object> order =
        selectById(
            FIXTURE_METADATA_VIEW.sortOrders(),
            "order-id",
            FIXTURE_METADATA_VIEW.defaultSortOrderId(),
            "fixture sort order");
    return JSON.valueToTree(order);
  }

  private static Map<String, Object> selectById(
      List<Map<String, Object>> candidates, String key, Integer targetId, String label) {
    if (candidates == null || candidates.isEmpty()) {
      throw new IllegalStateException(label + " list is empty");
    }
    if (targetId != null) {
      for (Map<String, Object> candidate : candidates) {
        if (candidate == null) {
          continue;
        }
        Integer value = asInteger(candidate.get(key));
        if (value != null && value.equals(targetId)) {
          return candidate;
        }
      }
    }
    throw new IllegalStateException(label + " not found for " + key + "=" + targetId);
  }

  private static Integer asInteger(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number number) {
      return number.intValue();
    }
    if (value instanceof String text) {
      try {
        return Integer.parseInt(text);
      } catch (NumberFormatException ignored) {
        return null;
      }
    }
    return null;
  }
}
