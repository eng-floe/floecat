package ai.floedb.floecat.gateway.iceberg.rest.services.catalog;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.StageCommitProcessor.StageCommitResult;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StagedTableEntry;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StagedTableKey;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StagedTableService;
import ai.floedb.floecat.gateway.iceberg.rest.services.tenant.TenantContext;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import org.jboss.logging.Logger;

@ApplicationScoped
public class StageMaterializationService {
  private static final Logger LOG = Logger.getLogger(StageMaterializationService.class);

  @Inject TenantContext tenantContext;
  @Inject StagedTableService stagedTableService;
  @Inject StageCommitProcessor stageCommitProcessor;

  public StageMaterializationResult materializeIfTableMissing(
      StatusRuntimeException resolutionFailure,
      String prefix,
      String catalogName,
      List<String> namespacePath,
      String table,
      TableRequests.Commit request,
      String transactionStageId)
      throws StageCommitException {
    if (resolutionFailure.getStatus().getCode() != Status.Code.NOT_FOUND) {
      return null;
    }
    String stageIdToUse =
        stageIdOrFallback(
            resolveStageId(request, transactionStageId), catalogName, namespacePath, table);
    if (stageIdToUse == null) {
      throw StageCommitException.validation(
          "stage-id is required when committing a staged create for "
              + String.join(".", namespacePath)
              + "."
              + table);
    }

    LOG.infof(
        "Table not found for commit, attempting staged materialization namespace=%s table=%s"
            + " stageId=%s",
        namespacePath, table, stageIdToUse);
    return commitStage(prefix, catalogName, namespacePath, table, stageIdToUse);
  }

  public StageMaterializationResult materializeExplicitStage(
      String prefix,
      String catalogName,
      List<String> namespacePath,
      String table,
      TableRequests.Commit request,
      String transactionStageId)
      throws StageCommitException {
    String stageIdToUse =
        stageIdOrFallback(
            resolveStageId(request, transactionStageId), catalogName, namespacePath, table);
    if (stageIdToUse == null) {
      return null;
    }
    LOG.infof(
        "Stage-id provided for commit namespace=%s table=%s stageId=%s (attempting staged"
            + " materialization)",
        namespacePath, table, stageIdToUse);
    return commitStage(prefix, catalogName, namespacePath, table, stageIdToUse);
  }

  public String resolveStageId(TableRequests.Commit req, String headerStageId) {
    if (req != null && req.stageId() != null && !req.stageId().isBlank()) {
      return req.stageId();
    }
    if (headerStageId != null && !headerStageId.isBlank()) {
      return headerStageId;
    }
    return null;
  }

  public record StageMaterializationResult(String stageId, StageCommitResult result) {
    public Table table() {
      return result.table();
    }
  }

  public StageMaterializationResult materializeTransactionStage(
      String prefix, String catalogName, List<String> namespacePath, String table, String stageId)
      throws StageCommitException {
    String stageIdToUse = stageIdOrFallback(stageId, catalogName, namespacePath, table);
    if (stageIdToUse == null) {
      throw StageCommitException.validation(
          "stage-id is required when committing a staged update for "
              + String.join(".", namespacePath)
              + "."
              + table);
    }
    LOG.infof(
        "Processing staged transaction payload namespace=%s table=%s stageId=%s",
        namespacePath, table, stageIdToUse);
    return commitStage(prefix, catalogName, namespacePath, table, stageIdToUse);
  }

  private StageMaterializationResult commitStage(
      String prefix, String catalogName, List<String> namespacePath, String table, String stageId)
      throws StageCommitException {
    String tenantId = tenantContext.getTenantId();
    if (tenantId == null || tenantId.isBlank()) {
      throw StageCommitException.validation("tenant context is required");
    }
    StageCommitResult result =
        stageCommitProcessor.commitStage(
            prefix, catalogName, tenantId, namespacePath, table, stageId);
    return new StageMaterializationResult(stageId, result);
  }

  private String stageIdOrFallback(
      String stageId, String catalogName, List<String> namespacePath, String table) {
    if (stageId != null && !stageId.isBlank()) {
      return stageId;
    }
    String tenantId = tenantContext.getTenantId();
    if (tenantId == null) {
      return null;
    }
    String resolved =
        stagedTableService
            .findSingleStage(tenantId, catalogName, namespacePath, table)
            .map(StagedTableEntry::key)
            .map(StagedTableKey::stageId)
            .orElse(null);
    if (resolved != null) {
      LOG.infof(
          "Found single staged payload namespace=%s table=%s stageId=%s",
          namespacePath, table, resolved);
    }
    return resolved;
  }
}
