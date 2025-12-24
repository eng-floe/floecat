package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.services.account.AccountContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.StageCommitException;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.StageCommitProcessor.StageCommitResult;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import org.jboss.logging.Logger;

@ApplicationScoped
public class StageMaterializationService {
  private static final Logger LOG = Logger.getLogger(StageMaterializationService.class);

  @Inject AccountContext accountContext;
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
    String stageIdToUse = resolveStageId(request, transactionStageId);
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
    String stageIdToUse = resolveStageId(request, transactionStageId);
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
    String stageIdToUse = stageId;
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
    String accountId = accountContext.getAccountId();
    if (accountId == null || accountId.isBlank()) {
      throw StageCommitException.validation("account context is required");
    }
    StageCommitResult result =
        stageCommitProcessor.commitStage(
            prefix, catalogName, accountId, namespacePath, table, stageId);
    return new StageMaterializationResult(stageId, result);
  }
}
