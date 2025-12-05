package ai.floedb.metacat.gateway.iceberg.rest.services.catalog;

import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.metacat.gateway.iceberg.rest.services.catalog.StageCommitProcessor.StageCommitResult;
import ai.floedb.metacat.gateway.iceberg.rest.services.staging.StagedTableEntry;
import ai.floedb.metacat.gateway.iceberg.rest.services.staging.StagedTableService;
import ai.floedb.metacat.gateway.iceberg.rest.services.tenant.TenantContext;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import org.jboss.logging.Logger;

/**
 * Handles staged table materialization when a commit references a stage payload rather than an
 * existing table.
 */
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
    String stageIdToUse = resolveStageId(request, transactionStageId);
    String tenantId = tenantContext.getTenantId();
    if (stageIdToUse == null && tenantId != null) {
      Optional<StagedTableEntry> latest =
          stagedTableService.findLatestStage(tenantId, catalogName, namespacePath, table);
      stageIdToUse = latest.map(entry -> entry.key().stageId()).orElse(null);
      latest.ifPresent(
          entry ->
              LOG.infof(
                  "Found staged payload without explicit stage id namespace=%s table=%s stageId=%s",
                  namespacePath, table, entry.key().stageId()));
    }

    if (stageIdToUse == null) {
      LOG.warnf(
          "Commit request table not found and no stage id supplied namespace=%s table=%s "
              + "bodyStageId=%s headerStageId=%s",
          namespacePath, table, request == null ? null : request.stageId(), transactionStageId);
      return null;
    }

    LOG.infof(
        "Table not found for commit, attempting staged materialization namespace=%s table=%s"
            + " stageId=%s",
        namespacePath, table, stageIdToUse);
    StageCommitResult result =
        stageCommitProcessor.commitStage(
            prefix, catalogName, tenantId, namespacePath, table, stageIdToUse);
    return new StageMaterializationResult(stageIdToUse, result);
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
}
