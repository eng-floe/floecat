package ai.floedb.floecat.gateway.iceberg.rest.services.catalog;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.StageCommitProcessor.StageCommitResult;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;

@ApplicationScoped
public class CommitStageResolver {

  @Inject TableLifecycleService tableLifecycleService;
  @Inject StageMaterializationService stageMaterializationService;

  public StageResolution resolve(TableCommitService.CommitCommand command) {
    String prefix = command.prefix();
    String catalogName = command.catalogName();
    var namespacePath = command.namespacePath();
    String table = command.table();
    TableRequests.Commit req = command.request();
    String transactionId = command.transactionId();
    StageCommitResult stageMaterialization = null;
    String materializedStageId = null;

    ResourceId resolvedTableId = null;
    try {
      resolvedTableId = tableLifecycleService.resolveTableId(catalogName, namespacePath, table);
      StageMaterializationService.StageMaterializationResult explicitStage =
          stageMaterializationService.materializeExplicitStage(
              prefix, catalogName, namespacePath, table, req, transactionId);
      if (explicitStage != null) {
        stageMaterialization = explicitStage.result();
        materializedStageId = explicitStage.stageId();
        resolvedTableId = explicitStage.table().getResourceId();
      }
    } catch (io.grpc.StatusRuntimeException e) {
      StageMaterializationService.StageMaterializationResult materialization;
      try {
        materialization =
            stageMaterializationService.materializeIfTableMissing(
                e, prefix, catalogName, namespacePath, table, req, transactionId);
      } catch (StageCommitException sce) {
        return StageResolution.failure(sce.toResponse());
      }
      if (materialization != null) {
        stageMaterialization = materialization.result();
        materializedStageId = materialization.stageId();
        resolvedTableId = stageMaterialization.table().getResourceId();
      } else {
        throw e;
      }
    }

    if (resolvedTableId == null) {
      throw new IllegalStateException("table resolution failed");
    }
    return StageResolution.success(resolvedTableId, stageMaterialization, materializedStageId);
  }

  public record StageResolution(
      ResourceId tableId,
      StageCommitResult stageCommitResult,
      String materializedStageId,
      Response error) {
    static StageResolution success(
        ResourceId tableId, StageCommitResult stageCommitResult, String stageId) {
      return new StageResolution(tableId, stageCommitResult, stageId, null);
    }

    static StageResolution failure(Response error) {
      return new StageResolution(null, null, null, error);
    }

    public boolean hasError() {
      return error != null;
    }

    public Table stagedTable() {
      return stageCommitResult == null ? null : stageCommitResult.table();
    }
  }
}
