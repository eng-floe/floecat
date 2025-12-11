package ai.floedb.floecat.gateway.iceberg.rest.services.planning;

import ai.floedb.floecat.gateway.iceberg.rest.api.dto.ContentFileDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.FileScanTaskDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TablePlanResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.PlanRequests;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.TableRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.List;

@ApplicationScoped
public class TablePlanOrchestrationService {
  @Inject TablePlanService tablePlanService;
  @Inject PlanTaskManager planTaskManager;

  public Response plan(
      TableRequestContext tableContext,
      PlanRequests.Plan rawRequest,
      TableGatewaySupport tableSupport) {
    PlanRequests.Plan request = rawRequest == null ? PlanRequests.Plan.empty() : rawRequest;
    Long startSnapshotId = request.startSnapshotId();
    Long endSnapshotId = request.endSnapshotId();
    Long snapshotId = request.snapshotId();
    if (startSnapshotId != null && endSnapshotId != null && startSnapshotId >= endSnapshotId) {
      return IcebergErrorResponses.validation(
          "start-snapshot-id must be less than end-snapshot-id");
    }
    Long resolvedSnapshotId = endSnapshotId != null ? endSnapshotId : snapshotId;
    if (startSnapshotId != null && resolvedSnapshotId == null) {
      return IcebergErrorResponses.validation(
          "start-snapshot-id requires snapshot-id or end-snapshot-id");
    }
    boolean caseSensitive =
        request.caseSensitive() == null || Boolean.TRUE.equals(request.caseSensitive());
    boolean useSnapshotSchema = Boolean.TRUE.equals(request.useSnapshotSchema());
    try {
      var handle =
          tablePlanService.startPlan(
              tableContext.catalog().catalogName(),
              tableContext.tableId(),
              copyOfOrNull(request.select()),
              startSnapshotId,
              endSnapshotId,
              snapshotId,
              copyOfOrNull(request.statsFields()),
              request.filter(),
              caseSensitive,
              useSnapshotSchema,
              request.minRowsRequested());
      TablePlanResponseDto planned =
          tablePlanService.fetchPlan(handle.queryId(), tableSupport.defaultCredentials());
      PlanTaskManager.PlanDescriptor descriptor =
          planTaskManager.registerCompletedPlan(
              handle.queryId(),
              tableContext.namespaceName(),
              tableContext.table(),
              copyOfOrEmpty(planned.fileScanTasks()),
              copyOfOrEmpty(planned.deleteFiles()),
              planned.storageCredentials());
      return Response.ok(toPlanResponse(descriptor)).build();
    } catch (IllegalArgumentException ex) {
      return IcebergErrorResponses.validation(ex.getMessage());
    }
  }

  public Response fetchPlan(String planId) {
    return planTaskManager
        .findPlan(planId)
        .map(descriptor -> Response.ok(toPlanResponse(descriptor)).build())
        .orElseGet(() -> IcebergErrorResponses.notFound("plan " + planId + " not found"));
  }

  public Response cancelPlan(String planId) {
    planTaskManager.cancelPlan(planId);
    tablePlanService.cancelPlan(planId);
    return Response.noContent().build();
  }

  public Response consumeTask(TableRequestContext tableContext, String planTaskId) {
    return planTaskManager
        .consumeTask(tableContext.namespaceName(), tableContext.table(), planTaskId)
        .map(response -> Response.ok(response).build())
        .orElseGet(() -> IcebergErrorResponses.notFound("plan-task not found"));
  }

  private TablePlanResponseDto toPlanResponse(PlanTaskManager.PlanDescriptor descriptor) {
    List<FileScanTaskDto> fileScanTasks = copyOfOrEmpty(descriptor.fileScanTasks());
    List<ContentFileDto> deleteFiles = copyOfOrEmpty(descriptor.deleteFiles());
    return new TablePlanResponseDto(
        descriptor.status().value(),
        descriptor.planId(),
        descriptor.planTasks(),
        fileScanTasks,
        deleteFiles,
        descriptor.credentials());
  }

  private static List<String> copyOfOrNull(List<String> values) {
    if (values == null || values.isEmpty()) {
      return null;
    }
    return List.copyOf(values);
  }

  private static <T> List<T> copyOfOrEmpty(List<T> values) {
    if (values == null || values.isEmpty()) {
      return List.of();
    }
    return List.copyOf(values);
  }
}
