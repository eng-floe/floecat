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

package ai.floedb.floecat.gateway.iceberg.rest.services.planning;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.ContentFileDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.FileScanTaskDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TablePlanResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.PlanRequests;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.CatalogResolver;
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
  @Inject IcebergGatewayConfig config;
  @Inject GrpcWithHeaders grpc;

  public Response plan(
      TableRequestContext tableContext,
      PlanRequests.Plan rawRequest,
      TableGatewaySupport tableSupport) {
    PlanRequests.Plan request = rawRequest == null ? PlanRequests.Plan.empty() : rawRequest;
    Long startSnapshotId = request.startSnapshotId();
    Long endSnapshotId = request.endSnapshotId();
    Long snapshotId = request.snapshotId();
    if (snapshotId != null && (startSnapshotId != null || endSnapshotId != null)) {
      return IcebergErrorResponses.validation(
          "snapshot-id cannot be combined with start-snapshot-id or end-snapshot-id");
    }
    if (endSnapshotId != null && startSnapshotId == null) {
      return IcebergErrorResponses.validation("end-snapshot-id requires start-snapshot-id");
    }
    if (startSnapshotId != null && endSnapshotId == null) {
      return IcebergErrorResponses.validation("start-snapshot-id requires end-snapshot-id");
    }
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
    String planId = null;
    try {
      ResourceId catalogId =
          CatalogResolver.resolveCatalogId(grpc, config, tableContext.catalog().catalogName());
      var handle =
          tablePlanService.startPlan(
              catalogId,
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
      planId = handle.queryId();
      planTaskManager.registerSubmittedPlan(
          planId, tableContext.namespaceName(), tableContext.table());
      TablePlanResponseDto planned =
          tablePlanService.fetchPlan(planId, tableSupport.defaultCredentials());
      PlanTaskManager.PlanDescriptor descriptor =
          planTaskManager.registerCompletedPlan(
              planId,
              tableContext.namespaceName(),
              tableContext.table(),
              copyOfOrEmpty(planned.fileScanTasks()),
              copyOfOrEmpty(planned.deleteFiles()),
              planned.storageCredentials());
      return Response.ok(toPlanResponse(descriptor, true)).build();
    } catch (IllegalArgumentException ex) {
      return IcebergErrorResponses.validation(ex.getMessage());
    } catch (RuntimeException ex) {
      if (planId != null) {
        try {
          planTaskManager.cancelPlan(planId);
          tablePlanService.cancelPlan(planId);
        } catch (RuntimeException ignored) {
          // Cancellation is best-effort; errors are surfaced via the original failure.
        }
      }
      String message = ex.getMessage();
      if (message == null || message.isBlank()) {
        message = "Scan planning failed";
      }
      return IcebergErrorResponses.failure(
          message, "InternalServerError", Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  public Response fetchPlan(String planId) {
    return planTaskManager
        .findPlan(planId)
        .map(
            descriptor -> {
              if ("failed".equals(descriptor.status().value())) {
                return IcebergErrorResponses.failure(
                    "Scan planning failed",
                    "InternalServerError",
                    Response.Status.INTERNAL_SERVER_ERROR);
              }
              return Response.ok(toPlanResponse(descriptor, false)).build();
            })
        .orElseGet(() -> IcebergErrorResponses.noSuchPlanId("plan " + planId + " not found"));
  }

  public Response cancelPlan(String planId) {
    var plan = planTaskManager.findPlan(planId);
    if (plan.isEmpty()) {
      return IcebergErrorResponses.noSuchPlanId("plan " + planId + " not found");
    }
    planTaskManager.cancelPlan(planId);
    tablePlanService.cancelPlan(planId);
    return Response.noContent().build();
  }

  public Response consumeTask(TableRequestContext tableContext, String planTaskId) {
    return planTaskManager
        .consumeTask(tableContext.namespaceName(), tableContext.table(), planTaskId)
        .map(response -> Response.ok(response).build())
        .orElseGet(() -> IcebergErrorResponses.noSuchPlanTask("plan-task not found"));
  }

  private TablePlanResponseDto toPlanResponse(
      PlanTaskManager.PlanDescriptor descriptor, boolean includePlanId) {
    String status = descriptor.status().value();
    if ("cancelled".equals(status)) {
      return new TablePlanResponseDto(status, null, null, null, null, null);
    }
    if (!"completed".equals(status)) {
      return new TablePlanResponseDto(
          status, includePlanId ? descriptor.planId() : null, null, null, null, null);
    }
    List<FileScanTaskDto> fileScanTasks = copyOfOrEmpty(descriptor.fileScanTasks());
    List<ContentFileDto> deleteFiles = copyOfOrEmpty(descriptor.deleteFiles());
    return new TablePlanResponseDto(
        descriptor.status().value(),
        includePlanId ? descriptor.planId() : null,
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
