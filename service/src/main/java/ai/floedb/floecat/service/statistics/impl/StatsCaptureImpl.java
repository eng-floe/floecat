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

package ai.floedb.floecat.service.statistics.impl;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.FIELD;
import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.NAMESPACE;
import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.TABLE;

import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.impl.ReconcilerService;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.statistics.rpc.AnalyzeRequest;
import ai.floedb.floecat.statistics.rpc.AnalyzeResponse;
import ai.floedb.floecat.statistics.rpc.AnalyzeTableRequest;
import ai.floedb.floecat.statistics.rpc.AnalyzeTableResponse;
import ai.floedb.floecat.statistics.rpc.CancelJobRequest;
import ai.floedb.floecat.statistics.rpc.CancelJobResponse;
import ai.floedb.floecat.statistics.rpc.GetJobRequest;
import ai.floedb.floecat.statistics.rpc.GetJobResponse;
import ai.floedb.floecat.statistics.rpc.ListJobsRequest;
import ai.floedb.floecat.statistics.rpc.ListJobsResponse;
import ai.floedb.floecat.statistics.rpc.StatsCapture;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.jboss.logging.Logger;

@GrpcService
public class StatsCaptureImpl extends BaseServiceImpl implements StatsCapture {
  @Inject TableRepository tableRepo;
  @Inject NamespaceRepository namespaceRepo;
  @Inject ReconcilerService reconcilerService;
  @Inject PrincipalProvider principalProvider;
  @Inject Authorizer authz;

  private static final Logger LOG = Logger.getLogger(StatsCapture.class);

  @Override
  public Uni<ListJobsResponse> listJobs(ListJobsRequest request) {
    return Uni.createFrom()
        .item(
            () -> {
              return ListJobsResponse.newBuilder().build();
            });
  }

  @Override
  public Uni<AnalyzeResponse> analyze(AnalyzeRequest request) {
    return Uni.createFrom()
        .item(
            () -> {
              return AnalyzeResponse.newBuilder().build();
            });
  }

  @Override
  public Uni<AnalyzeTableResponse> analyzeTable(AnalyzeTableRequest request) {
    var L = LogHelper.start(LOG, "AnalyzeTable");
    return mapFailures(
            run(
                () -> {
                  var principalContext = principalProvider.get();
                  var correlationId = principalContext.getCorrelationId();

                  authz.require(principalContext, "catalog.write");

                  var tableId = request.getTableId();
                  ensureKind(tableId, ResourceKind.RK_TABLE, "table_id", correlationId);
                  var table =
                      tableRepo
                          .getById(tableId)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      correlationId, TABLE, Map.of("id", tableId.getId())));

                  if (!table.hasUpstream() || !table.getUpstream().hasConnectorId()) {
                    throw GrpcErrors.invalidArgument(
                        correlationId, FIELD, Map.of("field", "table.upstream.connector_id"));
                  }
                  ResourceId connectorId = table.getUpstream().getConnectorId();
                  ensureKind(
                      connectorId,
                      ResourceKind.RK_CONNECTOR,
                      "table.upstream.connector_id",
                      correlationId);

                  Namespace namespace =
                      namespaceRepo
                          .getById(table.getNamespaceId())
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      correlationId,
                                      NAMESPACE,
                                      Map.of("id", table.getNamespaceId().getId())));

                  List<String> scopePath = new ArrayList<>(namespace.getParentsList());
                  if (!namespace.getDisplayName().isBlank()) {
                    scopePath.add(namespace.getDisplayName());
                  }

                  var scope =
                      ReconcileScope.of(
                          List.of(scopePath),
                          table.getDisplayName(),
                          request.getDestinationTableColumnsList());

                  var metadataResult =
                      reconcilerService.reconcile(
                          principalContext,
                          connectorId,
                          false,
                          scope,
                          CaptureMode.METADATA_ONLY_CORE);
                  ensureReconcileSuccess("metadata", metadataResult);

                  var statsResult =
                      reconcilerService.reconcile(
                          principalContext,
                          connectorId,
                          false,
                          scope,
                          CaptureMode.STATS_ONLY_ASYNC);
                  ensureReconcileSuccess("stats", statsResult);

                  return AnalyzeTableResponse.newBuilder()
                      .setMetadataTablesScanned(metadataResult.scanned)
                      .setMetadataTablesChanged(metadataResult.changed)
                      .setMetadataErrors(metadataResult.errors)
                      .setStatsTablesScanned(statsResult.scanned)
                      .setStatsTablesChanged(statsResult.changed)
                      .setStatsErrors(statsResult.errors)
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<GetJobResponse> getJob(GetJobRequest request) {
    return Uni.createFrom()
        .item(
            () -> {
              return GetJobResponse.newBuilder().build();
            });
  }

  @Override
  public Uni<CancelJobResponse> cancelJob(CancelJobRequest request) {
    return Uni.createFrom()
        .item(
            () -> {
              return CancelJobResponse.newBuilder().build();
            });
  }

  private static void ensureReconcileSuccess(String phase, ReconcilerService.Result result) {
    if (result != null && result.ok()) {
      return;
    }
    Throwable cause = result == null ? null : result.error;
    String message =
        "AnalyzeTable "
            + phase
            + " reconcile failed: "
            + (result == null ? "no result" : result.message());
    if (cause instanceof RuntimeException runtimeException) {
      throw runtimeException;
    }
    throw new IllegalStateException(message, cause);
  }
}
