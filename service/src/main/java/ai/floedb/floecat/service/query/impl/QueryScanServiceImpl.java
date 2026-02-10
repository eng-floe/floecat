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

package ai.floedb.floecat.service.query.impl;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.*;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.DataFileBatch;
import ai.floedb.floecat.query.rpc.DeleteFileBatch;
import ai.floedb.floecat.query.rpc.InitScanRequest;
import ai.floedb.floecat.query.rpc.InitScanResponse;
import ai.floedb.floecat.query.rpc.QueryScanService;
import ai.floedb.floecat.query.rpc.QueryScanServiceGrpc;
import ai.floedb.floecat.query.rpc.ScanHandle;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.GrpcContextUtil;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.execution.impl.ScanBundleService;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import com.google.protobuf.Empty;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.inject.Inject;
import java.util.Map;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@GrpcService
public class QueryScanServiceImpl extends BaseServiceImpl implements QueryScanService {

  private static final Logger LOG = Logger.getLogger(QueryScanServiceGrpc.class);

  @ConfigProperty(name = "floecat.scan.default-batch-items", defaultValue = "1000")
  int defaultBatchItems; // count of files per batch, overridden by request.target_batch_items

  @ConfigProperty(name = "floecat.scan.default-batch-bytes", defaultValue = "1000000")
  int defaultBatchBytes; // approximate bytes per batch, overridden by request.target_batch_bytes

  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject QueryContextStore queryStore;
  @Inject ScanBundleService scanBundles;

  @Override
  /**
   * Handles the stream initialization RPC: validates the query/table, ensures the snapshot is
   * pinned, and creates a server-side scan session that captures pruning hints + batch knobs.
   */
  public Uni<InitScanResponse> initScan(InitScanRequest request) {
    var L = LogHelper.start(LOG, "InitScan");
    return run(() -> {
          String correlationId = principal.get().getCorrelationId();
          authz.require(principal.get(), "catalog.read");
          String queryId = mustNonEmpty(request.getQueryId(), "query_id", correlationId);
          if (!request.hasTableId()) {
            throw GrpcErrors.invalidArgument(
                correlationId, QUERY_TABLE_ID_REQUIRED, Map.of("query_id", queryId));
          }
          var ctx =
              queryStore
                  .get(queryId)
                  .orElseThrow(
                      () ->
                          GrpcErrors.notFound(
                              correlationId, QUERY_NOT_FOUND, Map.of("query_id", queryId)));
          ResourceId tableId = request.getTableId();
          var pin = ctx.requireSnapshotPin(tableId, correlationId);
          var initData = scanBundles.initScan(correlationId, tableId, pin.getSnapshotId());
          var session =
              ScanSession.builder()
                  .queryId(queryId)
                  .tableId(tableId)
                  .snapshotId(initData.snapshotId())
                  .tableInfo(initData.tableInfo())
                  .includeColumnStats(request.getIncludeColumnStats())
                  .excludePartitionDataJson(request.getExcludePartitionDataJson())
                  .targetBatchItems(capped(request.getTargetBatchItems(), defaultBatchItems))
                  .targetBatchBytes(capped(request.getTargetBatchBytes(), defaultBatchBytes))
                  .requiredColumns(request.getRequiredColumnsList())
                  .predicates(request.getPredicatesList())
                  .build();
          ScanHandle handle = queryStore.createScanSession(correlationId, session);
          return InitScanResponse.newBuilder()
              .setHandle(handle)
              .setTableInfo(initData.tableInfo())
              .build();
        })
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(item -> L.ok());
  }

  @Override
  /**
   * Streams delete files for a scan handle. The returned {@linkplain Multi} caches metadata so
   * retries can replay the same batches, and the handle is automatically removed on
   * termination/cancellation.
   */
  public Multi<DeleteFileBatch> streamDeleteFiles(ScanHandle handle) {
    var L = LogHelper.start(LOG, "StreamDeleteFiles");
    GrpcContextUtil grpcCtx = GrpcContextUtil.capture();
    return Multi.createFrom()
        .deferred(
            () ->
                grpcCtx.call(
                    () -> {
                      String correlationId = principal.get().getCorrelationId();
                      ScanSession session = requireSession(handle, correlationId);
                      return scanBundles
                          .streamDeleteFiles(session, correlationId)
                          .onFailure()
                          .invoke(e -> markPlanningFailed(session, correlationId))
                          .onFailure()
                          .invoke(e -> queryStore.removeScanSession(handle))
                          .onCompletion()
                          .invoke(L::ok);
                    }))
        .runSubscriptionOn(Infrastructure.getDefaultExecutor())
        .onFailure()
        .invoke(L::fail);
  }

  @Override
  /**
   * Streams data files after deletes are complete. Failing to observe deletes first results in
   * FAILED_PRECONDITION; successful completion marks planning done and releases the session.
   */
  public Multi<DataFileBatch> streamDataFiles(ScanHandle handle) {
    var L = LogHelper.start(LOG, "StreamDataFiles");
    GrpcContextUtil grpcCtx = GrpcContextUtil.capture();
    return Multi.createFrom()
        .deferred(
            () ->
                grpcCtx.call(
                    () -> {
                      String correlationId = principal.get().getCorrelationId();
                      ScanSession session = requireSession(handle, correlationId);
                      return scanBundles
                          .streamDataFiles(session, correlationId)
                          .onFailure()
                          .invoke(e -> markPlanningFailed(session, correlationId))
                          .onFailure()
                          .invoke(e -> queryStore.removeScanSession(handle))
                          .onCompletion()
                          .invoke(
                              () -> {
                                markPlanningCompleted(session, correlationId);
                                L.ok();
                              });
                    }))
        .runSubscriptionOn(Infrastructure.getDefaultExecutor())
        .onTermination()
        .invoke(() -> queryStore.removeScanSession(handle))
        .onFailure()
        .invoke(L::fail);
  }

  @Override
  /**
   * Best-effort request to release server state for a handle. Close may be skipped if streams
   * already finished, but it is provided so clients can proactively clean up resources.
   */
  public Uni<Empty> closeScan(ScanHandle handle) {
    var L = LogHelper.start(LOG, "CloseScan");
    return run(() -> {
          String correlationId = principal.get().getCorrelationId();
          queryStore
              .getScanSession(handle)
              .ifPresent(session -> markPlanningCompleted(session, correlationId));
          queryStore.removeScanSession(handle);
          return Empty.newBuilder().build();
        })
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(item -> L.ok());
  }

  private ScanSession requireSession(ScanHandle handle, String correlationId) {
    return queryStore
        .getScanSession(handle)
        .orElseThrow(
            () ->
                GrpcErrors.notFound(correlationId, SCAN_HANDLE, Map.of("handle", handle.getId())));
  }

  private void markPlanningFailed(ScanSession session, String correlationId) {
    queryStore.update(
        session.queryId(),
        ctx -> {
          ctx.markPlanningFailed();
          return ctx;
        });
  }

  private void markPlanningCompleted(ScanSession session, String correlationId) {
    queryStore.update(
        session.queryId(),
        ctx -> {
          ctx.markPlanningCompleted();
          return ctx;
        });
  }

  private int capped(int value, int fallback) {
    return value > 0 ? value : fallback;
  }
}
