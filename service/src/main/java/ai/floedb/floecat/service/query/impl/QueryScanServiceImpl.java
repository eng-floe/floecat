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

import ai.floedb.floecat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.floecat.common.rpc.Predicate;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.execution.rpc.ScanFile;
import ai.floedb.floecat.query.rpc.FetchScanBundleRequest;
import ai.floedb.floecat.query.rpc.QueryScanService;
import ai.floedb.floecat.query.rpc.QueryScanServiceGrpc;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.GrpcContextUtil;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.common.ScanPruningUtils;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.execution.impl.ScanBundleService;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.control.ActivateRequestContext;
import jakarta.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jboss.logging.Logger;

/**
 * gRPC implementation of {@link QueryScanService}.
 *
 * <p>This service handles all scan-file–related operations:
 *
 * <ul>
 *   <li>retrieving raw scan bundles from connectors,
 *   <li>pruning by projection,
 *   <li>pruning by predicate (min/max statistics).
 * </ul>
 *
 * <p>Lifecycle operations (create/renew/end query) are handled in {@link QueryServiceImpl}.
 */
@GrpcService
public class QueryScanServiceImpl extends BaseServiceImpl implements QueryScanService {

  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;

  @Inject QueryContextStore queryStore;
  @Inject ScanBundleService scanBundles;

  @Inject
  @GrpcClient("floecat")
  TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub stats;

  private static final Logger LOG = Logger.getLogger(QueryScanServiceGrpc.class);

  /**
   * FetchScanBundle
   *
   * <p>Retrieves raw scan bundles for a pinned table and applies both projection and predicate
   * pruning. Query status is updated based on success or failure.
   */
  @Override
  @ActivateRequestContext
  public Multi<ScanFile> fetchScanBundle(FetchScanBundleRequest request) {
    var L = LogHelper.start(LOG, "FetchScanBundle");
    GrpcContextUtil grpcCtx = GrpcContextUtil.capture();

    return Multi.createFrom()
        .<ScanFile>deferred(
            () ->
                grpcCtx.call(
                    () -> {
                      var principalContext = principal.get();
                      var correlationId = principalContext.getCorrelationId();

                      authz.require(principalContext, "catalog.read");

                      String queryId =
                          mustNonEmpty(request.getQueryId(), "query_id", correlationId);

                      if (!request.hasTableId()) {
                        throw GrpcErrors.invalidArgument(
                            correlationId, "query.table_id.required", Map.of("query_id", queryId));
                      }

                      var ctxOpt = queryStore.get(queryId);
                      if (ctxOpt.isEmpty()) {
                        throw GrpcErrors.notFound(
                            correlationId, "query.not_found", Map.of("query_id", queryId));
                      }
                      var ctx = ctxOpt.get();

                      ResourceId tableId = request.getTableId();
                      var pin = ctx.requireSnapshotPin(tableId, correlationId);
                      Set<String> requiredColumns =
                          request.getRequiredColumnsCount() == 0
                              ? Set.of()
                              : new HashSet<>(request.getRequiredColumnsList());
                      List<Predicate> predicates = request.getPredicatesList();

                      try {
                        Iterable<ScanFile> raw =
                            scanBundles.stream(correlationId, tableId, pin, stats);

                        return Multi.createFrom()
                            .iterable(raw)
                            .onItem()
                            .transform(
                                file ->
                                    ScanPruningUtils.pruneFile(file, requiredColumns, predicates))
                            .select()
                            .where(file -> file != null)
                            .onFailure()
                            .invoke(ctx::markPlanningFailed)
                            .onFailure()
                            .transform(t -> toStatus(t, correlationId))
                            .onCompletion()
                            .invoke(ctx::markPlanningCompleted);
                      } catch (RuntimeException e) {
                        ctx.markPlanningFailed();
                        throw toStatus(e, correlationId);
                      }
                    }))
        .runSubscriptionOn(Infrastructure.getDefaultExecutor())
        .onFailure()
        .invoke(L::fail)
        .onCompletion()
        .invoke(L::ok);
  }
}
