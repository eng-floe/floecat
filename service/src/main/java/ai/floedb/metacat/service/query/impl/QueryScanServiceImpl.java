package ai.floedb.metacat.service.query.impl;

import ai.floedb.metacat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.execution.rpc.ScanBundle;
import ai.floedb.metacat.query.rpc.FetchScanBundleRequest;
import ai.floedb.metacat.query.rpc.FetchScanBundleResponse;
import ai.floedb.metacat.query.rpc.QueryScanService;
import ai.floedb.metacat.query.rpc.QueryScanServiceGrpc;
import ai.floedb.metacat.service.common.BaseServiceImpl;
import ai.floedb.metacat.service.common.LogHelper;
import ai.floedb.metacat.service.common.ScanPruningUtils;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.execution.impl.ScanBundleService;
import ai.floedb.metacat.service.query.QueryContextStore;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.Map;
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
  @GrpcClient("metacat")
  TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub stats;

  private static final Logger LOG = Logger.getLogger(QueryScanServiceGrpc.class);

  /**
   * FetchScanBundle
   *
   * <p>Retrieves raw scan bundles for a pinned table and applies both projection and predicate
   * pruning. Query status is updated based on success or failure.
   */
  @Override
  public Uni<FetchScanBundleResponse> fetchScanBundle(FetchScanBundleRequest request) {
    var L = LogHelper.start(LOG, "FetchScanBundle");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();
                  var correlationId = principalContext.getCorrelationId();

                  authz.require(principalContext, "catalog.read");

                  String queryId = mustNonEmpty(request.getQueryId(), "query_id", correlationId);

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

                  try {
                    var raw = scanBundles.fetch(correlationId, request.getTableId(), pin, stats);

                    ScanBundle pruned =
                        ScanPruningUtils.pruneBundle(
                            raw, request.getRequiredColumnsList(), request.getPredicatesList());

                    ctx.markPlanningCompleted();

                    return FetchScanBundleResponse.newBuilder().setBundle(pruned).build();

                  } catch (RuntimeException e) {
                    ctx.markPlanningFailed();
                    throw e;
                  }
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }
}
