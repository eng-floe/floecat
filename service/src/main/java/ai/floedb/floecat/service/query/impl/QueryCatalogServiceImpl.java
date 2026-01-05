package ai.floedb.floecat.service.query.impl;

import ai.floedb.floecat.query.rpc.CatalogBundleChunk;
import ai.floedb.floecat.query.rpc.GetCatalogBundleRequest;
import ai.floedb.floecat.query.rpc.QueryCatalogService;
import ai.floedb.floecat.query.rpc.QueryCatalogServiceGrpc;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.GrpcContextUtil;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.catalog.CatalogBundleService;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.control.ActivateRequestContext;
import jakarta.inject.Inject;
import java.util.Map;
import org.jboss.logging.Logger;

@GrpcService
public class QueryCatalogServiceImpl extends BaseServiceImpl implements QueryCatalogService {

  @Inject PrincipalProvider principal;

  @Inject Authorizer authz;

  @Inject QueryContextStore queryStore;

  @Inject CatalogBundleService bundles;

  private static final Logger LOG = Logger.getLogger(QueryCatalogServiceGrpc.class);

  @ActivateRequestContext
  @Override
  public Multi<CatalogBundleChunk> getCatalogBundle(GetCatalogBundleRequest request) {
    var L = LogHelper.start(LOG, "GetCatalogBundle");
    // Capture the incoming gRPC context so the principal/correlation-id stays available
    // while authz/QueryContext lookup happen; the bundle builder itself is context-free.
    GrpcContextUtil grpcCtx = GrpcContextUtil.capture();

    return Multi.createFrom()
        .<CatalogBundleChunk>deferred(
            () ->
                grpcCtx.call(
                    () -> {
                      var principalContext = principal.get();
                      var correlationId = principalContext.getCorrelationId();
                      authz.require(principalContext, "catalog.read");

                      String queryId =
                          mustNonEmpty(request.getQueryId(), "query_id", correlationId);
                      var ctxOpt = queryStore.get(queryId);
                      if (ctxOpt.isEmpty()) {
                        throw GrpcErrors.notFound(
                            correlationId, "query.not_found", Map.of("query_id", queryId));
                      }

                      QueryContext ctx = ctxOpt.get();
                      if (!ctx.isActive()) {
                        throw GrpcErrors.preconditionFailed(
                            correlationId,
                            "query.not_active",
                            Map.of("query_id", queryId, "state", ctx.getState().name()));
                      }

                      return bundles.stream(correlationId, ctx, request.getTablesList());
                    }))
        .runSubscriptionOn(Infrastructure.getDefaultExecutor())
        .onFailure()
        .invoke(L::fail)
        .onCompletion()
        .invoke(L::ok);
  }
}
