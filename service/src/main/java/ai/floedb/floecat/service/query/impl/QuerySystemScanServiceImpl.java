package ai.floedb.floecat.service.query.impl;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.resolver.SystemScannerResolver;
import ai.floedb.floecat.service.query.system.SystemRowFilter;
import ai.floedb.floecat.service.query.system.SystemRowMappers;
import ai.floedb.floecat.service.query.system.SystemRowProjector;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.system.rpc.QuerySystemScanService;
import ai.floedb.floecat.system.rpc.ScanSystemTableRequest;
import ai.floedb.floecat.system.rpc.ScanSystemTableResponse;
import ai.floedb.floecat.systemcatalog.spi.scanner.CatalogOverlay;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanner;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.Map;
import org.jboss.logging.Logger;

@GrpcService
public class QuerySystemScanServiceImpl extends BaseServiceImpl implements QuerySystemScanService {

  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject CatalogOverlay graph;
  @Inject SystemScannerResolver scanners;
  @Inject QueryContextStore queryStore;

  private static final Logger LOG = Logger.getLogger(QuerySystemScanServiceImpl.class);

  @Override
  public Uni<ScanSystemTableResponse> scanSystemTable(ScanSystemTableRequest request) {

    var L = LogHelper.start(LOG, "ScanSystemTable");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();
                  var correlationId = principalContext.getCorrelationId();

                  authz.require(principalContext, "catalog.read");

                  String queryId = mustNonEmpty(request.getQueryId(), "query_id", correlationId);

                  var ctxOpt = queryStore.get(queryId);
                  if (ctxOpt.isEmpty()) {
                    throw GrpcErrors.notFound(
                        correlationId, "query.not_found", Map.of("query_id", queryId));
                  }
                  var queryCtx = ctxOpt.get();

                  if (!request.hasTableId()) {
                    throw GrpcErrors.invalidArgument(
                        correlationId, "system.table_id.required", Map.of());
                  }

                  ResourceId tableId = request.getTableId();

                  // 1. Resolve scanner
                  SystemObjectScanner scanner = scanners.resolve(correlationId, tableId);

                  // For system scans, engine scoping is implicit via CatalogOverlay.
                  // The catalogId here must represent the *current query catalog/database*.
                  SystemObjectScanContext ctx =
                      new SystemObjectScanContext(graph, null, queryCtx.getQueryDefaultCatalogId());

                  // 3. Scan rows
                  var rows = scanner.scan(ctx).toList();

                  // 4. Apply predicates (exact)
                  rows =
                      SystemRowFilter.applyPredicates(
                          rows, scanner.schema(), request.getPredicatesList());

                  // 5. Apply projection
                  rows =
                      SystemRowProjector.project(
                          rows, scanner.schema(), request.getRequiredColumnsList());

                  // 6. Build response
                  return ScanSystemTableResponse.newBuilder()
                      .addAllRows(rows.stream().map(SystemRowMappers::toProto).toList())
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }
}
