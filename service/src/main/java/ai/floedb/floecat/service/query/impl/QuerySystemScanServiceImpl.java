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
import static java.util.Objects.requireNonNullElse;

import ai.floedb.floecat.common.rpc.Predicate;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.GrpcContextUtil;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.context.EngineContextProvider;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.catalog.StatsProviderFactory;
import ai.floedb.floecat.service.query.impl.arrow.ArrowScanPlan;
import ai.floedb.floecat.service.query.impl.arrow.ArrowScanPlanner;
import ai.floedb.floecat.service.query.impl.arrow.ArrowSink;
import ai.floedb.floecat.service.query.impl.arrow.GrpcArrowSink;
import ai.floedb.floecat.service.query.resolver.SystemScannerResolver;
import ai.floedb.floecat.service.query.system.SystemRowFilter;
import ai.floedb.floecat.service.query.system.SystemRowMappers;
import ai.floedb.floecat.service.query.system.SystemRowProjector;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.system.rpc.OutputFormat;
import ai.floedb.floecat.system.rpc.QuerySystemScanService;
import ai.floedb.floecat.system.rpc.ScanSystemTableChunk;
import ai.floedb.floecat.system.rpc.ScanSystemTableRequest;
import ai.floedb.floecat.system.rpc.SystemTableRow;
import ai.floedb.floecat.scanner.expr.Expr;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.scanner.spi.SystemObjectRow;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.scanner.spi.SystemObjectScanner;
import ai.floedb.floecat.scanner.utils.EngineContext;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.subscription.MultiEmitter;
import jakarta.inject.Inject;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@GrpcService
public class QuerySystemScanServiceImpl extends BaseServiceImpl implements QuerySystemScanService {

  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject CatalogOverlay graph;
  @Inject EngineContextProvider engineContext;
  @Inject SystemScannerResolver scanners;
  @Inject QueryContextStore queryStore;
  @Inject StatsProviderFactory statsFactory;

  @ConfigProperty(name = "ai.floedb.floecat.arrow.max-bytes", defaultValue = "1073741824")
  long arrowMaxBytes;

  private static final Logger LOG = Logger.getLogger(QuerySystemScanServiceImpl.class);

  private final ArrowScanPlanner arrowPlanner = new ArrowScanPlanner();
  private final ArrowSink arrowSink = new GrpcArrowSink();

  @Override
  public Multi<ScanSystemTableChunk> scanSystemTable(ScanSystemTableRequest request) {
    var L = LogHelper.start(LOG, "ScanSystemTable");
    GrpcContextUtil grpcCtx = GrpcContextUtil.capture();

    return Multi.createFrom()
        .<ScanSystemTableChunk>emitter(emitter -> grpcCtx.run(() -> execute(request, emitter)))
        .runSubscriptionOn(Infrastructure.getDefaultExecutor())
        .onFailure()
        .invoke(L::fail)
        .onCompletion()
        .invoke(L::ok);
  }

  private void execute(
      ScanSystemTableRequest request, MultiEmitter<? super ScanSystemTableChunk> emitter) {
    AtomicReference<String> correlationIdHolder = new AtomicReference<>("unknown");
    try {
      var principalContext = principal.get();
      correlationIdHolder.set(principalContext.getCorrelationId());
      authz.require(principalContext, "catalog.read");

      String queryId = mustNonEmpty(request.getQueryId(), "query_id", correlationIdHolder.get());
      var ctxOpt = queryStore.get(queryId);
      if (ctxOpt.isEmpty()) {
        throw GrpcErrors.notFound(
            correlationIdHolder.get(), QUERY_NOT_FOUND, Map.of("query_id", queryId));
      }
      var queryCtx = ctxOpt.get();

      if (!request.hasTableId()) {
        throw GrpcErrors.invalidArgument(
            correlationIdHolder.get(), SYSTEM_TABLE_ID_REQUIRED, Map.of());
      }

      ResourceId tableId = request.getTableId();
      SystemObjectScanner scanner = scanners.resolve(correlationIdHolder.get(), tableId);
      EngineContext engineCtx = engineContext.engineContext();
      var statsProvider = statsFactory.forQuery(queryCtx, correlationIdHolder.get());
      SystemObjectScanContext ctx =
          new SystemObjectScanContext(
              graph, null, queryCtx.getQueryDefaultCatalogId(), engineCtx, statsProvider);
      List<SchemaColumn> schema = scanner.schema();
      List<String> requiredColumns = request.getRequiredColumnsList();
      List<Predicate> predicates = request.getPredicatesList();
      OutputFormat format = request.getOutputFormat();
      if (format == OutputFormat.UNRECOGNIZED) {
        throw GrpcErrors.invalidArgument(
            correlationIdHolder.get(),
            SYSTEM_OUTPUT_FORMAT_UNRECOGNIZED,
            Map.of("output_format", format.toString()));
      }
      boolean arrowRequested = format != OutputFormat.ROWS;
      Expr arrowExpr =
          arrowRequested ? SystemRowFilter.EXPRESSION_PROVIDER.toExpr(predicates) : null;

      if (arrowRequested) {
        BufferAllocator allocator = new RootAllocator(arrowMaxBytes);
        ArrowScanPlan plan =
            arrowPlanner.plan(
                scanner, ctx, schema, predicates, requiredColumns, arrowExpr, allocator);
        arrowSink.sink(
            emitter,
            plan,
            allocator,
            t -> toStatus(t, requireNonNullElse(correlationIdHolder.get(), "unknown")));
        return;
      }

      try (Stream<SystemObjectRow> rows = scanner.scan(ctx);
          Stream<SystemObjectRow> filtered = SystemRowFilter.filter(rows, schema, predicates);
          Stream<SystemObjectRow> projected =
              SystemRowProjector.project(filtered, schema, requiredColumns)) {
        Iterator<SystemObjectRow> iterator = projected.iterator();
        while (!emitter.isCancelled() && iterator.hasNext()) {
          SystemTableRow row = SystemRowMappers.toProto(iterator.next());
          emitter.emit(ScanSystemTableChunk.newBuilder().setRow(row).build());
        }
        if (!emitter.isCancelled()) {
          emitter.complete();
        }
      }
    } catch (Throwable t) {
      emitter.fail(toStatus(t, requireNonNullElse(correlationIdHolder.get(), "unknown")));
    }
  }
}
