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

import ai.floedb.floecat.arrow.ArrowScanPlan;
import ai.floedb.floecat.common.rpc.Predicate;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.flight.context.ResolvedCallContext;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.expr.Expr;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.scanner.spi.SystemObjectRow;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.scanner.spi.SystemObjectScanner;
import ai.floedb.floecat.scanner.spi.SystemScanRequest;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.GrpcContextUtil;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.context.EngineContextProvider;
import ai.floedb.floecat.service.context.impl.ResolvedCallContexts;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.catalog.ConstraintProviderFactory;
import ai.floedb.floecat.service.query.catalog.StatsProviderFactory;
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
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.PhaseDiagnostics;
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
  @Inject ConstraintProviderFactory constraintFactory;
  @Inject Observability observability;

  @ConfigProperty(name = "ai.floedb.floecat.arrow.max-bytes", defaultValue = "1073741824")
  long arrowMaxBytes;

  private static final Logger LOG = Logger.getLogger(QuerySystemScanServiceImpl.class);

  private final ArrowScanPlanner arrowPlanner = new ArrowScanPlanner();
  private final ArrowSink arrowSink = new GrpcArrowSink();

  @Override
  public Multi<ScanSystemTableChunk> scanSystemTable(ScanSystemTableRequest request) {
    var L = LogHelper.start(LOG, "ScanSystemTable");
    // Read the resolved call context at method entry — before the executor hop — and carry it by
    // reference into the body; the captured io.grpc.Context alone is unreliable across the hop
    // (eng-floe/floecat#361). The providers the body reads (principal, engine context) pull from
    // this scope.
    ResolvedCallContext callCtx = ResolvedCallContexts.currentOrUnauthenticated();
    GrpcContextUtil grpcCtx = GrpcContextUtil.capture();

    return Multi.createFrom()
        .<ScanSystemTableChunk>emitter(
            emitter ->
                grpcCtx.run(
                    () -> ResolvedCallContexts.runWith(callCtx, () -> execute(request, emitter))))
        .runSubscriptionOn(Infrastructure.getDefaultExecutor())
        .onFailure()
        .invoke(L::fail)
        .onCompletion()
        .invoke(L::ok);
  }

  private void execute(
      ScanSystemTableRequest request, MultiEmitter<? super ScanSystemTableChunk> emitter) {
    AtomicReference<String> correlationIdHolder = new AtomicReference<>("unknown");
    PhaseDiagnostics diagnostics = diagnostics("system_scan");
    long startedNanos = System.nanoTime();
    String outcome = "completed";
    try {
      var principalContext = diagnostics.time("principal_get", principal::get);
      correlationIdHolder.set(principalContext.getCorrelationId());
      diagnostics.time("authz", () -> authz.require(principalContext, "catalog.read"));

      String queryId = mustNonEmpty(request.getQueryId(), "query_id", correlationIdHolder.get());
      diagnostics.put("query_id", queryId);
      var ctxOpt = diagnostics.time("query_context_get", () -> queryStore.get(queryId));
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
      diagnostics.put("table_id", tableId.getId());
      EngineContext engineCtx = diagnostics.time("engine_context", engineContext::engineContext);
      SystemObjectScanner scanner =
          diagnostics.time(
              "scanner_resolve",
              () -> scanners.resolve(correlationIdHolder.get(), tableId, engineCtx, diagnostics));
      var statsProvider =
          diagnostics.time(
              "stats_provider",
              () -> statsFactory.forSystemScan(queryCtx, correlationIdHolder.get()));
      SystemObjectScanContext ctx =
          new SystemObjectScanContext(
              graph,
              null,
              queryCtx.getQueryDefaultCatalogId(),
              engineCtx,
              statsProvider,
              constraintFactory.provider());
      List<SchemaColumn> schema = diagnostics.time("schema", scanner::schema);
      List<String> requiredColumns = request.getRequiredColumnsList();
      List<Predicate> predicates = request.getPredicatesList();
      OutputFormat format = request.getOutputFormat();
      diagnostics.put("format", format.name());
      diagnostics.put("schema_columns", schema.size());
      diagnostics.put("required_columns", requiredColumns.size());
      diagnostics.put("predicates", predicates.size());
      if (format == OutputFormat.UNRECOGNIZED) {
        throw GrpcErrors.invalidArgument(
            correlationIdHolder.get(),
            SYSTEM_OUTPUT_FORMAT_UNRECOGNIZED,
            Map.of("output_format", format.toString()));
      }
      boolean arrowRequested = format != OutputFormat.ROWS;
      diagnostics.put("arrow_requested", arrowRequested);
      Expr predicateExpr =
          diagnostics.time(
              "predicate_build", () -> SystemRowFilter.EXPRESSION_PROVIDER.toExpr(predicates));
      SystemScanRequest scanRequest =
          diagnostics.time(
              "scan_request_build", () -> SystemScanRequest.of(predicateExpr, requiredColumns));

      if (arrowRequested) {
        BufferAllocator allocator =
            diagnostics.time("arrow_allocator", () -> new RootAllocator(arrowMaxBytes));
        ArrowScanPlan plan =
            diagnostics.time(
                "arrow_plan",
                () ->
                    arrowPlanner.plan(
                        scanner, ctx, schema, predicates, requiredColumns, scanRequest, allocator));
        diagnostics.time(
            "arrow_sink",
            () ->
                arrowSink.sink(
                    emitter,
                    plan,
                    allocator,
                    t -> toStatus(t, requireNonNullElse(correlationIdHolder.get(), "unknown"))));
        if (emitter.isCancelled()) {
          outcome = "cancelled";
        }
        return;
      }

      try (var ignored = diagnostics.timer("row_scan");
          Stream<SystemObjectRow> rows = scanner.scan(ctx, scanRequest);
          Stream<SystemObjectRow> filtered = SystemRowFilter.filter(rows, schema, predicates);
          Stream<SystemObjectRow> projected =
              SystemRowProjector.project(filtered, schema, requiredColumns)) {
        Iterator<SystemObjectRow> iterator = projected.iterator();
        long emittedRows = 0;
        while (!emitter.isCancelled() && iterator.hasNext()) {
          SystemTableRow row = SystemRowMappers.toProto(iterator.next());
          emitter.emit(ScanSystemTableChunk.newBuilder().setRow(row).build());
          emittedRows++;
        }
        diagnostics.put("rows_emitted", emittedRows);
        if (!emitter.isCancelled()) {
          emitter.complete();
        } else {
          outcome = "cancelled";
        }
      }
    } catch (Throwable t) {
      outcome = "failed";
      diagnostics.put("error", t.getClass().getSimpleName());
      emitter.fail(toStatus(t, requireNonNullElse(correlationIdHolder.get(), "unknown")));
    } finally {
      diagnostics.put("correlation_id", correlationIdHolder.get());
      diagnostics.put("outcome", outcome);
      diagnostics.nanos("total", System.nanoTime() - startedNanos);
      diagnostics.emit("floecat.system_scan.summary");
    }
  }

  private PhaseDiagnostics diagnostics(String operation) {
    return observability == null
        ? PhaseDiagnostics.NOOP
        : observability.diagnostics("service", operation);
  }
}
