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

package ai.floedb.floecat.service.query.flight;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.QUERY_NOT_FOUND;

import ai.floedb.floecat.arrow.ArrowScanPlan;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.Predicate;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.flight.FlightExecutor;
import ai.floedb.floecat.flight.SystemTableFlightProducerBase;
import ai.floedb.floecat.flight.context.ResolvedCallContext;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.expr.Expr;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.scanner.spi.StatsProvider;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.scanner.spi.SystemObjectScanner;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.catalog.StatsProviderFactory;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.query.impl.arrow.ArrowScanPlanner;
import ai.floedb.floecat.service.query.resolver.SystemScannerResolver;
import ai.floedb.floecat.service.query.system.SystemRowFilter;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.system.rpc.SystemTableFlightCommand;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import org.apache.arrow.flight.FlightProducer.CallContext;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/**
 * Arrow Flight producer for FLOECAT system tables.
 *
 * <p>The shared {@link SystemTableFlightProducerBase} handles the command/ticket lifecycle; this
 * class provides the FLOECAT-specific scanner resolution and authorization logic.
 */
@ApplicationScoped
public final class SystemTableFlightProducer extends SystemTableFlightProducerBase {

  private static final Logger LOG = Logger.getLogger(SystemTableFlightProducer.class);

  @Inject CatalogOverlay graph;
  @Inject SystemScannerResolver scannerResolver;
  @Inject QueryContextStore queryStore;
  @Inject StatsProviderFactory statsFactory;
  @Inject Authorizer authz;
  @Inject SystemNodeRegistry nodeRegistry;

  @ConfigProperty(name = "ai.floedb.floecat.arrow.max-bytes", defaultValue = "1073741824")
  long arrowMaxBytes;

  @ConfigProperty(name = "floecat.flight.host", defaultValue = "localhost")
  String flightHost;

  @ConfigProperty(name = "floecat.flight.port", defaultValue = "47470")
  int flightPort;

  private Location flightLocation;
  private final ArrowScanPlanner arrowPlanner = new ArrowScanPlanner();

  @Inject
  public SystemTableFlightProducer(
      FlightServerAllocator allocatorProvider, FlightExecutor flightExecutor) {
    super(allocatorProvider, flightExecutor);
  }

  @PostConstruct
  void initFlightLocation() {
    flightLocation = Location.forGrpcInsecure(flightHost, flightPort);
  }

  @Override
  protected Location selfLocation() {
    return flightLocation;
  }

  @Override
  protected long streamAllocatorLimit() {
    return arrowMaxBytes > 0 ? arrowMaxBytes : Long.MAX_VALUE;
  }

  @Override
  protected ResolvedCallContext resolveCallContext(CallContext context) {
    InboundContextFlightMiddleware mw = context.getMiddleware(InboundContextFlightMiddleware.KEY);
    if (mw != null) {
      return mw.callContext();
    }
    return ResolvedCallContext.unauthenticated();
  }

  @Override
  protected void authorize(ResolvedCallContext ctx) {
    authz.require(ctx.principalContext(), "catalog.read");
  }

  @Override
  protected List<SchemaColumn> schemaColumns(
      String tableName,
      Optional<ResourceId> tableId,
      SystemTableFlightCommand command,
      ResolvedCallContext context) {
    ResourceId resolvedTableId = requireTableId(tableId, tableName);
    return resolveScanner(resolvedTableId, context).schema();
  }

  @Override
  protected ArrowScanPlan buildPlan(
      String tableName,
      Optional<ResourceId> tableId,
      SystemTableFlightCommand command,
      ResolvedCallContext context,
      BufferAllocator allocator,
      BooleanSupplier cancelled) {
    ResourceId resolvedTableId = requireTableId(tableId, tableName);
    String effectiveQueryId = requireQueryId(context, command);
    LOG.debugf(
        "getStream table=%s query=%s correlation=%s",
        resolvedTableId.getId(), effectiveQueryId, context.correlationId());

    QueryContext queryCtx =
        queryStore
            .get(effectiveQueryId)
            .orElseThrow(
                () ->
                    GrpcErrors.notFound(
                        context.correlationId(),
                        QUERY_NOT_FOUND,
                        Map.of("query_id", effectiveQueryId)));
    StatsProvider statsProvider = statsFactory.forQuery(queryCtx, context.correlationId());
    SystemObjectScanner scanner = resolveScanner(resolvedTableId, context);
    List<String> requiredColumns = command.getRequiredColumnsList();
    List<Predicate> predicates = command.getPredicatesList();
    Expr arrowExpr = SystemRowFilter.EXPRESSION_PROVIDER.toExpr(predicates);
    SystemObjectScanContext scanContext =
        new SystemObjectScanContext(
            graph,
            null,
            queryCtx.getQueryDefaultCatalogId(),
            context.engineContext(),
            statsProvider);

    return arrowPlanner.plan(
        scanner, scanContext, scanner.schema(), predicates, requiredColumns, arrowExpr, allocator);
  }

  @Override
  protected boolean supportsResolvedHandle(SystemTableHandle handle, ResolvedCallContext context) {
    if (!super.supportsResolvedHandle(handle, context)) {
      return false;
    }
    Optional<ResourceId> tableId = handle.tableId();
    if (tableId.isEmpty()) {
      return false;
    }
    return graph
        .resolve(tableId.get())
        .filter(SystemTableNode.FloeCatSystemTableNode.class::isInstance)
        .isPresent();
  }

  @Override
  protected Collection<String> tableNames(ResolvedCallContext context) {
    EngineContext ctx = context.engineContext();
    var nodes = nodeRegistry.nodesFor(ctx);
    if (nodes == null) {
      return List.of();
    }
    return List.copyOf(nodes.tableNames().keySet());
  }

  @Override
  protected Optional<ResourceId> resolveSystemTableId(NameRef name, ResolvedCallContext context) {
    return graph.resolveSystemTable(name);
  }

  @Override
  protected Optional<String> resolveSystemTableName(ResourceId id, ResolvedCallContext context) {
    return graph.resolveSystemTableName(id).map(NameRefUtil::canonical);
  }

  private SystemObjectScanner resolveScanner(ResourceId tableId, ResolvedCallContext ctx) {
    return scannerResolver.resolve(ctx.correlationId(), tableId, ctx.engineContext());
  }

  private static ResourceId requireTableId(Optional<ResourceId> tableId, String tableName) {
    return tableId.orElseThrow(
        () -> new IllegalStateException("System table id is required for " + tableName));
  }
}
