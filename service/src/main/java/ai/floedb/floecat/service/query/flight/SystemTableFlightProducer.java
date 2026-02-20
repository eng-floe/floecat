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

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.FIELD;
import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.QUERY_NOT_FOUND;

import ai.floedb.floecat.arrow.ArrowScanPlan;
import ai.floedb.floecat.common.rpc.Predicate;
import ai.floedb.floecat.flight.SystemTableFlightProducerBase;
import ai.floedb.floecat.flight.context.ResolvedCallContext;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.expr.Expr;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.scanner.spi.StatsProvider;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.scanner.spi.SystemObjectScanner;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.catalog.StatsProviderFactory;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.query.impl.arrow.ArrowScanPlanner;
import ai.floedb.floecat.service.query.resolver.SystemScannerResolver;
import ai.floedb.floecat.service.query.system.SystemRowFilter;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.system.rpc.SystemTableFlightCommand;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
public final class SystemTableFlightProducer extends SystemTableFlightProducerBase {

  private static final Logger LOG = Logger.getLogger(SystemTableFlightProducer.class);

  @Inject CatalogOverlay graph;
  @Inject SystemScannerResolver scannerResolver;
  @Inject QueryContextStore queryStore;
  @Inject StatsProviderFactory statsFactory;
  @Inject Authorizer authz;

  @ConfigProperty(name = "floecat.flight.host", defaultValue = "localhost")
  String flightHost;

  @ConfigProperty(name = "floecat.flight.port", defaultValue = "47470")
  int flightPort;

  private Location flightLocation;
  private final ArrowScanPlanner arrowPlanner = new ArrowScanPlanner();

  @PostConstruct
  void initFlightLocation() {
    flightLocation = Location.forGrpcInsecure(flightHost, flightPort);
  }

  @Override
  protected boolean supportsCommand(SystemTableFlightCommand command) {
    if (command == null || command.getTableId() == null) {
      return false;
    }
    return graph
        .resolve(command.getTableId())
        .filter(SystemTableNode.FloeCatSystemTableNode.class::isInstance)
        .isPresent();
  }

  @Override
  protected void authorize(ResolvedCallContext ctx) {
    authz.require(ctx.principalContext(), "catalog.read");
  }

  @Override
  protected List<SchemaColumn> resolveSchemaColumns(
      SystemTableFlightCommand command, ResolvedCallContext ctx) {
    return resolveScanner(command, ctx).schema();
  }

  @Override
  protected ArrowScanPlan buildPlan(
      SystemTableFlightCommand command, ResolvedCallContext ctx, BufferAllocator allocator) {
    String effectiveQueryId = requireQueryId(command, ctx);
    QueryContext queryCtx =
        queryStore
            .get(effectiveQueryId)
            .orElseThrow(
                () ->
                    GrpcErrors.notFound(
                        ctx.correlationId(),
                        QUERY_NOT_FOUND,
                        Map.of("query_id", effectiveQueryId)));

    StatsProvider statsProvider = statsFactory.forQuery(queryCtx, ctx.correlationId());
    SystemObjectScanner scanner = resolveScanner(command, ctx);
    List<String> requiredColumns = command.getRequiredColumnsList();
    List<Predicate> predicates = command.getPredicatesList();
    Expr arrowExpr = SystemRowFilter.EXPRESSION_PROVIDER.toExpr(predicates);
    SystemObjectScanContext scanContext =
        new SystemObjectScanContext(
            graph, null, queryCtx.getQueryDefaultCatalogId(), ctx.engineContext(), statsProvider);

    return arrowPlanner.plan(
        scanner, scanContext, scanner.schema(), predicates, requiredColumns, arrowExpr, allocator);
  }

  @Override
  protected Location selfLocation() {
    return flightLocation;
  }

  private SystemObjectScanner resolveScanner(
      SystemTableFlightCommand command, ResolvedCallContext ctx) {
    return scannerResolver.resolve(ctx.correlationId(), command.getTableId(), ctx.engineContext());
  }

  private String requireQueryId(SystemTableFlightCommand command, ResolvedCallContext ctx) {
    return requireQueryId(ctx.queryId(), command.getQueryId(), ctx.correlationId());
  }

  private static String requireQueryId(
      String headerQueryId, String commandQueryId, String correlationId) {
    boolean headerPresent = headerQueryId != null && !headerQueryId.isBlank();
    boolean commandPresent = commandQueryId != null && !commandQueryId.isBlank();
    if (!headerPresent && !commandPresent) {
      throw GrpcErrors.invalidArgument(correlationId, FIELD, Map.of("field", "query_id"));
    }
    if (headerPresent && commandPresent && !headerQueryId.equals(commandQueryId)) {
      throw CallStatus.INVALID_ARGUMENT
          .withDescription(
              "query_id mismatch: header x-query-id=\""
                  + headerQueryId
                  + "\" does not match command query_id=\""
                  + commandQueryId
                  + "\"")
          .toRuntimeException();
    }
    return headerPresent ? headerQueryId : commandPresent ? commandQueryId : "";
  }
}
