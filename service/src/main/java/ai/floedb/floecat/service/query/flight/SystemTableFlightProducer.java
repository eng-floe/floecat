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
import ai.floedb.floecat.service.telemetry.ServiceMetrics;
import ai.floedb.floecat.system.rpc.SystemTableFlightCommand;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.flight.FlightProducer.CallContext;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
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
  @Inject Observability observability;

  @ConfigProperty(name = "ai.floedb.floecat.arrow.max-bytes", defaultValue = "1073741824")
  long arrowMaxBytes;

  @ConfigProperty(name = "floecat.flight.host", defaultValue = "localhost")
  String flightHost;

  @ConfigProperty(name = "floecat.flight.port", defaultValue = "47470")
  int flightPort;

  private Location flightLocation;
  private final ArrowScanPlanner arrowPlanner = new ArrowScanPlanner();
  private final AtomicInteger inflightStreams = new AtomicInteger();

  @Inject
  public SystemTableFlightProducer(
      FlightServerAllocator allocatorProvider, FlightExecutor flightExecutor) {
    super(allocatorProvider, flightExecutor);
  }

  @PostConstruct
  void initFlightLocation() {
    flightLocation = Location.forGrpcInsecure(flightHost, flightPort);
    observability.gauge(
        ServiceMetrics.Flight.INFLIGHT,
        inflightStreams::get,
        "Current number of in-flight Flight streams",
        Tag.of(TagKey.COMPONENT, "service"),
        Tag.of(TagKey.OPERATION, "getStream"));
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

  @Override
  protected void onGetFlightInfoSuccess(
      ResolvedCallContext context, String tableName, long elapsedNanos) {
    recordOutcome("getFlightInfo", tableName, "success", null, elapsedNanos);
  }

  @Override
  protected void onGetFlightInfoError(
      ResolvedCallContext context, String tableName, Throwable error, long elapsedNanos) {
    recordFailure("getFlightInfo", tableName, error, elapsedNanos);
  }

  @Override
  protected void onGetStreamDispatchError(
      ResolvedCallContext context, Throwable error, long elapsedNanos) {
    recordFailure("getStream", "unknown", error, elapsedNanos);
  }

  @Override
  protected void onGetStreamStart(ResolvedCallContext context, String tableName) {
    inflightStreams.incrementAndGet();
  }

  @Override
  protected void onGetStreamSuccess(
      ResolvedCallContext context, String tableName, long elapsedNanos) {
    inflightStreams.updateAndGet(current -> Math.max(0, current - 1));
    recordOutcome("getStream", tableName, "success", null, elapsedNanos);
  }

  @Override
  protected void onGetStreamCancelled(
      ResolvedCallContext context, String tableName, long elapsedNanos) {
    inflightStreams.updateAndGet(current -> Math.max(0, current - 1));
    recordOutcome("getStream", tableName, "cancelled", "cancelled", elapsedNanos);
    observability.counter(
        ServiceMetrics.Flight.CANCELLED,
        1,
        metricTags("getStream", tableName, "cancelled", "cancelled"));
  }

  @Override
  protected void onGetStreamError(
      ResolvedCallContext context, String tableName, Throwable error, long elapsedNanos) {
    inflightStreams.updateAndGet(current -> Math.max(0, current - 1));
    recordFailure("getStream", tableName, error, elapsedNanos);
  }

  private void recordFailure(
      String operation, String tableName, Throwable error, long elapsedNanos) {
    String reason = failureReason(error);
    String status = "error";
    if ("cancelled".equals(reason)) {
      status = "cancelled";
      observability.counter(
          ServiceMetrics.Flight.CANCELLED,
          1,
          metricTags(operation, tableName, status, reason));
    } else {
      observability.counter(
          ServiceMetrics.Flight.ERRORS, 1, metricTags(operation, tableName, status, reason));
    }
    recordOutcome(operation, tableName, status, reason, elapsedNanos);
  }

  private void recordOutcome(
      String operation, String tableName, String status, String reason, long elapsedNanos) {
    observability.counter(
        ServiceMetrics.Flight.REQUESTS, 1, metricTags(operation, tableName, status, reason));
    observability.timer(
        ServiceMetrics.Flight.LATENCY,
        Duration.ofNanos(Math.max(0, elapsedNanos)),
        metricTags(operation, tableName, status, reason));
  }

  private static Tag[] metricTags(String operation, String tableName, String status, String reason) {
    List<Tag> tags = new ArrayList<>(5);
    tags.add(Tag.of(TagKey.COMPONENT, "service"));
    tags.add(Tag.of(TagKey.OPERATION, operation));
    tags.add(Tag.of(TagKey.STATUS, status));
    tags.add(Tag.of(TagKey.RESOURCE, normalizeResource(tableName)));
    if (reason != null && !reason.isBlank()) {
      tags.add(Tag.of(TagKey.REASON, reason));
    }
    return tags.toArray(Tag[]::new);
  }

  private static String normalizeResource(String tableName) {
    if (tableName == null || tableName.isBlank()) {
      return "unknown";
    }
    return tableName;
  }

  private static String failureReason(Throwable error) {
    Throwable current = error;
    while (current != null) {
      if (current instanceof FlightRuntimeException flight) {
        return mapFlightCode(flight.status().code());
      }
      if (current instanceof StatusRuntimeException grpc) {
        return mapGrpcCode(grpc.getStatus().getCode());
      }
      current = current.getCause();
    }
    return "internal";
  }

  private static String mapFlightCode(FlightStatusCode code) {
    if (code == null) {
      return "internal";
    }
    return switch (code) {
      case CANCELLED -> "cancelled";
      case UNAVAILABLE, TIMED_OUT -> "unavailable";
      case NOT_FOUND -> "not_found";
      case INVALID_ARGUMENT -> "invalid_argument";
      case UNAUTHENTICATED -> "unauthenticated";
      case UNAUTHORIZED -> "unauthorized";
      default -> "internal";
    };
  }

  private static String mapGrpcCode(Status.Code code) {
    if (code == null) {
      return "internal";
    }
    return switch (code) {
      case CANCELLED -> "cancelled";
      case UNAVAILABLE, DEADLINE_EXCEEDED -> "unavailable";
      case NOT_FOUND -> "not_found";
      case INVALID_ARGUMENT -> "invalid_argument";
      case UNAUTHENTICATED -> "unauthenticated";
      case PERMISSION_DENIED -> "unauthorized";
      default -> "internal";
    };
  }
}
