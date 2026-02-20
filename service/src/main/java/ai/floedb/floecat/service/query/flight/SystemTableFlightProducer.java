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

import ai.floedb.floecat.common.rpc.Predicate;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.expr.Expr;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.scanner.spi.StatsProvider;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.scanner.spi.SystemObjectScanner;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.service.context.impl.InboundCallContextHelper.ResolvedCallContext;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.catalog.StatsProviderFactory;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.query.impl.arrow.ArrowBatchSerializer;
import ai.floedb.floecat.service.query.impl.arrow.ArrowScanPlan;
import ai.floedb.floecat.service.query.impl.arrow.ArrowScanPlanner;
import ai.floedb.floecat.service.query.resolver.SystemScannerResolver;
import ai.floedb.floecat.service.query.system.SystemRowFilter;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.system.rpc.SystemTableFlightCommand;
import ai.floedb.floecat.system.rpc.SystemTableFlightTicket;
import com.google.protobuf.InvalidProtocolBufferException;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

/**
 * Arrow Flight producer for FLOECAT-kind system tables.
 *
 * <p>Implements the standard two-phase Flight protocol:
 *
 * <ol>
 *   <li>{@link #getFlightInfo}: decodes {@link SystemTableFlightCommand} from the descriptor
 *       command, resolves the projected schema via {@link ArrowBatchSerializer#schemaForColumns},
 *       and returns a {@link FlightInfo} with an opaque ticket (re-serialised command bytes wrapped
 *       in {@link SystemTableFlightTicket}). This call is cheap — no scanning, no IO.
 *   <li>{@link #getStream}: decodes the ticket, resolves the scanner, builds the Arrow execution
 *       plan via {@link ArrowScanPlanner}, and streams batches to the worker using {@link
 *       FlightArrowBatchSink}. Scanning runs on a worker-executor thread (not an event-loop
 *       thread).
 * </ol>
 *
 * <p>Both methods enforce {@code catalog.read} permission via {@link Authorizer#require} before
 * doing any work. Auth context is resolved by {@link InboundContextFlightMiddleware} (registered on
 * the {@link org.apache.arrow.flight.FlightServer}) which provides parity with the gRPC path's
 * {@code InboundContextInterceptor}.
 *
 * <p>Error mapping follows the plan contract: {@code INVALID_ARGUMENT} for bad/missing {@code
 * table_id} or unknown predicate operators; {@code NOT_FOUND} for unknown tables.
 *
 * <p>{@code query_id} in the command is optional and used for logging/correlation only. It never
 * affects scan output or auth.
 */
@ApplicationScoped
class SystemTableFlightProducer extends NoOpFlightProducer {

  private static final Logger LOG = Logger.getLogger(SystemTableFlightProducer.class);

  /** Version tag for {@link SystemTableFlightTicket}. */
  private static final int TICKET_VERSION = 1;

  @Inject CatalogOverlay graph;
  @Inject SystemScannerResolver scannerResolver;
  @Inject QueryContextStore queryStore;
  @Inject StatsProviderFactory statsFactory;
  @Inject FlightAllocatorHolder allocatorHolder;
  @Inject Authorizer authz;

  @ConfigProperty(name = "ai.floedb.floecat.arrow.max-bytes", defaultValue = "1073741824")
  long arrowMaxBytes;

  @ConfigProperty(name = "floecat.flight.host", defaultValue = "localhost")
  String flightHost;

  @ConfigProperty(name = "floecat.flight.port", defaultValue = "47470")
  int flightPort;

  private Location flightLocation;

  private final ArrowScanPlanner arrowPlanner = new ArrowScanPlanner();
  private final ExecutorService executor =
      Executors.newCachedThreadPool(
          r -> {
            Thread t = new Thread(r, "flight-scan");
            t.setDaemon(true);
            return t;
          });

  @PreDestroy
  void shutdownExecutor() {
    executor.shutdownNow();
  }

  @PostConstruct
  void initFlightLocation() {
    flightLocation = Location.forGrpcInsecure(flightHost, flightPort);
  }

  // -------------------------------------------------------------------------
  //  getFlightInfo — cheap, schema-only
  // -------------------------------------------------------------------------

  @Override
  public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
    try {
      ResolvedCallContext callCtx = resolvedCallContext(context);
      authz.require(callCtx.principalContext(), "catalog.read");

      SystemTableFlightCommand command = decodeCommand(descriptor);
      String correlationId = callCtx.correlationId();
      String queryId = resolveQueryId(callCtx.queryId(), command.getQueryId());
      LOG.debugf(
          "getFlightInfo table=%s query=%s correlation=%s",
          command.getTableId().getId(), queryId, correlationId);

      EngineContext engineCtx = callCtx.engineContext();
      SystemObjectScanner scanner =
          scannerResolver.resolve(correlationId, command.getTableId(), engineCtx);
      List<SchemaColumn> scannerSchema = scanner.schema();
      List<String> requiredColumns = command.getRequiredColumnsList();
      Schema projectedSchema =
          ArrowBatchSerializer.schemaForColumns(scannerSchema, requiredColumns);

      Ticket ticket = encodeTicket(command);
      FlightEndpoint endpoint = new FlightEndpoint(ticket, flightLocation);
      return FlightInfo.builder(projectedSchema, descriptor, List.of(endpoint)).build();
    } catch (Throwable t) {
      throw toFlightStatus(t).toRuntimeException();
    }
  }

  // -------------------------------------------------------------------------
  //  getStream — actual scanning
  // -------------------------------------------------------------------------

  @Override
  public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
    try {
      ResolvedCallContext callCtx = resolvedCallContext(context);
      authz.require(callCtx.principalContext(), "catalog.read");

      // Capture all per-call state from the dispatch thread BEFORE handing off to the executor;
      // CallContext must not be accessed from the worker thread.
      String correlationId = callCtx.correlationId();
      EngineContext engineCtx = callCtx.engineContext();
      // Snapshot the MDC set by InboundContextFlightMiddleware so worker-thread log lines carry
      // full context (JBoss MDC is not inherited across thread boundaries).
      Map<String, Object> mdcSnapshot = MDC.getMap();
      executor.submit(
          () ->
              streamOnWorkerThread(
                  ticket, listener, engineCtx, correlationId, callCtx.queryId(), mdcSnapshot));
    } catch (Throwable t) {
      listener.error(toFlightStatus(t).toRuntimeException());
    }
  }

  private void streamOnWorkerThread(
      Ticket ticket,
      ServerStreamListener listener,
      EngineContext engineCtx,
      String correlationId,
      String headerQueryId,
      Map<String, Object> mdcSnapshot) {
    // Restore MDC on the worker thread so scanner log lines carry full context.
    if (mdcSnapshot != null) {
      mdcSnapshot.forEach(MDC::put);
    }
    try {
      SystemTableFlightCommand command = decodeTicket(ticket);
      String effectiveQueryId = requireQueryId(headerQueryId, command.getQueryId(), correlationId);
      LOG.debugf(
          "getStream table=%s query=%s correlation=%s",
          command.getTableId().getId(), effectiveQueryId, correlationId);

      long streamCap = arrowMaxBytes > 0 ? arrowMaxBytes : Long.MAX_VALUE;
      BufferAllocator allocator =
          allocatorHolder.allocator().newChildAllocator("flight-stream", 0, streamCap);
      FlightArrowBatchSink sink = new FlightArrowBatchSink(listener, allocator);
      AtomicBoolean closed = new AtomicBoolean(false);
      Runnable cleanup =
          () -> {
            if (closed.compareAndSet(false, true)) {
              sink.close();
              try {
                allocator.close();
              } catch (Exception ignored) {
              }
            }
          };
      try {
        QueryContext queryCtx =
            queryStore
                .get(effectiveQueryId)
                .orElseThrow(
                    () ->
                        GrpcErrors.notFound(
                            correlationId, QUERY_NOT_FOUND, Map.of("query_id", effectiveQueryId)));
        StatsProvider statsProvider = statsFactory.forQuery(queryCtx, correlationId);
        SystemObjectScanner scanner =
            scannerResolver.resolve(correlationId, command.getTableId(), engineCtx);
        List<String> requiredColumns = command.getRequiredColumnsList();
        List<Predicate> predicates = command.getPredicatesList();
        Expr arrowExpr = SystemRowFilter.EXPRESSION_PROVIDER.toExpr(predicates);
        SystemObjectScanContext ctx =
            new SystemObjectScanContext(
                graph, null, queryCtx.getQueryDefaultCatalogId(), engineCtx, statsProvider);

        ArrowScanPlan plan =
            arrowPlanner.plan(
                scanner, ctx, scanner.schema(), predicates, requiredColumns, arrowExpr, allocator);

        ArrowBatchSerializer.serialize(plan, sink, listener::isCancelled, cleanup);
      } catch (Throwable t) {
        cleanup.run();
        listener.error(toFlightStatus(t).toRuntimeException());
      }
    } finally {
      MDC.clear();
    }
  }

  // -------------------------------------------------------------------------
  //  Call-context extraction
  // -------------------------------------------------------------------------

  /**
   * Extracts the {@link ResolvedCallContext} captured by {@link InboundContextFlightMiddleware} for
   * the current call.
   *
   * <p>The middleware always runs before any producer method (registered on the {@link
   * org.apache.arrow.flight.FlightServer} builder), so this should never return {@code null} in
   * production. If it does (e.g. in a test that bypasses the middleware), we return a default
   * context that will fail the {@code authz.require} check.
   */
  private static ResolvedCallContext resolvedCallContext(CallContext context) {
    InboundContextFlightMiddleware mw = context.getMiddleware(InboundContextFlightMiddleware.KEY);
    if (mw != null) {
      return mw.callContext();
    }
    // Middleware not registered — return a context that will fail authorization.
    return new ResolvedCallContext(
        ai.floedb.floecat.common.rpc.PrincipalContext.getDefaultInstance(),
        /* queryId */ "",
        /* correlationId */ "",
        EngineContext.empty(),
        /* sessionHeaderValue */ null,
        /* authorizationHeaderValue */ null);
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

  // -------------------------------------------------------------------------
  //  Encoding helpers
  // -------------------------------------------------------------------------

  private static SystemTableFlightCommand decodeCommand(FlightDescriptor descriptor) {
    if (!descriptor.isCommand()) {
      throw CallStatus.INVALID_ARGUMENT
          .withDescription("FlightDescriptor must be of type CMD")
          .toRuntimeException();
    }
    try {
      byte[] cmdBytes = descriptor.getCommand();
      return SystemTableFlightCommand.parseFrom(cmdBytes);
    } catch (InvalidProtocolBufferException e) {
      throw CallStatus.INVALID_ARGUMENT
          .withDescription("Cannot decode SystemTableFlightCommand: " + e.getMessage())
          .toRuntimeException();
    }
  }

  private static Ticket encodeTicket(SystemTableFlightCommand command) {
    SystemTableFlightTicket ticket =
        SystemTableFlightTicket.newBuilder()
            .setVersion(TICKET_VERSION)
            .setCmd(command.toByteString())
            .build();
    return new Ticket(ticket.toByteArray());
  }

  private static SystemTableFlightCommand decodeTicket(Ticket ticket) {
    try {
      SystemTableFlightTicket wrapper = SystemTableFlightTicket.parseFrom(ticket.getBytes());
      if (wrapper.getVersion() != TICKET_VERSION) {
        throw CallStatus.INVALID_ARGUMENT
            .withDescription("Unsupported ticket version: " + wrapper.getVersion())
            .toRuntimeException();
      }
      return SystemTableFlightCommand.parseFrom(wrapper.getCmd());
    } catch (InvalidProtocolBufferException e) {
      throw CallStatus.INVALID_ARGUMENT
          .withDescription("Cannot decode Flight ticket: " + e.getMessage())
          .toRuntimeException();
    }
  }

  /**
   * Resolves the effective query ID from the transport header and the command field.
   *
   * <p>The {@code x-query-id} header (set by the caller on the Flight call) is authoritative. The
   * {@code query_id} field embedded in the {@link SystemTableFlightCommand} proto must match it
   * when both are present — a mismatch indicates the client sent a command that belongs to a
   * different request context.
   *
   * <p>Either value may be absent (blank/empty); if both are present they must be equal.
   *
   * @throws FlightRuntimeException {@code INVALID_ARGUMENT} when both are present but differ
   */
  private static String resolveQueryId(String headerQueryId, String commandQueryId) {
    boolean headerPresent = headerQueryId != null && !headerQueryId.isBlank();
    boolean commandPresent = commandQueryId != null && !commandQueryId.isBlank();
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
    if (headerPresent) return headerQueryId;
    if (commandPresent) return commandQueryId;
    return "";
  }

  /** Maps gRPC/general exceptions to Arrow Flight call statuses. */
  private static CallStatus toFlightStatus(Throwable t) {
    if (t instanceof FlightRuntimeException fre) {
      return fre.status();
    }
    if (t instanceof io.grpc.StatusRuntimeException sre) {
      return switch (sre.getStatus().getCode()) {
        case NOT_FOUND -> CallStatus.NOT_FOUND.withDescription(sre.getMessage());
        case INVALID_ARGUMENT -> CallStatus.INVALID_ARGUMENT.withDescription(sre.getMessage());
        case UNAUTHENTICATED -> CallStatus.UNAUTHENTICATED.withDescription(sre.getMessage());
        case PERMISSION_DENIED -> CallStatus.UNAUTHORIZED.withDescription(sre.getMessage());
        case UNAVAILABLE -> CallStatus.UNAVAILABLE.withDescription(sre.getMessage());
        default -> CallStatus.INTERNAL.withDescription(sre.getMessage());
      };
    }
    return CallStatus.INTERNAL.withDescription(t.getMessage()).withCause(t);
  }
}
