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

package ai.floedb.floecat.flight;

import ai.floedb.floecat.arrow.ArrowBatchSerializer;
import ai.floedb.floecat.arrow.ArrowScanPlan;
import ai.floedb.floecat.arrow.ArrowSchemaUtil;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.flight.context.ResolvedCallContext;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.system.rpc.SystemTableFlightCommand;
import ai.floedb.floecat.system.rpc.SystemTableFlightTicket;
import ai.floedb.floecat.system.rpc.SystemTableTarget;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.PhaseDiagnostics;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer.CallContext;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.jboss.logging.Logger;

/** Shared base for Flight producers that speak the {@link SystemTableFlightCommand} protocol. */
public abstract class SystemTableFlightProducerBase extends NoOpFlightProducer
    implements FloecatRoutedFlightProducer {

  private static final int TICKET_VERSION = 1;

  private FlightAllocatorProvider allocatorProvider;
  private FlightExecutor flightExecutor;

  @Inject Observability observability;

  protected SystemTableFlightProducerBase() {}

  protected SystemTableFlightProducerBase(
      FlightAllocatorProvider allocatorProvider, FlightExecutor flightExecutor) {
    initFlightServices(allocatorProvider, flightExecutor);
  }

  protected SystemTableFlightProducerBase(
      BufferAllocator allocator, FlightExecutor flightExecutor) {
    this(() -> Objects.requireNonNull(allocator, "allocator"), flightExecutor);
  }

  @Inject
  protected void initFlightServices(
      FlightAllocatorProvider allocatorProvider, FlightExecutor flightExecutor) {
    this.allocatorProvider = Objects.requireNonNull(allocatorProvider, "allocatorProvider");
    this.flightExecutor = Objects.requireNonNull(flightExecutor, "flightExecutor");
  }

  private Set<String> supportedNames(ResolvedCallContext context) {
    ResolvedCallContext ctx = effectiveContext(context);
    Collection<String> names = tableNames(ctx);
    if (names == null || names.isEmpty()) {
      return Set.of();
    }
    LinkedHashSet<String> normalized =
        names.stream()
            .filter(Objects::nonNull)
            .map(SystemTableFlightProducerBase::normalizeTableName)
            .filter(name -> !name.isBlank())
            .collect(Collectors.toCollection(LinkedHashSet::new));
    return Set.copyOf(normalized);
  }

  protected record SystemTableHandle(String canonicalName, Optional<ResourceId> tableId) {}

  protected Logger logger() {
    return Logger.getLogger(getClass());
  }

  private PhaseDiagnostics diagnostics(String operation) {
    return observability == null
        ? PhaseDiagnostics.NOOP
        : observability.diagnostics("flight", operation);
  }

  // -----------------------------------------------------------------------
  // Abstract contract
  // -----------------------------------------------------------------------

  protected abstract ResolvedCallContext resolveCallContext(CallContext context);

  protected void authorize(ResolvedCallContext ctx) {}

  protected abstract Collection<String> tableNames(ResolvedCallContext context);

  protected ResolvedCallContext effectiveContext(ResolvedCallContext ctx) {
    return ctx == null ? ResolvedCallContext.unauthenticated() : ctx;
  }

  protected abstract List<SchemaColumn> schemaColumns(
      String tableName,
      Optional<ResourceId> tableId,
      SystemTableFlightCommand command,
      ResolvedCallContext context);

  protected abstract ArrowScanPlan buildPlan(
      String tableName,
      Optional<ResourceId> tableId,
      SystemTableFlightCommand command,
      ResolvedCallContext context,
      BufferAllocator allocator,
      BooleanSupplier cancelled);

  protected abstract Location selfLocation();

  protected long streamAllocatorLimit() {
    return Long.MAX_VALUE;
  }

  protected final boolean supportsCommand(
      SystemTableFlightCommand command, ResolvedCallContext context) {
    if (command == null || !command.hasTarget()) {
      return false;
    }
    ResolvedCallContext ctx = effectiveContext(context);
    Set<String> supported = supportedNames(ctx);
    if (supported.isEmpty()) {
      return false;
    }
    SystemTableTarget target = command.getTarget();
    if (target.hasName()) {
      String canonical = normalizeTableName(NameRefUtil.canonical(target.getName()));
      return !canonical.isBlank() && supported.contains(canonical);
    }
    if (target.hasId()) {
      return resolveHandleById(target.getId(), ctx).isPresent();
    }
    return false;
  }

  protected final Optional<SystemTableHandle> handleForCommand(SystemTableFlightCommand command) {
    return handleForCommand(command, null);
  }

  protected final Optional<SystemTableHandle> handleForCommand(
      SystemTableFlightCommand command, ResolvedCallContext context) {
    if (command == null || !command.hasTarget()) {
      return Optional.empty();
    }
    SystemTableTarget target = command.getTarget();
    if (target.hasId()) {
      return resolveHandleById(target.getId(), context);
    }
    if (target.hasName()) {
      return resolveHandleByName(target.getName(), context);
    }
    return Optional.empty();
  }

  protected boolean supportsResolvedHandle(SystemTableHandle handle, ResolvedCallContext context) {
    return handle != null
        && !handle.canonicalName().isBlank()
        && supportedNames(context).contains(handle.canonicalName());
  }

  protected boolean supportsResolvedHandle(SystemTableHandle handle) {
    return supportsResolvedHandle(handle, ResolvedCallContext.unauthenticated());
  }

  // -----------------------------------------------------------------------
  // Routing helpers
  // -----------------------------------------------------------------------

  @Override
  public boolean supportsDescriptor(FlightDescriptor descriptor) {
    return supportsDescriptor(null, descriptor);
  }

  @Override
  public boolean supportsDescriptor(CallContext context, FlightDescriptor descriptor) {
    ResolvedCallContext routingCtx = routingContext(context);
    return decodeCommandQuietly(descriptor)
        .map(cmd -> supportsCommand(cmd, routingCtx))
        .orElse(false);
  }

  @Override
  public boolean supportsTicket(Ticket ticket) {
    return supportsTicket(null, ticket);
  }

  @Override
  public boolean supportsTicket(CallContext context, Ticket ticket) {
    ResolvedCallContext routingCtx = routingContext(context);
    return decodeTicketCommandQuietly(ticket)
        .map(cmd -> supportsCommand(cmd, routingCtx))
        .orElse(false);
  }

  // -----------------------------------------------------------------------
  // Flight protocol
  // -----------------------------------------------------------------------

  @Override
  public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
    long startedNanos = System.nanoTime();
    ResolvedCallContext callCtx = ResolvedCallContext.unauthenticated();
    String tableName = null;
    try {
      callCtx = requireAuth(context);
      SystemTableFlightCommand command = decodeCommand(descriptor);
      requireQueryId(callCtx, command);
      SystemTableHandle handle = requireHandle(command, callCtx);
      if (!supportsResolvedHandle(handle, callCtx)) {
        throw CallStatus.NOT_FOUND
            .withDescription("No Flight producer registered for descriptor")
            .toRuntimeException();
      }
      tableName = handle.canonicalName();
      Optional<ResourceId> tableId = handle.tableId();
      SystemTableFlightCommand enriched = commandWithTableId(command, tableId);
      Schema projected = projectSchema(handle.canonicalName(), tableId, enriched, callCtx);
      Ticket ticket = encodeTicket(enriched);
      FlightEndpoint endpoint = new FlightEndpoint(ticket, selfLocation());
      ResolvedCallContext successCallCtx = callCtx;
      String successTableName = tableName;
      safeHook(
          "onGetFlightInfoSuccess",
          () ->
              onGetFlightInfoSuccess(
                  successCallCtx, successTableName, Math.max(0, System.nanoTime() - startedNanos)));
      return FlightInfo.builder(projected, descriptor, List.of(endpoint)).build();
    } catch (Throwable t) {
      ResolvedCallContext finalCallCtx = callCtx;
      String finalTableName = tableName;
      safeHook(
          "onGetFlightInfoError",
          () ->
              onGetFlightInfoError(
                  finalCallCtx, finalTableName, t, Math.max(0, System.nanoTime() - startedNanos)));
      throw toFlightStatus(t).toRuntimeException();
    }
  }

  @Override
  public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
    long startedNanos = System.nanoTime();
    ResolvedCallContext callCtx = ResolvedCallContext.unauthenticated();
    try {
      callCtx = requireAuth(context);
      ContextSnapshot snapshot = ContextSnapshot.capture();
      ResolvedCallContext streamCallCtx = callCtx;
      flightExecutor
          .executor()
          .submit(() -> streamOnWorkerThread(ticket, listener, streamCallCtx, snapshot));
    } catch (Throwable t) {
      ResolvedCallContext finalCallCtx = callCtx;
      safeHook(
          "onGetStreamDispatchError",
          () ->
              onGetStreamDispatchError(
                  finalCallCtx, t, Math.max(0, System.nanoTime() - startedNanos)));
      listener.error(toFlightStatus(t).toRuntimeException());
    }
  }

  private void streamOnWorkerThread(
      Ticket ticket,
      ServerStreamListener listener,
      ResolvedCallContext callCtx,
      ContextSnapshot snapshot) {
    AutoCloseable snapshotScope = null;
    long startedNanos = System.nanoTime();
    String tableName = null;
    Throwable failure = null;
    boolean cancelled = false;
    boolean streamStarted = false;
    Span streamSpan = null;
    Scope streamScope = null;
    String effectiveQueryId = null;
    FlightArrowBatchSink sink = null;
    FlightStreamTelemetry telemetry = new FlightStreamTelemetry();
    try {
      snapshotScope = snapshot.apply();
      SystemTableFlightCommand command = decodeTicket(ticket);
      SystemTableHandle handle = requireHandle(command, callCtx);
      if (!supportsResolvedHandle(handle, callCtx)) {
        throw CallStatus.NOT_FOUND
            .withDescription("No Flight producer registered for ticket: " + ticket)
            .toRuntimeException();
      }
      tableName = handle.canonicalName();
      Optional<ResourceId> tableId = handle.tableId();
      String tableIdLog = tableId.map(ResourceId::getId).orElse("<name-only>");
      effectiveQueryId = requireQueryId(callCtx, command);
      logger()
          .debugf(
              "getStream table=%s name=%s query=%s correlation=%s",
              tableIdLog, tableName, effectiveQueryId, callCtx.correlationId());
      streamSpan =
          startSpan("floecat.flight.get_stream.worker", tableName, effectiveQueryId, callCtx);
      if (streamSpan != null) {
        streamScope = streamSpan.makeCurrent();
      }
      streamStarted = true;
      String finalTableName = tableName;
      safeHook("onGetStreamStart", () -> onGetStreamStart(callCtx, finalTableName));

      long allocatorStartNanos = System.nanoTime();
      BufferAllocator allocator;
      try {
        allocator = createStreamAllocator();
      } finally {
        telemetry.addAllocatorNanos(System.nanoTime() - allocatorStartNanos);
      }
      sink = new FlightArrowBatchSink(listener, allocator);
      FlightArrowBatchSink streamSink = sink;
      AtomicBoolean cleanupCalled = new AtomicBoolean(false);
      BufferAllocator streamAllocator = allocator;
      Runnable cleanup =
          () -> {
            if (cleanupCalled.compareAndSet(false, true)) {
              long cleanupStartNanos = System.nanoTime();
              try {
                streamSink.close();
                closeAllocator(streamAllocator);
              } finally {
                telemetry.addCleanupNanos(System.nanoTime() - cleanupStartNanos);
              }
            }
          };
      ArrowScanPlan plan;
      long buildPlanStartNanos = System.nanoTime();
      try {
        plan =
            withSpan(
                "floecat.flight.build_plan",
                tableName,
                effectiveQueryId,
                callCtx,
                () ->
                    buildPlan(
                        finalTableName,
                        tableId,
                        command,
                        callCtx,
                        allocator,
                        listener::isCancelled));
      } finally {
        telemetry.addBuildPlanNanos(System.nanoTime() - buildPlanStartNanos);
      }
      long serializeStartNanos = System.nanoTime();
      try {
        runWithSpan(
            "floecat.flight.serialize",
            tableName,
            effectiveQueryId,
            callCtx,
            () -> ArrowBatchSerializer.serialize(plan, streamSink, listener::isCancelled, cleanup));
        telemetry.addSerializeNanos(System.nanoTime() - serializeStartNanos);
        cancelled = listener.isCancelled();
      } catch (Throwable t) {
        telemetry.addSerializeNanos(System.nanoTime() - serializeStartNanos);
        failure = t;
        if (cleanupCalled.compareAndSet(false, true)) {
          long cleanupStartNanos = System.nanoTime();
          sink.close();
          closeAllocator(allocator);
          telemetry.addCleanupNanos(System.nanoTime() - cleanupStartNanos);
        }
        throw t;
      }
    } catch (Throwable t) {
      if (failure == null) {
        failure = t;
      }
      listener.error(toFlightStatus(t).toRuntimeException());
    } finally {
      long elapsedNanos = Math.max(0, System.nanoTime() - startedNanos);
      String finalTableName = tableName;
      if (failure != null) {
        Throwable finalFailure = failure;
        safeHook(
            "onGetStreamError",
            () -> onGetStreamError(callCtx, finalTableName, finalFailure, elapsedNanos));
      } else if (streamStarted && cancelled) {
        safeHook(
            "onGetStreamCancelled",
            () -> onGetStreamCancelled(callCtx, finalTableName, elapsedNanos));
      } else if (streamStarted) {
        safeHook(
            "onGetStreamSuccess", () -> onGetStreamSuccess(callCtx, finalTableName, elapsedNanos));
      }
      try {
        if (streamSpan != null) {
          streamSpan.setAttribute("flight.cancelled", cancelled);
          if (failure != null) {
            recordSpanError(streamSpan, failure);
          }
        }
        PhaseDiagnostics diagnostics = diagnostics("get_stream");
        emitFlightSummaryEvent(
            diagnostics,
            streamSpan,
            callCtx,
            finalTableName,
            effectiveQueryId,
            elapsedNanos,
            cancelled,
            failure,
            sink,
            telemetry);
      } catch (Throwable telemetryFailure) {
        logger().debugf(telemetryFailure, "Failed to finish Flight stream telemetry");
      }
      if (streamScope != null) {
        try {
          streamScope.close();
        } catch (Throwable telemetryFailure) {
          logger().debugf(telemetryFailure, "Failed to close Flight stream telemetry scope");
        }
      }
      if (streamSpan != null) {
        try {
          streamSpan.end();
        } catch (Throwable telemetryFailure) {
          logger().debugf(telemetryFailure, "Failed to end Flight stream telemetry span");
        }
      }
      if (snapshotScope != null) {
        try {
          snapshotScope.close();
        } catch (Exception ignored) {
        }
      }
    }
  }

  private static Tracer tracer() {
    return io.opentelemetry.api.GlobalOpenTelemetry.getTracer("ai.floedb.floecat.flight");
  }

  private static Span startSpan(
      String name, String tableName, String queryId, ResolvedCallContext context) {
    Span parent = Span.current();
    if (!parent.getSpanContext().isValid() || !parent.isRecording()) {
      return null;
    }
    var builder =
        tracer().spanBuilder(name).setAttribute("floecat.component", "flight");
    if (tableName != null && !tableName.isBlank()) {
      builder.setAttribute("system.table", tableName);
    }
    if (queryId != null && !queryId.isBlank()) {
      builder.setAttribute("query.id", queryId);
    }
    if (context != null && context.correlationId() != null && !context.correlationId().isBlank()) {
      builder.setAttribute("correlation.id", context.correlationId());
    }
    return builder.startSpan();
  }

  private static void emitFlightSummaryEvent(
      PhaseDiagnostics diagnostics,
      Span span,
      ResolvedCallContext context,
      String tableName,
      String queryId,
      long elapsedNanos,
      boolean cancelled,
      Throwable failure,
      FlightArrowBatchSink sink,
      FlightStreamTelemetry telemetry) {
    String outcome = failure == null ? (cancelled ? "cancelled" : "completed") : "failed";
    long rows = sink == null ? 0L : sink.rows();
    int batches = sink == null ? 0 : sink.batches();
    long copyNanos = sink == null ? 0L : sink.copyNanos();
    long putNextNanos = sink == null ? 0L : sink.putNextNanos();
    long completeNanos = sink == null ? 0L : sink.completeNanos();
    boolean emptyStream = sink != null && sink.completedEmptyStream();
    long schedulingNanos =
        Math.max(
            0L,
            elapsedNanos
                - telemetry.allocatorNanos()
                - telemetry.buildPlanNanos()
                - telemetry.serializeNanos()
                - telemetry.cleanupNanos());

    if (span != null && span.getSpanContext().isValid()) {
      span.setAttribute("floecat.flight.outcome", outcome);
      span.setAttribute("floecat.flight.duration_ms", millis(elapsedNanos));
      span.setAttribute("floecat.flight.rows", rows);
      span.setAttribute("floecat.flight.batches", batches);
      span.setAttribute("floecat.flight.empty_stream", emptyStream);
    }

    diagnostics.put("query_id", safe(queryId));
    diagnostics.put("correlation_id", safe(context == null ? "" : context.correlationId()));
    diagnostics.put("system_table", safe(tableName));
    diagnostics.put("outcome", outcome);
    diagnostics.put("cancelled", cancelled);
    diagnostics.put("rows", rows);
    diagnostics.put("batches", batches);
    diagnostics.put("empty_stream", emptyStream);
    diagnostics.nanos("total", elapsedNanos);
    diagnostics.nanos("allocator", telemetry.allocatorNanos());
    diagnostics.nanos("build_plan", telemetry.buildPlanNanos());
    diagnostics.nanos("serialize", telemetry.serializeNanos());
    diagnostics.nanos("sink_copy", copyNanos);
    diagnostics.nanos("sink_put_next", putNextNanos);
    diagnostics.nanos("sink_complete", completeNanos);
    diagnostics.nanos("cleanup", telemetry.cleanupNanos());
    diagnostics.nanos("scheduling", schedulingNanos);
    diagnostics.emit("floecat.flight.summary");
  }

  private static double millis(long nanos) {
    return Math.max(0L, nanos) / 1_000_000.0;
  }

  private static String safe(String value) {
    return value == null ? "" : value;
  }

  private static <T> T withSpan(
      String name, String tableName, String queryId, ResolvedCallContext context, SpanSupplier<T> op) {
    Span span = startSpan(name, tableName, queryId, context);
    if (span == null) {
      return op.get();
    }
    try (Scope ignored = span.makeCurrent()) {
      return op.get();
    } catch (RuntimeException e) {
      recordSpanError(span, e);
      throw e;
    } catch (Error e) {
      recordSpanError(span, e);
      throw e;
    } finally {
      span.end();
    }
  }

  private static void runWithSpan(
      String name, String tableName, String queryId, ResolvedCallContext context, SpanRunnable op) {
    Span span = startSpan(name, tableName, queryId, context);
    if (span == null) {
      op.run();
      return;
    }
    try (Scope ignored = span.makeCurrent()) {
      op.run();
    } catch (RuntimeException e) {
      recordSpanError(span, e);
      throw e;
    } catch (Error e) {
      recordSpanError(span, e);
      throw e;
    } finally {
      span.end();
    }
  }

  private static void recordSpanError(Span span, Throwable t) {
    span.recordException(t);
    span.setStatus(StatusCode.ERROR);
  }

  @FunctionalInterface
  private interface SpanSupplier<T> {
    T get();
  }

  @FunctionalInterface
  private interface SpanRunnable {
    void run();
  }

  private static final class FlightStreamTelemetry {
    private long allocatorNanos;
    private long buildPlanNanos;
    private long serializeNanos;
    private long cleanupNanos;

    private void addAllocatorNanos(long nanos) {
      allocatorNanos += Math.max(0L, nanos);
    }

    private long allocatorNanos() {
      return allocatorNanos;
    }

    private void addBuildPlanNanos(long nanos) {
      buildPlanNanos += Math.max(0L, nanos);
    }

    private long buildPlanNanos() {
      return buildPlanNanos;
    }

    private void addSerializeNanos(long nanos) {
      serializeNanos += Math.max(0L, nanos);
    }

    private long serializeNanos() {
      return serializeNanos;
    }

    private void addCleanupNanos(long nanos) {
      cleanupNanos += Math.max(0L, nanos);
    }

    private long cleanupNanos() {
      return cleanupNanos;
    }
  }

  private BufferAllocator createStreamAllocator() {
    long limit = streamAllocatorLimit();
    long cap = limit > 0 ? limit : Long.MAX_VALUE;
    return allocatorProvider.allocator().newChildAllocator("flight-stream", 0, cap);
  }

  private void closeAllocator(BufferAllocator allocator) {
    try {
      allocator.close();
    } catch (Exception ignored) {
    }
  }

  protected final ResolvedCallContext requireAuth(CallContext context) {
    ResolvedCallContext resolved = resolveCallContext(context);
    authorize(resolved);
    return resolved;
  }

  private ResolvedCallContext routingContext(CallContext context) {
    if (context == null) {
      return ResolvedCallContext.unauthenticated();
    }
    try {
      return effectiveContext(resolveCallContext(context));
    } catch (Throwable ignored) {
      return ResolvedCallContext.unauthenticated();
    }
  }

  private Schema projectSchema(
      String tableName,
      Optional<ResourceId> tableId,
      SystemTableFlightCommand command,
      ResolvedCallContext context) {
    List<SchemaColumn> columns = schemaColumns(tableName, tableId, command, context);
    if (columns == null) {
      throw new IllegalStateException("Schema columns must not be null");
    }
    return ArrowBatchSerializer.schemaForColumns(columns, command.getRequiredColumnsList());
  }

  protected final FlightDescriptor descriptorForTable(
      String tableName, ResolvedCallContext context) {
    requireKnownTable(tableName, context);
    return SystemTableCommands.descriptor(tableName, descriptorQueryId(context));
  }

  private static String descriptorQueryId(ResolvedCallContext context) {
    String queryId = context == null ? null : context.queryId();
    if (queryId != null && !queryId.isBlank()) {
      return queryId;
    }
    String correlationId = context == null ? null : context.correlationId();
    if (correlationId != null && !correlationId.isBlank()) {
      return correlationId;
    }
    return UUID.randomUUID().toString();
  }

  private SystemTableHandle requireHandle(
      SystemTableFlightCommand command, ResolvedCallContext context) {
    return handleForCommand(command, context)
        .orElseThrow(
            () ->
                CallStatus.NOT_FOUND
                    .withDescription("Unsupported system table: " + command)
                    .toRuntimeException());
  }

  private void requireKnownTable(String tableName) {
    requireKnownTable(tableName, ResolvedCallContext.unauthenticated());
  }

  private void requireKnownTable(String tableName, ResolvedCallContext context) {
    ResolvedCallContext ctx = effectiveContext(context);
    if (tableName == null || !supportedNames(ctx).contains(normalizeTableName(tableName))) {
      throw new IllegalArgumentException("Unknown system table: " + tableName);
    }
  }

  public static List<SchemaColumn> schemaColumnsFromSchema(Schema schema) {
    if (schema == null) {
      return List.of();
    }
    List<SchemaColumn> columns = new ArrayList<>(schema.getFields().size());
    int ordinal = 1;
    for (Field field : schema.getFields()) {
      columns.add(
          SchemaColumn.newBuilder()
              .setName(field.getName())
              .setLogicalType(ArrowSchemaUtil.logicalType(field))
              .setFieldId(ordinal)
              .setNullable(field.isNullable())
              .setOrdinal(ordinal)
              .setLeaf(true)
              .build());
      ordinal++;
    }
    return List.copyOf(columns);
  }

  protected Optional<ResourceId> resolveSystemTableId(NameRef name, ResolvedCallContext context) {
    return Optional.empty();
  }

  protected Optional<String> resolveSystemTableName(ResourceId id, ResolvedCallContext context) {
    return Optional.empty();
  }

  /**
   * Candidate engine kinds used when deriving deterministic system-table IDs from table names.
   *
   * <p>The request's engine headers (if any) are the source of truth. We include normalized forms
   * and always keep floecat_internal as a fallback namespace.
   */
  protected Collection<String> systemIdEngineKinds(ResolvedCallContext context) {
    EngineContext engineContext = context == null ? EngineContext.empty() : context.engineContext();
    return List.of(engineContext.effectiveEngineKind());
  }

  private Optional<SystemTableHandle> resolveHandleById(
      ResourceId id, ResolvedCallContext context) {
    if (id == null || id.getId() == null || id.getId().isBlank()) {
      return Optional.empty();
    }
    ResolvedCallContext ctx = effectiveContext(context);
    Set<String> supported = supportedNames(ctx);
    return resolveSystemTableName(id, context)
        .map(SystemTableFlightProducerBase::normalizeTableName)
        .filter(supported::contains)
        .map(
            name -> {
              Optional<ResourceId> resolvedId =
                  resolveSystemTableId(NameRefUtil.fromCanonical(name), context);
              return new SystemTableHandle(name, resolvedId.or(() -> Optional.of(id)));
            })
        .or(() -> resolveFromSupportedNames(id, supported, context))
        .or(() -> resolveByGeneratedSystemIds(id, supported, context));
  }

  private Optional<SystemTableHandle> resolveHandleByName(
      NameRef name, ResolvedCallContext context) {
    if (name == null || NameRefUtil.canonical(name).isBlank()) {
      return Optional.empty();
    }
    String canonical = NameRefUtil.canonical(name);
    ResolvedCallContext ctx = effectiveContext(context);
    if (!supportedNames(ctx).contains(canonical)) {
      return Optional.empty();
    }
    Optional<ResourceId> resolvedId =
        resolveSystemTableId(NameRefUtil.fromCanonical(canonical), context);
    return Optional.of(new SystemTableHandle(canonical, resolvedId));
  }

  private Optional<SystemTableHandle> resolveFromSupportedNames(
      ResourceId id, Set<String> supported, ResolvedCallContext context) {
    if (supported.isEmpty()) {
      return Optional.empty();
    }
    for (String tableName : supported) {
      Optional<ResourceId> candidate =
          resolveSystemTableId(NameRefUtil.fromCanonical(tableName), context);
      if (candidate.filter(candidateId -> resourceIdsEqual(candidateId, id)).isPresent()) {
        return Optional.of(new SystemTableHandle(tableName, candidate));
      }
    }
    return Optional.empty();
  }

  private Optional<SystemTableHandle> resolveByGeneratedSystemIds(
      ResourceId id, Set<String> supported, ResolvedCallContext context) {
    if (supported.isEmpty()) {
      return Optional.empty();
    }
    ResolvedCallContext ctx = effectiveContext(context);
    for (String tableName : supported) {
      for (String engineKind : systemIdEngineKinds(ctx)) {
        ResourceId candidate =
            SystemNodeRegistry.resourceId(engineKind, ResourceKind.RK_TABLE, tableName);
        if (resourceIdsEqual(candidate, id)) {
          Optional<ResourceId> resolvedId =
              resolveSystemTableId(NameRefUtil.fromCanonical(tableName), context);
          return Optional.of(
              new SystemTableHandle(tableName, resolvedId.or(() -> Optional.of(candidate))));
        }
      }
    }
    return Optional.empty();
  }

  private static boolean resourceIdsEqual(ResourceId a, ResourceId b) {
    if (a == null || b == null) {
      return false;
    }
    if (a.getId() == null || b.getId() == null) {
      return false;
    }
    if (!a.getId().equals(b.getId())) {
      return false;
    }
    return kindsCompatible(a.getKind(), b.getKind());
  }

  private static boolean kindsCompatible(ResourceKind left, ResourceKind right) {
    return left == ResourceKind.RK_UNSPECIFIED
        || right == ResourceKind.RK_UNSPECIFIED
        || left == right;
  }

  private SystemTableFlightCommand commandWithTableId(
      SystemTableFlightCommand command, Optional<ResourceId> tableId) {
    if (tableId == null || tableId.isEmpty()) {
      return command;
    }
    SystemTableTarget target = SystemTableTarget.newBuilder().setId(tableId.get()).build();
    return SystemTableFlightCommand.newBuilder(command).setTarget(target).build();
  }

  private static String normalizeTableName(String value) {
    if (value == null) {
      return "";
    }
    return value.trim().toLowerCase(Locale.ROOT);
  }

  protected String requireQueryId(ResolvedCallContext ctx, SystemTableFlightCommand command) {
    Objects.requireNonNull(ctx, "context");
    Objects.requireNonNull(command, "command");
    return requireQueryId(ctx.queryId(), command.getQueryId(), ctx.correlationId());
  }

  private static String requireQueryId(
      String headerQueryId, String commandQueryId, String correlationId) {
    boolean headerPresent = headerQueryId != null && !headerQueryId.isBlank();
    boolean commandPresent = commandQueryId != null && !commandQueryId.isBlank();
    if (!headerPresent && !commandPresent) {
      throw CallStatus.INVALID_ARGUMENT
          .withDescription("query_id is required")
          .toRuntimeException();
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
    if (headerPresent) {
      return headerQueryId;
    }
    return commandPresent ? commandQueryId : "";
  }

  // -------------------------------------------------------------------------
  // Utils
  // -------------------------------------------------------------------------

  private static Optional<SystemTableFlightCommand> decodeCommandQuietly(
      FlightDescriptor descriptor) {
    try {
      return Optional.of(decodeCommand(descriptor));
    } catch (Throwable t) {
      return Optional.empty();
    }
  }

  private static Optional<SystemTableFlightTicket> decodeTicketQuietly(Ticket ticket) {
    try {
      return Optional.of(decodeTicketWrapper(ticket));
    } catch (Throwable t) {
      return Optional.empty();
    }
  }

  private static Optional<SystemTableFlightCommand> decodeTicketCommandQuietly(Ticket ticket) {
    try {
      return Optional.of(decodeTicket(ticket));
    } catch (Throwable t) {
      return Optional.empty();
    }
  }

  private static SystemTableFlightCommand decodeCommand(FlightDescriptor descriptor) {
    if (descriptor == null || !descriptor.isCommand()) {
      throw CallStatus.INVALID_ARGUMENT
          .withDescription("FlightDescriptor must be a command")
          .toRuntimeException();
    }
    try {
      return SystemTableFlightCommand.parseFrom(descriptor.getCommand());
    } catch (Exception e) {
      throw CallStatus.INVALID_ARGUMENT
          .withDescription("Cannot decode Flight descriptor command: " + e.getMessage())
          .toRuntimeException();
    }
  }

  private static Ticket encodeTicket(SystemTableFlightCommand command) {
    SystemTableFlightTicket wrapper =
        SystemTableFlightTicket.newBuilder()
            .setVersion(TICKET_VERSION)
            .setCmd(command.toByteString())
            .build();
    return new Ticket(wrapper.toByteArray());
  }

  private static SystemTableFlightTicket decodeTicketWrapper(Ticket ticket) {
    if (ticket == null) {
      throw CallStatus.INVALID_ARGUMENT
          .withDescription("Flight ticket cannot be null")
          .toRuntimeException();
    }
    try {
      SystemTableFlightTicket wrapper = SystemTableFlightTicket.parseFrom(ticket.getBytes());
      if (wrapper.getVersion() != TICKET_VERSION) {
        throw CallStatus.INVALID_ARGUMENT
            .withDescription("Unsupported ticket version: " + wrapper.getVersion())
            .toRuntimeException();
      }
      return wrapper;
    } catch (Exception e) {
      throw CallStatus.INVALID_ARGUMENT
          .withDescription("Cannot decode Flight ticket: " + e.getMessage())
          .toRuntimeException();
    }
  }

  private static SystemTableFlightCommand decodeTicket(Ticket ticket) {
    SystemTableFlightTicket wrapper = decodeTicketWrapper(ticket);
    try {
      return SystemTableFlightCommand.parseFrom(wrapper.getCmd());
    } catch (Exception e) {
      throw CallStatus.INVALID_ARGUMENT
          .withDescription("Cannot decode Flight ticket command: " + e.getMessage())
          .toRuntimeException();
    }
  }

  protected static CallStatus toFlightStatus(Throwable t) {
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

  protected void onGetFlightInfoSuccess(
      ResolvedCallContext context, String tableName, long elapsedNanos) {}

  protected void onGetFlightInfoError(
      ResolvedCallContext context, String tableName, Throwable error, long elapsedNanos) {}

  protected void onGetStreamDispatchError(
      ResolvedCallContext context, Throwable error, long elapsedNanos) {}

  protected void onGetStreamStart(ResolvedCallContext context, String tableName) {}

  protected void onGetStreamSuccess(
      ResolvedCallContext context, String tableName, long elapsedNanos) {}

  protected void onGetStreamCancelled(
      ResolvedCallContext context, String tableName, long elapsedNanos) {}

  protected void onGetStreamError(
      ResolvedCallContext context, String tableName, Throwable error, long elapsedNanos) {}

  private void safeHook(String hookName, Runnable runnable) {
    try {
      runnable.run();
    } catch (Throwable hookError) {
      logger().debugf("Ignoring %s failure: %s", hookName, hookError.getMessage());
    }
  }
}
