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
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.flight.context.ResolvedCallContext;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.system.rpc.SystemTableFlightCommand;
import ai.floedb.floecat.system.rpc.SystemTableFlightTicket;
import ai.floedb.floecat.system.rpc.SystemTableTarget;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
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
      return resolveSystemTableName(target.getId(), ctx)
          .map(SystemTableFlightProducerBase::normalizeTableName)
          .filter(supported::contains)
          .isPresent();
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
    ResolvedCallContext defaultCtx = ResolvedCallContext.unauthenticated();
    return decodeCommandQuietly(descriptor)
        .map(cmd -> supportsCommand(cmd, defaultCtx))
        .orElse(false);
  }

  @Override
  public boolean supportsTicket(Ticket ticket) {
    ResolvedCallContext defaultCtx = ResolvedCallContext.unauthenticated();
    return decodeTicketCommandQuietly(ticket)
        .map(cmd -> supportsCommand(cmd, defaultCtx))
        .orElse(false);
  }

  // -----------------------------------------------------------------------
  // Flight protocol
  // -----------------------------------------------------------------------

  @Override
  public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
    try {
      ResolvedCallContext callCtx = requireAuth(context);
      SystemTableFlightCommand command = decodeCommand(descriptor);
      requireQueryId(callCtx, command);
      SystemTableHandle handle = requireHandle(command, callCtx);
      if (!supportsResolvedHandle(handle, callCtx)) {
        throw CallStatus.NOT_FOUND
            .withDescription("No Flight producer registered for descriptor")
            .toRuntimeException();
      }
      Optional<ResourceId> tableId = handle.tableId();
      SystemTableFlightCommand enriched = commandWithTableId(command, tableId);
      Schema projected = projectSchema(handle.canonicalName(), tableId, enriched, callCtx);
      Ticket ticket = encodeTicket(enriched);
      FlightEndpoint endpoint = new FlightEndpoint(ticket, selfLocation());
      return FlightInfo.builder(projected, descriptor, List.of(endpoint)).build();
    } catch (Throwable t) {
      throw toFlightStatus(t).toRuntimeException();
    }
  }

  @Override
  public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
    try {
      ResolvedCallContext callCtx = requireAuth(context);
      ContextSnapshot snapshot = ContextSnapshot.capture();
      flightExecutor
          .executor()
          .submit(() -> streamOnWorkerThread(ticket, listener, callCtx, snapshot));
    } catch (Throwable t) {
      listener.error(toFlightStatus(t).toRuntimeException());
    }
  }

  private void streamOnWorkerThread(
      Ticket ticket,
      ServerStreamListener listener,
      ResolvedCallContext callCtx,
      ContextSnapshot snapshot) {
    AutoCloseable snapshotScope = null;
    try {
      snapshotScope = snapshot.apply();
      SystemTableFlightCommand command = decodeTicket(ticket);
      SystemTableHandle handle = requireHandle(command, callCtx);
      if (!supportsResolvedHandle(handle, callCtx)) {
        throw CallStatus.NOT_FOUND
            .withDescription("No Flight producer registered for ticket: " + ticket)
            .toRuntimeException();
      }
      String tableName = handle.canonicalName();
      Optional<ResourceId> tableId = handle.tableId();
      String tableIdLog = tableId.map(ResourceId::getId).orElse("<name-only>");
      String effectiveQueryId = requireQueryId(callCtx, command);
      logger()
          .debugf(
              "getStream table=%s name=%s query=%s correlation=%s",
              tableIdLog, tableName, effectiveQueryId, callCtx.correlationId());

      BufferAllocator allocator = createStreamAllocator();
      FlightArrowBatchSink sink = new FlightArrowBatchSink(listener, allocator);
      AtomicBoolean cleanupCalled = new AtomicBoolean(false);
      Runnable cleanup =
          () -> {
            if (cleanupCalled.compareAndSet(false, true)) {
              sink.close();
              closeAllocator(allocator);
            }
          };
      try {
        ArrowScanPlan plan =
            buildPlan(tableName, tableId, command, callCtx, allocator, listener::isCancelled);
        ArrowBatchSerializer.serialize(plan, sink, listener::isCancelled, cleanup);
      } catch (Throwable t) {
        if (cleanupCalled.compareAndSet(false, true)) {
          sink.close();
          closeAllocator(allocator);
        }
        listener.error(toFlightStatus(t).toRuntimeException());
      }
    } catch (Throwable t) {
      listener.error(toFlightStatus(t).toRuntimeException());
    } finally {
      if (snapshotScope != null) {
        try {
          snapshotScope.close();
        } catch (Exception ignored) {
        }
      }
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
              .setLogicalType(field.getFieldType().getType().toString())
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

  private Optional<SystemTableHandle> resolveHandleById(
      ResourceId id, ResolvedCallContext context) {
    if (id == null || id.getId() == null || id.getId().isBlank()) {
      return Optional.empty();
    }
    ResolvedCallContext ctx = effectiveContext(context);
    Set<String> supported = supportedNames(ctx);
    return resolveSystemTableName(id, context)
        .filter(supported::contains)
        .map(name -> new SystemTableHandle(name, Optional.of(id)))
        .or(() -> resolveFromSupportedNames(id, supported, context));
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
        return Optional.of(new SystemTableHandle(tableName, Optional.of(id)));
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
    return a.getId().equals(b.getId())
        && a.getAccountId().equals(b.getAccountId())
        && a.getKind() == b.getKind();
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
}
