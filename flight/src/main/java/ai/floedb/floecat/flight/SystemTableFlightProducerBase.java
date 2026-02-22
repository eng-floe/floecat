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
import ai.floedb.floecat.flight.context.ResolvedCallContext;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.system.rpc.SystemTableFlightCommand;
import ai.floedb.floecat.system.rpc.SystemTableFlightTicket;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.apache.arrow.vector.types.pojo.Schema;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

/**
 * Shared base for Flight producers that speak the {@link SystemTableFlightCommand} protocol.
 *
 * <p>Subclasses provide the command-specific plan/schema hooks while this class drives the full
 * ticket lifecycle and serialization loop.
 *
 * <p>Ownership & lifecycle notes for external implementers:
 *
 * <ul>
 *   <li>The base owns the {@link ArrowScanPlan} produced by {@link #buildPlan}; it always hands the
 *       plan to {@link ArrowBatchSerializer#serialize}, so callers should not close it themselves.
 *   <li>An allocator is created by {@link #createStreamAllocator()} and guaranteed closed via the
 *       shared cleanup callback (including the defensive double-call guard in {@link
 *       #streamOnWorkerThread}).
 *   <li>Schema roots and batches are owned by the plan/serializer pair; implementations should
 *       allocate roots, write into them, and leave {@code close()}/row-count management to the
 *       plan/serializer.
 *   <li>{@link ArrowBatchSerializer#serialize} always runs the provided cleanup callback whether
 *       the stream completes normally or throws, so resources (allocator, sink) are released even
 *       when {@link #buildPlan} fails; the base adds its own defensive check so cleanup never runs
 *       twice.
 * </ul>
 */
public abstract class SystemTableFlightProducerBase extends NoOpFlightProducer
    implements RoutedFlightProducer {

  private static final int TICKET_VERSION = 1;

  @Inject FlightAllocatorHolder allocatorHolder;
  @Inject FlightExecutor flightExecutor;

  protected Logger logger() {
    return Logger.getLogger(getClass());
  }

  // -----------------------------------------------------------------------
  //  Abstract contract
  // -----------------------------------------------------------------------

  protected abstract ResolvedCallContext resolveCallContext(CallContext context);

  protected void authorize(ResolvedCallContext ctx) {}

  protected abstract List<SchemaColumn> schemaColumns(
      SystemTableFlightCommand command, ResolvedCallContext context);

  protected abstract ArrowScanPlan buildPlan(
      SystemTableFlightCommand command, ResolvedCallContext context, BufferAllocator allocator);

  protected abstract boolean supportsCommand(SystemTableFlightCommand command);

  protected abstract Location selfLocation();

  protected long streamAllocatorLimit() {
    return Long.MAX_VALUE;
  }

  // -----------------------------------------------------------------------
  //  Routing helpers
  // -----------------------------------------------------------------------

  @Override
  public boolean supportsDescriptor(FlightDescriptor descriptor) {
    return decodeCommandQuietly(descriptor).map(this::supportsCommand).orElse(false);
  }

  @Override
  public boolean supportsTicket(Ticket ticket) {
    return decodeTicketCommandQuietly(ticket).map(this::supportsCommand).orElse(false);
  }

  // -----------------------------------------------------------------------
  //  Flight protocol
  // -----------------------------------------------------------------------

  @Override
  public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
    try {
      ResolvedCallContext callCtx = requireAuth(context);
      SystemTableFlightCommand command = decodeCommand(descriptor);
      if (!supportsCommand(command)) {
        throw CallStatus.NOT_FOUND
            .withDescription("No Flight producer registered for descriptor")
            .toRuntimeException();
      }
      Schema projected = projectSchema(command, callCtx);
      Ticket ticket = encodeTicket(command);
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
      Map<String, Object> mdcSnapshot = copyContext(MDC.getMap());
      flightExecutor
          .executor()
          .submit(() -> streamOnWorkerThread(ticket, listener, callCtx, mdcSnapshot));
    } catch (Throwable t) {
      listener.error(toFlightStatus(t).toRuntimeException());
    }
  }

  private void streamOnWorkerThread(
      Ticket ticket,
      ServerStreamListener listener,
      ResolvedCallContext callCtx,
      Map<String, Object> mdcSnapshot) {
    applyContext(mdcSnapshot);
    try {
      SystemTableFlightCommand command = decodeTicket(ticket);
      if (!supportsCommand(command)) {
        throw CallStatus.NOT_FOUND
            .withDescription("No Flight producer registered for ticket: " + ticket)
            .toRuntimeException();
      }
      String effectiveQueryId = requireQueryId(callCtx, command);
      logger()
          .debugf(
              "getStream table=%s query=%s correlation=%s",
              command.getTableId().getId(), effectiveQueryId, callCtx.correlationId());

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
        ArrowScanPlan plan = buildPlan(command, callCtx, allocator);
        ArrowBatchSerializer.serialize(plan, sink, listener::isCancelled, cleanup);
      } catch (Throwable t) {
        if (cleanupCalled.compareAndSet(false, true)) {
          sink.close();
          closeAllocator(allocator);
        }
        listener.error(toFlightStatus(t).toRuntimeException());
      }
    } finally {
      MDC.clear();
    }
  }

  private BufferAllocator createStreamAllocator() {
    long limit = streamAllocatorLimit();
    long cap = limit > 0 ? limit : Long.MAX_VALUE;
    return allocatorHolder.allocator().newChildAllocator("flight-stream", 0, cap);
  }

  private void closeAllocator(BufferAllocator allocator) {
    try {
      allocator.close();
    } catch (Exception ignored) {
    }
  }

  private ResolvedCallContext requireAuth(CallContext context) {
    ResolvedCallContext resolved = resolveCallContext(context);
    authorize(resolved);
    return resolved;
  }

  private Schema projectSchema(SystemTableFlightCommand command, ResolvedCallContext context) {
    List<SchemaColumn> columns = schemaColumns(command, context);
    if (columns == null) {
      throw new IllegalStateException("Schema columns must not be null");
    }
    return ArrowBatchSerializer.schemaForColumns(columns, command.getRequiredColumnsList());
  }

  protected String requireQueryId(ResolvedCallContext context, SystemTableFlightCommand command) {
    return requireQueryId(context.queryId(), command.getQueryId(), context.correlationId());
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
  //  Utils
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

  private static void applyContext(Map<String, Object> context) {
    MDC.clear();
    if (context == null || context.isEmpty()) {
      return;
    }
    context.forEach((k, v) -> MDC.put(k, v == null ? "" : v.toString()));
  }

  private static Map<String, Object> copyContext(Map<String, Object> source) {
    if (source == null || source.isEmpty()) {
      return Map.of();
    }
    return new HashMap<>(source);
  }

  protected static CallStatus toFlightStatus(Throwable t) {
    if (t instanceof FlightRuntimeException fre) {
      return fre.status();
    }
    if (t instanceof io.grpc.StatusRuntimeException sre) {
      // PERMISSION_DENIED indicates the caller lacks catalog.read; map to Flight's UNAUTHORIZED.
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
